#include <postgres.h>
#include <catalog/namespace.h>
#include <libpq/pqformat.h>
#include <nodes/value.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* aggregate median:
 *	 median(value) returns the median value of a values passed into the function
 *
 * Usage:
 *	 SELECT median(field) FROM table.
 */

PG_FUNCTION_INFO_V1(median_transfn);
PG_FUNCTION_INFO_V1(median_finalfn);
PG_FUNCTION_INFO_V1(median_combinefn);
PG_FUNCTION_INFO_V1(median_serializefn);
PG_FUNCTION_INFO_V1(median_deserializefn);

/* Internal state used by median aggregate function */
typedef struct MedianState
{
	Oid			arg_type;
	bool		arg_typbyval;
	int16		arg_typlen;
	Oid			arg_typioparam;

	/* Various routines */
	Oid			cmp_proc;
	Oid			send_proc;
	Oid			recv_proc;

	/* Array of accumulated Datums */
	Datum	   *values;
	/* Number of rows in the array values */
	uint32		values_num;
	/* Allocated length of the array values */
	uint32		values_alloc;
}	MedianState;

/* Values sorting internal context */
typedef struct MedianSortContext
{
	Oid			collation;
	FmgrInfo	cmp_finfo;
}	MedianSortContext;

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	MedianState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	/* If first call, initalize the transition state */
	if (PG_ARGISNULL(0))
	{
		TypeCacheEntry *typentry;
		bool		typisvarlena;
		Oid			arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

		if (!OidIsValid(arg_type))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not determine input data type")));

		old_context = MemoryContextSwitchTo(agg_context);

		state = (MedianState *) palloc(sizeof(MedianState));
		state->arg_type = arg_type;
		get_typlenbyval(arg_type, &state->arg_typlen, &state->arg_typbyval);

		/* Initialize routines */
		typentry = lookup_type_cache(arg_type, TYPECACHE_CMP_PROC);
		if (!OidIsValid(typentry->cmp_proc))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
			   errmsg("could not identify a comparison function for type %s",
					  format_type_be(arg_type))));
		state->cmp_proc = typentry->cmp_proc;

		getTypeBinaryOutputInfo(arg_type, &state->send_proc, &typisvarlena);
		getTypeBinaryInputInfo(arg_type, &state->recv_proc,
							   &state->arg_typioparam);

		/* Initialize the values array */
		state->values_alloc = 8;
		state->values_num = 0;
		state->values = (Datum *) palloc(state->values_alloc * sizeof(Datum));

		MemoryContextSwitchTo(old_context);
	}
	else
		state = (MedianState *) PG_GETARG_POINTER(0);

	/* Copy the datum into the values array, but only if it's not null */
	if (!PG_ARGISNULL(1))
	{
		Datum		val;

		old_context = MemoryContextSwitchTo(agg_context);

		/* Enlarge values[] if needed */
		if (state->values_num >= state->values_alloc)
		{
			state->values_alloc *= 2;
			state->values = (Datum *)
				repalloc(state->values, state->values_alloc * sizeof(Datum));
		}

		/* Detoast the argument if it's varlena into the agg_context */
		if (!state->arg_typbyval && state->arg_type == -1)
			val = PointerGetDatum(PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(1)));
		else
			val = datumCopy(PG_GETARG_DATUM(1),
							state->arg_typbyval, state->arg_typlen);

		state->values[state->values_num] = val;
		state->values_num++;

		MemoryContextSwitchTo(old_context);
	}

	PG_RETURN_POINTER(state);
}

/*
 * Comparison function for qsort_arg() over values.
 */
static int
values_compare(const void *a, const void *b, void *arg)
{
	MedianSortContext *state = (MedianSortContext *) arg;
	Datum		val1 = *((const Datum *) a);
	Datum		val2 = *((const Datum *) b);

	return DatumGetInt32(FunctionCall2Coll(&state->cmp_finfo, state->collation,
										   val1, val2));
}

/*
 * Get underliyng function's OID of the operator specified by oprname.
 */
static Oid
operator_funcid(Oid arg_type, char *oprname, char *oprdesc)
{
	Oid			oprid;
	Oid			oprfunc;

	oprid = OpernameGetOprid(list_make1(makeString(oprname)), arg_type, arg_type);
	if (!OidIsValid(oprid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a %s operator for type %s",
						oprdesc, format_type_be(arg_type))));

	oprfunc = get_opcode(oprid);
	if (!OidIsValid(oprfunc))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify the procedure for the %s operator for type %s",
						oprdesc, format_type_be(arg_type))));

	return oprfunc;
}

/*
 * Get mean of two datums.
 *
 * We get plus and division operators from the catalog for corresponding
 * arg_type.
 */
static Datum
datum_mean(Oid arg_type, Oid collation, Datum arg1, Datum arg2)
{
	Oid			plus_func;
	Oid			div_func;
	Oid			typinput;
	Oid			typioparam;
	FmgrInfo	flinfo;
	Datum		sumd;
	Datum		countd;

	/* First calculate sum of two arguments */
	plus_func = operator_funcid(arg_type, "+", "plus");
	fmgr_info(plus_func, &flinfo);

	sumd = FunctionCall2Coll(&flinfo, collation, arg1, arg2);

	/* Then return mean of two arguments */
	getTypeInputInfo(arg_type, &typinput, &typioparam);
	countd = OidInputFunctionCall(typinput, "2", typioparam, -1);

	div_func = operator_funcid(arg_type, "/", "division");
	fmgr_info(div_func, &flinfo);

	return FunctionCall2Coll(&flinfo, collation, sumd, countd);
}

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	MedianState *state;
	MedianSortContext ctx;
	MemoryContext agg_context;
	Datum		val1;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (MedianState *) PG_GETARG_POINTER(0);

	/* values_num could be zero if we only saw NULL input values */
	if (state->values_num == 0)
		PG_RETURN_NULL();

	/* Sort the values */
	fmgr_info(state->cmp_proc, &(ctx.cmp_finfo));
	ctx.collation = PG_GET_COLLATION();
	qsort_arg(state->values, state->values_num, sizeof(Datum), values_compare,
			  (void *) &ctx);
	/* For even number of rows get mean of two middle elements */
	if (state->values_num % 2 == 0)
	{
		Datum		val2;

		/* Get the first middle element */
		val1 = state->values[state->values_num / 2 - 1];
		/* Get the second middle element */
		val2 = state->values[state->values_num / 2];

		PG_RETURN_DATUM(datum_mean(state->arg_type, PG_GET_COLLATION(),
								   val1, val2));
	}
	/* For odd number of rows return the middle element */
	else
	{
		/* Return the middle element */
		val1 = state->values[state->values_num / 2];

		PG_RETURN_DATUM(val1);
	}
}

/*
 * Copy median internal state items from source into destination.
 */
static void
medianitems_copy(MedianState * source, MedianState * dest,
				 MemoryContext agg_context)
{
	MemoryContext old_context;

	old_context = MemoryContextSwitchTo(agg_context);

	/* Enlarge values[] if needed */
	if (dest->values_num + source->values_num > dest->values_alloc)
	{
		dest->values_alloc = dest->values_num + source->values_num;
		dest->values = (Datum *)
			repalloc(dest->values, dest->values_alloc * sizeof(Datum));
	}

	for (int i = 0; i < source->values_num; i++)
	{
		Datum		val;

		/* Detoast the argument if it's varlena into the agg_context */
		if (!source->arg_typbyval && source->arg_type == -1)
			val = PointerGetDatum(PG_DETOAST_DATUM_COPY(source->values[i]));
		else
			val = datumCopy(source->values[i],
							source->arg_typbyval, source->arg_typlen);

		dest->values[dest->values_num] = val;
		dest->values_num++;
	}

	MemoryContextSwitchTo(old_context);
}

/*
 * Median combine function.
 */
Datum
median_combinefn(PG_FUNCTION_ARGS)
{
	MedianState *state1;
	MedianState *state2;
	MemoryContext agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_combinefn called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (MedianState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* Manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		MemoryContext old_context;

		old_context = MemoryContextSwitchTo(agg_context);

		state1 = (MedianState *) palloc(sizeof(MedianState));
		state1->arg_type = state2->arg_type;
		state1->arg_typbyval = state2->arg_typbyval;
		state1->arg_typlen = state2->arg_typlen;
		state1->arg_typioparam = state2->arg_typioparam;

		state1->cmp_proc = state2->cmp_proc;
		state1->send_proc = state2->send_proc;
		state1->recv_proc = state2->recv_proc;

		state1->values_alloc = state2->values_num;
		state1->values_num = 0;
		state1->values = (Datum *) palloc(state1->values_alloc * sizeof(Datum));

		MemoryContextSwitchTo(old_context);

		medianitems_copy(state2, state1, agg_context);
	}
	else if (state2->values_num > 0)
		medianitems_copy(state2, state1, agg_context);

	PG_RETURN_POINTER(state1);
}

/*
 * Median serialize function.
 */
Datum
median_serializefn(PG_FUNCTION_ARGS)
{
	MedianState *state;
	StringInfoData buf;
	FmgrInfo	send_finfo;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "median_serializefn called in non-aggregate context");

	state = (MedianState *) PG_GETARG_POINTER(0);
	pq_begintypsend(&buf);

	pq_sendint(&buf, (int) state->arg_type, sizeof(state->arg_type));
	pq_sendbyte(&buf, state->arg_typbyval ? 1 : 0);
	pq_sendint(&buf, (int) state->arg_typlen, sizeof(state->arg_typlen));
	pq_sendint(&buf, (int) state->arg_typioparam,
			   sizeof(state->arg_typioparam));

	pq_sendint(&buf, (int) state->cmp_proc, sizeof(state->cmp_proc));
	pq_sendint(&buf, (int) state->send_proc, sizeof(state->send_proc));
	pq_sendint(&buf, (int) state->recv_proc, sizeof(state->recv_proc));

	/* For values_alloc and values_num use same value */
	pq_sendint(&buf, (int) state->values_num, sizeof(state->values_num));

	fmgr_info(state->send_proc, &(send_finfo));
	for (int i = 0; i < state->values_num; i++)
	{
		bytea	   *outputbytes;

		outputbytes = SendFunctionCall(&send_finfo, state->values[i]);

		pq_sendint(&buf, VARSIZE(outputbytes) - VARHDRSZ, 4);
		pq_sendbytes(&buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);

		pfree(outputbytes);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Median deserialize function.
 */
Datum
median_deserializefn(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	MedianState *result;
	StringInfoData buf;
	FmgrInfo	recv_finfo;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "median_deserializefn called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_P(0);

	/*
	 * Copy the bytea into a StringInfo so that we can "receive" it using the
	 * standard recv-function infrastructure.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(sstate), VARSIZE(sstate) - VARHDRSZ);

	result = (MedianState *) palloc(sizeof(MedianState));

	result->arg_type = pq_getmsgint(&buf, sizeof(result->arg_type));
	result->arg_typbyval = pq_getmsgbyte(&buf) == 1;
	result->arg_typlen = pq_getmsgint(&buf, sizeof(result->arg_typlen));
	result->arg_typioparam = pq_getmsgint(&buf, sizeof(result->arg_typioparam));

	result->cmp_proc = pq_getmsgint(&buf, sizeof(result->cmp_proc));
	result->send_proc = pq_getmsgint(&buf, sizeof(result->send_proc));
	result->recv_proc = pq_getmsgint(&buf, sizeof(result->recv_proc));

	result->values_num = result->values_alloc = pq_getmsgint(&buf,
												 sizeof(result->values_num));
	result->values = (Datum *) palloc(result->values_alloc * sizeof(Datum));

	fmgr_info(result->recv_proc, &(recv_finfo));
	for (int i = 0; i < result->values_num; i++)
	{
		int			value_len = pq_getmsgint(&buf, 4);
		const char *value_data = pq_getmsgbytes(&buf, value_len);
		StringInfoData value_buf;

		initStringInfo(&value_buf);
		appendBinaryStringInfo(&value_buf, value_data, value_len);

		result->values[i] = ReceiveFunctionCall(&recv_finfo, &value_buf,
												result->arg_typioparam, -1);
		pfree(value_buf.data);
	}

	pq_getmsgend(&buf);
	pfree(buf.data);

	PG_RETURN_POINTER(result);
}
