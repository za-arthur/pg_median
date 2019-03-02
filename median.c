#include <postgres.h>
#include <miscadmin.h>
#include <catalog/namespace.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/tuplesort.h>
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

/* Internal state used by median aggregate function */
typedef struct MedianState
{
	Oid			argtype;
	/* Sort object we're accumulating data in */
	Tuplesortstate *sortstate;
	/* Number of normal rows inserted into sortstate */
	int64		number_of_rows;
}	MedianState;

/*
 * Clean up any potential non-memory resources when evaluation of an aggregate
 * is complete.
 */
static void
median_shutdown(Datum arg)
{
	MedianState *state = (MedianState *) DatumGetPointer(arg);

	/* Tuplesort object might have temp files. */
	if (state->sortstate)
		tuplesort_end(state->sortstate);
	state->sortstate = NULL;
}

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

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");

	/* If first call, initalize the transition state */
	if (PG_ARGISNULL(0))
	{
		MemoryContext oldcontext;
		TypeCacheEntry *typentry;
		Oid			argtype = get_fn_expr_argtype(fcinfo->flinfo, 1);

		if (!OidIsValid(argtype))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not determine input data type")));

		typentry = lookup_type_cache(argtype, TYPECACHE_LT_OPR);
		if (!OidIsValid(typentry->lt_opr))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
				errmsg("could not identify an ordering operator for type %s",
					   format_type_be(argtype))));

		oldcontext = MemoryContextSwitchTo(agg_context);

		state = (MedianState *) palloc(sizeof(MedianState));
		state->argtype = argtype;
		state->sortstate = tuplesort_begin_datum(argtype, typentry->lt_opr,
												 fcinfo->fncollation, false,
												 work_mem, false);
		state->number_of_rows = 0;

		/* Now register a shutdown callback to clean non-memory resources */
		AggRegisterCallback(fcinfo, median_shutdown, PointerGetDatum(state));

		MemoryContextSwitchTo(oldcontext);
	}
	else
		state = (MedianState *) PG_GETARG_POINTER(0);

	/* Load the datum into the tuplesort object, but only if it's not null */
	if (!PG_ARGISNULL(1))
	{
		tuplesort_putdatum(state->sortstate, PG_GETARG_DATUM(1), false);
		state->number_of_rows++;
	}

	PG_RETURN_POINTER(state);
}

/*
 * Get underliyng function's OID of the operator specified by oprname.
 */
static Oid
operator_funcid(Oid argtype, char *oprname, char *oprdesc)
{
	Oid			oprid;
	Oid			oprfunc;

	oprid = OpernameGetOprid(list_make1(makeString(oprname)), argtype, argtype);
	if (!OidIsValid(oprid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a %s operator for type %s",
						oprdesc, format_type_be(argtype))));

	oprfunc = get_opcode(oprid);
	if (!OidIsValid(oprfunc))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify the procedure for the %s operator for type %s",
						oprdesc, format_type_be(argtype))));

	return oprfunc;
}

/*
 * Get mean of two datums.
 */
static Datum
datum_mean(Oid argtype, Oid collation, Datum arg1, Datum arg2)
{
	Oid			plus_func;
	Oid			div_func;
	Oid			typinput;
	Oid			typioparam;
	FmgrInfo	flinfo;
	Datum		sumd;
	Datum		countd;

	/* First calculate sum of two arguments */
	plus_func = operator_funcid(argtype, "+", "plus");
	fmgr_info(plus_func, &flinfo);

	sumd = FunctionCall2Coll(&flinfo, collation, arg1, arg2);

	/* Then return mean of two arguments */
	getTypeInputInfo(argtype, &typinput, &typioparam);
	countd = OidInputFunctionCall(typinput, "2", typioparam, -1);

	div_func = operator_funcid(argtype, "/", "division");
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
	MemoryContext agg_context;
	bool		tuplesort_empty;
	Datum		val1;
	bool		isnull;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (MedianState *) PG_GETARG_POINTER(0);

	/* Number_of_rows could be zero if we only saw NULL input values */
	if (state->number_of_rows == 0)
		PG_RETURN_NULL();

	/* Sort the values */
	tuplesort_performsort(state->sortstate);
	/* For even number of rows get mean of two middle elements */
	if (state->number_of_rows % 2 == 0)
	{
		Datum		val2;

		tuplesort_skiptuples(state->sortstate, state->number_of_rows / 2 - 1,
							 true);

		/* Get the first middle element */
		tuplesort_empty = !tuplesort_getdatum(state->sortstate, true,
											  &val1, &isnull, NULL);
		Assert(!tuplesort_empty && !isnull);

		/* Get the second middle element */
		tuplesort_empty = !tuplesort_getdatum(state->sortstate, true,
											  &val2, &isnull, NULL);
		Assert(!tuplesort_empty && !isnull);

		PG_RETURN_DATUM(datum_mean(state->argtype, PG_GET_COLLATION(),
								   val1, val2));
	}
	/* For odd number of rows return the middle element */
	else
	{
		tuplesort_skiptuples(state->sortstate, state->number_of_rows / 2, true);
		tuplesort_empty = !tuplesort_getdatum(state->sortstate, true,
											  &val1, &isnull, NULL);
		Assert(!tuplesort_empty && !isnull);

		PG_RETURN_DATUM(val1);
	}

	/* Note: the tuplesort is cleaned up by the cleanup callback */

	PG_RETURN_NULL();
}
