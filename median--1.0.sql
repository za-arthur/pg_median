CREATE OR REPLACE FUNCTION _median_transfn(state internal, val anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _median_finalfn(state internal, val anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'median_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _median_combinefn(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_combinefn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _median_serializefn(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'median_serializefn'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _median_deserializefn(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'median_deserializefn'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

DROP AGGREGATE IF EXISTS median (ANYELEMENT);
CREATE AGGREGATE median (ANYELEMENT)
(
    sfunc = _median_transfn,
    stype = internal,
    combinefunc = _median_combinefn,
    serialfunc = _median_serializefn,
    deserialfunc = _median_deserializefn,
    parallel = safe,
    finalfunc = _median_finalfn,
    finalfunc_extra
);
