from dbt.adapters.maxcompute.utils import _dbt_maxcompute_version

GLOBAL_SQL_HINTS = {
    "dbt.maxcompute.version": _dbt_maxcompute_version(),
    "odps.sql.type.system.odps2": "true",
    "odps.sql.decimal.odps2": "true",
    "odps.sql.allow.fullscan": "true",
    "odps.sql.select.output.format": "csv",
    "odps.sql.submit.mode": "script",
    # MaxCompute stores Python UDF code and registration metadata separately
    # from the runtime selection. Every SQL statement that calls a Python 3
    # UDF must opt into a Python 3 runtime. CPython 3.11 is the current
    # default; legacy CPython 3.7 calls can override this through sql_hints.
    "odps.sql.python.version": "cp311",
    "odps.sql.allow.cartesian": "true",
    "odps.sql.allow.schema.evolution": "true",
    "odps.table.append2.enable": "true",
}
