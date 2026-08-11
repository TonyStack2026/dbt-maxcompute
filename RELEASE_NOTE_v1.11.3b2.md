# dbt-maxcompute v1.11.3b2 Release Notes

**Release Date:** 2026-08-11

This Beta release introduces MaxFrame Python models as Production Preview and
dbt Python function resources as Beta. It is intended for white-tower and
controlled-project validation before a stable release.

## MaxFrame Python Models — Production Preview

- table and incremental Python models backed by MaxFrame DataFrames;
- `ref`, `source`, `config`, `this`, and `is_incremental` integration;
- regular and automatic MaxCompute partitions;
- `merge`, `append`, `delete+insert`, `insert_overwrite`, and `microbatch`;
- failure-safe, empty-output-safe staging; bounded transport retries; and
  session observability;
- partition-column visibility in downstream MaxFrame `ref` and `source`;
- production PythonPack cache by default and managed-image guidance for large
  scientific dependencies;
- exact session-scoped cleanup after normal, failed, and retried runs;
- configurable Python 3.11 custom-function compatibility checks without
  restricting package installation on other supported Python versions.

Install the optional runtime with:

```bash
pip install "dbt-maxcompute[maxframe]==1.11.3b2"
```

The exact pin installs the preview without globally enabling prerelease
dependency resolution. Do not add pip's global `--pre` flag.

Start with the
[MaxFrame Python user guide](docs/maxframe-python-user-guide.md), then review
the [production readiness checklist](docs/maxframe-production-readiness.md)
before promoting a workload.

## Python UDFs — Beta

- persistent scalar and aggregate Python functions through dbt `functions:`;
- MaxCompute CPython 3.11 (`cp311`) by default;
- generated MaxCompute handlers and signatures;
- content-addressed resources, safe updates, and managed cleanup;
- existing MaxCompute resources and compatible archive dependencies.

## Compatibility notice

SQL submitted through dbt now defaults to `odps.sql.python.version=cp311`.
Projects that call legacy CPython 3.7 UDFs must override that SQL session hint
to `cp37` on the relevant execution path.

## Maturity and known limitations

- MaxFrame cancellation is not yet propagated to an active remote DAG.
- MaxFrame model-level packages are not dynamically installed.
- Python functions do not yet support SQL UDFs, UDTFs, overloads, grants,
  default arguments, or dynamic PyPI installation.
- Review `docs/maxframe-python-models.md` and `docs/python-udfs.md` before using
  these capabilities in production.
