# Complex Python workloads

This directory is an intentionally demanding integration lab for the two
Python execution paths in dbt-maxcompute.

| Path | Dependency mechanism | Workload |
|---|---|---|
| Catalog Python UDF/UDAF | Prebuilt MaxCompute ARCHIVE resource | Nested JSON, phone and timestamp normalization, deterministic scoring, bounded distributed aggregation |
| MaxFrame feature job | with_python_requirements / PythonPack | Row UDF, four third-party packages, two dimension joins, feature arithmetic, partitions |
| MaxFrame incremental job | Upstream packaged result | Multiple group-bys, merges, distinct counts, composite key merge, schema sync |
| MaxFrame heavy job | PythonPack plus managed CP311 image | RapidFuzz plus scikit-learn, sparse hashed vectors, cosine similarity, pair scoring |

The jobs use tiny inputs so compute cost stays low. Complexity comes from the
execution graph, serialization, dependency loading, types, state merging, and
materialization behavior.

## Dependency boundary

The two runtimes deliberately use different dependency flows:

- Catalog UDFs cannot install from PyPI while a function is being created.
  Build a pure-Python zip and upload it as
  dbt_showcase_pydeps_cp311.zip.
- MaxFrame UDFs declare PEP 508 requirements with
  with_python_requirements. The remote PythonPack node resolves and caches
  them. The first run is expected to take longer.
- risk_identity_similarity_maxframe loads RapidFuzz and text-unidecode through
  PythonPack, and uses the managed `sklearn` CP311 image for scikit-learn,
  NumPy, and SciPy. It is the highest-risk compatibility test and may require
  a DPE-capable MaxFrame quota.
- Pin native packages to releases that publish CP311 manylinux wheels.
  `prefer_binary=True` is a preference, not a prohibition on source builds.
  An unavailable wheel can otherwise spend several minutes in PythonPack
  before failing in a compiler or build backend. Prefer a managed or custom
  image for a large native scientific stack.

## Prepare the project

This workload deliberately serializes custom Python functions, so use Python
3.11 for a reproducible baseline against the default CP311 worker image. This
is a requirement of this stress test, not a package-install restriction in the
adapter. Install the adapter and MaxFrame extra, then seed only the risk inputs:

    python3.11 -m venv .venv
    . .venv/bin/activate
    python -m pip install -e "../..[maxframe]"
    python -m dbt seed --select risk_events risk_customers device_reputation

Build and verify the pure-Python dependency archive locally:

    python scripts/prepare_python_udf_dependencies.py

Upload it to the same MaxCompute project and schema where dbt creates function
resources. With the standard profile that is normally the target schema:

    python scripts/prepare_python_udf_dependencies.py \
      --upload \
      --project YOUR_PROJECT \
      --schema YOUR_TARGET_SCHEMA \
      --endpoint https://service.YOUR_REGION.maxcompute.aliyun.com/api

The uploader reads credentials from ODPS_ACCESS_ID /
ODPS_SECRET_ACCESS_KEY or their ALIBABA_CLOUD_* equivalents. It refuses to
replace an existing resource unless --replace is explicitly supplied.

## Run the catalog UDF/UDAF path

    dbt build --select enrich_risk_event merge_risk_signals
    dbt run --select risk_events_via_udf risk_customer_daily_via_udaf
    dbt test --select risk_events_via_udf risk_customer_daily_via_udaf

Expected invariants:

- 10 row-level profiles are produced, including one malformed JSON event;
- invalid JSON, timestamp, and phone values produce deterministic error
  fields rather than crashing a worker;
- daily UDAF output is grouped by customer_id, ds;
- distributed merges keep a bounded buffer and stable sorted JSON output.

## Run the MaxFrame dependency path

    dbt run --select risk_event_features_maxframe
    dbt run --select risk_customer_daily_maxframe --full-refresh
    dbt run --select risk_customer_daily_maxframe
    dbt test --select risk_event_features_maxframe risk_customer_daily_maxframe

This covers automatic third-party packaging, row-wise DataFrame UDF
serialization, two left joins, regular partitions, multiple aggregations, a
composite incremental key, and idempotent reruns.

Run the native-wheel stress model separately because its first PythonPack
build can be slow:

    dbt run --select risk_identity_similarity_maxframe
    dbt test --select risk_identity_similarity_maxframe

The two UK customer records should produce one candidate pair and exercise
RapidFuzz, scikit-learn, NumPy, and SciPy in the remote runtime.

## How to classify failures

1. Failure before a MaxFrame session is created: dbt parsing, adapter config,
   or local MaxFrame SDK issue.
2. declared resource does not exist: archive uploaded to the wrong function
   schema, or the resource name differs.
3. ModuleNotFoundError inside a catalog UDF: archive layout or CP311
   compatibility issue.
4. Failure in a PythonPack LogView node: package resolution, repository
   access, native wheel, or DPE image compatibility issue.
5. Job computes successfully but table creation fails: adapter
   materialization, partition, schema, or incremental behavior.
6. Worker stays at zero input or exits with a native signal: compare the local
   Python minor version with the remote image, then reproduce with Python 3.11
   to isolate custom-function serialization from user-code failures.

Record the dbt invocation ID, MaxFrame session ID, failing node, package name,
and LogView error text. Do not paste token-bearing LogView URLs into issues.
