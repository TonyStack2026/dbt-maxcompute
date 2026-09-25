# SQL integration regression

Unit tests and package builds in `main.yml` never contact a MaxCompute project,
so everything that is really decided by the server - materialization SQL,
incremental strategy output, persisted docs, error propagation - had no
regression gate. This page documents the entry point that covers it.

## What runs

| Suite | Selection | Contents |
| --- | --- | --- |
| minimal (default) | `pytest -m integration_smoke` | `tests/functional/test_minimal_sql_set.py`: table, view, incremental, dbt tests (passing and failing), persisted docs, invalid SQL, schema cleanup |
| release core | `pytest -m core_test` | `tests/functional/test_core.py`: the broader materialization suite run before a release |

Both suites create their own schema per test class (prefix `test`), and drop it
again in teardown. Every assertion that claims a model worked reads the object
back from the server - a row count, a relation type, a comment - rather than
trusting dbt's own exit status.

## Prerequisites

* A **three-tier (schema-enabled) MaxCompute project**. dbt discovers schemas at
  startup; on a two-tier project it fails with
  `ODPS-0110061 ... Invalid database operations on two-tier model`, which is an
  unusable environment, not an adapter bug.
* Python 3.10+ with the development requirements installed:

```bash
pip install -r dev-requirements.txt
pip install -e .
```

## Run it locally

```bash
./scripts/run-integration-tests.sh                        # minimal set
./scripts/run-integration-tests.sh --suite core            # release core suite
./scripts/run-integration-tests.sh -- -k TestMinimalView   # extra pytest arguments
```

The script resolves a profile in this order:

1. `$DBT_PROFILE_PATH`, or `./dbt_profile.yml` next to the repository root if it
   exists (that file is git-ignored). It holds one profile target mapping:

   ```yaml
   type: maxcompute
   project: your_three_tier_project
   endpoint: http://service.cn-hangzhou.maxcompute.aliyun.com/api
   auth_type: access_key
   access_key_id: <your access key id>
   access_key_secret: <your access key secret>
   ```

2. Otherwise, with `MC_PROJECT` and `MC_ENDPOINT` exported, the script writes a
   temporary profile that uses `auth_type: chain`, so the access keys stay in
   the environment and are never copied into a file in the repository:

   ```bash
   export ALIBABA_CLOUD_ACCESS_KEY_ID=...      # or ODPS_ACCESS_ID / ODPS_ACCESS_KEY
   export ALIBABA_CLOUD_ACCESS_KEY_SECRET=...
   export MC_PROJECT=your_three_tier_project
   export MC_ENDPOINT=http://service.cn-hangzhou.maxcompute.aliyun.com/api
   ./scripts/run-integration-tests.sh
   ```

`dbt-core` 1.11 stopped rendering Jinja inside `profiles.yml`, so
`env_var(...)` in a profile file does not work; generating the profile from the
environment as shown above is the supported way to keep credentials out of
committed files.

### Reading the result

```text
target:  project=... endpoint_host=... auth_type=chain schema_prefix=test*
suite:     smoke (pytest -m integration_smoke)
INTEGRATION RESULTS
  cases:   executed=6 passed=6 failed=0 errors=0 skipped=0
  cleanup: ok (no new test schemas remain in the project)
  status:  PASSED (server-side evidence produced)
```

Exit codes:

| Code | Meaning |
| --- | --- |
| 0 | Cases ran against the project and all passed |
| 1 | Cases ran and at least one failed, or test schemas were left behind |
| 2 | **Blocked** - no credentials, a two-tier project, or the project was unreachable. Nothing reached a server, so this is never reported as a pass |

Cases skipped because credentials are missing also make the script exit `2`.
Running `pytest` directly is fine too: every case that needs a server is
skipped with the reason computed by `tests/maxcompute_gating.py`.

## Run it in CI

`.github/workflows/integration.yml` runs on push to `master`/`develop`, on pull
requests, and on a manual dispatch (with an optional `pytest -k` expression).

* Credentials come from the repository secret `DBT_PROFILE_YAML`, the same
  secret the release workflow uses: the profile mapping above, written to the
  runner's temporary directory with mode `600` and never echoed.
* A workflow triggered by a **pull request from a fork cannot read secrets**.
  The `gate` job reports that case as `integration: NOT RUN`, and the
  integration job stays *skipped* (grey) - it does not run and does not turn
  green.
* If a run that is supposed to produce evidence (push or manual dispatch) has
  no configured profile, the gate job fails with `integration blocked:
  DBT_PROFILE_YAML is not configured`. A missing credential is an outage of the
  gate, not a pass.
* The script writes the case counts and the verdict into the job summary, so
  "how many cases actually ran" is visible without opening the log.
