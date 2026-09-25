#!/usr/bin/env bash
# Run the real-SQL integration regression for dbt-maxcompute.
#
# This is the single entry point for a developer laptop and for a trusted CI
# job. It decides up front whether a real run is possible and says so out loud:
# a run that never reached a server is reported as BLOCKED and exits non-zero,
# never as a passing integration.
#
#   scripts/run-integration-tests.sh                     # minimal SQL set (fast)
#   scripts/run-integration-tests.sh --suite core        # broader release suite
#   scripts/run-integration-tests.sh -- -k TestMinimalView
#
# Profile resolution, in order:
#   1. $DBT_PROFILE_PATH, or ./dbt_profile.yml if it exists (git-ignored).
#      This is what CI uses: the profile comes from a repository secret.
#   2. otherwise, when MC_PROJECT and MC_ENDPOINT are exported, a profile is
#      written to a private temp file for the duration of the run with
#      auth_type: chain, so access keys stay in the environment and are never
#      copied into the repository.
#
# Exit status:
#   0  cases ran on the server and all passed
#   1  cases ran and at least one failed (including leftover test schemas)
#   2  integration could not run: no credentials, two-tier project, or the
#      project was unreachable. No evidence was produced.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYTHON="${PYTHON:-python3}"
GATE="$REPO_ROOT/tests/maxcompute_gating.py"
SUITE="smoke"
PASSTHROUGH=()

die() {
  echo "$*" >&2
  exit 2
}

# finish <status-line> <exit-code>: print the verdict and, when the caller is a
# GitHub Actions job, mirror the same numbers into the job summary.
finish() {
  local status="$1" code="$2"
  echo "  status:  ${status}"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo ""
      echo "### SQL integration regression (${SUITE})"
      echo ""
      echo "| target | cases | result |"
      echo "| --- | --- | --- |"
      printf '| `%s` | `%s` | %s |\n' "$TARGET" "$SUMMARY" "$status"
    } >>"$GITHUB_STEP_SUMMARY"
  fi
  exit "$code"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --suite)
      [ $# -ge 2 ] || die "--suite needs smoke|core"
      SUITE="$2"
      shift 2
      ;;
    --help | -h)
      sed -n '2,26p' "${BASH_SOURCE[0]}"
      exit 0
      ;;
    --)
      shift
      PASSTHROUGH=("$@")
      break
      ;;
    *)
      PASSTHROUGH+=("$1")
      shift
      ;;
  esac
done

case "$SUITE" in
  smoke) MARKER="integration_smoke" ;;
  core) MARKER="core_test" ;;
  *) die "unknown --suite '$SUITE' (expected smoke or core)" ;;
esac

if [ ! -f "$GATE" ]; then
  die "gating helper missing at $GATE (run from a repository checkout)"
fi

# --------------------------------------------------------------------------
# 1. resolve a profile
# --------------------------------------------------------------------------
TEMP_DIR=""
cleanup() {
  if [ -n "$TEMP_DIR" ]; then
    rm -rf "$TEMP_DIR"
  fi
}
trap cleanup EXIT

if [ -z "${DBT_PROFILE_PATH:-}" ] && [ ! -f "$REPO_ROOT/dbt_profile.yml" ]; then
  if [ -n "${MC_PROJECT:-}" ] && [ -n "${MC_ENDPOINT:-}" ]; then
    TEMP_DIR="$(umask 077; mktemp -d)"
    AUTH_TYPE="chain"
    {
      echo "type: maxcompute"
      echo "project: ${MC_PROJECT}"
      echo "schema: ${MC_SCHEMA:-integration}"
      echo "endpoint: ${MC_ENDPOINT}"
      echo "auth_type: ${AUTH_TYPE}"
      echo "threads: 4"
    } >"$TEMP_DIR/dbt_profile.yml"
    export DBT_PROFILE_PATH="$TEMP_DIR/dbt_profile.yml"
    echo "profile: generated a temp profile with auth_type=${AUTH_TYPE} (credentials stay in the environment)"
  fi
fi

TARGET="$("$PYTHON" "$GATE" summary 2>/dev/null || echo 'no profile configured')"
echo "target:  ${TARGET}"

# --------------------------------------------------------------------------
# 2. preflight: credentials, three-tier project, reachability
# --------------------------------------------------------------------------
SUMMARY="executed=0 passed=0 failed=0 errors=0 skipped=0"
if ! REASON="$("$PYTHON" "$GATE" preflight 2>&1)"; then
  echo "INTEGRATION: BLOCKED (no cases were executed)"
  echo "reason:      ${REASON}"
  echo "not a pass:  the SQL regression did not reach a MaxCompute project"
  finish "BLOCKED - no evidence produced (preflight failed: ${REASON})" 2
fi

SCHEMAS_BEFORE="$(mktemp)"
"$PYTHON" "$GATE" test-schemas >"$SCHEMAS_BEFORE" 2>/dev/null || true

# --------------------------------------------------------------------------
# 3. run the cases
# --------------------------------------------------------------------------
JUNIT_XML="$(mktemp)"
echo "suite:     ${SUITE} (pytest -m ${MARKER})"
rc=0
"$PYTHON" -m pytest \
  -m "$MARKER" \
  -v -rA --tb=short \
  --junitxml="$JUNIT_XML" \
  tests/functional \
  ${DBT_INTEGRATION_KEYWORD:+-k "$DBT_INTEGRATION_KEYWORD"} \
  ${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"} || rc=$?

# --------------------------------------------------------------------------
# 4. report what actually ran, from the JUnit record
# --------------------------------------------------------------------------
SUMMARY="$("$PYTHON" - "$JUNIT_XML" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
suites = [root] if root.tag == "testsuite" else root.iter("testsuite")
total = errors = failures = skipped = 0
for suite in suites:
    total += int(suite.get("tests", 0))
    errors += int(suite.get("errors", 0))
    failures += int(suite.get("failures", 0))
    skipped += int(suite.get("skipped", 0))
executed = total - skipped
print(f"executed={executed} passed={executed - failures - errors} failed={failures} errors={errors} skipped={skipped}")
PY
)"

echo
echo "INTEGRATION RESULTS"
echo "  cases:   ${SUMMARY}"

case "$rc" in
  0) outcome="ran" ;;
  1) outcome="ran with failures" ;;
  5) outcome="no tests collected" ;;
  *) outcome="abnormal exit (pytest rc=${rc})" ;;
esac

EXECUTED="$(printf '%s' "$SUMMARY" | sed -nE 's/.*executed=([0-9]+).*/\1/p')"
if [ "$EXECUTED" = "0" ]; then
  finish "BLOCKED - ${outcome}; no case reached the MaxCompute project, which is not a pass" 2
fi

# --------------------------------------------------------------------------
# 5. cleanup check: schemas created by this run must be gone
# --------------------------------------------------------------------------
LEAKED=""
if SCHEMAS_AFTER="$("$PYTHON" "$GATE" test-schemas 2>/dev/null)"; then
  LEAKED="$(comm -13 <(sort "$SCHEMAS_BEFORE") <(printf '%s\n' "$SCHEMAS_AFTER" | sort) | grep -v '^$' || true)"
fi

if [ -n "$LEAKED" ]; then
  finish "FAILED - test schemas left behind: $(printf '%s ' $LEAKED)" 1
fi
echo "  cleanup: ok (no new test schemas remain in the project)"
echo "  cases:   server-side assertions are listed under SERVER[...] in the run output"

if [ "$rc" != "0" ]; then
  finish "FAILED (${outcome})" 1
fi
finish "PASSED (server-side evidence produced)" 0
