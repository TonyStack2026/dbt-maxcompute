"""Profile and credential gating for the MaxCompute integration tests.

The functional suite talks to a real MaxCompute project, so an environment
without usable credentials has to be reported as *skipped* or *blocked* and
never as a passing integration run.  This module is the single place that
decides whether a real run can happen; ``tests/conftest.py``, the functional
tests and ``scripts/run-integration-tests.sh`` all ask the same question.

Two environment facts are checked up front because both otherwise surface as
confusing test failures:

* ``dbt-maxcompute`` requires a three-tier (schema-enabled) project.  On a
  two-tier project dbt fails during schema discovery with
  ``ODPS-0110061 ... Invalid database operations on two-tier model``.
* ``auth_type: chain`` resolves credentials from the environment (or another
  credential provider).  An empty chain is a missing credential, not a product
  failure.

Nothing here prints credential material: profiles are read but never echoed,
and ``summary()`` reports the endpoint host only.

Command line (used by the runner script)::

    python tests/maxcompute_gating.py blocked      # exit 1 and print why we cannot run
    python tests/maxcompute_gating.py preflight    # also probes the project over the network
    python tests/maxcompute_gating.py summary      # project / endpoint host / auth type
    python tests/maxcompute_gating.py test-schemas # list leftover test schemas, one per line
"""

from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROFILE_PATH = REPO_ROOT / "dbt_profile.yml"
PROFILE_PATH_ENV = "DBT_PROFILE_PATH"

#: Every schema the dbt test fixtures create starts with this prefix
#: (``dbt/tests/fixtures/project.py`` builds it from ``prefix`` + test module).
TEST_SCHEMA_PREFIX = "test"

#: Environment variable pairs that the credential chain reads.  Only used to
#: produce a clear "no credentials" message without contacting any metadata
#: service.
CREDENTIAL_ENV_PAIRS = (
    ("ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
    ("ODPS_ACCESS_ID", "ODPS_ACCESS_KEY"),
)

REQUIRED_PROFILE_KEYS = ("type", "project", "endpoint")

DOC_URL = "docs/integration-tests.md"


def profile_path() -> Optional[Path]:
    """The resolved profile file, or ``None`` when no profile is configured."""
    configured = os.environ.get(PROFILE_PATH_ENV)
    candidate = Path(configured).expanduser() if configured else DEFAULT_PROFILE_PATH
    return candidate if candidate.is_file() else None


def load_profile() -> Dict[str, Any]:
    """Read the profile target mapping.

    The file is a single mapping (``type``/``project``/``endpoint``/auth keys),
    which is what ``tests/conftest.py`` hands to the dbt test fixtures.
    """
    path = profile_path()
    if path is None:
        raise FileNotFoundError(_no_profile_message())
    with open(path) as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a single profile target mapping")
    return data


def _no_profile_message() -> str:
    configured = os.environ.get(PROFILE_PATH_ENV)
    where = f"DBT_PROFILE_PATH={configured}" if configured else f"{DEFAULT_PROFILE_PATH}"
    return f"no MaxCompute profile found at {where}; see {DOC_URL}"


def environment_credentials_present() -> bool:
    """True when an access key pair is already exported in the environment."""
    return any(
        os.environ.get(id_key) and os.environ.get(secret_key)
        for id_key, secret_key in CREDENTIAL_ENV_PAIRS
    )


def auth_type(profile: Dict[str, Any]) -> str:
    return str(profile.get("auth_type") or "access_key")


@lru_cache(maxsize=1)
def blocked_reason() -> Optional[str]:
    """Why a credentialed run cannot happen, or ``None`` when it can.

    Offline and cheap: it inspects the profile and the environment only.  The
    network probe lives in :func:`preflight_reason`.
    """
    path = profile_path()
    if path is None:
        return _no_profile_message()
    try:
        profile = load_profile()
    except Exception as exc:  # unreadable or malformed profile: nothing to run against
        return f"profile {path} could not be read: {exc}"

    missing = [key for key in REQUIRED_PROFILE_KEYS if not profile.get(key)]
    if missing:
        return f"profile {path.name} is missing required key(s): {', '.join(missing)}"

    kind = auth_type(profile)
    if kind == "chain":
        if not environment_credentials_present():
            return (
                f"profile {path.name} uses auth_type=chain but no credentials are exported "
                "(set ALIBABA_CLOUD_ACCESS_KEY_ID/ALIBABA_CLOUD_ACCESS_KEY_SECRET)"
            )
    elif not (profile.get("access_key_id") and profile.get("access_key_secret")):
        return (
            f"profile {path.name} uses auth_type={kind} but has no access_key_id/access_key_secret"
        )
    return None


def odps_client(profile: Optional[Dict[str, Any]] = None):
    """A PyODPS client for the configured profile (no credential material returned)."""
    from odps import ODPS

    profile = profile if profile is not None else load_profile()
    kwargs: Dict[str, Any] = {"project": profile["project"], "endpoint": profile["endpoint"]}
    if profile.get("tunnel_endpoint"):
        kwargs["tunnel_endpoint"] = profile["tunnel_endpoint"]
    if auth_type(profile) == "chain":
        from odps.accounts import CredentialProviderAccount

        from alibabacloud_credentials.client import Client as CredentialClient

        kwargs["account"] = CredentialProviderAccount(CredentialClient())
        return ODPS(**kwargs)
    return ODPS(profile["access_key_id"], profile["access_key_secret"], **kwargs)


@lru_cache(maxsize=1)
def preflight_reason() -> Optional[str]:
    """:func:`blocked_reason` plus a one-time network probe of the project.

    Cached per process: a pytest session probes the server once, not once per
    test class.  Call :func:`clear_caches` when a test changes the environment.
    """
    reason = blocked_reason()
    if reason:
        return reason
    profile = load_profile()
    project = profile["project"]
    try:
        client = odps_client(profile)
        next(iter(client.list_schemas(project)), None)
    except Exception as exc:
        message = " ".join(str(exc).split())[:300]
        if "two-tier" in message or "ODPS-0110061" in message:
            return (
                f"project {project} is not a three-tier (schema-enabled) project, "
                f"which dbt-maxcompute requires: {message}"
            )
        return f"project {project} is not reachable with the configured profile: {message}"
    return None


def clear_caches() -> None:
    """Drop cached decisions (used by unit tests that change the environment)."""
    blocked_reason.cache_clear()
    preflight_reason.cache_clear()


def test_schemas() -> List[str]:
    """Schemas in the configured project whose name marks it as test-owned."""
    profile = load_profile()
    client = odps_client(profile)
    return sorted(
        schema.name
        for schema in client.list_schemas(profile["project"])
        if schema.name.startswith(TEST_SCHEMA_PREFIX)
    )


def summary() -> str:
    """One-line, credential-free description of what a run would target."""
    profile = load_profile()
    host = urlparse(profile["endpoint"]).netloc or profile["endpoint"]
    return (
        f"project={profile['project']} endpoint_host={host} "
        f"auth_type={auth_type(profile)} schema_prefix={TEST_SCHEMA_PREFIX}*"
    )


def main(argv: List[str]) -> int:
    command = argv[0] if argv else "preflight"
    if command == "blocked":
        reason = blocked_reason()
    elif command == "preflight":
        reason = preflight_reason()
    elif command == "summary":
        print(summary())
        return 0
    elif command == "test-schemas":
        for schema in test_schemas():
            print(schema)
        return 0
    else:
        print(f"unknown command: {command}", file=sys.stderr)
        return 64

    if reason:
        print(reason)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
