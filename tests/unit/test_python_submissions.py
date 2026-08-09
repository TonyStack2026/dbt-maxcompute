from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dbt.cli.main import dbtRunner
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.maxcompute.impl import MaxComputeAdapter
from dbt.adapters.maxcompute.python_submissions import (
    MaxFramePythonJobHelper,
    MaxFrameSubmissionResult,
    _load_maxframe_runtime,
)
from dbt.adapters.maxcompute.relation_configs._partition import PartitionConfig


class FakeSession:
    def __init__(self, session_id="maxframe-session-123"):
        self.session_id = session_id
        self.destroyed = False
        self.executed_with = None

    def get_logview_address(self):
        return "https://logview.example/maxframe-session-123"

    def destroy(self):
        self.destroyed = True


class FakeMaxFrame:
    def __init__(self, session):
        self.session = session
        self.new_session_kwargs = None

    def new_session(self, **kwargs):
        self.new_session_kwargs = kwargs
        return self.session


class SequencedMaxFrame:
    def __init__(self, *sessions):
        self.sessions = list(sessions)
        self.new_session_kwargs = []

    def new_session(self, **kwargs):
        self.new_session_kwargs.append(kwargs)
        return self.sessions.pop(0)


def make_credentials():
    credentials = MagicMock()
    credentials.schema = "analytics"
    credentials.timezone = "Asia/Shanghai"
    credentials.tunnel_endpoint = "https://dt.example/api"
    credentials.maxframe_quota_name = "profile_quota"
    credentials.maxframe_retries = None
    credentials.odps.return_value = MagicMock()
    return credentials


def make_parsed_model(**config):
    model_config = {
        "packages": [],
        "timeout": 600,
        "sql_hints": {
            "odps.sql.allow.fullscan": "false",
            "dbt.execution_mode": "offline",
        },
        **config,
    }
    return {
        "unique_id": "model.test.python_model",
        "original_file_path": "models/python_model.py",
        "config": model_config,
    }


@contextmanager
def capturing_option_context(captured, options):
    captured.update(options)
    yield SimpleNamespace()


def test_submit_executes_compiled_code_and_cleans_up_session():
    session = FakeSession()
    maxframe = FakeMaxFrame(session)
    captured_options = {}
    credentials = make_credentials()
    helper = MaxFramePythonJobHelper(make_parsed_model(), credentials)

    option_context = lambda options: capturing_option_context(  # noqa: E731
        captured_options, options
    )
    compiled_code = "maxframe_session.executed_with = odps_entry"

    with patch(
        "dbt.adapters.maxcompute.python_submissions._load_maxframe_runtime",
        return_value=(maxframe, option_context),
    ):
        result = helper.submit(compiled_code)

    odps_entry = credentials.odps.return_value
    odps_entry.to_global.assert_called_once_with(overwritable=True)
    assert odps_entry.tunnel_endpoint == "https://dt.example/api"
    assert session.executed_with is odps_entry
    assert session.destroyed
    assert maxframe.new_session_kwargs == {
        "odps_entry": odps_entry,
        "default": False,
        "timeout": 600,
    }
    assert captured_options["session.default_schema"] == "analytics"
    assert captured_options["session.quota_name"] == "profile_quota"
    assert captured_options["local_timezone"] == "Asia/Shanghai"
    assert captured_options["sql.settings"]["odps.sql.allow.fullscan"] == "false"
    assert "dbt.execution_mode" not in captured_options["sql.settings"]
    assert result == MaxFrameSubmissionResult(
        run_id="maxframe-session-123",
        compiled_code=compiled_code,
        logview_available=True,
    )


def test_submit_drops_intermediate_table_and_session_after_failure():
    session = FakeSession()
    maxframe = FakeMaxFrame(session)
    credentials = make_credentials()
    helper = MaxFramePythonJobHelper(make_parsed_model(), credentials)

    @contextmanager
    def option_context(_):
        yield SimpleNamespace()

    compiled_code = """
_dbt_maxframe_target_relation = "project.schema.model__dbt_tmp"
raise ValueError("model exploded")
"""

    with patch(
        "dbt.adapters.maxcompute.python_submissions._load_maxframe_runtime",
        return_value=(maxframe, option_context),
    ):
        with pytest.raises(DbtRuntimeError, match="model exploded"):
            helper.submit(compiled_code)

    credentials.odps.return_value.delete_table.assert_called_once_with(
        "project.schema.model__dbt_tmp", if_exists=True
    )
    assert session.destroyed


def test_submit_retries_intermediate_cleanup_after_transient_failure():
    session = FakeSession()
    maxframe = FakeMaxFrame(session)
    credentials = make_credentials()
    credentials.odps.return_value.delete_table.side_effect = [
        ConnectionResetError("transient reset"),
        None,
    ]
    helper = MaxFramePythonJobHelper(make_parsed_model(), credentials)

    @contextmanager
    def option_context(_):
        yield SimpleNamespace()

    compiled_code = """
_dbt_maxframe_target_relation = "project.schema.model__dbt_tmp"
raise ValueError("model exploded")
"""

    with (
        patch(
            "dbt.adapters.maxcompute.python_submissions._load_maxframe_runtime",
            return_value=(maxframe, option_context),
        ),
        patch("dbt.adapters.maxcompute.python_submissions.time.sleep") as sleep,
        pytest.raises(DbtRuntimeError, match="model exploded"),
    ):
        helper.submit(compiled_code)

    assert credentials.odps.return_value.delete_table.call_count == 2
    sleep.assert_called_once_with(0.5)
    assert session.destroyed


def test_model_level_quota_overrides_profile_quota():
    credentials = make_credentials()
    helper = MaxFramePythonJobHelper(
        make_parsed_model(maxframe_quota_name="model_quota"), credentials
    )

    assert helper._maxframe_options()["session.quota_name"] == "model_quota"


def test_maxframe_retries_default_and_profile_override():
    credentials = make_credentials()
    helper = MaxFramePythonJobHelper(make_parsed_model(), credentials)
    assert helper._maxframe_retries() == 2

    credentials.maxframe_retries = 4
    assert helper._maxframe_retries() == 4


def test_submit_retries_dag_transport_failure_with_new_session():
    first_session = FakeSession("first-session")
    retry_session = FakeSession("retry-session")
    maxframe = SequencedMaxFrame(first_session, retry_session)
    credentials = make_credentials()
    helper = MaxFramePythonJobHelper(
        make_parsed_model(maxframe_retries=1), credentials
    )

    @contextmanager
    def option_context(_):
        yield SimpleNamespace()

    compiled_code = """
class RetryOnceTileable:
    attempts = 0

    def execute(self, session):
        RetryOnceTileable.attempts += 1
        if RetryOnceTileable.attempts == 1:
            raise ConnectionResetError("transient reset")
        maxframe_session.executed_with = session

_dbt_maxframe_execute(RetryOnceTileable())
"""

    with patch(
        "dbt.adapters.maxcompute.python_submissions._load_maxframe_runtime",
        return_value=(maxframe, option_context),
    ):
        result = helper.submit(compiled_code)

    assert result.run_id == "retry-session"
    assert first_session.destroyed
    assert retry_session.destroyed
    assert first_session.executed_with is retry_session
    assert len(maxframe.new_session_kwargs) == 2


@pytest.mark.parametrize("retries", [-1, "invalid"])
def test_invalid_maxframe_retries_fail_with_actionable_error(retries):
    helper = MaxFramePythonJobHelper(
        make_parsed_model(maxframe_retries=retries), make_credentials()
    )

    with pytest.raises(DbtRuntimeError, match="non-negative integer"):
        helper._maxframe_retries()


def test_packages_fail_with_actionable_error():
    with pytest.raises(
        DbtRuntimeError, match="does not support model-level `packages`"
    ):
        MaxFramePythonJobHelper(
            make_parsed_model(packages=["scikit-learn==1.7.0"]), make_credentials()
        )


def test_missing_maxframe_has_install_hint():
    with patch.dict("sys.modules", {"maxframe": None}):
        with pytest.raises(DbtRuntimeError, match=r"dbt-maxcompute\[maxframe\]"):
            _load_maxframe_runtime()


def test_adapter_response_contains_session_without_logview_token():
    result = MaxFrameSubmissionResult(
        run_id="session-1",
        compiled_code="print('hello')",
        logview_available=True,
    )

    response = MaxComputeAdapter.generate_python_submission_response(None, result)

    assert response.query_id == "session-1"
    assert response.code == "print('hello')"
    assert response._message == "OK (MaxFrame session: session-1)"
    assert "https://logview.example/session-1" not in response._message


def test_adapter_response_contains_session_when_logview_is_unavailable():
    result = MaxFrameSubmissionResult(
        run_id="session-without-logview",
        compiled_code="print('hello')",
        logview_available=False,
    )

    response = MaxComputeAdapter.generate_python_submission_response(None, result)

    assert response._message == "OK (MaxFrame session: session-without-logview)"


@pytest.mark.parametrize(
    ("raw_config", "expected_fields", "expected_types"),
    [
        (
            {"fields": "pt,region", "data_types": "string,string"},
            ["pt", "region"],
            ["string", "string"],
        ),
        (
            {"field": "event_time", "data_type": "timestamp"},
            ["event_time"],
            ["timestamp"],
        ),
        (
            {"fields": ["pt"], "data_types": ["string"]},
            ["pt"],
            ["string"],
        ),
    ],
)
def test_partition_config_accepts_maxcompute_and_bigquery_shapes(
    raw_config, expected_fields, expected_types
):
    config = PartitionConfig.parse(raw_config)

    assert config.fields == expected_fields
    assert config.data_types == expected_types


def test_partition_config_rejects_conflicting_aliases():
    with pytest.raises(DbtRuntimeError, match="use either `field` or `fields`"):
        PartitionConfig.parse({"field": "pt", "fields": "other"})


def test_dbt_parse_accepts_maxframe_python_model(tmp_path: Path):
    project_dir = tmp_path / "project"
    profiles_dir = tmp_path / "profiles"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)
    profiles_dir.mkdir()

    (project_dir / "dbt_project.yml").write_text(
        """
name: maxframe_parse_test
version: 1.0.0
config-version: 2
profile: maxframe_parse_test
model-paths: [models]
""",
        encoding="utf-8",
    )
    (profiles_dir / "profiles.yml").write_text(
        """
maxframe_parse_test:
  target: dev
  outputs:
    dev:
      type: maxcompute
      project: test_project
      schema: default
      endpoint: http://example.invalid/api
      access_key_id: test
      access_key_secret: test
      submission_method: maxframe
      maxframe_quota_name: test_quota
""",
        encoding="utf-8",
    )
    (models_dir / "upstream.sql").write_text("select 1 as id\n", encoding="utf-8")
    (models_dir / "python_model.py").write_text(
        """
def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="maxframe",
        timeout=600,
    )
    return dbt.ref("upstream")
""",
        encoding="utf-8",
    )
    (models_dir / "incremental_python_model.py").write_text(
        """
def model(dbt, session):
    dbt.config(
        materialized="incremental",
        submission_method="maxframe",
        incremental_strategy="merge",
        unique_key="id",
        partition_by={"field": "pt", "data_type": "string"},
    )
    return dbt.ref("upstream")
""",
        encoding="utf-8",
    )
    (models_dir / "microbatch_python_model.py").write_text(
        """
def model(dbt, session):
    dbt.config(
        materialized="incremental",
        submission_method="maxframe",
        incremental_strategy="microbatch",
        unique_key="id",
        event_time="event_time",
        batch_size="day",
        begin="2026-08-06",
        partition_by={
            "field": "event_time",
            "data_type": "timestamp",
            "granularity": "day",
            "generate_column_name": "ds",
        },
    )
    return dbt.ref("upstream")
""",
        encoding="utf-8",
    )

    parse_result = dbtRunner().invoke(
        [
            "parse",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
            "--no-partial-parse",
        ]
    )

    assert parse_result.success, parse_result.exception
