from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from dbt.adapters.base import PythonJobHelper, PythonSubmissionResult
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError
from odps.config import option_context as odps_option_context
from odps.errors import ODPSError

from dbt.adapters.maxcompute.context import GLOBAL_SQL_HINTS
from dbt.adapters.maxcompute.credentials import MaxComputeCredentials

logger = AdapterLogger("MaxCompute")


@dataclass
class MaxFrameSubmissionResult(PythonSubmissionResult):
    """Result metadata exposed through the dbt adapter response."""

    logview_available: bool = False


def _load_maxframe_runtime() -> Tuple[Any, Any]:
    """Lazy-load MaxFrame so SQL-only dbt commands stay lightweight."""
    try:
        import maxframe
        from maxframe.config import option_context as maxframe_option_context
    except ImportError as exc:
        raise DbtRuntimeError(
            "MaxFrame is required for Python models. Install it with "
            '`pip install "dbt-maxcompute[maxframe]"`.'
        ) from exc

    return maxframe, maxframe_option_context


class MaxFramePythonJobHelper(PythonJobHelper):
    """Execute a compiled dbt Python model with a dedicated MaxFrame session."""

    _SENTINEL_COLUMN = "__dbt_maxframe_sentinel"

    def __init__(self, parsed_model: Dict[str, Any], credentials: MaxComputeCredentials) -> None:
        packages = parsed_model["config"].get("packages", [])
        if packages:
            raise DbtRuntimeError(
                "The MaxFrame submission method does not support model-level "
                "`packages` yet. Install dependencies in the dbt runtime instead."
            )

        self._parsed_model = parsed_model
        self._credentials = credentials

    def _maxframe_options(self) -> Dict[str, Any]:
        model_config = self._parsed_model["config"]
        sql_settings = GLOBAL_SQL_HINTS.copy()
        sql_settings.update(model_config.get("sql_hints") or {})

        # These are dbt adapter routing keys, not MaxCompute SQL settings.
        sql_settings.pop("dbt.execution_mode", None)
        sql_settings.pop("dbt.quota_name", None)

        options: Dict[str, Any] = {"sql.settings": sql_settings}
        options["local_timezone"] = self._credentials.timezone or "UTC"
        quota_name = model_config.get("maxframe_quota_name") or getattr(
            self._credentials, "maxframe_quota_name", None
        )
        if quota_name:
            options["session.quota_name"] = quota_name
        default_schema = self._parsed_model.get("schema") or self._credentials.schema
        if default_schema:
            options["session.default_schema"] = default_schema
        return options

    def _model_filename(self) -> str:
        return self._parsed_model.get("original_file_path") or self._parsed_model.get(
            "path", "dbt_maxframe_model.py"
        )

    def _maxframe_retries(self) -> int:
        raw_retries = self._parsed_model["config"].get("maxframe_retries")
        if raw_retries is None:
            raw_retries = getattr(self._credentials, "maxframe_retries", None)
        if raw_retries is None:
            raw_retries = 2
        try:
            retries = int(raw_retries)
        except (TypeError, ValueError) as exc:
            raise DbtRuntimeError("`maxframe_retries` must be a non-negative integer") from exc
        if retries < 0:
            raise DbtRuntimeError("`maxframe_retries` must be a non-negative integer")
        return retries

    def _maxframe_timeout(self) -> int:
        raw_timeout = self._parsed_model["config"].get("timeout")
        if raw_timeout is None:
            raw_timeout = 600
        try:
            timeout = int(raw_timeout)
        except (TypeError, ValueError) as exc:
            raise DbtRuntimeError("`timeout` must be a positive integer") from exc
        if timeout <= 0:
            raise DbtRuntimeError("`timeout` must be a positive integer")
        return timeout

    @staticmethod
    def _logview_is_available(session: Any) -> bool:
        try:
            # The returned URL embeds a temporary access token.  Verify that
            # LogView is available, then discard the signed URL immediately.
            session.get_logview_address()
            return True
        except Exception:
            logger.warning(
                f"Unable to obtain MaxFrame LogView URL for session: " f"{session.session_id}"
            )
            return False

    @classmethod
    def _is_transient_maxframe_error(cls, exc: BaseException) -> bool:
        if isinstance(exc, OSError):
            return True
        # MaxFrame's local session client uses Tornado and surfaces task-server
        # failures as HTTPClientError rather than PyODPS exceptions.
        if exc.__class__.__module__ == "tornado.httpclient":
            status_code = getattr(exc, "code", None)
            if status_code in (408, 429):
                return True
        if isinstance(exc, ODPSError):
            status_code = getattr(exc, "status_code", None)
            if status_code is not None and status_code >= 500:
                return True
            nested_error = getattr(exc, "nested_error", None)
            if nested_error is not None and nested_error is not exc:
                return cls._is_transient_maxframe_error(nested_error)
        return False

    @classmethod
    def _with_sentinel(
        cls,
        dataframe: Any,
        target_relation: str,
        odps_entry: Any,
        temporary_relations: list[str],
    ) -> Any:
        import maxframe.dataframe as md
        from maxframe.io.odpsio import pandas_to_odps_schema
        from odps.types import Column, OdpsSchema

        inferred_schema, _ = pandas_to_odps_schema(
            dataframe, unknown_as_string=True, ignore_index=True
        )
        if any(
            column.name.lower() == cls._SENTINEL_COLUMN.lower()
            for column in inferred_schema.columns
        ):
            raise DbtRuntimeError(
                f"MaxFrame models reserve the column name `{cls._SENTINEL_COLUMN}` "
                "for empty-output materialization"
            )

        relation_prefix = target_relation.rsplit(".", 1)[0]
        sentinel_relation = f"{relation_prefix}.__dbt_mf_sentinel_{uuid.uuid4().hex[:12]}"
        sentinel_schema = OdpsSchema(
            columns=list(inferred_schema.columns) + [Column(cls._SENTINEL_COLUMN, "boolean")]
        )
        sentinel_table = odps_entry.create_table(sentinel_relation, sentinel_schema, lifecycle=1)
        temporary_relations.append(sentinel_relation)
        with sentinel_table.open_writer() as writer:
            writer.write([[None] * len(inferred_schema.columns) + [True]])

        sentinel = md.read_odps_table(sentinel_relation, odps_entry=odps_entry)
        tagged_dataframe = dataframe.assign(**{cls._SENTINEL_COLUMN: False})
        return md.concat([tagged_dataframe, sentinel], ignore_index=True)

    def submit(self, compiled_code: str) -> MaxFrameSubmissionResult:
        maxframe, maxframe_option_context = _load_maxframe_runtime()
        timeout = self._maxframe_timeout()
        odps_entry = None
        session = None
        active_sessions: list[Any] = []
        namespace: Dict[str, Any] = {}
        temporary_relations: list[str] = []
        succeeded = False

        def destroy_active_session(active_session: Any) -> None:
            if not any(item is active_session for item in active_sessions):
                return
            try:
                active_session.destroy()
                logger.debug(f"Destroyed MaxFrame session: {active_session.session_id}")
            except Exception as cleanup_exc:
                logger.warning(
                    f"Failed to destroy MaxFrame session "
                    f"{active_session.session_id}: {cleanup_exc}"
                )
                return
            active_sessions[:] = [item for item in active_sessions if item is not active_session]

        try:
            with (
                odps_option_context(),
                maxframe_option_context(self._maxframe_options()),
            ):
                odps_entry = self._credentials.odps()
                model_schema = self._parsed_model.get("schema")
                if model_schema:
                    odps_entry.schema = model_schema
                if self._credentials.tunnel_endpoint:
                    odps_entry.tunnel_endpoint = self._credentials.tunnel_endpoint
                # MaxFrame's ODPS table sink currently resolves the ODPS entry
                # from PyODPS global options while the graph is being built.
                # option_context confines that state to this dbt execution.
                odps_entry.to_global(overwritable=True)
                retries = self._maxframe_retries()
                for retry_number in range(retries + 1):
                    session = maxframe.new_session(
                        odps_entry=odps_entry,
                        default=False,
                        timeout=timeout,
                    )
                    active_sessions.append(session)
                    session_id = str(session.session_id)
                    logview_available = self._logview_is_available(session)

                    if retry_number:
                        logger.info(f"Created retry MaxFrame session: {session_id}")
                    else:
                        logger.info(f"Created MaxFrame session: {session_id}")
                    if logview_available:
                        logger.info(f"MaxFrame LogView is available for session: {session_id}")

                    namespace = {
                        "__name__": "__dbt_maxframe_model__",
                        "__file__": self._model_filename(),
                        "maxframe_session": session,
                        "odps_entry": odps_entry,
                        "_dbt_maxframe_execute": (
                            lambda tileable, session=session: tileable.execute(session=session)
                        ),
                        "_dbt_maxframe_with_sentinel": (
                            lambda dataframe, target_relation: self._with_sentinel(
                                dataframe,
                                target_relation,
                                odps_entry,
                                temporary_relations,
                            )
                        ),
                    }
                    try:
                        exec(
                            compile(compiled_code, self._model_filename(), "exec"),
                            namespace,
                            namespace,
                        )
                    except Exception as exc:
                        if retry_number == retries or not self._is_transient_maxframe_error(exc):
                            raise
                        destroy_active_session(session)
                        self._cleanup_failed_relation(namespace, odps_entry)
                        logger.warning(
                            "MaxFrame transport or service failed while building or "
                            f"executing the DAG; retrying the model with a new session "
                            f"({retry_number + 1}/{retries})"
                        )
                        continue

                    succeeded = True
                    return MaxFrameSubmissionResult(
                        run_id=session_id,
                        compiled_code=compiled_code,
                        logview_available=logview_available,
                    )
                raise DbtRuntimeError("MaxFrame submission completed without a result")
        except DbtRuntimeError:
            raise
        except Exception as exc:
            raise DbtRuntimeError(
                f"MaxFrame model {self._parsed_model.get('unique_id', '')} failed: {exc}"
            ) from exc
        finally:
            if not succeeded and odps_entry is not None:
                self._cleanup_failed_relation(namespace, odps_entry)
            if odps_entry is not None:
                for temporary_relation in reversed(temporary_relations):
                    self._delete_relation_with_retry(temporary_relation, odps_entry)
            destroyed_session_objects = set()
            for active_session in reversed(active_sessions):
                session_object_id = id(active_session)
                if session_object_id in destroyed_session_objects:
                    continue
                destroyed_session_objects.add(session_object_id)
                destroy_active_session(active_session)

    @staticmethod
    def _cleanup_failed_relation(namespace: Dict[str, Any], odps_entry: Any) -> None:
        target_relation = namespace.get("_dbt_maxframe_target_relation")
        if not target_relation:
            return
        MaxFramePythonJobHelper._delete_relation_with_retry(target_relation, odps_entry)

    @staticmethod
    def _delete_relation_with_retry(target_relation: str, odps_entry: Any) -> None:
        for cleanup_attempt in range(1, 4):
            try:
                odps_entry.delete_table(target_relation, if_exists=True)
                logger.debug(f"Dropped temporary MaxFrame table: {target_relation}")
                return
            except Exception as cleanup_exc:
                if cleanup_attempt == 3:
                    logger.warning(
                        f"Failed to drop temporary MaxFrame table "
                        f"{target_relation} after 3 attempts: {cleanup_exc}"
                    )
                    return
                logger.debug(
                    f"Retrying temporary MaxFrame table cleanup for "
                    f"{target_relation} after attempt {cleanup_attempt}"
                )
                time.sleep(0.5 * cleanup_attempt)
