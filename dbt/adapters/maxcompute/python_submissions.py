from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from dbt.adapters.base import PythonJobHelper, PythonSubmissionResult
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError
from odps.config import option_context as odps_option_context

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

    def __init__(
        self, parsed_model: Dict[str, Any], credentials: MaxComputeCredentials
    ) -> None:
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
        if self._credentials.schema:
            options["session.default_schema"] = self._credentials.schema
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

    @staticmethod
    def _logview_is_available(session: Any) -> bool:
        try:
            # The returned URL embeds a temporary access token.  Verify that
            # LogView is available, then discard the signed URL immediately.
            session.get_logview_address()
            return True
        except Exception:
            logger.warning(
                f"Unable to obtain MaxFrame LogView URL for session: "
                f"{session.session_id}"
            )
            return False

    def submit(self, compiled_code: str) -> MaxFrameSubmissionResult:
        maxframe, maxframe_option_context = _load_maxframe_runtime()
        timeout = self._parsed_model["config"].get("timeout")
        odps_entry = None
        session = None
        active_sessions = []
        namespace: Dict[str, Any] = {}
        succeeded = False

        try:
            with (
                odps_option_context(),
                maxframe_option_context(self._maxframe_options()),
            ):
                odps_entry = self._credentials.odps()
                if self._credentials.tunnel_endpoint:
                    odps_entry.tunnel_endpoint = self._credentials.tunnel_endpoint
                # MaxFrame's ODPS table sink currently resolves the ODPS entry
                # from PyODPS global options while the graph is being built.
                # option_context confines that state to this dbt execution.
                odps_entry.to_global(overwritable=True)
                session = maxframe.new_session(
                    odps_entry=odps_entry,
                    default=False,
                    timeout=timeout,
                )
                active_sessions.append(session)
                session_id = str(session.session_id)
                logview_available = self._logview_is_available(session)

                logger.info(f"Created MaxFrame session: {session_id}")
                if logview_available:
                    logger.info(
                        f"MaxFrame LogView is available for session: {session_id}"
                    )

                def execute_with_retry(tileable):
                    nonlocal session, session_id, logview_available
                    retries = self._maxframe_retries()
                    for retry_number in range(retries + 1):
                        try:
                            return tileable.execute(session=session)
                        except OSError:
                            if retry_number == retries:
                                raise
                            logger.warning(
                                "MaxFrame transport failed while waiting for the "
                                f"DAG; retrying with a new session "
                                f"({retry_number + 1}/{retries})"
                            )
                            session = maxframe.new_session(
                                odps_entry=odps_entry,
                                default=False,
                                timeout=timeout,
                            )
                            active_sessions.append(session)
                            session_id = str(session.session_id)
                            logview_available = self._logview_is_available(session)
                            logger.info(f"Created retry MaxFrame session: {session_id}")

                namespace = {
                    "__name__": "__dbt_maxframe_model__",
                    "__file__": self._model_filename(),
                    "maxframe_session": session,
                    "odps_entry": odps_entry,
                    "_dbt_maxframe_execute": execute_with_retry,
                }
                exec(
                    compile(compiled_code, self._model_filename(), "exec"),
                    namespace,
                    namespace,
                )
                succeeded = True

                return MaxFrameSubmissionResult(
                    run_id=session_id,
                    compiled_code=compiled_code,
                    logview_available=logview_available,
                )
        except DbtRuntimeError:
            raise
        except Exception as exc:
            raise DbtRuntimeError(
                f"MaxFrame model {self._parsed_model.get('unique_id', '')} failed: {exc}"
            ) from exc
        finally:
            intermediate_relation = namespace.get("_dbt_maxframe_target_relation")
            if not succeeded and intermediate_relation and odps_entry is not None:
                for cleanup_attempt in range(1, 4):
                    try:
                        odps_entry.delete_table(intermediate_relation, if_exists=True)
                        logger.debug(
                            f"Dropped failed MaxFrame intermediate table: "
                            f"{intermediate_relation}"
                        )
                        break
                    except Exception as cleanup_exc:
                        if cleanup_attempt == 3:
                            logger.warning(
                                f"Failed to drop MaxFrame intermediate table "
                                f"{intermediate_relation} after 3 attempts: "
                                f"{cleanup_exc}"
                            )
                            break
                        logger.debug(
                            f"Retrying failed MaxFrame intermediate table cleanup "
                            f"for {intermediate_relation} after attempt "
                            f"{cleanup_attempt}"
                        )
                        time.sleep(0.5 * cleanup_attempt)
            destroyed_session_objects = set()
            for active_session in reversed(active_sessions):
                session_object_id = id(active_session)
                if session_object_id in destroyed_session_objects:
                    continue
                destroyed_session_objects.add(session_object_id)
                try:
                    active_session.destroy()
                    logger.debug(
                        f"Destroyed MaxFrame session: {active_session.session_id}"
                    )
                except Exception as cleanup_exc:
                    logger.warning(
                        f"Failed to destroy MaxFrame session "
                        f"{active_session.session_id}: "
                        f"{cleanup_exc}"
                    )
