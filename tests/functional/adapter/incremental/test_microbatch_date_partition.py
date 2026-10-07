"""A `date` event column: refused where the adapter can see it, and what to write instead.

dbt-core selects a batch with `event_time >= '<start>' and event_time < '<end>'`, and renders those
boundaries timestamp-shaped - `2025-05-01 00:00:00+00:00`. MaxCompute does not convert that text
against a `DATE` column and does not raise either: the comparison evaluates to `NULL`. No row
satisfies a `NULL` predicate, so a batch over a DATE event column selects nothing; a batch write is
an `insert overwrite`, and an empty one is a no-op - so the target stays empty while `dbt run`
reports the model as a success. Measured on a real project with four rows across two days: `PASS=2`,
zero rows in the target, and the same `NULL` in `Etc/GMT`, `Asia/Shanghai` and `Etc/GMT+8` sessions,
so unlike the profile-`timezone` defect this is not a clock disagreement.

The window is attached to *upstream* relations that declare `event_time`, which is what makes the
remedy non-obvious: the column that has to stop being a DATE is the one the window is compared
against, not the one the model finally writes. Three consequences are pinned here.

* `TestMicrobatchDateColumnIsRefused` - the shape a user reports: the model declares its event
  column as a `date` partition column. That is refused at compile time, in
  `mc_validate_microbatch_config`, because the declared `partition_by.data_type` is the only signal
  available where the boundary is rendered (dbt-core's `render_event_time_filtered` is handed a
  field name, not a column type).
* `TestMicrobatchCastInsideModelIsStillEmpty` - the same model, refused no longer, casting the DATE
  to `timestamp` *in its own SELECT*: still empty, still a successful run, because the window was
  applied to the DATE column of the upstream relation. This is a residual limitation, pinned so the
  next person does not present it as a fix.
* `TestMicrobatchDateColumnWorkaround` - the working shape: cast in the upstream model and declare
  `event_time` on that instant. Each day window writes its own rows into its own partition, and
  replaying the first window does not erase the second.
"""

import os

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

# A DATE has no clock, so these literals read the same day in every session timezone; the
# from_unixtime() trick was only needed for the instant-valued columns of the timezone case.
_input_sql = (
    "{{ config(materialized='table', event_time='event_date') }}\n"
    "select 1 as id, cast('2025-05-01' as date) as event_date\n"
    "union all\n"
    "select 2 as id, cast('2025-05-01' as date) as event_date\n"
    "union all\n"
    "select 3 as id, cast('2025-05-02' as date) as event_date\n"
    "union all\n"
    "select 4 as id, cast('2025-05-02' as date) as event_date\n"
)

# The remedy, one step up: the relation the window is attached to exposes an instant and declares
# event_time on it.
_staged_sql = (
    "{{ config(materialized='table', event_time='event_time') }}\n"
    "select id, cast(event_date as timestamp) as event_time from {{ ref('input_model') }}\n"
)

_REASON = "cannot batch on a `date` event column"

DAY1 = ("2025-05-01", "2025-05-02")
DAY2 = ("2025-05-02", "2025-05-03")


def _microbatch_model(event_field, data_type, select_columns, upstream="input_model"):
    return (
        "{{ config(\n"
        "    materialized='incremental',\n"
        "    incremental_strategy='microbatch',\n"
        "    unique_key='id',\n"
        f"    event_time='{event_field}',\n"
        "    batch_size='day',\n"
        "    begin=modules.datetime.datetime(2025, 5, 1, 0, 0, 0),\n"
        f"    partition_by={{'field': '{event_field}', 'data_type': '{data_type}', "
        "'granularity': 'day'}\n"
        ") }}\n"
        f"select {select_columns} from {{{{ ref('{upstream}') }}}}" + "\n"
    )


class _WindowedScenario:
    model_name = "microbatch_model"
    upstream = "input_model"

    def _args(self, *extra):
        selected = list(dict.fromkeys(["input_model", self.upstream, self.model_name]))
        return ["run", "--select", *selected, *extra]

    def _relation(self, project, identifier=None):
        return project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier=identifier or self.model_name,
        )

    def _ids(self, project):
        rows = project.run_sql(
            f"select id from {self._relation(project)} order by id", fetch="all"
        )
        return sorted(int(row[0]) for row in rows or [])

    def _partitions(self, project):
        table = project.adapter.get_odps_client().get_table(
            self.model_name, schema=project.test_schema
        )
        return sorted(p.name for p in table.partitions)

    def _window(self, project, start, end, select=None):
        args = ["run", "--event-time-start", start, "--event-time-end", end]
        if os.environ.get("MB_DEBUG_SQL"):
            args.append("--debug")
        args += ["--select", "input_model"]
        if self.upstream != "input_model":
            args.append(self.upstream)
        args.append(select or self.model_name)
        return run_dbt(args)


class TestMicrobatchDateColumnIsRefused(_WindowedScenario):
    """The reported shape: the event column is also the partition column, declared `date`."""

    model_sql = _microbatch_model("event_date", "date", "id, event_date")

    @pytest.fixture(scope="class")
    def models(self):
        return {"input_model.sql": _input_sql, "microbatch_model.sql": self.model_sql}

    def test_run_fails_loudly_instead_of_writing_an_empty_target(self, project):
        _, output = run_dbt_and_capture(
            self._args(
                "--event-time-start",
                DAY1[0],
                "--event-time-end",
                DAY1[1],
            ),
            expect_pass=None,
        )
        assert _REASON in output, f"expected the refusal in:\n{output}"


class TestMicrobatchCastInsideModelIsStillEmpty(_WindowedScenario):
    """A `cast()` in the microbatch model's own SELECT does not fix the window.

    The batch predicate is attached to the upstream relation, whose `event_date` is still a DATE, so
    it is still `NULL` and the batch is still empty - and the run still succeeds. Nothing in this
    adapter can see the upstream column's type, so this residual is documented, not refused.
    """

    model_sql = _microbatch_model(
        "event_date", "timestamp", "id, cast(event_date as timestamp) as event_date"
    )

    @pytest.fixture(scope="class")
    def models(self):
        return {"input_model.sql": _input_sql, "microbatch_model.sql": self.model_sql}

    def test_upstream_date_column_still_empties_the_batch(self, project):
        self._window(project, *DAY1)
        assert self._ids(project) == [], (
            "measured: the window is compared against the *upstream* DATE column, so the target "
            "stays empty even though this model's own column is a timestamp"
        )


class TestMicrobatchDateColumnWorkaround(_WindowedScenario):
    """The shape the refusal points at: an instant *at the relation the window is attached to*.

    Measured on the adapter's default session (`timezone` unset, so `odps.sql.timezone=Etc/GMT`).
    Under a profile that sets `timezone`, the DATE->TIMESTAMP reading is taken on the session clock
    and can fall on the previous partition day - that is the session-timezone behaviour pinned in
    `test_microbatch_partition_timezone.py`, not something this case re-litigates.
    """

    upstream = "staged_model"
    model_sql = _microbatch_model(
        "event_time", "timestamp", "id, event_time", upstream="staged_model"
    )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_sql,
            "staged_model.sql": _staged_sql,
            "microbatch_model.sql": self.model_sql,
        }

    def test_day_windows_land_on_their_own_partitions(self, project):
        self._window(project, *DAY1)
        assert self._ids(project) == [1, 2], "the first day window writes its own two rows"
        assert len(self._partitions(project)) == 1, "one day window touches one partition"

        self._window(project, *DAY2, select=self.model_name)
        assert self._ids(project) == [1, 2, 3, 4], "the adjacent window adds its rows"
        assert len(self._partitions(project)) == 2, "two day windows, two partitions"

        self._window(project, *DAY1, select=self.model_name)
        assert self._ids(project) == [1, 2, 3, 4], "replaying window 1 must not erase window 2"
        assert len(self._partitions(project)) == 2, "the replay must not open a third partition"
