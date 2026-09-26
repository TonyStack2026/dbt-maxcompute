"""Declared column widths on the incremental temp path, measured on a real server.

dbt-core's incremental materialization calls
``adapter.expand_target_column_types(from_relation=temp, to_relation=target)`` before
merging a temp relation into an existing model. This adapter's copy does not call it.
Leaving the call out is not the reason users lose data, and not the reason they are
safe; these three tests record what is actually true on MaxCompute.

1. An over-long value reaching a ``varchar(n)`` target is **truncated silently**: the
   run succeeds, the row lands, the extra characters are gone.
2. Core's call would not help: it changes nothing here. ``SQLAdapter.alter_column_type``
   only evaluates the ``alter_column_type`` macro and discards its result, and this
   dialect's ``maxcompute__alter_column_type`` just renders the statement as text -- no
   ``{% call statement(...) %}``, so nothing is submitted.
3. The statement itself is accepted by the server, which is what makes (2) a bug rather
   than a deliberate dialect choice: the widening works, it is simply never sent.

Together those mean the type-widening safety net is inert in this adapter for both
``incremental`` and ``snapshot`` (the latter does call it), and it fails silently.
The hazard when someone fixes (2) is recorded in the last test:
``MaxComputeColumn.can_expand_to`` answers True for *any* pair of string columns -- it
ignores width -- and ``string_type()`` always renders ``string``, so a fixing must make
that comparison size-aware or every string column of every target starts being rewritten
to unbounded ``string``.
"""

import pytest
from dbt.tests.util import relation_from_name, run_dbt

# First run creates the target with a declared varchar(10); the incremental run selects an
# unbounded 16-character string, which is what an upstream column that grew produces.
NARROW_MODEL = """
{{ config(materialized='incremental', incremental_strategy='append') }}
{%- if is_incremental() %}
select 'xxxxxxxxxxxxxxxxy' as c
{%- else %}
select cast('abcdefghij' as varchar(10)) as c
{%- endif %}
"""


def column_shapes(adapter, name):
    """Server read-back of (dtype, char_size) for every column of a table."""
    return [
        (col.dtype, col.char_size)
        for col in adapter.get_columns_in_relation(relation_from_name(adapter, name))
    ]


class TestOverlongValueIsTruncatedNotRejected:
    """What a narrow target costs today: silent truncation, and a green run."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"narrow_target.sql": NARROW_MODEL}

    def test_append_from_a_wider_source_silently_truncates(self, project):
        run_dbt(["run"])
        assert column_shapes(project.adapter, "narrow_target") == [("varchar(10)", 10)]

        result = run_dbt(["run"])  # appends a 16-character value through the temp path
        assert result[0].status == "success"
        rows = project.run_sql(
            f"select length(c) from {project.test_schema}.narrow_target", fetch="all"
        )
        # Two rows, both 10 characters: MaxCompute cast the value down on insert and the
        # extra 6 characters are gone. Nothing in the run result or the logs says so.
        assert sorted(int(row[0]) for row in rows) == [10, 10]
        assert column_shapes(project.adapter, "narrow_target") == [("varchar(10)", 10)]


class TestExpandTargetColumnTypesIsInertHere:
    """Core's widening call does not run against this dialect."""

    TARGET = "expand_target"
    STAGE = "expand_stage"

    @pytest.fixture(scope="class")
    def relations(self, project):
        project.run_sql(
            f"create table {project.test_schema}.{self.TARGET} (c varchar(10), d string)"
        )
        project.run_sql(
            f"create table {project.test_schema}.{self.STAGE} as select 'yyyy' as c, 'z' as d"
        )
        return (
            relation_from_name(project.adapter, self.TARGET),
            relation_from_name(project.adapter, self.STAGE),
        )

    def test_call_changes_nothing(self, project, relations):
        target, stage = relations
        before = column_shapes(project.adapter, self.TARGET)
        assert before == [("varchar(10)", 10), ("string", None)]

        # Exactly the call core makes on the temp path: must not raise, must not submit.
        project.adapter.expand_target_column_types(from_relation=stage, to_relation=target)
        assert column_shapes(project.adapter, self.TARGET) == before

    def test_the_same_statement_does_work_when_it_is_sent(self, project, relations):
        target, _stage = relations
        project.run_sql(
            f"alter table {project.test_schema}.{self.TARGET} " "change column c c string"
        )
        # The engine accepts it, so the no-op above is the macro not sending SQL, not a
        # MaxCompute restriction. Read-back proves the widening is available.
        assert column_shapes(project.adapter, self.TARGET) == [("string", None), ("string", None)]

    def test_can_expand_to_ignores_width_so_a_fix_must_be_size_aware(self, project, relations):
        target, stage = relations
        target_cols = project.adapter.get_columns_in_relation(target)
        stage_cols = project.adapter.get_columns_in_relation(stage)
        pairs = {
            (t.name, r.name): t.can_expand_to(r)
            for t in target_cols
            for r in stage_cols
            if t.name == r.name
        }
        # `d` is string -> string: nothing to widen, yet it still answers True; and the
        # type the widening would emit is always unbounded `string`, never a larger varchar.
        assert pairs == {("c", "c"): True, ("d", "d"): True}, pairs
        assert project.adapter.Column.string_type(256) == "string"
