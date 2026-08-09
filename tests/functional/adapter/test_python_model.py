import pytest
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
    basic_python,
    basic_sql,
    incremental_python,
    m_1,
    schema_yml,
    second_sql,
)
from dbt.tests.util import run_dbt

pytest.importorskip("maxframe", reason="requires dbt-maxcompute[maxframe]")


class TestMaxFramePythonModel(BasePythonModelTests):
    @pytest.fixture(scope="class")
    def models(self):
        maxframe_python = basic_python.replace(
            "materialized='table',",
            "materialized='table', submission_method='maxframe',",
        ).replace("df.limit(2)", "df.head(2)")
        return {
            "schema.yml": schema_yml,
            "my_sql_model.sql": basic_sql,
            "my_versioned_sql_model_v1.sql": basic_sql,
            "my_python_model.py": maxframe_python,
            "second_sql_model.sql": second_sql,
        }


class TestMaxFramePythonIncremental(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Empty incremental output must be handled even when transport retries
        # are disabled; retry behavior itself is covered by unit tests.
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+maxframe_retries": 0,
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        # MaxFrame reserves ``DataFrame.id`` for its tileable identifier and
        # ``DataFrame.filter`` selects labels rather than rows. Use pandas-style
        # item access and boolean indexing for the equivalent row predicate.
        maxframe_incremental = incremental_python.replace(
            "df = df.filter(df.id > 5)", 'df = df[df["id"] > 5]'
        )
        return {"m_1.sql": m_1, "incremental.py": maxframe_incremental}


class TestMaxFrameEmptyPartitionedTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "partition_input.sql": "select 1 as id, 'cn' as pt",
            "empty_partitioned.py": """
def model(dbt, session):
    dbt.config(
        materialized="table",
        submission_method="maxframe",
        partition_by={"field": "pt", "data_type": "string"},
        lifecycle=1,
    )
    dataframe = dbt.ref("partition_input")
    return dataframe[dataframe["id"] > 10]
""",
        }

    def test_empty_table_preserves_partition_schema(self, project):
        run_dbt(["run"])
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="empty_partitioned",
        )

        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 0
        columns = project.adapter.get_columns_in_relation(relation)
        assert [column.name for column in columns] == ["id", "pt"]
