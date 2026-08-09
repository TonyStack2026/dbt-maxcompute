import pytest
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
    basic_python,
    basic_sql,
    schema_yml,
    second_sql,
)

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
    pass
