import pytest
from pyspark.sql.types import StructType, StructField, StringType, Row

from pyspark_explorer.data_table import DataTable

def test_dummy() -> None:
    assert True

class TestDataTable:
    def test_simple_table(self) -> None:
        schema = [StructField("text", StringType(), True)]
        row = [Row(name="some text 1"),Row(name="some text 2")]

        tab = DataTable(schema, row)

        expected_cols = [{"index": 0, "name": "text", "type": "StringType", "field_type": schema[0]}]
        assert tab.columns == expected_cols
        expected_rows = [
            {"index": 0, "column": expected_cols[0], "value": "some text 1", "display_value": "some text 1"},
            {"index": 1, "column": expected_cols[0], "value": "some text 2", "display_value": "some text 2"},
        ]
        assert tab.rows[0] == expected_rows[0]
        assert tab.rows[1] == expected_rows[1]
