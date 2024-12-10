import pytest
from pyspark.sql.types import StructType, StructField, StringType, Row, DateType, LongType, IntegerType, ArrayType

from pyspark_explorer.data_table import DataTable

class TestDataTable:
    def test_one_simple_field(self) -> None:
        schema = [StructField("text", StringType())]
        rows = [Row(text="some text 1"),Row(text="some text 2")]
        tab = DataTable(schema, rows)

        expected_cols = [{"col_index": 0, "name": "text", "type": "StringType", "field_type": schema[0].dataType}]
        assert tab.columns == expected_cols
        expected_rows = [
            {"row_index": 0, "row": [{"column": expected_cols[0], "kind": "simple", "value": "some text 1", "display_value": "some text 1"}]},
            {"row_index": 1, "row": [{"column": expected_cols[0], "kind": "simple", "value": "some text 2", "display_value": "some text 2"}]},
        ]
        assert tab.rows[0] == expected_rows[0]
        assert tab.rows[1] == expected_rows[1]

    def test_multiple_simple_fields(self) -> None:
        schema = [StructField("id", LongType()), StructField("text", StringType()), StructField("date", DateType())]
        rows = [Row(id=100, text="some text 1", date="2024-01-01"), Row(id=101, text="some text 2", date="2024-01-02")]
        tab = DataTable(schema, rows)

        expected_cols = [
            {"col_index": 0, "name": "id", "type": "LongType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "text", "type": "StringType", "field_type": schema[1].dataType},
            {"col_index": 2, "name": "date", "type": "DateType", "field_type": schema[2].dataType}
        ]
        assert tab.columns == expected_cols
        expected_rows = [
            {"row_index": 0, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": 100, "display_value": "100"},
                {"column": expected_cols[1], "kind": "simple", "value": "some text 1", "display_value": "some text 1"},
                {"column": expected_cols[2], "kind": "simple", "value": "2024-01-01", "display_value": "2024-01-01"}
            ]},
            {"row_index": 1, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": 101, "display_value": "101"},
                {"column": expected_cols[1], "kind": "simple", "value": "some text 2", "display_value": "some text 2"},
                {"column": expected_cols[2], "kind": "simple", "value": "2024-01-02", "display_value": "2024-01-02"}
            ]},
        ]
        assert tab.rows[0] == expected_rows[0]
        assert tab.rows[1] == expected_rows[1]

    def test_array_of_single_field(self) -> None:
        # first test internal fields containing arrays
        inner_schema = [StructField("nums", IntegerType())]
        inner_rows1 = [Row(nums=1),Row(nums=2)]
        inner_rows2 = [Row(nums=3), Row(nums=4)]
        inner_tab1 = DataTable(inner_schema, inner_rows1)
        inner_tab2 = DataTable(inner_schema, inner_rows2)
        inner_expected_cols = [
            {"col_index": 0, "name": "nums", "type": "IntegerType", "field_type": inner_schema[0].dataType},
        ]
        assert inner_tab1.columns == inner_expected_cols
        assert inner_tab2.columns == inner_expected_cols

        inner_expected_rows1 = [
            {"row_index": 0, "row": [{"column": inner_expected_cols[0], "kind": "simple", "value": 1, "display_value": "1"}]},
            {"row_index": 1, "row": [{"column": inner_expected_cols[0], "kind": "simple", "value": 2, "display_value": "2"}]},
        ]
        inner_expected_rows2 = [
            {"row_index": 0, "row": [{"column": inner_expected_cols[0], "kind": "simple", "value": 3, "display_value": "3"}]},
            {"row_index": 1, "row": [{"column": inner_expected_cols[0], "kind": "simple", "value": 4, "display_value": "4"}]},
        ]
        assert inner_tab1.rows == inner_expected_rows1
        assert inner_tab2.rows == inner_expected_rows2

        # now test complex schema with embedded array
        schema = [StructField("nums", ArrayType(IntegerType()))]
        rows = [Row(num=[1,2]),Row(num=[3,4])]
        tab = DataTable(schema, rows)

        expected_cols = [
            {"col_index": 0, "name": "nums", "type": "ArrayType", "field_type": schema[0].dataType},
        ]
        assert tab.columns == expected_cols
        expected_rows = [
            {"row_index": 0, "row": [
                {"column": expected_cols[0], "kind": "array", "value": inner_expected_rows1, "display_value": str([1,2])},
            ]},
            {"row_index": 1, "row": [
                {"column": expected_cols[0], "kind": "array", "value": inner_expected_rows2, "display_value": str([3,4])},
            ]},
        ]
        assert tab.rows[0] == expected_rows[0]
        assert tab.rows[1] == expected_rows[1]
