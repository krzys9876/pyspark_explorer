import pytest
from pyspark.sql.types import StructType, StructField, StringType, Row, DateType, LongType, IntegerType, ArrayType

from pyspark_explorer.data_table import DataTable



class TestDataTable:
    @staticmethod
    def __prepare_simple_text_field__(val1: str, val2: str) -> ([StructField], [Row], {}, {}):
        schema = [StructField("text", StringType())]
        rows = [Row(text=val1), Row(text=val2)]
        expected_cols = [{"col_index": 0, "name": "text", "type": "StringType", "field_type": schema[0].dataType}]
        expected_rows = [
            {"row_index": 0, "row": [{"column": expected_cols[0], "kind": "simple", "value": val1, "display_value": val1}]},
            {"row_index": 1, "row": [{"column": expected_cols[0], "kind": "simple", "value": val2, "display_value": val2}]},
        ]
        return schema, rows, expected_cols, expected_rows

    @staticmethod
    def __prepare_simple_num_field__(val1: int, val2: int) -> ([StructField], [Row], {}, {}):
        schema = [StructField("nums", IntegerType())]
        rows = [Row(nums=val1),Row(nums=val2)]
        expected_cols = [
            {"col_index": 0, "name": "nums", "type": "IntegerType", "field_type": schema[0].dataType},
        ]
        expected_rows = [
            {"row_index": 0, "row": [{"column": expected_cols[0], "kind": "simple", "value": val1, "display_value": str(val1)}]},
            {"row_index": 1, "row": [{"column": expected_cols[0], "kind": "simple", "value": val2, "display_value": str(val2)}]},
        ]
        return schema, rows, expected_cols, expected_rows


    @staticmethod
    def __prepare_multiple_simple_fields__(val1: [], val2: []) -> ([StructField], [Row], {}, {}):
        schema = [StructField("id", LongType()), StructField("text", StringType()), StructField("date", DateType())]
        rows = [Row(id=val1[0], text=val1[1], date=val1[2]), Row(id=val2[0], text=val2[1], date=val2[2])]
        expected_cols = [
            {"col_index": 0, "name": "id", "type": "LongType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "text", "type": "StringType", "field_type": schema[1].dataType},
            {"col_index": 2, "name": "date", "type": "DateType", "field_type": schema[2].dataType}
        ]
        expected_rows = [
            {"row_index": 0, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": val1[0], "display_value": str(val1[0])},
                {"column": expected_cols[1], "kind": "simple", "value": val1[1], "display_value": val1[1]},
                {"column": expected_cols[2], "kind": "simple", "value": val1[2], "display_value": val1[2]}
            ]},
            {"row_index": 1, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": val2[0], "display_value": str(val2[0])},
                {"column": expected_cols[1], "kind": "simple", "value": val2[1], "display_value": val2[1]},
                {"column": expected_cols[2], "kind": "simple", "value": val2[2], "display_value": val2[2]}
            ]},
        ]
        return schema, rows, expected_cols, expected_rows


    def test_one_simple_field(self) -> None:
        schema, rows, expected_cols, expected_rows = TestDataTable.__prepare_simple_text_field__("some text 1", "some text 2")
        tab = DataTable(schema, rows)
        assert tab.columns == expected_cols
        assert tab.rows[0] == expected_rows[0]
        assert tab.rows[1] == expected_rows[1]

    def test_multiple_simple_fields(self) -> None:
        schema, rows, expected_cols, expected_rows = TestDataTable.__prepare_multiple_simple_fields__(
             [100, "some text 1", "2024-01-01"],[101, "some text 2", "2024-01-02"])
        tab = DataTable(schema, rows)
        assert tab.columns == expected_cols
        assert tab.rows[0] == expected_rows[0]
        assert tab.rows[1] == expected_rows[1]

    def test_array_of_single_field(self) -> None:
        # first test internal fields containing arrays
        inner_schema1, inner_rows1, inner_expected_cols1, inner_expected_rows1 = TestDataTable.__prepare_simple_num_field__(1,2)
        inner_schema2, inner_rows2, inner_expected_cols2, inner_expected_rows2 = TestDataTable.__prepare_simple_num_field__(3,4)
        inner_tab1 = DataTable(inner_schema1, inner_rows1)
        inner_tab2 = DataTable(inner_schema2, inner_rows2)
        assert inner_tab1.columns == inner_expected_cols1
        assert inner_tab2.columns == inner_expected_cols2
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


