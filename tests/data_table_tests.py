import pytest
from pyspark.sql.types import StructType, StructField, StringType, Row, DateType, LongType, IntegerType, ArrayType

from pyspark_explorer.data_table import DataFrameTable, extract_embedded_table


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
    def __prepare_multiple_simple_fields_schema__() -> ([StructField], []):
        schema = [StructField("id", LongType()), StructField("text", StringType()), StructField("date", DateType())]
        expected_cols = [
            {"col_index": 0, "name": "id", "type": "LongType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "text", "type": "StringType", "field_type": schema[1].dataType},
            {"col_index": 2, "name": "date", "type": "DateType", "field_type": schema[2].dataType}
        ]
        return (schema, expected_cols)


    @staticmethod
    def __prepare_multiple_simple_fields_row__(val: [], index: int) -> (Row, {}):
        row = Row(id=val[0], text=val[1], date=val[2])
        _, expected_cols = TestDataTable.__prepare_multiple_simple_fields_schema__()
        expected_row = {"row_index": index, "row": [
            {"column": expected_cols[0], "kind": "simple", "value": val[0], "display_value": str(val[0])},
            {"column": expected_cols[1], "kind": "simple", "value": val[1], "display_value": val[1]},
            {"column": expected_cols[2], "kind": "simple", "value": val[2], "display_value": val[2]}
        ]}
        return row, expected_row


    @staticmethod
    def __prepare_multiple_simple_fields__(val1: [], val2: []) -> ([StructField], [Row], {}, {}):
        schema, expected_cols = TestDataTable.__prepare_multiple_simple_fields_schema__()
        row1, expected_row1 = TestDataTable.__prepare_multiple_simple_fields_row__(val1, 0)
        row2, expected_row2 = TestDataTable.__prepare_multiple_simple_fields_row__(val2, 1)
        rows = [row1, row2]
        expected_rows = [expected_row1, expected_row2]
        return schema, rows, expected_cols, expected_rows


    def test_one_simple_field(self) -> None:
        schema, rows, expected_cols, expected_rows = TestDataTable.__prepare_simple_text_field__("some text 1", "some text 2")
        tab = DataFrameTable(schema, rows)
        assert tab.columns == expected_cols
        assert tab.rows == expected_rows
        assert tab.column_names == [schema[0].name]
        assert tab.row_values == [["some text 1"],["some text 2"]]


    def test_multiple_simple_fields(self) -> None:
        schema, rows, expected_cols, expected_rows = TestDataTable.__prepare_multiple_simple_fields__(
             [100, "some text 1", "2024-01-01"],[101, "some text 2", "2024-01-02"])
        tab = DataFrameTable(schema, rows)
        assert tab.columns == expected_cols
        assert tab.rows == expected_rows
        assert tab.column_names == [schema[0].name, schema[1].name, schema[2].name]
        assert tab.row_values == [["100", "some text 1", "2024-01-01"],["101", "some text 2", "2024-01-02"]]


    def test_array_of_single_field(self) -> None:
        # first test internal fields containing arrays
        inner_schema1, inner_rows1, inner_expected_cols1, inner_expected_rows1 = TestDataTable.__prepare_simple_num_field__(1,2)
        inner_schema2, inner_rows2, inner_expected_cols2, inner_expected_rows2 = TestDataTable.__prepare_simple_num_field__(3,4)
        inner_tab1 = DataFrameTable(inner_schema1, inner_rows1)
        inner_tab2 = DataFrameTable(inner_schema2, inner_rows2)
        assert inner_tab1.columns == inner_expected_cols1
        assert inner_tab2.columns == inner_expected_cols2
        assert inner_tab1.rows == inner_expected_rows1
        assert inner_tab2.rows == inner_expected_rows2

        # now test complex schema with embedded array of a simple field
        schema = [StructField("nums", ArrayType(IntegerType()))]
        rows = [Row(num=[1,2]),Row(num=[3,4])]
        tab = DataFrameTable(schema, rows)

        expected_cols = [
            {"col_index": 0, "name": "nums", "type": "ArrayType", "field_type": schema[0].dataType},
        ]
        assert tab.columns == expected_cols
        assert tab.column_names == ["nums"]
        expected_rows = [
            {"row_index": 0, "row": [
                {"column": expected_cols[0], "kind": "array", "value": inner_expected_rows1, "display_value": str([1,2])},
            ]},
            {"row_index": 1, "row": [
                {"column": expected_cols[0], "kind": "array", "value": inner_expected_rows2, "display_value": str([3,4])},
            ]},
        ]
        assert tab.rows == expected_rows
        assert tab.row_values == [[str([1,2])],[str([3,4])]]


    def test_embedded_struct_field(self) -> None:
        # first test internal fields (struct fields)
        inner_row1, inner_expected_row1 = TestDataTable.__prepare_multiple_simple_fields_row__([11, "some text 1", "2024-02-01"], 0)
        inner_row2, inner_expected_row2 = TestDataTable.__prepare_multiple_simple_fields_row__([13, "some text 3", "2024-02-03"], 0)
        inner_schema, inner_expected_cols = TestDataTable.__prepare_multiple_simple_fields_schema__()

        inner_tab1 = DataFrameTable(inner_schema, [inner_row1])
        inner_tab2 = DataFrameTable(inner_schema, [inner_row2])

        assert inner_tab1.columns == inner_expected_cols
        assert inner_tab2.columns == inner_expected_cols
        assert inner_tab1.rows == [inner_expected_row1]
        assert inner_tab2.rows == [inner_expected_row2]

        # now test complex schema with embedded struct field
        schema = [StructField("id", IntegerType()), StructField("struct", StructType(inner_schema))]
        rows = [Row(id=1, struct=inner_row1), Row(id=2, struct=inner_row2)]
        tab = DataFrameTable(schema, rows)

        expected_cols = [
            {"col_index": 0, "name": "id", "type": "IntegerType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "struct", "type": "StructType", "field_type": schema[1].dataType}
        ]

        assert tab.columns == expected_cols
        assert tab.column_names == ["id", "struct"]

        expected_rows = [
            {"row_index": 0, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": 1, "display_value": "1"},
                {"column": expected_cols[1], "kind": "struct", "value": inner_expected_row1, "display_value": str(inner_row1)},
            ]},
            {"row_index": 1, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": 2, "display_value": "2"},
                {"column": expected_cols[1], "kind": "struct", "value": inner_expected_row2, "display_value": str(inner_row2)},
            ]},
        ]

        assert tab.rows == expected_rows
        assert tab.row_values == [["1", str(inner_row1)[:DataFrameTable.TEXT_LEN]],["2", str(inner_row2)[:DataFrameTable.TEXT_LEN]]]


    def __array_to_row__(self, schema:[StructField], arr: []) -> [Row]:
        field_names = list(map(lambda f: f.name, schema))
        res_rows: [Row] = []
        for elem in arr:
            pairs = zip(field_names, elem)
            row_to_add = Row(**dict(pairs))
            res_rows.append(row_to_add)

        return res_rows

    def test_array_of_struct_field(self) -> None:
        # first test internal fields (struct fields)
        input_rows1 = [[11, "some text 1", "2024-02-01"], [12, "some text 2", "2024-02-02"]]
        input_rows2 = [[13, "some text 3", "2024-02-03"], [14, "some text 4", "2024-02-04"]]
        inner_schema1, inner_rows1, inner_expected_cols1, inner_expected_rows1 = TestDataTable.__prepare_multiple_simple_fields__(
            input_rows1[0], input_rows1[1])
        inner_schema2, inner_rows2, inner_expected_cols2, inner_expected_rows2 = TestDataTable.__prepare_multiple_simple_fields__(
            input_rows2[0], input_rows2[1])
        inner_tab1 = DataFrameTable(inner_schema1, inner_rows1)
        inner_tab2 = DataFrameTable(inner_schema2, inner_rows2)
        assert inner_tab1.columns == inner_expected_cols1
        assert inner_tab2.columns == inner_expected_cols2
        assert inner_tab1.rows == inner_expected_rows1
        assert inner_tab2.rows == inner_expected_rows2

        # then embed the struct into single field (2 separate tabs, each for one row in the final table)
        inner_embedded_schema1 = [StructField("structs",StructType(inner_schema1))]
        inner_embedded_schema2 = [StructField("structs",StructType(inner_schema2))]
        inner_embedded_rows1 = [Row(structs=inner_rows1[0]),Row(structs=inner_rows1[1])]
        inner_embedded_rows2 = [Row(structs=inner_rows2[0]),Row(structs=inner_rows2[1])]
        inner_embedded_tab1 = DataFrameTable(inner_embedded_schema1, inner_embedded_rows1)
        inner_embedded_tab2 = DataFrameTable(inner_embedded_schema2, inner_embedded_rows2)
        inner_embedded_expected_cols1 = [
            {"col_index": 0, "name": "structs", "type": "StructType", "field_type": inner_embedded_schema1[0].dataType}
        ]
        inner_embedded_expected_cols2 = [
            {"col_index": 0, "name": "structs", "type": "StructType", "field_type": inner_embedded_schema2[0].dataType}
        ]

        assert inner_embedded_tab1.columns == inner_embedded_expected_cols1
        assert inner_embedded_tab2.columns == inner_embedded_expected_cols2

        inner_expected_rows1_upd = inner_expected_rows1[1]
        inner_expected_rows1_upd["row_index"]=0
        inner_embedded_expected_rows1 = [
            {"row_index": 0, "row": [
                {"column": inner_embedded_expected_cols1[0], "kind": "struct", "value": inner_expected_rows1[0], "display_value": str(inner_rows1[0])}
            ]},
            {"row_index": 1, "row": [
                {"column": inner_embedded_expected_cols1[0], "kind": "struct", "value": inner_expected_rows1_upd, "display_value": str(inner_rows1[1])}
            ]}]
        inner_expected_rows2_upd = inner_expected_rows2[1]
        inner_expected_rows2_upd["row_index"]=0
        inner_embedded_expected_rows2 = [
            {"row_index": 0, "row": [
                {"column": inner_embedded_expected_cols2[0], "kind": "struct", "value": inner_expected_rows2[0], "display_value": str(inner_rows2[0])}
            ]},
            {"row_index": 1, "row": [
                {"column": inner_embedded_expected_cols2[0], "kind": "struct", "value": inner_expected_rows2_upd, "display_value": str(inner_rows2[1])}
            ]}]

        assert inner_embedded_tab1.rows == inner_embedded_expected_rows1
        assert inner_embedded_tab2.rows == inner_embedded_expected_rows2

        # now test complex schema with embedded array of struct field
        inner_rows1_as_rows = self.__array_to_row__(inner_schema1, inner_rows1)
        inner_rows2_as_rows = self.__array_to_row__(inner_schema2, inner_rows2)
        schema = [StructField("id", IntegerType()), StructField("structs", ArrayType(StructType(inner_schema1)))]
        rows = [Row(id=1, structs=inner_rows1_as_rows), Row(id=2, structs=inner_rows2_as_rows)]
        tab = DataFrameTable(schema, rows)

        expected_cols = [
            {"col_index": 0, "name": "id", "type": "IntegerType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "structs", "type": "ArrayType", "field_type": schema[1].dataType}
        ]
        assert tab.columns == expected_cols
        assert tab.column_names == ["id", "structs"]

        expected_rows = [
            {"row_index": 0, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": 1, "display_value": "1"},
                {"column": expected_cols[1], "kind": "array", "value": inner_embedded_expected_rows1, "display_value": str(inner_rows1_as_rows)},
            ]},
            {"row_index": 1, "row": [
                {"column": expected_cols[0], "kind": "simple", "value": 2, "display_value": "2"},
                {"column": expected_cols[1], "kind": "array", "value": inner_embedded_expected_rows2, "display_value": str(inner_rows2_as_rows)},
            ]},
        ]

        assert tab.rows == expected_rows
        assert tab.row_values == [["1",str(inner_rows1_as_rows)[:DataFrameTable.TEXT_LEN]],["2",str(inner_rows2_as_rows)[:DataFrameTable.TEXT_LEN]]]

        # now drill down to details and make sure the results are the same
        extracted_tab1 = extract_embedded_table(tab, 1, 0)
        assert extracted_tab1 is not None
        # this is the same as expected_cols[1] but with index 0
        assert extracted_tab1.columns == [{"col_index": 0, "name": "structs", "type": "ArrayType", "field_type": schema[1].dataType}]
        assert extracted_tab1.rows == inner_embedded_expected_rows1

        extracted_tab2 = extract_embedded_table(extracted_tab1, 0, 0)
        assert extracted_tab2 is not None
        assert extracted_tab2.columns == inner_expected_cols1
        assert extracted_tab2.rows == [inner_expected_rows1[0]]
