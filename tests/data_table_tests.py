from pyspark.sql.types import StructType, StructField, StringType, Row, DateType, LongType, IntegerType, ArrayType

from pyspark_explorer.data_table import DataFrameTable, extract_embedded_table


class TestDataTable:
    @staticmethod
    def __prepare_simple_text_field__(val1: str, val2: str) -> ([StructField], [Row], {}, {}):
        schema = [StructField("text", StringType())]
        rows = [Row(text=val1), Row(text=val2)]
        expected_cols = [{"col_index": 0, "name": "text", "kind": "simple", "type": "StringType", "field_type": schema[0].dataType}]
        expected_rows = [
            {"row_index": 0, "row": [{"value": val1, "display_value": val1}]},
            {"row_index": 1, "row": [{"value": val2, "display_value": val2}]},
        ]
        return schema, rows, expected_cols, expected_rows

    @staticmethod
    def __prepare_simple_num_field__(val1: int, val2: int) -> ([StructField], [Row], {}, {}):
        schema = [StructField("nums", IntegerType())]
        rows = [Row(nums=val1),Row(nums=val2)]
        expected_cols = [
            {"col_index": 0, "name": "nums", "kind": "simple", "type": "IntegerType", "field_type": schema[0].dataType},
        ]
        expected_rows = [
            {"row_index": 0, "row": [{"value": val1, "display_value": DataFrameTable.disp_value(val1)}]},
            {"row_index": 1, "row": [{"value": val2, "display_value": DataFrameTable.disp_value(val2)}]},
        ]
        return schema, rows, expected_cols, expected_rows


    @staticmethod
    def __prepare_multiple_simple_fields_schema__() -> ([StructField], []):
        schema = [StructField("id", LongType()), StructField("text", StringType()), StructField("date", DateType())]
        expected_cols = [
            {"col_index": 0, "name": "id", "kind": "simple", "type": "LongType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "text", "kind": "simple", "type": "StringType", "field_type": schema[1].dataType},
            {"col_index": 2, "name": "date", "kind": "simple", "type": "DateType", "field_type": schema[2].dataType}
        ]
        return schema, expected_cols


    @staticmethod
    def __prepare_multiple_simple_fields_row__(val: [], index: int) -> (Row, {}):
        row = Row(id=val[0], text=val[1], date=val[2])
        _, expected_cols = TestDataTable.__prepare_multiple_simple_fields_schema__()
        expected_row = {"row_index": index, "row": [
            {"value": val[0], "display_value": DataFrameTable.disp_value(val[0])},
            {"value": val[1], "display_value": val[1]},
            {"value": val[2], "display_value": val[2]}
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
            {"col_index": 0, "name": "nums", "kind": "array", "type": "ArrayType", "field_type": schema[0].dataType.elementType},
        ]
        assert tab.columns == expected_cols
        assert tab.column_names == ["nums"]
        expected_rows = [
            {"row_index": 0, "row": [{"value": inner_expected_rows1, "display_value": DataFrameTable.disp_value([1,2])}]},
            {"row_index": 1, "row": [{"value": inner_expected_rows2, "display_value": DataFrameTable.disp_value([3,4])}]},
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
        schema = [StructField("row_id", IntegerType()), StructField("struct", StructType(inner_schema))]
        rows = [Row(id=1, struct=inner_row1), Row(id=2, struct=inner_row2)]
        tab = DataFrameTable(schema, rows)

        expected_cols = [
            {"col_index": 0, "name": "row_id", "kind": "simple", "type": "IntegerType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "struct", "kind": "struct", "type": "StructType", "field_type": schema[1].dataType}
        ]

        assert tab.columns == expected_cols
        assert tab.column_names == ["row_id", "struct"]

        expected_rows = [
            {"row_index": 0, "row": [
                {"value": 1, "display_value": "1"},
                {"value": inner_expected_row1, "display_value": DataFrameTable.disp_value(inner_row1)},
            ]},
            {"row_index": 1, "row": [
                {"value": 2, "display_value": "2"},
                {"value": inner_expected_row2, "display_value": DataFrameTable.disp_value(inner_row2)},
            ]},
        ]

        assert tab.rows == expected_rows
        assert tab.row_values == [
            ["1", DataFrameTable.disp_value(inner_row1)[:DataFrameTable.TEXT_LEN]],
            ["2", DataFrameTable.disp_value(inner_row2)[:DataFrameTable.TEXT_LEN]]]

        # now drill down to details and make sure the results are the same
        extracted_tab1 = extract_embedded_table(tab, 1, 0)
        assert extracted_tab1.columns == inner_expected_cols
        assert extracted_tab1.rows == [inner_expected_row1]

    @staticmethod
    def __array_to_row__(schema:[StructField], arr: []) -> [Row]:
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
            {"col_index": 0, "name": "structs", "kind": "struct", "type": "StructType", "field_type": inner_embedded_schema1[0].dataType}
        ]
        inner_embedded_expected_cols2 = [
            {"col_index": 0, "name": "structs", "kind": "struct", "type": "StructType", "field_type": inner_embedded_schema2[0].dataType}
        ]

        assert inner_embedded_tab1.columns == inner_embedded_expected_cols1
        assert inner_embedded_tab2.columns == inner_embedded_expected_cols2

        inner_expected_rows1_upd = inner_expected_rows1[1]
        inner_expected_rows1_upd["row_index"]=0
        inner_embedded_expected_rows1 = [
            {"row_index": 0, "row": [{"value": inner_expected_rows1[0], "display_value": DataFrameTable.disp_value(inner_rows1[0])}]},
            {"row_index": 1, "row": [{"value": inner_expected_rows1_upd, "display_value": DataFrameTable.disp_value(inner_rows1[1])}]}]
        inner_expected_rows2_upd = inner_expected_rows2[1]
        inner_expected_rows2_upd["row_index"]=0
        inner_embedded_expected_rows2 = [
            {"row_index": 0, "row": [{"value": inner_expected_rows2[0], "display_value": DataFrameTable.disp_value(inner_rows2[0])}]},
            {"row_index": 1, "row": [{"value": inner_expected_rows2_upd, "display_value": DataFrameTable.disp_value(inner_rows2[1])}]}]

        assert inner_embedded_tab1.rows == inner_embedded_expected_rows1
        assert inner_embedded_tab2.rows == inner_embedded_expected_rows2

        # now test complex schema with embedded array of struct field
        inner_rows1_as_rows = self.__array_to_row__(inner_schema1, inner_rows1)
        inner_rows2_as_rows = self.__array_to_row__(inner_schema2, inner_rows2)
        schema = [StructField("id", IntegerType()), StructField("structs", ArrayType(StructType(inner_schema1)))]
        rows = [Row(id=1, structs=inner_rows1_as_rows), Row(id=2, structs=inner_rows2_as_rows)]
        tab = DataFrameTable(schema, rows)

        expected_cols = [
            {"col_index": 0, "name": "id", "kind": "simple", "type": "IntegerType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "structs", "kind": "array", "type": "ArrayType", "field_type": schema[1].dataType.elementType}
        ]
        assert tab.columns == expected_cols
        assert tab.column_names == ["id", "structs"]

        expected_rows = [
            {"row_index": 0, "row": [
                {"value": 1, "display_value": "1"},
                {"value": inner_embedded_expected_rows1, "display_value": DataFrameTable.disp_value(inner_rows1_as_rows)},
            ]},
            {"row_index": 1, "row": [
                {"value": 2, "display_value": "2"},
                {"value": inner_embedded_expected_rows2, "display_value": DataFrameTable.disp_value(inner_rows2_as_rows)},
            ]},
        ]

        assert tab.rows == expected_rows
        assert tab.row_values == [
            ["1",DataFrameTable.disp_value(inner_rows1_as_rows)[:DataFrameTable.TEXT_LEN]],
            ["2",DataFrameTable.disp_value(inner_rows2_as_rows)[:DataFrameTable.TEXT_LEN]]]

        # now drill down to details and make sure the results are the same
        extracted_tab1 = extract_embedded_table(tab, 1, 0)
        assert extracted_tab1 is not None
        # this is the same as expected_cols[1] but with index 0
        assert extracted_tab1.columns == [{"col_index": 0, "name": "structs", "kind": "struct", "type": "StructType", "field_type": schema[1].dataType.elementType}]
        assert extracted_tab1.rows == inner_embedded_expected_rows1
        assert extracted_tab1.column_names == ["structs"]
        assert extracted_tab1.row_values == [
            [DataFrameTable.disp_value(inner_rows1[0])[:DataFrameTable.TEXT_LEN]],
            [DataFrameTable.disp_value(inner_rows1[1])[:DataFrameTable.TEXT_LEN]]]

        extracted_tab2 = extract_embedded_table(extracted_tab1, 0, 0)
        assert extracted_tab2 is not None
        assert extracted_tab2.columns == inner_expected_cols1
        assert extracted_tab2.rows == [inner_expected_rows1[0]]
        assert extracted_tab2.column_names == ["id", "text", "date"]
        # this is an input row but mapped to strings
        assert extracted_tab2.row_values == [['11', 'some text 1', '2024-02-01']]


    def test_struct_field_expansion(self) -> None:
        # first test internal fields (struct fields)
        inner_row1, inner_expected_row1 = TestDataTable.__prepare_multiple_simple_fields_row__([11, "some text 1", "2024-02-01"], 0)
        inner_row2, inner_expected_row2 = TestDataTable.__prepare_multiple_simple_fields_row__([13, "some text 3", "2024-02-03"], 0)
        inner_schema, inner_expected_cols = TestDataTable.__prepare_multiple_simple_fields_schema__()

        schema = [StructField("row_id", IntegerType()), StructField("struct", StructType(inner_schema))]
        rows = [Row(id=1, struct=inner_row1), Row(id=2, struct=inner_row2)]
        tab = DataFrameTable(schema, data = rows, expand_structs = True)

        expected_cols = [
            {"col_index": 0, "name": "row_id", "kind": "simple", "type": "IntegerType", "field_type": schema[0].dataType},
            {"col_index": 1, "name": "struct", "kind": "struct", "type": "StructType", "field_type": schema[1].dataType},
            {"col_index": 2, "name": "*id", "kind": "simple", "type": "LongType", "field_type": inner_schema[0].dataType},
            {"col_index": 3, "name": "*text", "kind": "simple", "type": "StringType", "field_type": inner_schema[1].dataType},
            {"col_index": 4, "name": "*date", "kind": "simple", "type": "DateType", "field_type": inner_schema[2].dataType}
        ]

        assert tab.columns == expected_cols
        assert tab.column_names == ["row_id", "struct", "*id", "*text", "*date"]

        expected_rows = [
            {"row_index": 0, "row": [
                {"value": 1, "display_value": "1"},
                {"value": inner_expected_row1, "display_value": DataFrameTable.disp_value(inner_row1)},
                {"value": inner_expected_row1["row"][0]["value"], "display_value": DataFrameTable.disp_value(inner_expected_row1["row"][0]["value"])},
                {"value": inner_expected_row1["row"][1]["value"], "display_value": DataFrameTable.disp_value(inner_expected_row1["row"][1]["value"])},
                {"value": inner_expected_row1["row"][2]["value"], "display_value": DataFrameTable.disp_value(inner_expected_row1["row"][2]["value"])},
            ]},
            {"row_index": 1, "row": [
                {"value": 2, "display_value": "2"},
                {"value": inner_expected_row2, "display_value": DataFrameTable.disp_value(inner_row2)},
                {"value": inner_expected_row2["row"][0]["value"],"display_value": DataFrameTable.disp_value(inner_expected_row2["row"][0]["value"])},
                {"value": inner_expected_row2["row"][1]["value"],"display_value": DataFrameTable.disp_value(inner_expected_row2["row"][1]["value"])},
                {"value": inner_expected_row2["row"][2]["value"],"display_value": DataFrameTable.disp_value(inner_expected_row2["row"][2]["value"])},
            ]},
        ]

        assert tab.rows == expected_rows
        assert tab.row_values == [
            ["1", DataFrameTable.disp_value(inner_row1)[:DataFrameTable.TEXT_LEN], *[str(v["value"]) for v in inner_expected_row1["row"]]],
            ["2", DataFrameTable.disp_value(inner_row2)[:DataFrameTable.TEXT_LEN], *[str(v["value"]) for v in inner_expected_row2["row"]]]]


    @staticmethod
    def __double_embedded_struct__() -> [StructField]:
        return [
            StructField("row_id1", IntegerType()),
            StructField("struct1", ArrayType(StructType([
                StructField("row_id2", IntegerType()),
                StructField("struct2", ArrayType(StructType([
                    StructField("row_id3", IntegerType()),
                    StructField("row_value3", IntegerType())
                ])))])))]


    def test_embedded_array_of_struct_field_expansion(self) -> None:
        # this test is quite long but this form should be readable enough
        schema = self.__double_embedded_struct__()
        rows = [Row(
            row_id1=1,
            struct1 = [Row(
                row_id2=11,
                struct2 = [Row(
                    row_id3=111, row_value3=911
                )])])]

        tab = DataFrameTable(schema, data = rows, expand_structs = True)

        expected_cols = [
            {'col_index': 0, 'name': 'row_id1', 'kind': 'simple', 'type': 'IntegerType', 'field_type': IntegerType()},
            {'col_index': 1, 'name': 'struct1', 'kind': 'array', 'type': 'ArrayType', 'field_type': schema[1].dataType.elementType}
        ]

        expected_rows = [
            {"row_index": 0, "row": [
                {"display_value": "1", "value": 1},
                {"display_value": DataFrameTable.disp_value(rows[0].struct1), "value": [
                    {"row_index": 0, "row": [
                        {"display_value": DataFrameTable.disp_value(rows[0].struct1[0]), "value":
                            {"row_index": 0, "row": [
                                {"display_value": "11", "value": 11},
                                {"display_value": DataFrameTable.disp_value(rows[0].struct1[0].struct2), "value": [
                                    {"row_index": 0, "row": [
                                        {"display_value": DataFrameTable.disp_value(rows[0].struct1[0].struct2[0]), "value":
                                            {"row_index": 0, "row": [
                                                {"display_value": "111", "value": 111},
                                                {"display_value": "911", "value": 911}
                                            ]}
                                        }]
                                     }]
                                }]
                            }
                         }]
                    }]
                }]
             }]

        assert tab.columns==expected_cols
        assert tab.rows == expected_rows

        # now extract embedded table for struct1
        tab2 = extract_embedded_table(tab, 1,0,expand_structs = True)

        expected_cols2 = [
            {'col_index': 0, 'name': 'struct1', 'kind': 'struct', 'type': 'StructType', 'field_type': schema[1].dataType.elementType},
            {'col_index': 1, 'name': '*row_id2', 'kind': 'simple', 'type': 'IntegerType', 'field_type': IntegerType()},
            {'col_index': 2, 'name': '*struct2', 'kind': 'array', 'type': 'ArrayType',
             # here we drill down from array to array contents (elementType)
             'field_type': schema[1].dataType.elementType[1].dataType.elementType}
        ]

        assert tab2.columns==expected_cols2

        expected_rows2 = [
            {"row_index": 0, "row": [
                {"display_value": DataFrameTable.disp_value(rows[0].struct1[0]), "value":
                    {"row_index": 0, "row": [
                        {"display_value": "11", "value": 11},
                        {"display_value": DataFrameTable.disp_value(rows[0].struct1[0].struct2), "value": [
                            {"row_index": 0, "row": [
                                {"display_value": DataFrameTable.disp_value(rows[0].struct1[0].struct2[0]), "value":
                                    {"row_index": 0, "row": [
                                        {"display_value": "111", "value": 111},
                                        {"display_value": "911", "value": 911}
                                    ]}
                                 }]
                             }]
                         }]
                     }
                 },
                {"display_value": "11", "value": 11},
                {"display_value": DataFrameTable.disp_value(rows[0].struct1[0].struct2), "value": [
                    {"row_index": 0, "row": [
                        {"display_value": "row_id3=111, row_value3=911", "value":
                            {"row_index": 0, "row": [
                                {"display_value": "111", "value": 111},
                                {"display_value": "911", "value": 911}
                            ]}
                         }
                    ]}
                ]}]
            }]

        assert tab2.rows==expected_rows2

        # now extract embedded table for struct2
        tab3 = extract_embedded_table(tab2, 0,0,expand_structs = True)

        expected_cols3 = [
            {'col_index': 0, 'name': 'row_id2', 'kind': 'simple', 'type': 'IntegerType', 'field_type': IntegerType()},
            {'col_index': 1, 'name': 'struct2', 'kind': 'array', 'type': 'ArrayType',
             # here we drill down from array to array contents (elementType)
             'field_type': schema[1].dataType.elementType[1].dataType.elementType}
        ]

        assert tab3.columns==expected_cols3

        expected_rows3 = [
            {"row_index": 0, "row": [
                {"display_value": "11", "value": 11},
                {"display_value": DataFrameTable.disp_value(rows[0].struct1[0].struct2), "value": [
                    {"row_index": 0, "row": [
                        {"display_value": DataFrameTable.disp_value(rows[0].struct1[0].struct2[0]), "value":
                            {"row_index": 0, "row": [
                                {"display_value": "111", "value": 111},
                                {"display_value": "911", "value": 911}
                            ]}
                         },
                    ]}
                ]}
            ]}
        ]

        assert tab3.rows==expected_rows3

    def test_generate_structure_as_tree_with_embedded_array(self) -> None:
        schema = self.__double_embedded_struct__()

        tab = DataFrameTable(schema = schema, data=[], expand_structs = False)

        expected_tree = [
            {"name": "row_id1", "kind": "simple", "type": "IntegerType", "subfields": []},
            {"name": "struct1", "kind": "array", "type": "ArrayType", "subfields": [
                {"name": "row_id2", "kind": "simple", "type": "IntegerType", "subfields": []},
                {"name": "struct2", "kind": "array", "type": "ArrayType", "subfields": [
                    {"name": "row_id3", "kind": "simple", "type": "IntegerType", "subfields": []},
                    {"name": "row_value3", "kind": "simple", "type": "IntegerType", "subfields": []}
                ]}
            ]}
        ]

        assert tab.schema_tree == expected_tree


    def test_generate_structure_as_tree_with_embedded_struct(self) -> None:
        schema = [
            StructField("row_id1", IntegerType()),
            StructField("struct1", ArrayType(StructType([
                StructField("row_id2", IntegerType()),
                StructField("struct2", StructType([
                    StructField("row_id3", IntegerType()),
                    StructField("row_value3", IntegerType())
                ]))])))]

        tab = DataFrameTable(schema = schema, data=[], expand_structs = False)

        expected_tree = [
            {"name": "row_id1", "kind": "simple", "type": "IntegerType", "subfields": []},
            {"name": "struct1", "kind": "array", "type": "ArrayType", "subfields": [
                {"name": "row_id2", "kind": "simple", "type": "IntegerType", "subfields": []},
                {"name": "struct2", "kind": "struct", "type": "StructType", "subfields": [
                    {"name": "row_id3", "kind": "simple", "type": "IntegerType", "subfields": []},
                    {"name": "row_value3", "kind": "simple", "type": "IntegerType", "subfields": []}
                ]}
            ]}
        ]

        assert tab.schema_tree == expected_tree
