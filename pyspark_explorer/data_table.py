from typing import Any

from pyspark.sql.types import StructField, Row, StructType, ArrayType, DataType
import copy


class DataFrameTable:
    TEXT_LEN = 50
    TAKE_ROWS = 100

    # allow original rows (Row type) or previously transformed rows (when drilling to details)
    def __init__(self, schema: [StructField], data=None, transformed_data=None, expand_structs: bool = False):
        if transformed_data is None:
            transformed_data = []
        if data is None:
            data = []
        self._schema: [StructField] = schema
        self._data: [Row] = data
        self._expand_structs: bool = expand_structs

        self.columns = []
        self.column_names = []
        self.schema_tree = []
        self.rows = []
        self.row_values = []
        self.__extract_columns__()

        if len(data):
            self.__extract_rows__()
        else:
            self.__set_rows__(transformed_data)

        if self._expand_structs:
            self.__expand_structs__()


    @staticmethod
    def __extract_kind__(field: StructField) -> str:
        if type(field.dataType) == StructType:
            kind = "struct"
        elif type(field.dataType) == ArrayType:
            kind = "array"
        else:
            kind = "simple"
        return kind


    def __expand_structs__(self):
        new_cols = [] # NOTE: we cannot modify self.columns on the fly in the loop below, we would modify the loop
        new_rows = copy.deepcopy(self.rows)
        col_index = 0
        existing_columns = copy.deepcopy(self.columns)
        for ci, col in enumerate(existing_columns):
            col["col_index"] = col_index
            new_cols.append(col)
            col_index += 1
            if col["kind"] == "struct":
                for fi,field in enumerate(col["field_type"].fields):
                    kind = self.__extract_kind__(field)
                    field_type = self.__extract_type__(field)
                    new_col = {"col_index": col_index, "name": f"*{field.name}", "kind": kind, "type": type(field.dataType).__name__, "field_type": field_type}
                    new_cols.append(new_col)

                    for row in new_rows:
                        struct_value = row["row"][ci]["value"]
                        row["row"].insert(col_index, struct_value["row"][fi])

                    col_index += 1

        self.columns = new_cols
        self.rows = new_rows
        self.__extract_column_names__()
        self.__extract_row_values__()


    @staticmethod
    def __extract_type__(field) -> StructType:
        # extract inner type from ArrayType, return field type otherwise
        if type(field.dataType) == ArrayType:
            return field.dataType.elementType

        return field.dataType


    @staticmethod
    def __extract_embedded_schema_tree__(fields: [StructField]) -> []:
        subfields = []
        for f in fields:
            subfield = {"name": f.name, "kind": DataFrameTable.__extract_kind__(f), "type": type(f.dataType).__name__}
            if subfield["kind"] == "array":
                embedded_subfields = DataFrameTable.__extract_embedded_array_to_tree__(f.dataType)
            elif subfield["kind"] == "struct":
                embedded_subfields = DataFrameTable.__extract_embedded_schema_tree__(f.dataType.fields)
            else:
                embedded_subfields = []
            subfield["subfields"] = embedded_subfields
            subfields.append(subfield)
        return subfields


    @staticmethod
    def __extract_embedded_array_to_tree__(subfield: DataType) -> []:
        if type(subfield) == StructType:
            subfields = DataFrameTable.__extract_embedded_schema_tree__(subfield.fields)
        elif type(subfield) == ArrayType:
            subfields = DataFrameTable.__extract_embedded_array_to_tree__(subfield.elementType)
        else:
            subfields = [{"name": "", "kind": "simple", "type": type(subfield).__name__, "subfields": []}]

        return subfields

    def __extract_schema_tree__(self) -> None:
        tree = []
        for col in self.columns:
            tree_field = {"name": col["name"], "kind": col["kind"], "type": col["type"]}
            if col["kind"] == "array":
                subfields = self.__extract_embedded_array_to_tree__(col["field_type"])
            elif col["kind"] == "struct":
                subfields = self.__extract_embedded_schema_tree__(col["field_type"])
            else:
                subfields = []
            tree_field["subfields"] = subfields
            tree.append(tree_field)

        self.schema_tree = tree


    def __extract_columns__(self) -> None:
        cols = []
        for i,field in enumerate(self._schema):
            field_type = self.__extract_type__(field)

            kind = self.__extract_kind__(field)
            cols.append({"col_index": i, "name": field.name, "kind": kind, "type": type(field.dataType).__name__, "field_type": field_type})

        self.columns = cols
        self.__extract_column_names__()
        self.__extract_schema_tree__()


    def __extract_column_names__(self) -> None:
        self.column_names = [c["name"] for c in self.columns]


    def __extract_rows__(self) -> None:
        assert len(self.columns) > 0  # ensure columns are calculated BEFORE rows

        rows = []
        for ri,data_row in enumerate(self._data):
            row=[]
            for fi, field in enumerate(data_row.__fields__):
                if self.columns[fi]["kind"] == "array":
                    # create internal schema as a single field
                    column = StructField(self.columns[fi]["name"], self.columns[fi]["field_type"])
                    # specify row schema in a form of name = value
                    values_as_row = list(map(lambda r: Row(**{self.columns[fi]["name"] : r}), data_row[field]))
                    value = DataFrameTable([column], values_as_row).rows
                    display_value = self.disp_value(data_row[field])
                elif self.columns[fi]["kind"] == "struct":
                    # extract internal schema as an array of fields
                    inner_schema = self.columns[fi]["field_type"].fields
                    # a value is just a single Row, so we must pack it as an array and then unpack it
                    value = DataFrameTable(inner_schema, [data_row[field]]).rows[0]
                    display_value = self.disp_value(data_row[field])
                else:
                    value = data_row[field]
                    display_value = self.disp_value(value)

                row.append({"display_value": display_value, "value": value})

            rows.append({"row_index": ri, "row": row})

        self.__set_rows__(rows)


    def __set_rows__(self, rows: []) -> None:
        self.rows = rows
        self.__extract_row_values__()


    def __extract_row_values__(self) -> None:
        # maybe it is not very readable but still it's one-liner
        self.row_values = [[c["display_value"][:DataFrameTable.TEXT_LEN] for c in r["row"]] for r in self.rows]


    def select(self, x: int, y: int) -> ({}, {}):
        return self.columns[x], self.rows[y]["row"][x]


    @staticmethod
    def disp_value(value: Any) -> str:
        disp_value = str(value)
        # remove single "Row(...)"
        disp_value = disp_value[4:-1] if disp_value.startswith("Row(") and disp_value.endswith(")") else disp_value
        # remove "Row" from array "[Row(...), ... , Row(...)]"
        disp_value = disp_value[4:-1].replace("), Row(", "), (") if disp_value.startswith("[Row(") and disp_value.endswith(")]") else disp_value
        return disp_value


def extract_embedded_table(tab: DataFrameTable, x: int, y: int, expand_structs: bool = False) -> DataFrameTable | None:
    # check for kind=array but field_type=StructType - this means that we want to drill down from array to structs
    column, cell = tab.select(x,y)
    kind = column["kind"]
    if kind == "struct": # or type(column["field_type"]) == StructType:
        columns = copy.deepcopy(column["field_type"].fields)
        rows = copy.deepcopy([cell["value"]])
        new_tab = DataFrameTable(columns, data= [], transformed_data=rows, expand_structs= expand_structs)
        return new_tab

    # other case for array
    if kind=="array":
        col_name = column["name"][1:] if column["name"].startswith("*") else column["name"]
        columns = copy.deepcopy([StructField(col_name, column["field_type"])])
        rows = copy.deepcopy(cell["value"])
        new_tab = DataFrameTable(columns, data= [], transformed_data=rows, expand_structs= expand_structs)
        return new_tab

    return None
