from pyspark.sql.types import StructField, Row, StructType


class DataFrameTable:
    TEXT_LEN = 15

    def __init__(self, schema: [StructField], data: [Row]):
        self._schema: [StructField] = schema
        self._data: [Row] = data

        self.columns = self.__extract_columns__()
        self.rows = self.__extract_rows__()

        self.column_names = self.__extract_column_names__()
        self.row_values = self.__extract_row_values__()


    def __extract_columns__(self) -> []:
        cols = []
        for i,field in enumerate(self._schema):
            cols.append({"col_index": i, "name": field.name, "type": type(field.dataType).__name__, "field_type": field.dataType})

        return cols


    def __extract_column_names__(self) -> [str]:
        return [c["name"] for c in self.columns]


    def __extract_rows__(self) -> []:
        assert len(self.columns) > 0  # ensure columns are calculated BEFORE rows

        rows = []
        for ri,data_row in enumerate(self._data):
            row=[]
            for fi, field in enumerate(data_row.__fields__):
                if self.columns[fi]["type"] == "ArrayType":
                    # create internal schema as a single field
                    column = StructField(self.columns[fi]["name"], self.columns[fi]["field_type"].elementType)
                    # specify row schema in a form of name = value
                    values_as_row = map(lambda r: Row(**{self.columns[fi]["name"] : r}), data_row[field])
                    value = DataFrameTable([column], values_as_row).rows
                    display_value = str(data_row[field])
                    kind = "array"
                elif self.columns[fi]["type"] == "StructType":
                    # extract internal schema as an array of fields
                    inner_schema = self.columns[fi]["field_type"].fields
                    # a value is just a single Row, so we must pack it as an array and then unpack it
                    value = DataFrameTable(inner_schema, [data_row[field]]).rows[0]
                    display_value = str(data_row[field])
                    kind = "struct"
                else:
                    value = data_row[field]
                    display_value = str(value)
                    kind = "simple"

                row.append({"column": self.columns[fi], "kind": kind, "display_value": display_value,
                            "value": value})

            rows.append({"row_index": ri, "row": row})

        return rows


    def __extract_row_values__(self) -> [[str]]:
        # maybe it is not very readable but still it's one-liner
        return [[c["display_value"][:DataFrameTable.TEXT_LEN] for c in r["row"]] for r in self.rows]


    def select(self, x: int, y: int) -> {}:
        return self.rows[y]["row"][x]





def extract_embedded_table(tab: DataFrameTable, x: int, y: int) -> DataFrameTable | None:
    cell = tab.select(x,y)
    kind = cell["kind"]
    if kind=="array":
        columns = [StructField(cell["column"]["name"], cell["column"]["field_type"])]
        new_tab = DataFrameTable(columns, [])
        rows = cell["value"]
        new_tab.rows = rows
        return new_tab
    elif kind == "struct":
        columns = cell["column"]["field_type"].fields
        new_tab = DataFrameTable(columns, [])
        rows = cell["value"]
        print(rows)
        new_tab.rows = [rows]
        return new_tab

    return None
