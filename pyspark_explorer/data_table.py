from pyspark.sql.types import StructField, Row, StructType, ArrayType
from pyspark.sql.types import StructField, Row, StructType, ArrayType


class DataFrameTable:
    TEXT_LEN = 50
    TAKE_ROWS = 100

    def __init__(self, schema: [StructField], data: [Row]):
        self._schema: [StructField] = schema
        self._data: [Row] = data

        self.columns = []
        self.column_names = []
        self.rows = []
        self.row_values = []
        self.__extract_columns__()
        self.__extract_rows__()


    def __extract_columns__(self) -> None:
        cols = []
        for i,field in enumerate(self._schema):
            if type(field.dataType) == ArrayType:
                field_type = field.dataType.elementType
            else:
                field_type = field.dataType

            if type(field.dataType) == StructType:
                kind = "struct"
            elif type(field.dataType) == ArrayType:
                kind = "array"
            else:
                kind = "simple"
            cols.append({"col_index": i, "name": field.name, "kind": kind, "type": type(field.dataType).__name__, "field_type": field_type})

        self.columns = cols
        self.__extract_column_names__()


    def __extract_column_names__(self) -> None:
        self.column_names = [c["name"] for c in self.columns]


    def __extract_rows__(self) -> None:
        assert len(self.columns) > 0  # ensure columns are calculated BEFORE rows

        rows = []
        for ri,data_row in enumerate(self._data):
            row=[]
            for fi, field in enumerate(data_row.__fields__):
                if self.columns[fi]["type"] == "ArrayType":
                    # create internal schema as a single field
                    column = StructField(self.columns[fi]["name"], self.columns[fi]["field_type"])
                    # specify row schema in a form of name = value
                    values_as_row = map(lambda r: Row(**{self.columns[fi]["name"] : r}), data_row[field])
                    value = DataFrameTable([column], values_as_row).rows
                    display_value = str(data_row[field])
                elif self.columns[fi]["type"] == "StructType":
                    # extract internal schema as an array of fields
                    inner_schema = self.columns[fi]["field_type"].fields
                    # a value is just a single Row, so we must pack it as an array and then unpack it
                    value = DataFrameTable(inner_schema, [data_row[field]]).rows[0]
                    display_value = str(data_row[field])
                else:
                    value = data_row[field]
                    display_value = str(value)

                row.append({"display_value": display_value, "value": value})

            rows.append({"row_index": ri, "row": row})

        self.set_rows(rows)


    def set_rows(self, rows: []) -> None:
        self.rows = rows
        self.__extract_row_values__()


    def __extract_row_values__(self) -> None:
        # maybe it is not very readable but still it's one-liner
        self.row_values = [[c["display_value"][:DataFrameTable.TEXT_LEN] for c in r["row"]] for r in self.rows]


    def select(self, x: int, y: int) -> ({}, {}):
        return self.columns[x], self.rows[y]["row"][x]


def extract_embedded_table(tab: DataFrameTable, x: int, y: int) -> DataFrameTable | None:
    column, cell = tab.select(x,y)
    kind = column["kind"]
    if kind=="array":
        columns = [StructField(column["name"], column["field_type"])]
        new_tab = DataFrameTable(columns, [])
        rows = cell["value"]
        new_tab.set_rows(rows)
        return new_tab
    elif kind == "struct":
        columns = column["field_type"].fields
        new_tab = DataFrameTable(columns, [])
        rows = [cell["value"]]
        new_tab.set_rows(rows)
        return new_tab

    return None
