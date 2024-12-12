from pyspark.sql.types import StructField, Row, StructType


class DataTable:
    def __init__(self, schema: [StructField], data: [Row]):
        self._schema: [StructField] = schema
        self._data: [Row] = data

        self.columns = self.__extract_columns__()
        self.rows = self.__extract_rows__()


    def __extract_columns__(self) -> []:
        cols = []
        for i,field in enumerate(self._schema):
            cols.append({"col_index": i, "name": field.name, "type": type(field.dataType).__name__, "field_type": field.dataType})

        return cols


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
                    value = DataTable([column], values_as_row).rows
                    display_value = str(data_row[field])
                    kind = "array"
                elif self.columns[fi]["type"] == "StructType":
                    # extract internal schema as an array of fields
                    inner_schema = self.columns[fi]["field_type"].fields
                    # a value is just a single Row, so we must pack it as an array and then unpack it
                    value = DataTable(inner_schema, [data_row[field]]).rows[0]
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