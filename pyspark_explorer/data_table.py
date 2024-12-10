from pyspark.sql.types import StructField, Row


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
                    value = data_row[field]
                    display_value = str(value)
                    kind = "array"
                else:
                    value = data_row[field]
                    display_value = str(value)
                    kind = "simple"

                row.append({"column": self.columns[fi], "kind": kind, "display_value": display_value,
                            "value": value})

            rows.append({"row_index": ri, "row": row})

        return rows