from pyspark.sql.types import StructField, Row


class DataTable:
    def __init__(self, schema: [StructField], data: [Row]):
        self._schema: [StructField] = schema
        self._data: [Row] = data

        self.columns = self.__extract_columns__()
        self.rows = [
            {"row_index": 0, "column": self.columns[0], "value": "some text 1", "display_value": "some text 1"},
            {"row_index": 1, "column": self.columns[0], "value": "some text 2", "display_value": "some text 2"},
        ]

    def __extract_columns__(self) -> []:
        cols = []
        for i,field in enumerate(self._schema):
            cols.append({"col_index": i, "name": field.name, "type": type(field.dataType).__name__, "field_type": field.dataType})

        return cols
