from pyspark.sql.types import StructField, Row


class DataTable:
    def __init__(self, schema: [StructField], data: [Row]):
        self._schema = schema
        self._data = data

        self.columns = [{"index": 0, "name": "text", "type": "StringType", "field_type": schema[0]}]
        self.rows = [
            {"index": 0, "column": self.columns[0], "value": "some text 1", "display_value": "some text 1"},
            {"index": 1, "column": self.columns[0], "value": "some text 2", "display_value": "some text 2"},
        ]
