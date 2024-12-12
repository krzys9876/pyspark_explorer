import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

from pyspark_explorer.data_table import DataTable


class TestSparkDataTable:
    def test_simple_fields(self) -> None:
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("date", DateType(), True),
        ])
        spark = SparkSession.builder.appName("pyspark_test01").getOrCreate()
        current_dir = os.path.dirname(__file__)
        df = spark.read.schema(schema).csv(path=f"file:///{current_dir}/spark_test01.csv")

        df_cols = df.schema.fields
        df_rows = df.take(2)
        tab = DataTable(df_cols, df_rows)

        spark.stop()

        expected_cols = [
            {'col_index': 0, 'name': 'id', 'type': 'IntegerType', 'field_type': IntegerType()},
            {'col_index': 1, 'name': 'text', 'type': 'StringType', 'field_type': StringType()},
            {'col_index': 2, 'name': 'date', 'type': 'DateType', 'field_type': DateType()}
        ]
        expected_rows = [
            {'row_index': 0, 'row': [
                {'column': {'col_index': 0, 'name': 'id', 'type': 'IntegerType', 'field_type': IntegerType()}, 'kind': 'simple', 'display_value': '1', 'value': 1},
                {'column': {'col_index': 1, 'name': 'text', 'type': 'StringType', 'field_type': StringType()}, 'kind': 'simple', 'display_value': 'abc', 'value': 'abc'},
                {'column': {'col_index': 2, 'name': 'date', 'type': 'DateType', 'field_type': DateType()}, 'kind': 'simple', 'display_value': '2024-01-01', 'value': datetime.date(2024, 1, 1)}]},
            {'row_index': 1, 'row': [
                {'column': {'col_index': 0, 'name': 'id', 'type': 'IntegerType', 'field_type': IntegerType()}, 'kind': 'simple', 'display_value': '2', 'value': 2},
                {'column': {'col_index': 1, 'name': 'text', 'type': 'StringType', 'field_type': StringType()}, 'kind': 'simple', 'display_value': 'def', 'value': 'def'},
                {'column': {'col_index': 2, 'name': 'date', 'type': 'DateType', 'field_type': DateType()}, 'kind': 'simple', 'display_value': '2024-01-02', 'value': datetime.date(2024, 1, 2)}]}
        ]

        assert tab.columns == expected_cols
        assert tab.rows == expected_rows
