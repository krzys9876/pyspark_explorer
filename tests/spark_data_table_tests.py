import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType

from pyspark_explorer.data_table import DataTable


class TestSparkDataTable:
    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder.appName("pyspark_test01").getOrCreate()

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    def test_simple_fields(self) -> None:
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("date", DateType(), True),
        ])
        current_dir = os.path.dirname(__file__)
        df = TestSparkDataTable.spark.read.schema(schema).csv(path=f"file:///{current_dir}/spark_test01.csv")

        df_cols = df.schema.fields
        df_rows = df.take(2)
        tab = DataTable(df_cols, df_rows)

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

    def test_embedded_array(self) -> None:
        schema = StructType([StructField("id", IntegerType()),
                             StructField("info", ArrayType(StructType([
                                 StructField("num", IntegerType(), True),
                                 StructField("text", StringType(), True)])))])

        current_dir = os.path.dirname(__file__)
        df = TestSparkDataTable.spark.read.schema(schema).json(path=f"file:///{current_dir}/spark_test02.json")

        df_cols = df.schema.fields
        df_rows = df.take(2)
        tab = DataTable(df_cols, df_rows)

        expected_cols = [
            {'col_index': 0, 'name': 'id', 'type': 'IntegerType', 'field_type': IntegerType()},
            {'col_index': 1, 'name': 'info', 'type': 'ArrayType', 'field_type': ArrayType(StructType([
                StructField('num', IntegerType(), True),
                StructField('text', StringType(), True)]), True)}]

        expected_rows = [
            {'row_index': 0,'row': [
                {'column': expected_cols[0], 'kind': 'simple', 'display_value': '1', 'value': 1},
                {'column': expected_cols[1], 'kind': 'array', 'display_value': "[Row(num=101, text='aaa'), Row(num=201, text='bbb')]",
                'value': [{'row_index': 0, 'row': [
                    {'column': {'col_index': 0, 'name': 'info', 'type': 'StructType', 'field_type': expected_cols[1]["field_type"].elementType}, 'kind': 'struct',
                'display_value': "Row(num=101, text='aaa')",
                'value':
                    {'row_index': 0, 'row': [
                        {'column': {'col_index': 0, 'name': 'num', 'type': 'IntegerType', 'field_type': IntegerType()}, 'kind': 'simple', 'display_value': '101', 'value': 101},
                        {'column': {'col_index': 1, 'name': 'text', 'type': 'StringType', 'field_type': StringType()}, 'kind': 'simple', 'display_value': 'aaa', 'value': 'aaa'}]}}]},
                    {'row_index': 1, 'row': [
                        {'column': {'col_index': 0, 'name': 'info', 'type': 'StructType', 'field_type': expected_cols[1]["field_type"].elementType}, 'kind': 'struct',
                'display_value': "Row(num=201, text='bbb')",
                'value':
                    {'row_index': 0, 'row': [
                        {'column': {'col_index': 0, 'name': 'num', 'type': 'IntegerType', 'field_type': IntegerType()}, 'kind': 'simple', 'display_value': '201', 'value': 201},
                        {'column': {'col_index': 1, 'name': 'text', 'type': 'StringType', 'field_type': StringType()}, 'kind': 'simple', 'display_value': 'bbb', 'value': 'bbb'}]}}]}]}]},

            {'row_index': 1,'row': [
                {'column': expected_cols[0], 'kind': 'simple', 'display_value': '2', 'value': 2},
                {'column': expected_cols[1], 'kind': 'array', 'display_value': "[Row(num=102, text='ccc'), Row(num=202, text='ddd')]",
                'value': [{'row_index': 0, 'row': [
                    {'column': {'col_index': 0, 'name': 'info', 'type': 'StructType', 'field_type': expected_cols[1]["field_type"].elementType}, 'kind': 'struct',
                'display_value': "Row(num=102, text='ccc')",
                'value':
                    {'row_index': 0, 'row': [
                        {'column': {'col_index': 0, 'name': 'num', 'type': 'IntegerType', 'field_type': IntegerType()}, 'kind': 'simple', 'display_value': '102', 'value': 102},
                        {'column': {'col_index': 1, 'name': 'text', 'type': 'StringType', 'field_type': StringType()}, 'kind': 'simple', 'display_value': 'ccc', 'value': 'ccc'}]}}]},
                    {'row_index': 1, 'row': [
                        {'column': {'col_index': 0, 'name': 'info', 'type': 'StructType', 'field_type': expected_cols[1]["field_type"].elementType}, 'kind': 'struct',
                'display_value': "Row(num=202, text='ddd')",
                'value':
                    {'row_index': 0, 'row': [
                        {'column': {'col_index': 0, 'name': 'num', 'type': 'IntegerType', 'field_type': IntegerType()}, 'kind': 'simple', 'display_value': '202', 'value': 202},
                        {'column': {'col_index': 1, 'name': 'text', 'type': 'StringType', 'field_type': StringType()}, 'kind': 'simple', 'display_value': 'ddd', 'value': 'ddd'}]}}]}]}]}]

        print(tab.columns)
        print(tab.rows)

        assert tab.columns == expected_cols
        assert tab.rows == expected_rows