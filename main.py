import sys

from pyspark.sql import SparkSession

import pyspark_explorer.ui as ui
from pyspark_explorer.data_table import DataFrameTable


def main() -> None:
    spark = SparkSession.builder.master("local[2]").appName("pyspark_test01").getOrCreate()

    df = spark.read.format("json").load(path=sys.argv[1])

    tab = DataFrameTable(df.schema.fields, df.take(DataFrameTable.TAKE_ROWS), True)

    app = ui.DataApp(data = tab)
    app.run()

    spark.stop()



if __name__ == "__main__":
    main()