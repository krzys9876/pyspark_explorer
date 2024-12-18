import sys

from pyspark.sql import SparkSession

import pyspark_explorer.ui as ui
from pyspark_explorer.data_table import DataFrameTable
from pyspark_explorer.explorer import Explorer


def main() -> None:
    spark = SparkSession.builder.master("local[2]").appName("pyspark_test01").getOrCreate()

    explorer = Explorer(spark, sys.argv[1])
    explorer.refresh_directory()

    df = spark.read.format("json").load(path=sys.argv[1])

    tab = DataFrameTable(df.schema.fields, df.take(DataFrameTable.TAKE_ROWS), True)

    app = ui.DataApp(tab,spark, sys.argv[1])
    app.run()

    # for entry in explorer.current_dir_content:
    #     print(entry)
    #
    # explorer.change_directory(2)
    # #explorer.refresh_directory()
    # for entry in explorer.current_dir_content:
    #     print(entry)
    #
    # explorer.change_to_parent()
    # #explorer.refresh_directory()
    # for entry in explorer.current_dir_content:
    #     print(entry)
    #
    # explorer.change_directory(1)
    # #explorer.refresh_directory()
    # for entry in explorer.current_dir_content:
    #     print(entry)

    spark.stop()



if __name__ == "__main__":
    main()