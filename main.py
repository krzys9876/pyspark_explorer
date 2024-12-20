import sys

from pyspark.sql import SparkSession

import pyspark_explorer.ui as ui
from pyspark_explorer.explorer import Explorer


def main() -> None:
    spark = SparkSession.builder.master("local[2]").appName("pyspark_test01").getOrCreate()

    explorer = Explorer(spark)
    app = ui.DataApp(explorer, sys.argv[1])
    app.run()

    spark.stop()


if __name__ == "__main__":
    main()