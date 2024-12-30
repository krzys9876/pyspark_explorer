import os
import sys
from pyspark.sql import SparkSession

import pyspark_explorer.ui as ui
from pyspark_explorer.explorer import Explorer


def main() -> None:
    spark = (SparkSession.builder
             .master("local[2]")
             #.config("spark.driver.extraJavaOptions", "-Dlog4j.shutdownHookEnabled=false")
             .config("spark.log.level", "FATAL")
             .appName("pyspark_explorer")
             .getOrCreate())

    explorer = Explorer(spark)
    app = ui.DataApp(explorer, sys.argv[1] if len(sys.argv)>1 else "/")
    app.run()

    spark.stop()


if __name__ == "__main__":
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    main()