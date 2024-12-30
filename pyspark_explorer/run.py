import os
import sys

from pyspark.sql import SparkSession

from pyspark_explorer import ui
from pyspark_explorer.explorer import Explorer


def run() -> None:
    # ensure no spark errors if firewall restrictions exists
    if os.getenv("SPARK_LOCAL_IP") is None:
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    spark = (SparkSession.builder
             .master("local[2]")
             # ERRORS will be briefly displayed on screen if log4j.properties file does not exist or does not forward logs to a file
             .config("spark.log.level", "ERROR")
             .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
             .appName("pyspark_explorer")
             .getOrCreate())

    explorer = Explorer(spark)
    app = ui.DataApp(explorer, sys.argv[1] if len(sys.argv)>1 else "/")
    app.run()

    spark.stop()
