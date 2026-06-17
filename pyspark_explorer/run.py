import os
import sys

from pyspark.sql import SparkSession

from pyspark_explorer import ui
from pyspark_explorer.explorer import Explorer


def run() -> None:
    # ensure no spark errors if firewall restrictions exists
    if os.getenv("SPARK_LOCAL_IP") is None:
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    explorer = Explorer(sys.argv[1] if len(sys.argv)>1 else "/")
    app = ui.PysparkExplorerUI(explorer)
    app.run()

    # Don't forget to stop spark session
    explorer.stop()
