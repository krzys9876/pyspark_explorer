import math

from pyspark.sql import SparkSession

from pyspark_explorer.data_table import DataFrameTable


def __ensure_path_separator__(path: str) -> str:
    res = path.strip()
    return res + ("" if res.endswith("/") else "/")


def __human_readable_size__(size: int) -> str:
    formats = [".0f", ".1f", ".3f", ".3f", ".3f"]
    units = ["B", "k", "M", "G", "T"]
    exp = math.log(size,10) if size>0 else 0
    ref_exp = math.log(10.24,10)
    #  -2 to scale properly and avoid too early rounding
    scale = max(0, min(round((exp / ref_exp - 2) / 3), 4))
    text = "{val:" + formats[scale]+"}" + units[scale]
    return format(text.format(val = size / math.pow(1024, scale)))


class Explorer:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        self.params = {
            "auto_refresh": True,
            "file_limit": 300,
            "take_rows": 1000,
            "sort_file_desc": False
        }


    def __file_info__(self, path) -> {}:
        file_status = self.fs.getFileStatus(path)
        file_name = path.getName()
        is_file = file_status.isFile()
        file = {"name": file_name, "full_path": path.toString(), "is_dir": not is_file,
                "size": 0, "hr_size": "", "type": ""}
        if is_file:
            file_info = self.fs.getContentSummary(path)
            file["size"] = file_info.getLength()
            file["hr_size"] = __human_readable_size__(file_info.getLength())
            file["type"] = "CSV" if file_name.lower().endswith(".csv") \
                else "JSON" if file_name.lower().endswith(".json") \
                else "PARQUET" if file_name.lower().endswith(".parquet") \
                else "OTHER"

        return file


    def read_directory(self, path: str) -> []:
        files: [dict] = []
        st = self.fs.getFileStatus(self.spark._jvm.org.apache.hadoop.fs.Path(path))
        if st.isFile():
            return []

        l = self.fs.listStatus(self.spark._jvm.org.apache.hadoop.fs.Path(path))
        if self.params["sort_file_desc"]:
            l = list(reversed(l))
        for f in l[:self.params["file_limit"]]:
            file = self.__file_info__(f.getPath())
            files.append(file)

        return files


    def file_info(self, path: str) -> {}:
        return self.__file_info__(self.spark._jvm.org.apache.hadoop.fs.Path(path))


    def read_file(self, file_format: str, path: str) -> DataFrameTable | None:
        try:
            df = self.spark.read.format(file_format).load(path)
            tab = DataFrameTable(df.schema.fields, df.take(self.params["take_rows"]), True)
        except Exception as e:
            tab = None

        return tab
