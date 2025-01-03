import json
import math
import os

from pyspark.sql import SparkSession

from pyspark_explorer.data_table import DataFrameTable


def __config_dir__() -> str:
    home_dir = os.path.expanduser('~')
    return os.path.join(home_dir, ".pyspark-explorer")


def __ensure_config_dir_exists__() -> None:
    if not os.path.exists(__config_dir__()):
        os.makedirs(__config_dir__())


def __config_file__() -> str:
    return os.path.join(__config_dir__(), "config.json")


def __spark_options_file__() -> str:
    return os.path.join(__config_dir__(), "spark-options.json")


def __read_config__(file: str) -> dict | None:
    if os.path.exists(file):
        with open(file, "r") as f:
            try:
                return json.loads(f.read())
            except Exception as e:
                # ignore any loading errors, just use default params
                return None


def __ensure_path_separator__(path: str) -> str:
    res = path.strip()
    return res + ("" if res.endswith("/") else "/")


def __human_readable_size__(size: int) -> str:
    formats = [".0f", ".1f", ".1f", ".1f", ".1f"]
    units = ["B", "k", "M", "G", "T"]
    exp = math.log(size,10) if size>0 else 0
    ref_exp = math.log(10.24,10)
    #  -2 to scale properly and avoid too early rounding
    scale = max(0, min(round((exp / ref_exp - 2) / 3), len(units)-1))
    text = "{val:" + formats[scale]+"}" + units[scale]
    return format(text.format(val = size / math.pow(1024, scale)))


class Explorer:
    def __init__(self, spark: SparkSession, base_path: str) -> None:
        self.spark = spark
        self.fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        # default params
        self.params = {
            "base_path": base_path,
            "file_limit": 300,
            "take_rows": 1000,
            "sort_files_desc": False,
            "sort_files_as_dirs": False,
        }
        self.spark_options = {
            "CSV": {"header": "false", "dateFormat": "yyyy-MM-dd", "timestampFormat": "yyyy-MM-dd HH:mm:ss", "delimiter": ";"},
            "JSON": {"dateFormat": "yyyy-MM-dd", "timestampFormat": "yyyy-MM-dd HH:mm:ss"}
        }
        # load params from file (if exists)
        self.load_params()



    def get_base_path(self) -> str:
        return self.params["base_path"]


    def get_take_rows(self) -> int:
        return self.params["take_rows"]


    def get_file_limit(self) -> int:
        return self.params["file_limit"]


    def get_sort_files_desc(self) -> bool:
        return self.params["sort_files_desc"]


    def get_sort_files_as_dirs(self) -> bool:
        return self.params["sort_files_as_dirs"]


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

        l = self.fs.listStatus(self.spark._jvm.org.apache.hadoop.fs.Path(path), self.spark._jvm.org.apache.hadoop.fs.GlobFilter("*"))
        for f in l[:self.get_file_limit()]:
            file = self.__file_info__(f.getPath())
            files.append(file)

        files_sorted = sorted(files,
                              key=lambda f: (f["name"]) if self.get_sort_files_as_dirs() else (0 if f["is_dir"] else 1, f["name"]),
                              reverse=self.get_sort_files_desc())

        return files_sorted


    def file_info(self, path: str) -> {}:
        return self.__file_info__(self.spark._jvm.org.apache.hadoop.fs.Path(path))


    def read_file(self, file_format: str, path: str) -> DataFrameTable | None:
        try:
            options = self.spark_options[file_format] if file_format in self.spark_options else {}
            df = self.spark.read.options(**options).format(file_format).load(path)
            tab = DataFrameTable(df.schema.fields, df.take(self.get_take_rows()), True)
        except Exception as e:
            tab = None

        return tab


    def save_params(self) -> None:
        __ensure_config_dir_exists__()
        with open(__config_file__(), "w") as f:
            f.write(json.dumps(self.params))
        with open(__spark_options_file__(), "w") as f:
            f.write(json.dumps(self.spark_options))


    def load_params(self) -> None:
        params_from_file = __read_config__(__config_file__())
        if params_from_file is not None:
            self.params.update(params_from_file)
        spark_options_from_file = __read_config__(__spark_options_file__())
        if spark_options_from_file is not None:
            self.spark_options.update(spark_options_from_file)
