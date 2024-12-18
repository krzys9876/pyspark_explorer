import math

from pyspark.sql import SparkSession


def __ensure_path_separator__(path: str) -> str:
    res = path.strip()
    return res + ("" if res.endswith("/") else "/")


def __human_readable_size__(size: int) -> str:
    formats = [".0f", ".1f", ".3f", ".3f", ".3f"]
    units = ["B", "k", "M", "G", "T"]
    exp = math.log(size,10)
    ref_exp = math.log(10.24,10)
    #  -2 to scale properly and avoid too early rounding
    scale = max(0, min(round((exp / ref_exp - 2) / 3), 4))
    text = "{val:" + formats[scale]+"}" + units[scale]
    return format(text.format(val = size / math.pow(1024, scale)))


class Explorer:
    def __init__(self, spark: SparkSession, init_path: str) -> None:
        self.current_path = None
        self.path_tree: [str] = []
        self.spark = spark
        self.init_path = __ensure_path_separator__(init_path)
        self.fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        self.current_dir_content:[dict] = []
        self.params = {
            "auto_refresh": True,
            "file_limit": 5,
            "sort_file_desc": True
        }

        self.reset_path()


    def reset_path(self) -> None:
        self.path_tree = [self.init_path]
        self.current_path = self.init_path
        if self.params["auto_refresh"]:
            self.refresh_directory()


    def refresh_directory(self) -> None:
        files: [dict] = []
        st = self.fs.getFileStatus(self.spark._jvm.org.apache.hadoop.fs.Path(self.current_path))
        if st.isFile():
            self.current_dir_content = []
            return

        l = self.fs.listStatus(self.spark._jvm.org.apache.hadoop.fs.Path(self.current_path))
        if self.params["sort_file_desc"]:
            l=list(reversed(l))
        for f in l[:self.params["file_limit"]]:
            file_name = f.getPath().getName()
            file = {"name": file_name, "full_path": f.getPath().toString(), "is_dir": f.isDirectory(),
                "size": 0, "hr_size": "", "type": "dir"}
            if f.isFile():
                file_info = self.fs.getContentSummary(f.getPath())
                file_type = "CSV" if file_name.lower().endswith(".csv") \
                    else "JSON" if file_name.lower().endswith(".json") \
                    else "PARQUET" if file_name.lower().endswith(".parquet") \
                    else "UNKNOWN"
                file["size"] = file_info.getLength()
                file["hr_size"] = __human_readable_size__(file_info.getLength())
                file["type"] = file_type

            files.append(file)

        self.current_dir_content = files


    @staticmethod
    def base_info(base_path: str) -> {}:
        return {"name": "[]", "full_path": base_path, "is_dir": True, "size": 0, "hr_size": "", "type": "dir"}


    def change_directory(self, index: int) -> bool:
        if len(self.current_dir_content)<=index:
            return False

        file_entry = self.current_dir_content[index]
        if not file_entry["is_dir"]:
            return False

        self.current_path = __ensure_path_separator__(self.current_path+file_entry["name"])
        self.path_tree.append(self.current_path)
        if self.params["auto_refresh"]:
            self.refresh_directory()
        return True


    def change_to_parent(self) -> None:
        path_len = len(self.path_tree)
        if path_len>1:
            self.current_path = self.path_tree[path_len-2]
            self.path_tree = self.path_tree[:path_len-1]
            if self.params["auto_refresh"]:
                self.refresh_directory()
