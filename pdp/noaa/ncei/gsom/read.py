
import pyspark.sql.functions as F

from pdp.job import DataJob, DataSource, DataTable


class GlobalSummaryOfMonth(DataJob):

    def __init__(self, data_folder_path: str):
        super().__init__("noaa-gsom")
        self.data_folder_path = data_folder_path

    def run(self) -> list[DataTable]:

        data_files = (
            self.spark
            .read
            .format("csv")
            .option("header", "true")
            .load(self.data_folder_path)
            .withColumn("filename", F.input_file_name())
        )

        data_files.show()

