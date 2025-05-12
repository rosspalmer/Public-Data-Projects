from pyspark.sql import SparkSession, Column
import pyspark.sql.functions as F

from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkTask

class GHCNDParseTextFiles(SparkTask):

    def __init__(self, all_daily_files_path: str):
        super().__init__("ghcnd-parse-text-files")
        self.all_daily_files_path = all_daily_files_path

    def read(self, spark: SparkSession) -> DataSet:

        raw_text = (
            spark.read
            .text(f'{self.all_daily_files_path}/*.dly')
            .withColumn("file_name", F.input_file_name())
        )
        raw_table = DataTable("weather", "raw_ghcnd_text", raw_text, "overwrite")

        return DataSet([raw_table])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        TEXT_COL = F.col("value")

        ID_COLUMNS = [
            ("ID", 11),
            ("YEAR", 4),
            ("MONTH", 2),
            ("ELEMENT", 4)
        ]

        VALUE_CHARS = 5
        SINGLE_CHAR_FLAGS = ['M', 'Q', 'S']
        observation_total_width = VALUE_CHARS + len(SINGLE_CHAR_FLAGS)

        raw_text = read_data.get_table('raw_ghcnd_text').df

        id_column_substr = []
        total_id_width = 0
        for name, width in ID_COLUMNS:
            id_column_substr.append((name, total_id_width, width))
            total_id_width += width

        def daily_observation(day: int) -> list[Column]:
            start_width = total_id_width + (day - 1) * observation_total_width
            value = TEXT_COL.substr(start_width, VALUE_CHARS).alias(f"VALUE{day}")
            flags = [
                TEXT_COL.substr(start_width + i, 1).alias()
                for i, c in enumerate(SINGLE_CHAR_FLAGS)
            ]
            return [value] + flags

        parsed = raw_text.select(
            [
                TEXT_COL.substr(start, width).alias(name) for name, start, width in id_column_substr
            ] + [
                c for n in range(31) for c in daily_observation(n)
            ] + [F.col("file_name")]
        )

        return DataSet([DataTable(
            "weather", "raw_ghcnd", parsed, "overwrite"
        )])
