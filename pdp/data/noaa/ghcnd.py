from pyspark.sql import SparkSession, Column, Row
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
            .text(f'{self.all_daily_files_path}/USW00093822.dly')
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
        total_id_width = 1
        for name, width in ID_COLUMNS:
            id_column_substr.append((name, total_id_width, width))
            total_id_width += width

        def daily_observation(day: int) -> list[Column]:
            start_width = total_id_width + (day - 1) * observation_total_width
            value = TEXT_COL.substr(start_width, VALUE_CHARS).cast("int").alias(f"VALUE{day}")
            flags = [
                TEXT_COL.substr(start_width + VALUE_CHARS + i, 1).alias(f"{f}FLAG{day}")
                for i, f in enumerate(SINGLE_CHAR_FLAGS)
            ]
            return [value] + flags

        select_ids = [
            TEXT_COL.substr(start, width).alias(name) for name, start, width in id_column_substr
        ]

        select_observations = [c for day in range(1, 32) for c in daily_observation(day)]

        parsed = raw_text.select(select_ids + select_observations + [F.col("file_name")])

        return DataSet([DataTable(
            "weather", "raw_ghcnd", parsed, "overwrite"
        )])


class GHCNDTransformedValues(SparkTask):

    # Defines measurements (decimals) to make as wide form columns,
    # includes full name (with unit) and multiplier to get whole units
    # (some values given in tenths of a unit or 10's of unit)
    MEASUREMENT_COLUMNS = [

        # Temperature measurements
        ("TMAX", "temperature_max_c", 0.1),
        ("TMIN", "temperature_min_c", 0.1),
        ("TAVG", "temperature_avg_c", 0.1),
        ("ADPT", "temperature_avg_dew_point_c", 0.1),
        ("AWBT", "temperature_avg_wet_bulb_c", 0.1),

        # Pressure measurements
        ("ASLP", "pressure_sea_level_hpa", 0.1),
        ("ASTP", "pressure_station_level_hpa", 0.1),

        # Humidity measurements
        ("RHAV", "relative_humidity_avg_pct", 1),
        ("RHMN", "relative_humidity_min_pct", 1),
        ("RHMX", "relative_humidity_max_pct", 1),

        # Precipitation measurements
        ("PRCP", "precipitation_mm", 0.1),
        ("SNOW", "snowfall_mm", 1),
        ("SNWD", "snow_depth_mm", 1),
        ("EVAP", "evaporation_mm", 0.1),

        # Cloud cover measurements
        ("ACMC", "cloudy_all_ceilo_avg_pct", 1),
        ("ACMH", "cloudy_all_manual_avg_pct", 1),
        ("ACSC", "cloudy_day_ceilo_avg_pct", 1),
        ("ACSH", "cloudy_day_manual_avg_pct", 1),
        ("PSUN", "sunshine_daily_pct", 1),

        # Wind measurements
        ("AWDR", "wind_direction_avg_degrees", 1),
        ("AWND", "wind_speed_avg_ms", 0.1),
	   
    ]

    tenths_columns = {d[1] for d in MEASUREMENT_COLUMNS if d[2] == 0.1}

    def __init__(self):
        super().__init__("ghcnd-transformed")

    def read(self, spark: SparkSession) -> DataSet:
        raw = DataTable("weather", "raw_ghcnd")
        return DataSet([raw])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        raw = read_data.get_table('raw_ghcnd').df

        measurement_lookups = spark.createDataFrame(data=[
            Row(ELEMENT=x, name=y, multiplier=float(z))
            for x,y,z in self.MEASUREMENT_COLUMNS
        ])

        measurement_lookups.show()

        value_cols = [c for c in raw.columns if c.startswith("VALUE")]

        long_form_values = (
            raw.melt(ids=["ID", "YEAR", "MONTH", "ELEMENT"], values=value_cols,
                     variableColumnName="DAY", valueColumnName="value")
            .withColumn("DAY", F.regexp_extract("DAY", "VALUE(\\d+)", 1).cast("int"))
            .withColumn("date", F.make_date("YEAR", "MONTH", "DAY"))
            .join(measurement_lookups, "ELEMENT", "inner")
            .withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
            .withColumn("value", F.col("value") * F.col("multiplier"))
            .withColumnRenamed("ID", "ghcn_id")
            .drop("YEAR", "MONTH")
        )

        long_form_values.show(1)

        pivot_values = (
            long_form_values
            .groupby("ghcn_id", "date")
            .pivot("ELEMENT")
            .sum("value")
        )

        formatted_values = (
            pivot_values
            .select(
                [F.col("ghcn_id"), F.col("date")] + [
                    F.col(c[1]).cast("int").alias(c[1]) if c[2] == 1
                    else F.col(c[1]).cast("decimal(4, 1)").alias(c[1])
                    for c in self.MEASUREMENT_COLUMNS
                ]
            )
        )
        formatted_values.show(1)

        values_table = DataTable("weather", "global_daily", formatted_values, "overwrite")

        return DataSet([values_table])
