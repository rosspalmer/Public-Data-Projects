from pyspark.sql import SparkSession, Column, Row
import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast

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
        )
        raw_table = DataTable("weather", "raw_ghcnd_text", raw_text, "overwrite")

        return DataSet([raw_table])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        TEXT_COL = F.col("value")

        ID_COLUMNS = [
            ("ghcn_id", 11),
            ("year", 4),
            ("month", 2),
            ("element", 4)
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
            value = TEXT_COL.substr(start_width, VALUE_CHARS).alias(f"VALUE{day}")
            flags = [
                TEXT_COL.substr(start_width + VALUE_CHARS + i, 1).alias(f"{f}FLAG{day}")
                for i, f in enumerate(SINGLE_CHAR_FLAGS)
            ]
            return [value] + flags

        select_ids = [
            TEXT_COL.substr(start, width).alias(name) for name, start, width in id_column_substr
        ]

        select_observations = [c for day in range(1, 32) for c in daily_observation(day)]

        parsed = raw_text.select(select_ids + select_observations)

        parsed.show(1)

        id_columns = [name for name, width in ID_COLUMNS]
        value_columns = [name for name in parsed.columns if name not in id_columns]

        long_form = (
            parsed
            .melt(ids=id_columns, values=value_columns,
                  variableColumnName="column_name", valueColumnName="value")
            .withColumn("column_type", F.regexp_extract("column_name", "^([A-Z]+)\\d+$", 1))
            .withColumn("day", F.regexp_extract("column_name", "^[A-Z]+(\\d+)$", 1))
            .withColumn("date", F.make_date("year", "month", "day"))
            .withColumn("filename", F.input_file_name())
            .drop("year", "month", "day")
        )

        return DataSet([DataTable(
            "weather", "ghcnd_long", long_form, "overwrite"
        )])


class GHCNDailyByStation(SparkTask):

    # Defines measurements (decimals) to make as wide form columns,
    # includes full name (with unit) and multiplier to get whole units
    # (some values given in tenths of a unit or 10's of unit)
    MEASUREMENT_COLUMNS = [

        # Temperature measurements
        ("TMAX", "temperature_max_c", 0.1),
        ("TMIN", "temperature_min_c", 0.1),
        ("TAVG", "temperature_avg_c", 0.1),
        ("ADPT", "temperature_dew_point_avg_c", 0.1),
        ("AWBT", "temperature_wet_bulb_avg_c", 0.1),

        # Pressure measurements
        ("ASLP", "pressure_sea_level_avg_hpa", 10),
        ("ASTP", "pressure_station_level_avg_hpa", 10),

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
        ("WDF1", "wind_direction_fast_1_min_degrees", 1),
        ("WSF1", "wind_speed_fast_1_min_ms", 0.1),
        ("WDF2", "wind_direction_fast_2_min_degrees", 1),
        ("WSF2", "wind_speed_fast_2_min_ms", 0.1),
        ("WDF5", "wind_direction_fast_5_sec_degrees", 1),
        ("WSF5", "wind_speed_fast_5_sec_ms", 0.1),

        # Weather events
        ("WT01", "event_fog", 1),
        ("WT02", "event_heavy_fog", 1),
        ("WT03", "event_thunder", 1),
        ("WT04", "event_ice_sleet", 1),
        ("WT05", "event_hail", 1),
        ("WT06", "event_glaze_rime", 1),
        ("WT07", "event_dust", 1),
        ("WT08", "event_smoke", 1),
        ("WT09", "event_blowing_snow", 1),
        ("WT10", "event_tornado", 1),
        ("WT11", "event_high_winds", 1),
        ("WT12", "event_blowing_spray", 1),
        ("WT13", "event_mist", 1),
        ("WT14", "event_drizzle", 1),
        ("WT15", "event_freezing_drizzle", 1),
        ("WT16", "event_rain", 1),
        ("WT17", "event_freezing_rain", 1),
        ("WT18", "event_snow", 1),
        ("WT19", "event_unknown_precipitation", 1),
        ("WT21", "event_ground_fog", 1),
        ("WT22", "event_freezing_fog", 1)

    ]

    def __init__(self):
        super().__init__("ghcn-daily-by-station")

    def read(self, spark: SparkSession) -> DataSet:
        raw = DataTable("weather", "ghcnd_long")
        return DataSet([raw])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        ghcnd_long = read_data.get_table('ghcnd_long').df

        # measurement_lookups = spark.createDataFrame(data=[
        #     Row(element=x, name=y, multiplier=float(z))
        #     for x,y,z in self.MEASUREMENT_COLUMNS
        # ])

        long_form_values = (
            ghcnd_long
            .filter(F.col("column_type") == "VALUE")
            .withColumn("value", F.col("value").cast("int"))
            .withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
        )

        def get_column(old_name: str, new_name: str, multiplier: float) -> Column:

            base_value = F.first_value(
                F.when(F.col("element") == old_name, F.col("value")),
                ignoreNulls=True
            )
            cast_value = base_value.cast("int") if multiplier >= 1 else base_value.cast("decimal(4,1)")
            multiplied_value = cast_value * F.lit(multiplier)

            return multiplied_value.alias(new_name)

        wide_form_values = (
            long_form_values
            .repartition(2000, "ghcn_id", "date")
            .groupby("ghcn_id", "date")
            .agg(*[
                get_column(old_name, new_name, multiplier)
                for old_name, new_name, multiplier in self.MEASUREMENT_COLUMNS
            ])
        )

        values_table = DataTable("weather", "global_daily_station", wide_form_values, "overwrite")

        return DataSet([values_table])
