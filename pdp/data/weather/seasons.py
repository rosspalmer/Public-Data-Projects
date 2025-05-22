
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkTask
from pdp.data.weather.gsom import GSOMByStation


class WeatherSeasonByStation(SparkTask):

    def __init__(self):
        super().__init__("global-weather-season-by-station")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([DataTable("weather", "global_daily")])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        seasons_doys = spark.createDataFrame(
            [Row(doy=n, season="winter") for n in range(1, 80)] +
            [Row(doy=n, season="spring") for n in range(80, 172)] +
            [Row(doy=n, season="summer") for n in range(172, 264)] +
            [Row(doy=n, season="fall") for n in range(264, 315)] +
            [Row(doy=n, season="winter") for n in range(315, 366)]
        )

        daily_with_season = (
            read_data.get_table("global_daily").df
            .withColumn("doy", F.dayofyear("date"))
            .join(seasons_doys, "doy", "inner")
            .drop("doy")
        )

        global_season = (
            daily_with_season
            .withColumn("year", F.date_part(F.lit("year"), "date"))
            .groupby("station_id", "year", "season")
            .agg(
                F.min("temperature_min_c").alias("temperature_absolute_min_c"),
                F.avg("temperature_min_c").alias("temperature_daily_min_avg_c"),
                F.avg("temperature_avg_c").alias("temperature_daily_avg_c"),
                F.avg("temperature_max_c").alias("temperature_daily_max_avg_c"),
                F.max("temperature_max_c").alias("temperature_absolute_max_c"),
                F.avg("temperature_dew_point_avg_c").alias("temperature_daily_dew_point_avg_c"),
                F.avg("temperature_wet_bulb_avg_c").alias("temperature_daily_wet_bulb_avg_c")
            )
        )

        global_season_table = DataTable("weather", "season_by_station", global_season, "overwrite")

        return DataSet([global_season_table])


class WeatherSeasonByArea(SparkTask):

    def __init__(self):
        super().__init__("weather-season-by-area")

    def read(self, spark: SparkSession) -> DataSet:

        tables = [
            DataTable("weather", "station"),
            DataTable("weather", "area"),
            DataTable("weather", "season_by_station")
        ]

        return DataSet(tables)

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        area = read_data.get_table("area").df
        season_by_station = read_data.get_table("season_by_station").df

        area_stations = area.select(
            F.col("area_id"),
            F.explode("station_ids").alias("station_id")
        )

        measurement_names = [
            name for name in season_by_station.columns.tolist()
            if name not in ["station_id", "season", "year"]
        ]

        season_by_area = (
            season_by_station
            .join(area_stations, "area_id", "inner")
            .groupby("area", "season", "year")
            .agg([
                F.struct(
                    F.avg(c).alias("avg"),
                    F.count(c).alias("count"),
                    F.std(c).alias("std")
                ).alias(c)
                for c in measurement_names
            ])
        )

        return DataSet([
            DataTable("weather", "season_by_area", season_by_area, "overwrite")
        ])


class WeatherSeasonTrends(SparkTask):

    START_YEAR = 1940
    END_YEAR = 2024
    TREND_N_YEARS = [5, 10, 20]

    def __init__(self, by_type):
        super().__init__(f"season-trends-by-{by_type}")
        self.by_type = by_type
        self.read_table = f"season_by_{self.by_type}"
        self.key = f"{self.by_type}_id"

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("weather", self.read_table)
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        measurements = (
            read_data.get_table(self.read_table).df
            .withColumn("has_data", F.lit(True))
        )

        station_range: DataFrame = measurements.select(f"{self.by_type}_id").distinct()
        years_range: DataFrame = spark.createDataFrame(data=[Row(year=y) for y in range(1900, 2025)])
        months_range: DataFrame = spark.createDataFrame(data=[Row(month=m) for m in range(1, 13)])
        full_data_range: DataFrame = station_range.crossJoin(years_range).crossJoin(months_range)

        measurement_column_names = [
            v[0] for v in GSOMByStation.MEASUREMENT_COLUMNS.values()
            if v[0] in set(measurements.columns)
        ]

        # Start `global_monthly_weather_trendsFIXME` table by calculating rolling
        # averages of n past years for each station and month
        partition_by = [self.key, "season"]
        grouping_window = Window().partitionBy(partition_by).orderBy("year")
        trend_windows = {n: grouping_window.rowsBetween(-(n - 1), 0) for n in self.TREND_N_YEARS}

        trend_columns = (
                [F.col(self.key), F.col("year"), F.col("season")] +
                [
                    F.when(F.count(c).over(w) >= F.lit(n - 1),
                           F.avg(c).over(w).cast("decimal(16,3)")
                           ).alias(f"{c}_avg{n}")
                    for c in measurement_column_names
                    for n, w in trend_windows.items()
                ]
        )

        trends = (
            full_data_range
            .join(measurements, [self.key, "year", "season"], "left")
            .select(trend_columns)
        )

        return DataSet([
            DataTable("weather", f"season_trend_by_{self.by_type}", trends, "overwrite")
        ])

