
from pyspark.sql import Row, SparkSession
import pyspark.sql.functions as F

from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkTask


class GlobalWeatherSeasonByStation(SparkTask):

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
            .groupby("ghcn_id", "year", "season")
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

        global_season_table = DataTable("weather", "global_season", global_season, "overwrite")

        return DataSet([global_season_table])
