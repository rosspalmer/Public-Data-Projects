from pprint import pprint
from typing import List

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, length, lit, regexp, to_date, when, udf

from pdp.spark import SharedSpark


class GlobalSurfaceSummaryOfDay(SharedSpark):

    def __init__(self, data_folder_path: str):
        super().__init__("noaa-gsod")
        self.data_folder_path = data_folder_path

        self.SCHEMA = "gsod"
        self.STATIONS_BRONZE = f"{self.schema('bronze')}.isd_history"
        self.STATIONS_SILVER = f"{self.schema('silver')}.stations"
        self.DAILY_BRONZE = f"{self.schema('bronze')}.daily"
        self.DAILY_SILVER = f"{self.schema('silver')}.daily"

    def schema(self, medallion: str):
        return f"{self.SCHEMA}_{medallion}"

    def run(self):

        for medallion in ['bronze', 'silver']:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema(medallion)}")

        self._ingest_bronze_stations()
        # self._generate_silver_stations()
        self._ingest_bronze_daily()
        self._generate_silver_daily()

    def _ingest_bronze_stations(self):

        df = self.spark.read.option("header", True) \
            .csv(f"{self.data_folder_path}/isd-history.csv") \
            .withColumnRenamed("STATION NAME", "STATION_NAME") \
            .withColumnRenamed("ELEV(M)", "ELEV_M") \
            .withColumn("station_id", concat_ws("-", col("USAF"), col("WBAN")))

        df.write.mode("overwrite").format("delta") \
            .saveAsTable(self.STATIONS_BRONZE)

    def _generate_silver_stations(self):

        isd_history = self.spark.table(self.STATIONS_BRONZE) \
            .withColumn("station_id", concat_ws("-", col("USAF"), col("WBAN")))

        if self.spark.catalog.tableExists(self.STATIONS_SILVER):
            isd_history = isd_history.join(
                self.spark.table(self.STATIONS_SILVER).select("station_id"),
                "station_id", "leftanti"
            )

        df = isd_history.select(
            col("station_id"),
            col("USAF").cast("string").alias("id_usaf"),
            col("WBAN").cast("string").alias("id_wban"),
            col("STATION_NAME").alias("station_name"),
            col("CTRY").alias("country"),
            col("STATE").alias("state"),
            col("ICAO"),
            col("LAT").cast("decimal(6, 3)").alias("latitude"),
            col("LON").cast("decimal(6, 3)").alias("longitude"),
            col("ELEV_M").cast("decimal(6, 1)").alias("elevation_meters"),
            to_date("BEGIN", "yyyyMMdd").alias("start_service"),
            to_date("END", "yyyyMMdd").alias("end_service")
        ).persist()

        geolocator = Nominatim(
            user_agent="public-data-gsod"
        )
        geocode = RateLimiter(lambda x: geolocator.reverse(x, language="en"), min_delay_seconds=1)

        coord_udf = udf(lambda lat, lon: f'{lat:.3f},{lon:.3f}'.format(lat=lat,lon=lon))

        # FIXME turn off row limits after testing
        location: pd.DataFrame = df \
            .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
            .sample(0.1) \
            .select(
                col("station_id"),
                coord_udf(col("latitude"), col("longitude")).alias("coord")
            ).toPandas()

        print(location['coord'])

        location['address'] = location['coord'].apply(geocode) \
            .apply(lambda r: r.raw['address'] if r else None)

        df = df.join(ps.DataFrame(location).to_spark(), "station_id")

        df.write.mode("append").format("delta") \
            .option("optimizeWrite", "True") \
            .option("mergeSchema", True) \
            .saveAsTable(self.STATIONS_SILVER)

    def _ingest_bronze_daily(self):

        df = self.spark.read.option("compression", "gzip") \
            .text(f"{self.data_folder_path}/*/*.op.gz") \
            .filter(~col("value").like('STN%')) \
            .withColumnRenamed("value", "v")

        def parse(name, dtype, start, num, nullable=True):
            string_col = df.v.substr(start, num)
            if nullable:
                null_match = regexp(string_col, lit("^9+\\.?9*$")) & (length(string_col) == lit(num))
                string_col = when(~null_match, string_col)
            return string_col.cast(dtype).alias(name)

        df = df.select(
            parse("stn", "string", 1, 6, False),
            parse("wban", "string", 8, 5, False),
            parse("year", "string", 15, 4),
            parse("month", "string", 19, 2),
            parse("day", "string", 21, 2),
            parse("temp", "decimal(5,1)", 25, 6),
            parse("temp_count", "int", 32, 2),
            parse("dewp", "decimal(5,1)", 36, 6),
            parse("dewp_count", "int", 43, 2),
            parse("slp", "decimal(5,1)", 47, 6),
            parse("slp_count", "int", 54, 2),
            parse("stp", "decimal(5,1)", 58, 6),
            parse("stp_count", "int", 65, 2),
            parse("visib", "decimal(4,1)", 69, 5),
            parse("visib_count", "int", 75, 2),
            parse("wdsp", "decimal(4,1)", 79, 5),
            parse("wdsp_count", "int", 85, 2),
            parse("mxspd", "decimal(4,1)", 89, 5),
            parse("gust", "float", 96, 5),
            parse("max", "float", 103, 6),
            parse("max_flag", "string", 109, 1),
            parse("min", "float", 111, 6),
            parse("min_flag", "string", 117, 1),
            parse("prcp", "float", 119, 5),
            parse("prcp_flag", "string", 124, 1),
            parse("sndp", "float", 126, 5),
            parse("frshtt", "string", 133, 6)
        )

        df = df.select(concat_ws("-", col("USAF"), col("WBAN")).alias("station_id"))

        df.write.mode("overwrite").format("delta") \
            .option("optimizeWrite", "True") \
            .saveAsTable(self.STATIONS_SILVER)

    def _generate_silver_daily(self):

        df = self.spark.table(self.DAILY_BRONZE) \
            .select(
                col("station_id"),
                to_date(concat_ws("-", "year", "month", "day")),
                col("temp").alias("avg_temperature_f"),
                col("temp_count").alias("num_obs_temperature"),
                col("max").alias("max_temperature_f"),
                col("min").alias("min_temperature_f"),
                col("dewp").alias("avg_dew_point_f"),
                col("dewp_count").alias("num_obs_dew_point"),
                col("slp").alias("avg_sea_level_pressure_mb"),
                col("slp_count").alias("num_obs_sea_level_pressure"),
                col("visib").alias("avg_visibility_miles"),
                col("visib_count").alias("num_obs_visibility"),
                col("wdsp").alias("avg_wind_speed_kt"),
                col("wdsp_count").alias("num_obs_wind_speed"),
                col("mxspd").alias("max_wind_speed_kt"),
                col("gust").alias("max_wind_gust_kt")
            )

        df.write.mode("overwrite").format("delta") \
            .option("optimizeWrite", "True") \
            .saveAsTable(self.STATIONS_SILVER)
