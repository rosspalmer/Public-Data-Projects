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

    def run(self):
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS gsod_bronze").show()
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS gsod_silver").show()

        # self._ingest_bronze_isd_history()
        # self._ingest_bronze_daily()
        self._generate_silver_stations()

    def _ingest_bronze_isd_history(self):
        df = self.spark.read.option("header", True) \
            .csv(f"{self.data_folder_path}/isd-history.csv") \
            .withColumnRenamed("STATION NAME", "STATION_NAME") \
            .withColumnRenamed("ELEV(M)", "ELEV_M")

        df.write.mode("overwrite").format("delta") \
            .saveAsTable("gsod_bronze.isd_history")

    def _generate_silver_stations(self):

        isd_history = self.spark.table("gsod_bronze.isd_history") \
            .withColumn("station_id", concat_ws("-", col("USAF"), col("WBAN")))

        if self.spark.catalog.tableExists("gsod_silver.stations"):
            isd_history = isd_history.join(
                self.spark.table("gsod_silver.stations").select("station_id"),
                "station_id", "leftanti"
            )

        df = isd_history.select(
            col("station_id"),
            col("USAF").alias("id_usaf"),
            col("WBAN").alias("id_wban"),
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
            .saveAsTable("gsod_silver.stations")

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
            parse("stn", "int", 1, 6, False),
            parse("wban", "int", 8, 5, False),
            parse("year", "int", 15, 4),
            parse("month", "int", 19, 2),
            parse("day", "int", 21, 2),
            parse("temp", "float", 25, 6),
            parse("temp_count", "int", 32, 2),
            parse("dewp", "float", 36, 6),
            parse("dewp_count", "int", 43, 2),
            parse("slp", "float", 47, 6),
            parse("slp_count", "int", 54, 2),
            parse("stp", "float", 58, 6),
            parse("stp_count", "int", 65, 2),
            parse("visib", "float", 69, 5),
            parse("visib_count", "int", 75, 2),
            parse("wdsp", "float", 79, 5),
            parse("wdsp_count", "int", 85, 2),
            parse("mxspd", "float", 89, 5),
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

        df.write.mode("overwrite").format("delta") \
            .option("optimizeWrite", "True") \
            .saveAsTable("gsod_bronze.daily")
