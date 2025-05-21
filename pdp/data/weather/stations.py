from typing import Any

import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType
import reverse_geocode


from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkTask
from pdp.utils import parse_fixed_width


class SurfaceWeatherStation(SparkTask):

    NETWORK_ID_NAMES = {
        "0": "unspecified",
        "1": "community_rain_hail_snow",
        "C": "us_cooperative_network",
        "E": "euro_climate_assessment",
        "M": "world_meteorological_org",
        "N": "national_meteo_hydro_center",
        "R": "raw",
        "S": "us_snowpack",
        "W": "wban"
    }

    STATION_COLUMNS = [
        ("ghcn_id", 11, "string"),
        ("latitude", 9, "float"),
        ("longitude", 10, "float"),
        ("elevation", 7, "float"),
        ("state", 3, "string"),
        ("name", 31, "string"),
        ("gsn_flag", 4, "string"),
        ("hcn_crn_flag", 4, "string"),
        ("wmo_id", 6, "string")
    ]

    STATION_HISTORY_COLUMNS = [
        ("ghcn_id", 11, "string"),
        ("latitude", 9, "float"),
        ("longitude", 10, "float"),
        ("element", 5, "string"),
        ("first_year", 5, "int"),
        ("last_year", 5, "int")
    ]

    def __init__(self, ncei_data_folder: str):
        super().__init__("surface-stations")
        self.ncei_data_folder = ncei_data_folder

    def read(self, spark: SparkSession) -> DataSet:

        spark.sql("CREATE SCHEMA IF NOT EXISTS weather")

        raw_station = (
            spark
            .read
            .text(f"{self.ncei_data_folder}/ghcnd-stations.txt")
        )

        raw_history = (
            spark
            .read
            .text(f"{self.ncei_data_folder}/ghcnd-inventory.txt")
        )

        return DataSet([
            DataTable("weather", "raw_station", raw_station, "overwrite"),
            DataTable("weather", "raw_station_history", raw_history, "overwrite")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        raw_station_parsed = parse_fixed_width(
            read_data.get_table("raw_station").df,
            self.STATION_COLUMNS
        )

        print("STATION PARSED")
        raw_station_parsed.show()

        raw_station_history_parsed = parse_fixed_width(
            read_data.get_table("raw_station_history").df,
            self.STATION_HISTORY_COLUMNS
        )

        print("STATION HISTORY PARSED")
        raw_station_history_parsed.show()

        network_name_map = {k: v for k, v in self.NETWORK_ID_NAMES.items()}
        network_name_udf = F.udf(lambda x: network_name_map.get(x), StringType())

        # TODO Parse out columns for station data
        parsed_and_enriched = (
            raw_station_parsed
            .withColumns({
                name: F.trim(name)
                for name, width, data_type in self.STATION_COLUMNS
                if data_type == "string"
            })
            .withColumn("country_code", F.left("ghcn_id", F.lit(2)))
            .withColumn("network_id", F.substring("ghcn_id", 3, 1))
            .withColumn("network_name", network_name_udf(F.col("network_id")))
            .withColumn("wban_id", F.when(F.col("network_id") == "W", F.right("ghcn_id", F.lit(5))))
        )

        # Build station and coordinate lists for reverse_geocode lookup below
        station_coords_data = [
            (r.ghcn_id, (r.lat, r.long))
            for r in raw_station_parsed.select("ghcn_id", "latitude", "longitude").collect()
        ]
        station_ids = [x[0] for x in station_coords_data]
        coords = [x[1] for x in station_coords_data]

        # Create DataFrame using reverse_geocode 'search'
        # on station coordinates (`data` map<string, string> column)
        lookups = zip(station_ids, reverse_geocode.search(coords))
        lookup_df = spark.createDataFrame(
            data=lookups,
            schema="ghcn_id string, data map<string, string>"
        ).persist()

        # Create list of unique key names from `data` map for use below
        data_keys = [r.data_key for r in (
            lookup_df.select(
                F.explode(
                    F.map_keys("data")
                ).alias("data_key"))
            .distinct()
            .collect()
        ) if r.data_key not in ['country_code']]

        for k in data_keys:
            lookup_df = lookup_df.withColumn(k, F.element_at("data", k))

        lookup_df = (
            lookup_df
            .drop("data")
            .withColumnsRenamed({"latitude": "city_lat", "longitude": "city_long"})
        )

        station_with_geo_data = (
            parsed_and_enriched
            .drop("state")
            .join(lookup_df, "ghcn_id", "left")
        )

        return DataSet([
            DataTable("weather", "station", station_with_geo_data, "overwrite"),
            DataTable("weather", "station_history", raw_station_history_parsed, "overwrite")
        ])


class StationGroupsFrontend(SparkTask):

    def __init__(self):
        super().__init__("station-groups-frontend")

    def read(self, spark: SparkSession) -> DataSet:
        tables = [
            DataTable("weather", "global_stations"),
            DataTable("weather", "station_groups")
        ]
        return DataSet(tables)

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        stations = read_data.get_table("global_stations").df
        groups = read_data.get_table("station_groups").df

        group_stations = groups.select(
            "group_id",
            F.explode("station_ids").alias("ghcn_id")
        )

        groups = groups.select("group_id", "network_id", "center_lat", "center_long")

        write_data = [
            DataTable("weather", "stations", stations, "overwrite"),
            DataTable("weather", "station_groups", groups, "overwrite"),
            DataTable("weather", "group_stations", group_stations, "overwrite"),
        ]

        return DataSet(write_data)

    def write(self, write_dataset: DataSet):
        write_dataset.write_all_jdbc()
