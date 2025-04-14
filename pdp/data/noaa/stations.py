from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkTask

import pyspark.sql.functions as F

import reverse_geocode


class SurfaceWeatherStations(SparkTask):

    def __init__(self, ncei_data_folder: str):
        super().__init__("surface-stations")
        self.ncei_data_folder = ncei_data_folder

    def read(self, spark: SparkSession) -> DataSet:

        spark.sql("CREATE SCHEMA IF NOT EXISTS noaa")

        raw = (
            spark
            .read
            .csv(f"{self.ncei_data_folder}/ghcnd-stations.csv")
            .withColumnsRenamed({
                "_c0": "ghcn_id", "_c1": "lat", "_c2": "long",
                "_c3": "elevation", "_c4": "state", "_c5": "name",
                "_c6": "gsn", "_c7": "hcn_crn", "_c8": "wmo_id"
            })
        )

        raw.show()

        return DataSet([
            DataTable("noaa", "raw_global_stations", raw, "overwrite")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        network_code_map = {
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
        network_name_udf = F.udf(lambda x: network_code_map.get(x), StringType())

        raw = (
            read_data.get_table("raw_global_stations").df
            .withColumn("country_code", F.left("ghcn_id", F.lit(2)))
            .withColumn("network_type_id", F.substring("ghcn_id", 2, 1))
            .withColumn("network_name", network_name_udf(F.col("network_type_id")))
            .withColumn("wban_id", F.when(F.col("network_type_id") == "W", F.right("ghcn_id", F.lit(5))))
        )

        stations = [(r.ghcn_id, (r.lat, r.long)) for r in raw.collect()]
        ids = [x[0] for x in stations]
        coords = [x[1] for x in stations]

        lookups = zip(ids, reverse_geocode.search(coords))
        lookup_df = spark.createDataFrame(
            data=lookups,
            schema="ghcn_id string, data map<string, string>"
        ).persist()

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

        lookup_df = lookup_df.drop("data")

        cast_type = {
            "lat": "float", "long": "float",
            "latitude": "float", "longitude": "float",
            "elevation": "long", "population": "long"
        }

        with_geo_data = (
            raw
            .drop("state")
            .join(lookup_df, "ghcn_id", "left")
            .withColumns({k: F.col(k).cast(v) for k, v in cast_type.items()})
            .withColumnsRenamed({"latitude": "city_lat", "longitude": "city_long"})
        )

        return DataSet([
            DataTable("noaa", "global_stations", with_geo_data, "overwrite"),
            read_data.get_table("raw_global_stations")
        ])


# class CityWeatherStations(SparkTask):
#
#     def __init__(self):
#         super().__init__("city-weather-stations")
#
#     def read(self, spark: SparkSession) -> DataSet:
#
#         return DataSet([
#             DataTable("noaa", "global_stations")
#         ])
#
#     def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:
#
#         stations = read_data.get_table("global_stations").df
#
#         city_lat_long = (
#             stations
#             .groupby("city", "state", "country")
#             .agg(
#                 F.element_at(F.collect_list("city_lat"), 1).alias("city_lat"),
#                 F.element_at(F.collect_list("city_long"), 1).alias("city_long")
#             )
#         )
#
#         cities = (
#             stations
#             .groupby("city", "state", "country")
#             .agg(
#                 F.collect_set("ghcn_id").alias("station_ids")
#             )
#             .join(city_lat_long, ["city", "state", "country"])
#             .withColumn("city_id", F.monotonically_increasing_id())
#         )
#
#         return DataSet([DataTable("noaa", "city_stations", cities, "overwrite")])


import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint


class StationClusters(SparkTask):

    def __init__(self):
        super().__init__("station-clusters")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("noaa", "global_stations")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        stations = (
            read_data.get_table("global_stations").df
            .select("ghcn_id", "lat", "long")
            .toPandas()
        )
        coords = stations.as_matrix(columns=['lat', 'long'])

        max_cluster_size_km = 30
        kms_per_radian = 6371.0088
        epsilon = max_cluster_size_km / kms_per_radian

        min_samples = 3

        db = DBSCAN(
            eps=epsilon,
            min_samples=min_samples,
            algorithm='ball_tree',
            metric='haversine'
        )\
        .fit(np.radians(coords))

        cluster_labels = db.labels_
        num_clusters = len(set(cluster_labels))
        clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
        print('Number of clusters: {}'.format(num_clusters))

        return None
