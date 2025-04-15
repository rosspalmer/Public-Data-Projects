from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructType

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
            .withColumn("name", F.trim("name"))
            .withColumn("country_code", F.left("ghcn_id", F.lit(2)))
            .withColumn("network_id", F.substring("ghcn_id", 3, 1))
            .withColumn("network_name", network_name_udf(F.col("network_id")))
            .withColumn("wban_id", F.when(F.col("network_id") == "W", F.right("ghcn_id", F.lit(5))))
        )

        stations = [(r.ghcn_id, (r.lat, r.long))
                    for r in raw.select("ghcn_id", "lat", "long").collect()]
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

        stations = read_data.get_table("global_stations").df.persist()

        network_types = ["W", "E", "M", "N", "0"]

        cluster_assignments = None
        for n in network_types:
            network_cluster_assignments = self._cluster_stations(spark, stations, n)
            if cluster_assignments is not None:
                cluster_assignments = cluster_assignments.unionByName(network_cluster_assignments)
            else:
                cluster_assignments = network_cluster_assignments
        cluster_assignments = cluster_assignments.persist()

        cluster_stats = (
            cluster_assignments
            .select("cluster_id", "network_id", F.explode("stations").alias("stations"))
            .groupby("cluster_id", "network_id")
            .agg(
                F.count("cluster_id").alias("station_count"),
                F.avg(F.col("stations").getField("lat")).alias("avg_lat"),
                F.avg(F.col("stations").getField("long")).alias("avg_long")
            )
        ).persist()

        centers = [(r.cluster_id, r.network_id, (r.avg_lat, r.avg_long))
                    for r in cluster_stats.select("cluster_id", "avg_lat", "avg_long").collect()]
        ids = [(x[0], x[1]) for x in centers]
        coords = [x[2] for x in centers]

        lookups = reverse_geocode.search(coords)
        lookup_df = spark.createDataFrame(
            data=[(ids[i][0], ids[i][1], lookups[i]) for i in range(len(ids))],
            schema="cluster_id string, network_id string, data map<string, string>"
        ).persist()

        station_clusters = (
            cluster_assignments
            .join(cluster_stats, ["cluster_id", "network_id"], "left")
            .join(lookup_df, ["cluster_id", "network_id"], "left")
        )

        # def get_centermost_point(cluster):
        #     centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        #     centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
        #     return tuple(centermost_point)


        return DataSet([
            DataTable("noaa", "station_clusters", station_clusters, "overwrite"),
        ])

    def _cluster_stations(self, spark: SparkSession, stations: DataFrame, network_id: str) -> DataFrame:

        station_coords: pd.DataFrame = (
            stations
            .filter(F.col("network_id") == network_id)
            .select("ghcn_id", "lat", "long")
            .toPandas()
        )

        max_cluster_size_km = 10
        kms_per_radian = 6371.0088
        epsilon = max_cluster_size_km / kms_per_radian

        min_samples = 1

        db = DBSCAN(
            eps=epsilon,
            min_samples=min_samples,
            algorithm='ball_tree',
            metric='haversine'
        )

        numpy_coords = station_coords[['lat', 'long']].to_numpy()
        cluster_assignments = db.fit_predict(np.radians(numpy_coords))

        cluster_labels = db.labels_
        num_clusters = len(set(cluster_labels))

        print(f'Fit for {network_id} network')
        print(f'Number of stations: {len(station_coords)}')
        print(f'Number of clusters: {num_clusters}')

        station_coords['cluster_id'] = cluster_assignments

        station_network_clusters = (
            spark.createDataFrame(station_coords[['cluster_id', 'ghcn_id']])
            .join(stations, "ghcn_id")
            .withColumn("station_data", F.struct(
                F.col("ghcn_id"),
                F.col("name"),
                F.col("lat"),
                F.col("long")
            ))
            .groupby("cluster_id")
            .agg(
                F.collect_list("station_data").alias("stations")
            )
            .withColumn("network_id", F.lit(network_id))
        )

        return station_network_clusters
