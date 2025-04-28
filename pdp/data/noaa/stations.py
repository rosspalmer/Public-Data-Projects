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


class SurfaceWeatherStations(SparkTask):
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

        return DataSet([
            DataTable("noaa", "raw_global_stations", raw, "overwrite")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        network_name_map = {k: v for k, v in self.NETWORK_ID_NAMES.items()}
        network_name_udf = F.udf(lambda x: network_name_map.get(x), StringType())

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

class StationGroups(SparkTask):

    def __init__(self):
        super().__init__("station-groups")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("noaa", "global_stations")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        stations = read_data.get_table("global_stations").df.persist()

        network_types = [n for n in SurfaceWeatherStations.NETWORK_ID_NAMES.keys()]

        group_assignments = None
        for n in network_types:
            network_group_assignments = self._group_stations(spark, stations, n)
            if group_assignments is not None:
                group_assignments = group_assignments.unionByName(network_group_assignments)
            else:
                group_assignments = network_group_assignments
        group_assignments = group_assignments.persist()

        def calculate_group_center(keys: Any, group: pd.DataFrame) -> pd.DataFrame:
            coord_list = list(zip(group["lat"].tolist(), group["long"].tolist()))
            mp = MultiPoint(coord_list)
            centroid = (mp.centroid.x, mp.centroid.y)
            centermost_point = min(coord_list, key=lambda point: great_circle(point, centroid).m)
            df = pd.DataFrame({
                "group_id": keys[0],
                "center_lat": [centermost_point[0]],
                "center_long": [centermost_point[1]]
            })
            return df

        # FIXME remove debug
        group_assignments.show()

        group_centers = (
            group_assignments
            .select("group_id", F.explode("station_ids").alias("ghcn_id"))
            .join(stations.select("ghcn_id", "lat", "long"), "ghcn_id")
            .groupby("group_id")
            .applyInPandas(
                calculate_group_center,
                "group_id string, center_lat float, center_long float"
            )
        ).persist()

        # FIXME remove debug
        group_centers.show()

        station_groups = (
            group_assignments
            .withColumn("stations_count", F.size("station_ids"))
            .join(group_centers, "group_id", "left")
        )

        return DataSet([
            DataTable("noaa", "station_groups", station_groups, "overwrite"),
        ])

    def _group_stations(self, spark: SparkSession, stations: DataFrame, network_id: str) -> DataFrame:

        station_coords: pd.DataFrame = (
            stations
            .filter(F.col("network_id") == network_id)
            .select("ghcn_id", "lat", "long")
            .toPandas()
        )

        max_cluster_size_km = 20
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
        group_assignments = db.fit_predict(np.radians(numpy_coords))

        group_labels = db.labels_
        num_groups = len(set(group_labels))

        print(f'Fit for {network_id} network')
        print(f'Number of stations: {len(station_coords)}')
        print(f'Number of groups: {num_groups}')

        station_coords['group_id'] = group_assignments

        station_network_groups = (
            spark.createDataFrame(station_coords[['group_id', 'ghcn_id']])
            .join(stations, "ghcn_id")
            .groupby("group_id")
            .agg(
                F.collect_set("ghcn_id").alias("station_ids")
            )
            .withColumn("group_id", F.concat(F.lit(f"{network_id}-"), F.col("group_id")))
            .withColumn("network_id", F.lit(network_id))
        )

        return station_network_groups


class StationGroupsFrontend(SparkTask):

    def __init__(self):
        super().__init__("station-groups-frontend")

    def read(self, spark: SparkSession) -> DataSet:
        tables = [
            DataTable("noaa", "global_stations"),
            DataTable("noaa", "station_groups")
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
