from typing import Any

import numpy as np
import pandas as pd
from geopy.distance import great_circle

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from shapely import MultiPoint
from sklearn.cluster import DBSCAN

from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkTask
from pdp.data.weather.stations import SurfaceWeatherStations


class WeatherAreas(SparkTask):

    def __init__(self):
        super().__init__("weather-areas")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("weather", "global_stations")
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

        station_groups = (
            group_assignments
            .withColumn("stations_count", F.size("station_ids"))
            .join(group_centers, "group_id", "left")
        )

        return DataSet([
            DataTable("weather", "station_groups", station_groups, "overwrite"),
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