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


class WeatherAreas(SparkTask):

    def __init__(self):
        super().__init__("weather-areas")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("weather", "station")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        stations = read_data.get_table("station").df.persist()

        # network_types = [n for n in SurfaceWeatherStations.NETWORK_ID_NAMES.keys()]
        network_types = ["W"]

        area_assignments = None
        for n in network_types:
            network_area_assignments = self._area_stations(spark, stations, n)
            if area_assignments is not None:
                area_assignments = area_assignments.unionByName(network_area_assignments)
            else:
                area_assignments = network_area_assignments
        area_assignments = area_assignments.persist()

        def calculate_area_center(keys: Any, group: pd.DataFrame) -> pd.DataFrame:
            coord_list = list(zip(group["latitude"].tolist(), group["longitude"].tolist()))
            mp = MultiPoint(coord_list)
            centroid = (mp.centroid.x, mp.centroid.y)
            centermost_point = min(coord_list, key=lambda point: great_circle(point, centroid).m)
            df = pd.DataFrame({
                "area_id": keys[0],
                "center_lat": [centermost_point[0]],
                "center_long": [centermost_point[1]]
            })
            return df

        area_centers = (
            area_assignments
            .select("area_id", F.explode("station_ids").alias("station_id"))
            .join(stations.select("station_id", "latitude", "longitude"), "station_id")
            .groupby("area_id")
            .applyInPandas(
                calculate_area_center,
                "area_id string, center_lat float, center_long float"
            )
        ).persist()

        areas = (
            area_assignments
            .withColumn("stations_count", F.size("station_ids"))
            .join(area_centers, "area_id", "left")
        )

        return DataSet([
            DataTable("weather", "area", areas, "overwrite"),
        ])

    def _group_stations(self, spark: SparkSession, stations: DataFrame, network_id: str) -> DataFrame:

        station_coords: pd.DataFrame = (
            stations
            .filter(F.col("network_id") == network_id)
            .select("station_id", "latitude", "longitude")
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

        numpy_coords = station_coords[['latitude', 'longitude']].to_numpy()
        area_assignments = db.fit_predict(np.radians(numpy_coords))

        group_labels = db.labels_
        num_areas = len(set(group_labels))

        print(f'Fit for {network_id} network')
        print(f'Number of stations: {len(station_coords)}')
        print(f'Number of areas: {num_areas}')

        station_coords['area_id'] = area_assignments

        station_network_areas = (
            spark.createDataFrame(station_coords[['area_id', 'station_id']])
            .join(stations, "station_id")
            .groupby("area_id")
            .agg(
                F.collect_set("station_id").alias("station_ids")
            )
            .withColumn("area_id", F.concat(F.lit(f"{network_id}-"), F.col("area_id")))
            .withColumn("network_id", F.lit(network_id))
        )

        return station_network_areas

class WeatherAreasFrontend(SparkTask):

    def __init__(self):
        super().__init__("station-groups-frontend")

    def read(self, spark: SparkSession) -> DataSet:
        tables = [
            DataTable("weather", "station"),
            DataTable("weather", "area")
        ]
        return DataSet(tables)

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        stations = read_data.get_table("station").df
        areas = read_data.get_table("area").df

        area_stations = areas.select(
            "area_id",
            F.explode("station_ids").alias("station_id")
        )

        areas = areas.select("area_id", "network_id", "center_lat", "center_long")

        write_data = [
            DataTable("weather", "station", stations, "overwrite"),
            DataTable("weather", "area", areas, "overwrite"),
            DataTable("weather", "area_stations", area_stations, "overwrite"),
        ]

        return DataSet(write_data)

    def write(self, write_dataset: DataSet):
        write_dataset.write_all_jdbc()
