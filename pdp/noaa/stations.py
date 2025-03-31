from pdp.data import DataSet, DataTable
from pdp.job import SparkJob

import pyspark.sql.functions as F

import reverse_geocode


class SurfaceWeatherStations(SparkJob):

    def __init__(self, ncei_data_folder: str):
        super().__init__("surface-stations")
        self.ncei_data_folder = ncei_data_folder

    def read(self) -> DataSet:

        self.spark.sql("CREATE SCHEMA IF NOT EXISTS noaa")

        raw = (
            self.spark
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
            DataTable("raw_global_stations", raw, "noaa")
        ])

    def transform(self, data: DataSet) -> DataSet:

        def lookup_map(lat: float, long: float) -> dict:
            coordinates = lat, long
            return reverse_geocode.get(coordinates)
        lookup_udf = F.udf(lookup_map)

        raw = data.get_table("raw_global_stations").df

        with_geo_data = (
            raw
            .withColumn("lookup", lookup_udf("lat", "long"))
            .select(
                F.col("ghcn_id"), F.col("wmo_id"), F.col("name"),
                F.col("lat"), F.col("long"), F.col("elevation"),
                F.col("lookup")
            )
        )

        return DataSet([
            DataTable("global_stations", with_geo_data, "noaa")
        ])

    def write(self, data: DataSet):
        data.write_all_tables("overwrite")
