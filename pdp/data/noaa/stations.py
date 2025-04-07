from pdp.data import DataSet, DataTable
from pdp.data.job import SparkJob

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
            DataTable("noaa", "raw_global_stations", raw, "overwrite")
        ])

    def transform(self, read_data: DataSet) -> DataSet:

        raw = read_data.get_table("raw_global_stations").df
        stations = [(r.ghcn_id, (r.lat, r.long)) for r in raw.collect()]
        ids = [x[0] for x in stations]
        coords = [x[1] for x in stations]

        lookups = zip(ids, reverse_geocode.search(coords))
        lookup_df = self.spark.createDataFrame(
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
        )]

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
        )

        return DataSet([
            DataTable("noaa", "global_stations", with_geo_data, "overwrite"),
            read_data.get_table("raw_global_stations")
        ])
