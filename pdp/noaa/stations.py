from pdp.data import DataSet, DataTable
from pdp.job import SparkJob


class SurfaceWeatherStations(SparkJob):

    def __init__(self, ncei_data_folder: str):
        super().__init__("surface-stations")
        self.ncei_data_folder = ncei_data_folder

    def read(self) -> DataSet:

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
            DataTable("raw_stations", raw, "noaa")
        ])

    def transform(self, data: DataSet) -> DataSet:

        raw = data.get_table("raw_stations")

        return data

    def write(self, data: DataSet):
        pass