from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame

from pdp.data import DataSet, merge_datasets
from pdp.spark import SharedSpark


class SparkJob(SharedSpark, ABC):

    def __init__(self, app_name: str):
        super().__init__(app_name)

    @abstractmethod
    def read(self) -> DataSet:
        pass

    @abstractmethod
    def transform(self, data: DataSet) -> DataSet:
        pass

    @abstractmethod
    def write(self, data: DataSet):
        pass

    def run(self):
        read_db = self.read()
        transform_db = self.transform(read_db)
        # Combine read data with transformed data in
        # case read data needs to be written
        write_db = merge_datasets([read_db, transform_db])
        self.write(write_db)
