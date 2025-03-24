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
        read_dataset = self.read()
        transform_dataset = self.transform(read_dataset)
        # Combine read data with transformed data in
        # case read data needs to be written
        write_db = merge_datasets([read_dataset, transform_dataset])
        self.write(write_db)
