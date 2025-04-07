from abc import ABC, abstractmethod

from pdp.data.data import DataSet
from pdp.data.spark import SharedSpark


class SparkJob(SharedSpark, ABC):

    def __init__(self, app_name: str):
        super().__init__(app_name)

    @abstractmethod
    def read(self) -> DataSet:
        pass

    @abstractmethod
    def transform(self, read_data: DataSet) -> DataSet:
        pass

    def run(self):

        read_dataset: DataSet = self.read()
        read_dataset.read_empty_tables(self.spark)

        write_dataset: DataSet = self.transform(read_dataset)
        write_dataset.write_all_tables()
