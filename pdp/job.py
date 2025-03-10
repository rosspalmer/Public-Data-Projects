from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame

from pdp.spark import SharedSpark

class DataSource:
    pass


@dataclass
class JobInputFile(DataSource):
    name: str
    file_path: str


@dataclass
class DataTable(DataSource):
    catalog: str
    schema: str
    table: str
    df: DataFrame


class DataJob(SharedSpark, ABC):

    def __init__(self, app_name: str):
        super().__init__(app_name)

    @abstractmethod
    def run(self) -> list[DataTable]:
        pass
