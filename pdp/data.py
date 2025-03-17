from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class DataTable:
    name: str
    df: DataFrame
    schema: str = ''

    def write_table(self, mode: str = 'append'):
        if self.schema == '':
            raise Exception('Schema must be defined to write table')
        full_table_name = f"{self.schema}.{self.name}"
        self.df.write.mode(mode).saveAsTable(full_table_name)


class DataSet:

    def __init__(self, data: list[DataTable]):
        self.tables = {d.name: d for d in data}

    def get_table(self, name: str):
        table = self.tables.get(name)
        if table is None:
            raise KeyError(f"Table {name} not found")
        return table

    def write_table(self, name: str):
        pass


def merge_datasets(datasets: list[DataSet]) -> DataSet:
    merged_tables = [table for dataset in datasets for table in dataset.tables.values()]
    return DataSet(merged_tables)
