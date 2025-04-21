
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame


@dataclass
class DataTable:
    schema: str
    name: str
    df: DataFrame = None
    mode: str = 'none'

    @property
    def full_table_name(self) -> str:
        return f"{self.schema}.{self.name}"

    def read_table(self, spark: SparkSession, exists_ok: bool = False):
        if exists_ok and not self.df is None:
            raise Exception(f"Dataframe for {self.full_table_name} already exists")
        self.df = spark.table(self.full_table_name)

    def write_table(self):

        if self.schema == '':
            raise Exception('Schema must be defined to write table')
        if self.mode == 'none':
            raise Exception('Mode must be defined to write table')

        self.df.write.mode(self.mode).saveAsTable(self.full_table_name)


class DataSet:

    def __init__(self, data: list[DataTable]):
        self.tables = {d.name: d for d in data}

    def get_table(self, name: str):
        table = self.tables.get(name)
        if table is None:
            raise KeyError(f"Table {name} not found")
        return table

    def read_empty_tables(self, spark: SparkSession):
        for table in self.tables.values():
            if table.df is None:
                table.read_table(spark)

    def write_table(self, name: str):
        table = self.get_table(name)
        print(f"Writing table: {table.full_table_name} ({table.mode})")
        table.write_table()

    def write_all_tables(self):
        for table_name in self.tables.keys():
            self.write_table(table_name)

    def write_jdbc(self, table_name: str):
        table = self.get_table(table_name)
        (
            table
            .df.write
            .mode(table.mode)
            .jdbc(
                url=f"jdbc:mysql://10.0.0.85:3306/{table.schema}?permitMysqlScheme",
                table=f"{table.name}",
                properties={"user": "ross", "password": "p@sswor!", "driver": "org.mariadb.jdbc.Driver"}
            )
        )

    def write_all_jdbc(self):
        for table_name in self.tables.keys():
            self.write_jdbc(table_name)


def merge_datasets(datasets: list[DataSet]) -> DataSet:
    merged_tables = [table for dataset in datasets for table in dataset.tables.values()]
    return DataSet(merged_tables)
