
from pyspark.sql import SparkSession


class SharedSpark:
    HOST = "spark://spark-driver:7077"

    def __init__(self, app_name: str):

        self.spark = (
           SparkSession.builder
           .master(SharedSpark.HOST)
           .appName(app_name)
           .config("spark.sql.warehouse.dir", "file:/mnt/lake-a/spark-warehouse")
           .config("spark.databricks.delta.schema.autoMerge.enabled", True)
           .enableHiveSupport()
           .getOrCreate()
        )

