
from pyspark.sql import SparkSession


class SharedSpark:
    # HOST = "spark://spark-driver:7077"

    def __init__(self, app_name: str):

        self.spark = (
           SparkSession.builder
           .master("spark:/10.0.0.2:7077")
           .appName(app_name)
           .config("spark.sql.warehouse.dir", "file:/mnt/lake-fs/spark-warehouse")
           .config("spark.databricks.delta.schema.autoMerge.enabled", True)
           .enableHiveSupport()
           .getOrCreate()
        )

