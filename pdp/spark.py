
from pyspark.sql import SparkSession


class SharedSpark:
    HOST = "spark://spark-master:7077"

    def __init__(self):

        self.spark = (
           SparkSession.builder
           .master(SharedSpark.HOST)
           .appName("ReadGSOP")
           .config("spark.sql.warehouse.dir", "file:/mnt/lake-a/spark-warehouse")
           .enableHiveSupport()
           .getOrCreate()
        )

