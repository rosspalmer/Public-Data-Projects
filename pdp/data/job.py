from abc import ABC, abstractmethod
import sys

from collections import deque


from pyspark.sql import SparkSession

from pdp.data.data import DataSet
from pdp.data.spark import SharedSpark


class SparkJob:

    def __init__(self, name: str):
        self.name = name
        self.spark: SparkSession | None = None
        self.task_dag: TaskDAG = None

    def __enter__(self):
        self.spark = (
           SparkSession.builder
           .master("spark://10.0.0.2:7077")
           .appName(self.name)
           .config("spark.sql.warehouse.dir", "file:/mnt/lake-fs/spark-warehouse")
           .config("spark.databricks.delta.schema.autoMerge.enabled", True)
           .enableHiveSupport()
           .getOrCreate()
        )


    def __exit__(self, exc_type, exc_value, traceback):

        self.file.close()


class SparkTask(SharedSpark, ABC):

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

# Mocking Airflow paterrn from here: task-sdk/src/airflow/sdk/definitions/_internal/contextmanager.py
# class ContextStack(Generic[T], metaclass=ContextStackMeta):
#     _context: deque[T]
#
#     @classmethod
#     def push(cls, obj: T):
#         cls._context.appendleft(obj)
#
#     @classmethod
#     def pop(cls) -> T | None:
#         return cls._context.popleft()
#
#     @classmethod
#     def get_current(cls) -> T | None:
#         try:
#             return cls._context[0]
#         except IndexError:
#             return None


class JobContext(Generic[SparkJob], metaclass=ContextStackMeta):
    _job_queue: deque[SparkJob]
    autoregistered_dags: set[tuple[SparkJob, ModuleType]] = set()
    current_autoregister_module_name: str | None = None

    @classmethod
    def pop(cls) -> SparkJob | None:
        job = cls._job_queue.popleft()
        # In a few cases around serialization we explicitly push None in to the stack
        if cls.current_autoregister_module_name is not None and job and getattr(job, "auto_register", True):
            mod = sys.modules[cls.current_autoregister_module_name]
            cls.autoregistered_dags.add((job, mod))
        return job

    @classmethod
    def get_current_job(cls) -> SparkJob | None:
        try:
            return cls._job_queue[0]
        except IndexError:
            return None