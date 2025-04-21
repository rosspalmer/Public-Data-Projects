from abc import ABC, abstractmethod
from collections import deque
from types import ModuleType
from typing import TYPE_CHECKING, Any, Generic, TypeVar
import sys


from pyspark.sql import SparkSession

from pdp.data.data import DataSet


class SparkTask(ABC):

    def __init__(self, task_id: str):
        self.task_id = task_id
        job = JobContext.get_current_job()
        if job is not None:
            job.add_task(self)
        else:
            print(f'WARN task not added to job')

    @abstractmethod
    def read(self, spark: SparkSession) -> DataSet:
        pass

    @abstractmethod
    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:
        pass

    def run(self, spark: SparkSession):

        read_dataset: DataSet = self.read(spark)
        read_dataset.read_empty_tables(spark)

        write_dataset: DataSet = self.transform(spark, read_dataset)
        self.write(write_dataset)

    def write(self, write_dataset: DataSet):
        write_dataset.write_all_tables()


class SparkJob:

    def __init__(self, name: str):
        self.name = name
        self.spark: SparkSession | None = None
        self.tasks: list[SparkTask] = list()
        self.task_dependencies: list[tuple[str, str]] = []

    def __enter__(self):
        self.spark = (
           SparkSession.builder
           .master("spark://10.0.0.2:7077")
           .appName(self.name)
           .config("spark.log.level", "ERROR")
           .config("spark.jars", "mariadb-java-client-3.5.3.jar")
           .config("spark.driver.maxResultSize", "4G")
           .config("spark.sql.warehouse.dir", "file:/mnt/lake-fs/spark-warehouse")
           .config("spark.databricks.delta.schema.autoMerge.enabled", True)
           .enableHiveSupport()
           .getOrCreate()
        )
        # self.spark = (
        #     SparkSession.builder
        #     .master("local")
        #     .appName(self.name)
        #     .getOrCreate()
        # )
        JobContext.push(self)

    def __exit__(self, exc_type, exc_value, traceback):

        for task in self.tasks:
            print(task.task_id)
            task.run(self.spark)

        # TODO
        self.spark.stop()

    def add_task(self, task: SparkTask):
        self.tasks.append(task)

    def add_task_dependency(self, task_before: SparkTask, task_after: SparkTask):
        self.task_dependencies.append((task_before.task_id, task_after.task_id))


# Mocking Airflow pattern from here: task-sdk/src/airflow/sdk/definitions/_internal/contextmanager.py

# In order to add a `@classproperty`-like thing we need to define a property on a metaclass.
class ContextStackMeta(type):
    _context: deque

    # TODO: Task-SDK:
    # share_parent_context can go away once the DAG and TaskContext manager in airflow.models are removed and
    # everything uses sdk fully for definition/parsing
    def __new__(cls, name, bases, namespace, share_parent_context: bool = False, **kwargs: Any):
        if not share_parent_context:
            namespace["_context"] = deque()

        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)

        return new_cls

    @property
    def active(self) -> bool:
        """The active property says if any object is currently in scope."""
        return bool(self._context)


class JobContext(metaclass=ContextStackMeta):
    _context: deque[SparkJob]
    autoregistered_dags: set[tuple[SparkJob, ModuleType]] = set()
    current_autoregister_module_name: str | None = None

    @classmethod
    def pop(cls) -> SparkJob | None:
        job = cls._context.popleft()
        # In a few cases around serialization we explicitly push None in to the stack
        if cls.current_autoregister_module_name is not None and job and getattr(job, "auto_register", True):
            mod = sys.modules[cls.current_autoregister_module_name]
            cls.autoregistered_dags.add((job, mod))
        return job

    @classmethod
    def push(cls, job: SparkJob):
        cls._context.appendleft(job)

    @classmethod
    def get_current_job(cls) -> SparkJob | None:
        try:
            return cls._context[0]
        except IndexError:
            return None