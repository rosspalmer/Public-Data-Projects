import os

import pytest
from pyspark.sql import Row, SparkSession

from pdp.utils import parse_fixed_width


def test_parse_fixed_width():

    # FIXME why aren't these passed through from underlying environment?
    os.environ['JAVA_HOME'] = '/home/ross/.sdkman/candidates/java/current'
    os.environ['SPARK_HOME'] = '/home/ross/.sdkman/candidates/spark/current'

    spark = SparkSession.builder.master("local").getOrCreate()

    text_data = spark.createDataFrame([
        Row(id=1, value="ABCDE10", other="This stuff"),
        Row(id=2, value="FGHIJ04", other="is other")
    ])

    columns = [
        ("a", 3, "string"), ("b", 2, "string"), ("c", 2, "int")
    ]

    parsed = parse_fixed_width(text_data, columns)

    assert parsed.columns == ["id", "other", "a", "b", "c"]
