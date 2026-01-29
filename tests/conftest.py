import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath("src"))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("liquidity-risk-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark
    spark.stop()

