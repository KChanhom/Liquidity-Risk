from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def write_parquet_partitioned(df: DataFrame, path: str, partitions: list[str]) -> None:
    (df.write.mode("overwrite").partitionBy(*partitions).parquet(path))


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)

