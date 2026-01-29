from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str = "liquidity-risk") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # sensible local defaults; override via spark-submit flags in prod
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

