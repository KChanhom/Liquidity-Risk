from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def build_intraday_snapshot(df_net_bucket: DataFrame) -> DataFrame:
    """
    Convert bucket-level net cashflow into cumulative liquidity index intraday per (value_date, currency).
    """
    w = Window.partitionBy("value_date", "currency").orderBy(F.col("bucket_start_ts").asc())
    return (
        df_net_bucket.withColumn("cum_net_cashflow", F.sum("net_cashflow").over(w))
        .withColumn("min_cum_net_cashflow", F.min("cum_net_cashflow").over(w))
        .withColumn("max_cum_net_cashflow", F.max("cum_net_cashflow").over(w))
    )


def flag_liquidity_drop_anomaly(df_snapshot: DataFrame, z_thresh: float) -> DataFrame:
    """
    Flag abnormal drops using z-score of bucket-to-bucket delta per (currency).
    This is a simple monitoring heuristic (illustrative).
    """
    w = Window.partitionBy("currency").orderBy(F.col("value_date").asc(), F.col("bucket_start_ts").asc())
    delta = F.col("cum_net_cashflow") - F.lag("cum_net_cashflow", 1).over(w)
    base = df_snapshot.withColumn("delta_cum", delta)

    w_stats = Window.partitionBy("currency")
    mu = F.avg("delta_cum").over(w_stats)
    sigma = F.stddev_pop("delta_cum").over(w_stats)
    z = F.when(sigma > 0, (F.col("delta_cum") - mu) / sigma).otherwise(F.lit(0.0))

    return base.withColumn("delta_z", z).withColumn("flag_liquidity_drop", F.col("delta_z") <= F.lit(-abs(z_thresh)))


def flag_concentration_anomaly(df_conc_top: DataFrame, share_threshold: float) -> DataFrame:
    """
    Flag if top1 share exceeds threshold for (value_date,currency).
    """
    flags = (
        df_conc_top.select("value_date", "currency", "top1_share", "topN_share", "topN")
        .dropDuplicates(["value_date", "currency", "topN"])
        .withColumn("flag_concentration", F.col("top1_share") >= F.lit(float(share_threshold)))
    )
    return flags


def daily_snapshot_from_intraday(df_intraday_snapshot: DataFrame) -> DataFrame:
    """
    Daily aggregation: end-of-day cumulative net cashflow and worst intraday drawdown proxy.
    """
    w = Window.partitionBy("value_date", "currency").orderBy(F.col("bucket_start_ts").desc())
    eod = df_intraday_snapshot.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    dd = df_intraday_snapshot.groupBy("value_date", "currency").agg(
        F.min("cum_net_cashflow").alias("worst_cum_net_cashflow"),
        F.max("cum_net_cashflow").alias("best_cum_net_cashflow"),
    )
    return (
        eod.select("value_date", "currency", "cum_net_cashflow")
        .withColumnRenamed("cum_net_cashflow", "eod_cum_net_cashflow")
        .join(dd, ["value_date", "currency"], "left")
    )


def daily_liquidity_drop_flag(df_intraday_flags: DataFrame) -> DataFrame:
    """
    Roll up intraday liquidity-drop flags to daily level per (value_date, currency).
    """
    return df_intraday_flags.groupBy("value_date", "currency").agg(
        F.max(F.col("flag_liquidity_drop").cast("int")).cast("boolean").alias("flag_liquidity_drop_any"),
        F.min("delta_z").alias("min_delta_z"),
    )

