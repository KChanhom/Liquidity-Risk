from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def intraday_time_bucket(df_tx: DataFrame, timezone: str, granularity: str = "hour") -> DataFrame:
    """
    Add `bucket_start_ts` aligned to intraday bucket (hour supported).
    Assumes `event_ts` is a timestamp; spark session timezone should match `timezone` for correct bucketing.
    """
    if granularity != "hour":
        raise ValueError(f"Unsupported granularity={granularity}. Only 'hour' implemented.")

    # date_trunc uses session timezone for timestamps
    return df_tx.withColumn("bucket_start_ts", F.date_trunc("hour", F.col("event_ts")))


def net_cashflow_by_bucket(df_tx: DataFrame) -> DataFrame:
    """
    Net cashflow = sum(IN) - sum(OUT) by (value_date, currency, bucket_start_ts).
    """
    signed_amt = F.when(F.col("direction") == F.lit("IN"), F.col("amount")).otherwise(-F.col("amount"))
    return (
        df_tx.withColumn("signed_amount", signed_amt)
        .groupBy("value_date", "currency", "bucket_start_ts")
        .agg(
            F.sum("signed_amount").alias("net_cashflow"),
            F.sum(F.when(F.col("direction") == "IN", F.col("amount")).otherwise(F.lit(0.0))).alias("inflow"),
            F.sum(F.when(F.col("direction") == "OUT", F.col("amount")).otherwise(F.lit(0.0))).alias("outflow"),
            F.count("*").alias("tx_count"),
        )
    )


def concentration_top_counterparties(df_tx: DataFrame, top_n: int) -> DataFrame:
    """
    For each (value_date, currency), compute counterparty shares and top-N summary.
    Output includes cp_rank, cp_share, cp_net_flow, and date-level concentration metrics.
    """
    signed_amt = F.when(F.col("direction") == F.lit("IN"), F.col("amount")).otherwise(-F.col("amount"))
    cp = (
        df_tx.withColumn("signed_amount", signed_amt)
        .groupBy("value_date", "currency", "counterparty_id")
        .agg(F.sum("signed_amount").alias("cp_net_flow"), F.sum(F.abs(F.col("signed_amount"))).alias("cp_gross_flow"))
    )

    tot = cp.groupBy("value_date", "currency").agg(F.sum("cp_gross_flow").alias("total_gross_flow"))
    cp = cp.join(tot, ["value_date", "currency"], "left").withColumn(
        "cp_share", F.when(F.col("total_gross_flow") > 0, F.col("cp_gross_flow") / F.col("total_gross_flow")).otherwise(F.lit(0.0))
    )

    w = Window.partitionBy("value_date", "currency").orderBy(F.col("cp_share").desc(), F.col("counterparty_id"))
    ranked = cp.withColumn("cp_rank", F.row_number().over(w))

    top = ranked.filter(F.col("cp_rank") <= F.lit(int(top_n)))
    # date-level concentration: top1 share + topN share
    conc = (
        ranked.groupBy("value_date", "currency")
        .agg(
            F.max(F.when(F.col("cp_rank") == 1, F.col("cp_share")).otherwise(F.lit(0.0))).alias("top1_share"),
            F.sum(F.when(F.col("cp_rank") <= F.lit(int(top_n)), F.col("cp_share")).otherwise(F.lit(0.0))).alias("topN_share"),
        )
        .withColumn("topN", F.lit(int(top_n)))
    )
    return top.join(conc, ["value_date", "currency"], "left")


def maturity_bucket_expr(col_maturity_date: str, col_as_of_date: str) -> F.Column:
    d = F.datediff(F.col(col_maturity_date), F.col(col_as_of_date))
    return (
        F.when(d < 0, F.lit("past_due"))
        .when(d == 0, F.lit("t0"))
        .when((d >= 1) & (d <= 7), F.lit("t1_7d"))
        .when((d >= 8) & (d <= 30), F.lit("t8_30d"))
        .when((d >= 31) & (d <= 90), F.lit("t31_90d"))
        .otherwise(F.lit("t90d_plus"))
    )


def maturity_ladder(df_cashflows: DataFrame) -> DataFrame:
    """
    Aggregate future cashflows into maturity buckets by (as_of_date, currency, bucket).
    Net = IN - OUT
    """
    signed_amt = F.when(F.col("direction") == F.lit("IN"), F.col("amount")).otherwise(-F.col("amount"))
    with_bucket = df_cashflows.withColumn("maturity_bucket", maturity_bucket_expr("maturity_date", "as_of_date")).withColumn(
        "signed_amount", signed_amt
    )
    return (
        with_bucket.groupBy("as_of_date", "currency", "maturity_bucket")
        .agg(
            F.sum("signed_amount").alias("net_cashflow"),
            F.sum(F.when(F.col("direction") == "IN", F.col("amount")).otherwise(F.lit(0.0))).alias("inflow"),
            F.sum(F.when(F.col("direction") == "OUT", F.col("amount")).otherwise(F.lit(0.0))).alias("outflow"),
            F.count("*").alias("cf_count"),
        )
        .orderBy("as_of_date", "currency", "maturity_bucket")
    )

