from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath("src"))

from pyspark.sql import functions as F

from liquidity_risk.config import load_config
from liquidity_risk.dq import check_no_duplicates, check_no_nulls, check_rowcount_between
from liquidity_risk.features import concentration_top_counterparties, intraday_time_bucket, maturity_ladder, net_cashflow_by_bucket
from liquidity_risk.io import ensure_dir, read_parquet, write_parquet_partitioned
from liquidity_risk.monitoring import (
    build_intraday_snapshot,
    daily_liquidity_drop_flag,
    daily_snapshot_from_intraday,
    flag_concentration_anomaly,
    flag_liquidity_drop_anomaly,
)
from liquidity_risk.spark import build_spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to YAML config")
    args = ap.parse_args()

    cfg = load_config(args.config)
    spark = build_spark("liquidity-risk-monitoring")

    tz = cfg.time_buckets.get("timezone", "UTC")
    granularity = cfg.time_buckets.get("granularity", "hour")
    spark.conf.set("spark.sql.session.timeZone", tz)

    perf = cfg.performance
    if perf.get("cache_intermediate", True):
        spark.conf.set("spark.sql.adaptive.enabled", "true")

    # ---- read raw ----
    tx = read_parquet(spark, f"{cfg.raw_path}/transactions")
    cf = read_parquet(spark, f"{cfg.raw_path}/cashflows")
    bal = read_parquet(spark, f"{cfg.raw_path}/balances")
    col = read_parquet(spark, f"{cfg.raw_path}/collateral")
    lim = read_parquet(spark, f"{cfg.raw_path}/limits")

    # ---- basic DQ checks (illustrative) ----
    dq = []
    dq.append(check_no_nulls(tx, ["tx_id", "event_ts", "value_date", "account_id", "currency", "direction", "amount"], "tx_no_nulls"))
    dq.append(check_no_duplicates(tx, ["tx_id"], "tx_pk_unique"))
    dq.append(check_rowcount_between(tx, 1, 10_000_000, "tx_volume_sanity"))
    dq.append(check_no_nulls(cf, ["cf_id", "as_of_date", "maturity_date", "counterparty_id", "currency", "direction", "amount"], "cf_no_nulls"))
    dq.append(check_no_duplicates(cf, ["cf_id"], "cf_pk_unique"))
    dq.append(check_no_nulls(bal, ["as_of_date", "account_id", "currency", "balance"], "bal_no_nulls"))
    dq.append(check_no_duplicates(bal, ["as_of_date", "account_id", "currency"], "bal_key_unique"))
    dq.append(check_no_nulls(col, ["as_of_date", "collateral_id", "counterparty_id", "currency", "market_value"], "col_no_nulls"))
    dq.append(check_no_duplicates(col, ["collateral_id"], "col_pk_unique"))
    dq.append(check_no_nulls(lim, ["as_of_date", "counterparty_id", "limit_type", "currency", "limit_amount"], "lim_no_nulls"))
    dq.append(check_no_duplicates(lim, ["as_of_date", "counterparty_id", "limit_type", "currency"], "lim_key_unique"))

    # write dq results as a tiny parquet
    dq_df = spark.createDataFrame([(r.check_name, r.passed, r.details) for r in dq], "check_name string, passed boolean, details string")
    ensure_dir(cfg.out_path)
    dq_df.write.mode("overwrite").parquet(f"{cfg.out_path}/dq_results")

    # ---- intraday buckets & net cashflow ----
    tx_b = intraday_time_bucket(tx, timezone=tz, granularity=granularity)

    # performance pattern: repartition by date to reduce skew in wide aggregations
    repart = int(perf.get("repartition_by_date", 0) or 0)
    if repart > 0:
        tx_b = tx_b.repartition(repart, "value_date")

    if perf.get("cache_intermediate", True):
        tx_b = tx_b.cache()
        tx_b.count()  # materialize cache (demo purpose)

    net_b = net_cashflow_by_bucket(tx_b)
    intraday = build_intraday_snapshot(net_b)

    # anomaly: liquidity drop
    z_thresh = float(cfg.monitoring.get("liquidity_drop_zscore_threshold", 3.0))
    intraday_flags = flag_liquidity_drop_anomaly(intraday, z_thresh=z_thresh)
    daily_liq_flags = daily_liquidity_drop_flag(intraday_flags)

    # ---- concentration ----
    top_n = int(cfg.monitoring.get("top_counterparties_n", 10))
    conc_top = concentration_top_counterparties(tx_b, top_n=top_n)
    conc_flags = flag_concentration_anomaly(
        conc_top, share_threshold=float(cfg.monitoring.get("concentration_share_threshold", 0.20))
    )

    # ---- maturity ladder (future cashflows) ----
    ladder = maturity_ladder(cf)

    # ---- limits utilization (illustrative settlement limit) ----
    outflow_cp = (
        tx_b.filter(F.col("direction") == F.lit("OUT"))
        .groupBy("value_date", "currency", "counterparty_id")
        .agg(F.sum("amount").alias("cp_outflow"))
    )
    lim_settle = lim.filter(F.col("limit_type") == F.lit("settlement")).withColumnRenamed("as_of_date", "value_date")
    util = (
        outflow_cp.join(lim_settle, ["value_date", "currency", "counterparty_id"], "left")
        .withColumn("utilization", F.when(F.col("limit_amount") > 0, F.col("cp_outflow") / F.col("limit_amount")).otherwise(F.lit(None)))
    )
    warn_th = float(cfg.monitoring.get("limit_warning_threshold", 0.90))
    breach_th = float(cfg.monitoring.get("limit_breach_threshold", 1.00))
    util = (
        util.withColumn("flag_limit_warning", F.col("utilization") >= F.lit(warn_th))
        .withColumn("flag_limit_breach", F.col("utilization") >= F.lit(breach_th))
    )
    daily_limit_flags = util.groupBy("value_date", "currency").agg(
        F.max(F.col("flag_limit_warning").cast("int")).cast("boolean").alias("flag_limit_warning_any"),
        F.max(F.col("flag_limit_breach").cast("int")).cast("boolean").alias("flag_limit_breach_any"),
        F.max("utilization").alias("max_utilization"),
    )

    # ---- balances + eligible collateral (simple liquidity proxy) ----
    bal_sum = bal.groupBy("as_of_date", "currency").agg(F.sum("balance").alias("total_balance")).withColumnRenamed("as_of_date", "value_date")
    col_sum = (
        col.filter(F.col("eligible_lcr") == F.lit(1))
        .withColumn("eligible_value", F.col("market_value") * (F.lit(1.0) - F.col("haircut_pct")))
        .groupBy("as_of_date", "currency")
        .agg(F.sum("eligible_value").alias("eligible_collateral_value"))
        .withColumnRenamed("as_of_date", "value_date")
    )
    liquidity_base = bal_sum.join(col_sum, ["value_date", "currency"], "left")

    # ---- daily snapshot ----
    daily = (
        daily_snapshot_from_intraday(intraday_flags)
        .join(daily_liq_flags, ["value_date", "currency"], "left")
        .join(daily_limit_flags, ["value_date", "currency"], "left")
        .join(liquidity_base, ["value_date", "currency"], "left")
        .join(
        conc_flags.select("value_date", "currency", "flag_concentration", "top1_share", "topN_share", "topN"),
        on=["value_date", "currency"],
        how="left",
        )
    )

    # compress boolean flags into a single string for quick scan
    daily = daily.withColumn(
        "anomaly_flags",
        F.array_compact(
            F.array(
                F.when(F.col("flag_concentration") == True, F.lit("CONCENTRATION")).otherwise(F.lit(None)),
                F.when(F.col("flag_liquidity_drop_any") == True, F.lit("LIQUIDITY_DROP")).otherwise(F.lit(None)),
                F.when(F.col("flag_limit_breach_any") == True, F.lit("LIMIT_BREACH")).otherwise(F.lit(None)),
            ),
        ),
    )

    # ---- write outputs (Parquet + partitions) ----
    ensure_dir(cfg.out_path)
    write_parquet_partitioned(intraday_flags, f"{cfg.out_path}/intraday_snapshots", partitions=["value_date", "currency"])
    write_parquet_partitioned(daily, f"{cfg.out_path}/daily_snapshots", partitions=["value_date"])
    write_parquet_partitioned(conc_top, f"{cfg.out_path}/concentration_top_counterparties", partitions=["value_date", "currency"])
    write_parquet_partitioned(ladder, f"{cfg.out_path}/maturity_ladder", partitions=["as_of_date", "currency"])
    write_parquet_partitioned(util, f"{cfg.out_path}/limit_utilization", partitions=["value_date", "currency"])

    spark.stop()


if __name__ == "__main__":
    main()

