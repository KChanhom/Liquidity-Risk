from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath("src"))

from liquidity_risk.config import load_config
from liquidity_risk.datasets.synthetic import (
    generate_balances,
    generate_cashflows,
    generate_collateral,
    generate_limits,
    generate_transactions,
)
from liquidity_risk.io import ensure_dir, write_parquet_partitioned
from liquidity_risk.spark import build_spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to YAML config")
    args = ap.parse_args()

    cfg = load_config(args.config)
    spark = build_spark("liquidity-risk-generate")

    tz = cfg.time_buckets.get("timezone", "UTC")
    spark.conf.set("spark.sql.session.timeZone", tz)

    ensure_dir(cfg.raw_path)

    syn = cfg.synthetic
    seed = int(syn.get("seed", 42))
    as_of_date = syn.get("as_of_date", "2026-01-23")
    days = int(syn.get("days", 3))
    tx_per_day = int(syn.get("tx_per_day", 20000))
    counterparties = int(syn.get("counterparties", 250))
    accounts = int(syn.get("accounts", 2000))
    currencies = list(syn.get("currencies", ["THB"]))

    tx = generate_transactions(
        spark,
        seed=seed,
        as_of_date=as_of_date,
        days=days,
        tx_per_day=tx_per_day,
        accounts=accounts,
        counterparties=counterparties,
        currencies=currencies,
        timezone=tz,
    )
    bal = generate_balances(spark, seed=seed, as_of_date=as_of_date, days=days, accounts=accounts, currencies=currencies)
    cf = generate_cashflows(
        spark, seed=seed, as_of_date=as_of_date, days=days, counterparties=counterparties, currencies=currencies
    )
    col = generate_collateral(
        spark, seed=seed, as_of_date=as_of_date, days=days, counterparties=counterparties, currencies=currencies
    )
    lim = generate_limits(
        spark, seed=seed, as_of_date=as_of_date, days=days, counterparties=counterparties, currencies=currencies
    )

    write_parquet_partitioned(tx, f"{cfg.raw_path}/transactions", partitions=["value_date"])
    write_parquet_partitioned(bal, f"{cfg.raw_path}/balances", partitions=["as_of_date"])
    write_parquet_partitioned(cf, f"{cfg.raw_path}/cashflows", partitions=["as_of_date"])
    write_parquet_partitioned(col, f"{cfg.raw_path}/collateral", partitions=["as_of_date"])
    write_parquet_partitioned(lim, f"{cfg.raw_path}/limits", partitions=["as_of_date"])

    spark.stop()


if __name__ == "__main__":
    main()

