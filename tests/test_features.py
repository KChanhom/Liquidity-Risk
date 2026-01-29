from datetime import date, datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row, functions as F

from liquidity_risk.features import (
    concentration_top_counterparties,
    intraday_time_bucket,
    maturity_ladder,
    net_cashflow_by_bucket,
)


def test_net_cashflow_by_bucket_hour(spark):
    rows = [
        Row(
            tx_id="t1",
            event_ts=datetime(2026, 1, 23, 10, 5, 0),
            value_date=date(2026, 1, 23),
            account_id="A000001",
            counterparty_id="CP00001",
            currency="THB",
            direction="IN",
            amount=100.0,
            product="payment",
            channel="RTGS",
        ),
        Row(
            tx_id="t2",
            event_ts=datetime(2026, 1, 23, 10, 55, 0),
            value_date=date(2026, 1, 23),
            account_id="A000001",
            counterparty_id="CP00002",
            currency="THB",
            direction="OUT",
            amount=40.0,
            product="payment",
            channel="RTGS",
        ),
    ]
    tx = spark.createDataFrame(rows)
    tx_b = intraday_time_bucket(tx, timezone="UTC", granularity="hour")
    got = net_cashflow_by_bucket(tx_b).select("value_date", "currency", "bucket_start_ts", "net_cashflow", "inflow", "outflow", "tx_count")

    exp = spark.createDataFrame(
        [
            Row(
                value_date=date(2026, 1, 23),
                currency="THB",
                bucket_start_ts=datetime(2026, 1, 23, 10, 0, 0),
                net_cashflow=60.0,
                inflow=100.0,
                outflow=40.0,
                tx_count=2,
            )
        ]
    )
    assert_df_equality(got.orderBy("bucket_start_ts"), exp.orderBy("bucket_start_ts"), ignore_nullable=True)


def test_maturity_ladder_bucket_assignment(spark):
    rows = [
        Row(
            cf_id="c1",
            as_of_date=date(2026, 1, 23),
            maturity_date=date(2026, 1, 23),
            counterparty_id="CP00001",
            currency="THB",
            direction="OUT",
            amount=10.0,
            source="loan",
        ),
        Row(
            cf_id="c2",
            as_of_date=date(2026, 1, 23),
            maturity_date=date(2026, 1, 30),  # +7d
            counterparty_id="CP00001",
            currency="THB",
            direction="IN",
            amount=25.0,
            source="bond",
        ),
        Row(
            cf_id="c3",
            as_of_date=date(2026, 1, 23),
            maturity_date=date(2026, 2, 7),  # +15d
            counterparty_id="CP00002",
            currency="THB",
            direction="OUT",
            amount=5.0,
            source="fx_swap",
        ),
    ]
    cf = spark.createDataFrame(rows)
    got = maturity_ladder(cf).select("as_of_date", "currency", "maturity_bucket", "net_cashflow", "cf_count")

    # expected: t0 net=-10, t1_7d net=+25, t8_30d net=-5
    exp = spark.createDataFrame(
        [
            Row(as_of_date=date(2026, 1, 23), currency="THB", maturity_bucket="t0", net_cashflow=-10.0, cf_count=1),
            Row(as_of_date=date(2026, 1, 23), currency="THB", maturity_bucket="t1_7d", net_cashflow=25.0, cf_count=1),
            Row(as_of_date=date(2026, 1, 23), currency="THB", maturity_bucket="t8_30d", net_cashflow=-5.0, cf_count=1),
        ]
    )
    assert_df_equality(
        got.orderBy("maturity_bucket"),
        exp.orderBy("maturity_bucket"),
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_concentration_top_counterparties_basic(spark):
    rows = [
        Row(
            tx_id="t1",
            event_ts=datetime(2026, 1, 23, 10, 5, 0),
            value_date=date(2026, 1, 23),
            account_id="A000001",
            counterparty_id="CP1",
            currency="THB",
            direction="IN",
            amount=100.0,
            product="payment",
            channel="RTGS",
        ),
        Row(
            tx_id="t2",
            event_ts=datetime(2026, 1, 23, 10, 6, 0),
            value_date=date(2026, 1, 23),
            account_id="A000001",
            counterparty_id="CP2",
            currency="THB",
            direction="IN",
            amount=50.0,
            product="payment",
            channel="RTGS",
        ),
    ]
    tx = spark.createDataFrame(rows)
    tx_b = intraday_time_bucket(tx, timezone="UTC", granularity="hour")
    top = concentration_top_counterparties(tx_b, top_n=1)

    got = (
        top.filter(F.col("cp_rank") == 1)
        .select("value_date", "currency", "counterparty_id", "cp_share", "top1_share", "topN_share", "topN")
        .collect()[0]
    )
    assert got["counterparty_id"] == "CP1"
    assert abs(got["cp_share"] - (100.0 / 150.0)) < 1e-6
    assert abs(got["top1_share"] - (100.0 / 150.0)) < 1e-6
    assert got["topN"] == 1

