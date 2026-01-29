from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TX_SCHEMA = StructType(
    [
        StructField("tx_id", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("value_date", DateType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("counterparty_id", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("direction", StringType(), nullable=False),  # IN / OUT
        StructField("amount", DoubleType(), nullable=False),  # positive
        StructField("product", StringType(), nullable=False),
        StructField("channel", StringType(), nullable=False),
    ]
)


BAL_SCHEMA = StructType(
    [
        StructField("as_of_date", DateType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("balance", DoubleType(), nullable=False),
        StructField("customer_segment", StringType(), nullable=False),
    ]
)


CASHFLOW_SCHEMA = StructType(
    [
        StructField("cf_id", StringType(), nullable=False),
        StructField("as_of_date", DateType(), nullable=False),
        StructField("maturity_date", DateType(), nullable=False),
        StructField("counterparty_id", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("direction", StringType(), nullable=False),  # IN / OUT
        StructField("amount", DoubleType(), nullable=False),
        StructField("source", StringType(), nullable=False),  # e.g. loan, bond, fx_swap
    ]
)


COLLATERAL_SCHEMA = StructType(
    [
        StructField("as_of_date", DateType(), nullable=False),
        StructField("collateral_id", StringType(), nullable=False),
        StructField("counterparty_id", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("market_value", DoubleType(), nullable=False),
        StructField("haircut_pct", DoubleType(), nullable=False),
        StructField("eligible_lcr", IntegerType(), nullable=False),  # 0/1
    ]
)


LIMITS_SCHEMA = StructType(
    [
        StructField("as_of_date", DateType(), nullable=False),
        StructField("counterparty_id", StringType(), nullable=False),
        StructField("limit_type", StringType(), nullable=False),  # intraday / settlement / credit
        StructField("limit_amount", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
    ]
)

