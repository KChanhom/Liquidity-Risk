from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F

from .schemas import BAL_SCHEMA, CASHFLOW_SCHEMA, COLLATERAL_SCHEMA, LIMITS_SCHEMA, TX_SCHEMA


def _date_range(start_date: str, days: int) -> list[str]:
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days)]


def generate_transactions(
    spark: SparkSession,
    *,
    seed: int,
    as_of_date: str,
    days: int,
    tx_per_day: int,
    accounts: int,
    counterparties: int,
    currencies: list[str],
    timezone: str,
) -> DataFrame:
    # create (day, i) grid then synthesize fields deterministically via Spark rand
    dates = _date_range(as_of_date, days)
    base = (
        spark.createDataFrame([(d,) for d in dates], "value_date string")
        .withColumn("value_date", F.to_date("value_date"))
        .withColumn("day_idx", F.datediff(F.col("value_date"), F.to_date(F.lit(dates[0]))))
    )

    grid = base.crossJoin(spark.range(0, tx_per_day).withColumnRenamed("id", "i"))
    r = F.rand(seed)  # one rand call per row used in multiple derivations via hash-like mixing below

    # event_ts distributed intraday; keep timezone conversion explicit for monitoring buckets
    seconds_in_day = 24 * 60 * 60
    intraday_sec = (F.pmod(F.hash("day_idx", "i"), F.lit(seconds_in_day))).cast("int")
    base_ts = F.to_timestamp(F.concat_ws(" ", F.col("value_date").cast("string"), F.lit("00:00:00")))
    event_ts_utc = F.from_unixtime(F.unix_timestamp(base_ts) + intraday_sec).cast("timestamp")

    df = (
        grid.select(
            F.concat(F.lit("TX-"), F.col("value_date").cast("string"), F.lit("-"), F.col("i").cast("string")).alias("tx_id"),
            event_ts_utc.alias("event_ts"),
            F.col("value_date"),
            F.concat(F.lit("A"), F.lpad((F.pmod(F.hash("i"), F.lit(accounts)) + 1).cast("string"), 6, "0")).alias(
                "account_id"
            ),
            F.concat(
                F.lit("CP"),
                F.lpad((F.pmod(F.hash("day_idx", "i"), F.lit(counterparties)) + 1).cast("string"), 5, "0"),
            ).alias("counterparty_id"),
            F.element_at(F.array([F.lit(c) for c in currencies]), (F.pmod(F.hash("i", "day_idx"), F.lit(len(currencies))) + 1)).alias(
                "currency"
            ),
            F.when(F.pmod(F.hash("i"), F.lit(2)) == 0, F.lit("IN")).otherwise(F.lit("OUT")).alias("direction"),
            (F.exp(r * 4) * 1000).alias("amount"),
            F.when(F.pmod(F.hash("i"), F.lit(3)) == 0, F.lit("deposit"))
            .when(F.pmod(F.hash("i"), F.lit(3)) == 1, F.lit("payment"))
            .otherwise(F.lit("securities"))
            .alias("product"),
            F.when(F.pmod(F.hash("i"), F.lit(3)) == 0, F.lit("SWIFT"))
            .when(F.pmod(F.hash("i"), F.lit(3)) == 1, F.lit("RTGS"))
            .otherwise(F.lit("INTERNAL"))
            .alias("channel"),
        )
        # normalize timestamp into configured timezone for bucket alignment (store as timestamp; zone set in spark session for downstream)
        .select("*")
    )
    return spark.createDataFrame(df.rdd, schema=TX_SCHEMA)


def generate_balances(
    spark: SparkSession,
    *,
    seed: int,
    as_of_date: str,
    days: int,
    accounts: int,
    currencies: list[str],
) -> DataFrame:
    dates = _date_range(as_of_date, days)
    base = spark.createDataFrame([(d,) for d in dates], "as_of_date string").withColumn("as_of_date", F.to_date("as_of_date"))
    acc = spark.range(1, accounts + 1).withColumn("account_id", F.concat(F.lit("A"), F.lpad(F.col("id").cast("string"), 6, "0"))).drop("id")
    cur = spark.createDataFrame([(c,) for c in currencies], "currency string")

    df = base.crossJoin(acc).crossJoin(cur)
    r = F.rand(seed)
    out = df.select(
        "as_of_date",
        "account_id",
        "currency",
        (F.exp(r * 3) * 1e5).alias("balance"),
        F.when(F.pmod(F.hash("account_id"), F.lit(3)) == 0, F.lit("Retail"))
        .when(F.pmod(F.hash("account_id"), F.lit(3)) == 1, F.lit("SME"))
        .otherwise(F.lit("Corporate"))
        .alias("customer_segment"),
    )
    return spark.createDataFrame(out.rdd, schema=BAL_SCHEMA)


def generate_cashflows(
    spark: SparkSession,
    *,
    seed: int,
    as_of_date: str,
    days: int,
    counterparties: int,
    currencies: list[str],
    rows_per_day: int = 8000,
) -> DataFrame:
    dates = _date_range(as_of_date, days)
    base = spark.createDataFrame([(d,) for d in dates], "as_of_date string").withColumn("as_of_date", F.to_date("as_of_date"))
    grid = base.crossJoin(spark.range(0, rows_per_day).withColumnRenamed("id", "i"))

    # maturity 0..90 days ahead
    maturity = F.date_add(F.col("as_of_date"), F.pmod(F.hash("i"), F.lit(91)).cast("int"))
    r = F.rand(seed)

    df = grid.select(
        F.concat(F.lit("CF-"), F.col("as_of_date").cast("string"), F.lit("-"), F.col("i").cast("string")).alias("cf_id"),
        "as_of_date",
        maturity.alias("maturity_date"),
        F.concat(F.lit("CP"), F.lpad((F.pmod(F.hash("i"), F.lit(counterparties)) + 1).cast("string"), 5, "0")).alias(
            "counterparty_id"
        ),
        F.element_at(F.array([F.lit(c) for c in currencies]), (F.pmod(F.hash("i", "as_of_date"), F.lit(len(currencies))) + 1)).alias(
            "currency"
        ),
        F.when(F.pmod(F.hash("i"), F.lit(2)) == 0, F.lit("IN")).otherwise(F.lit("OUT")).alias("direction"),
        (F.exp(r * 4) * 2000).alias("amount"),
        F.when(F.pmod(F.hash("i"), F.lit(3)) == 0, F.lit("loan"))
        .when(F.pmod(F.hash("i"), F.lit(3)) == 1, F.lit("bond"))
        .otherwise(F.lit("fx_swap"))
        .alias("source"),
    )
    return spark.createDataFrame(df.rdd, schema=CASHFLOW_SCHEMA)


def generate_collateral(
    spark: SparkSession,
    *,
    seed: int,
    as_of_date: str,
    days: int,
    counterparties: int,
    currencies: list[str],
    rows_per_day: int = 2000,
) -> DataFrame:
    dates = _date_range(as_of_date, days)
    base = spark.createDataFrame([(d,) for d in dates], "as_of_date string").withColumn("as_of_date", F.to_date("as_of_date"))
    grid = base.crossJoin(spark.range(0, rows_per_day).withColumnRenamed("id", "i"))
    r = F.rand(seed)
    df = grid.select(
        "as_of_date",
        F.concat(F.lit("COL-"), F.col("as_of_date").cast("string"), F.lit("-"), F.col("i").cast("string")).alias("collateral_id"),
        F.concat(F.lit("CP"), F.lpad((F.pmod(F.hash("i"), F.lit(counterparties)) + 1).cast("string"), 5, "0")).alias(
            "counterparty_id"
        ),
        F.element_at(F.array([F.lit(c) for c in currencies]), (F.pmod(F.hash("i", "as_of_date"), F.lit(len(currencies))) + 1)).alias(
            "currency"
        ),
        (F.exp(r * 4) * 5e4).alias("market_value"),
        (F.pmod(F.hash("i"), F.lit(21)).cast("double") / F.lit(100.0)).alias("haircut_pct"),
        F.when(F.pmod(F.hash("i"), F.lit(5)) == 0, F.lit(0)).otherwise(F.lit(1)).alias("eligible_lcr"),
    )
    return spark.createDataFrame(df.rdd, schema=COLLATERAL_SCHEMA)


def generate_limits(
    spark: SparkSession,
    *,
    seed: int,
    as_of_date: str,
    days: int,
    counterparties: int,
    currencies: list[str],
) -> DataFrame:
    dates = _date_range(as_of_date, days)
    base = spark.createDataFrame([(d,) for d in dates], "as_of_date string").withColumn("as_of_date", F.to_date("as_of_date"))
    cps = spark.range(1, counterparties + 1).withColumn(
        "counterparty_id", F.concat(F.lit("CP"), F.lpad(F.col("id").cast("string"), 5, "0"))
    ).drop("id")
    cur = spark.createDataFrame([(c,) for c in currencies], "currency string")
    limit_types = spark.createDataFrame([("intraday",), ("settlement",), ("credit",)], "limit_type string")

    df = base.crossJoin(cps).crossJoin(cur).crossJoin(limit_types)
    r = F.rand(seed)
    out = df.select(
        "as_of_date",
        "counterparty_id",
        "limit_type",
        (F.exp(r * 3) * 1e6).alias("limit_amount"),
        "currency",
    )
    return spark.createDataFrame(out.rdd, schema=LIMITS_SCHEMA)

