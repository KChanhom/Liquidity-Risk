from datetime import date, datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row

from liquidity_risk.monitoring import daily_liquidity_drop_flag


def test_daily_liquidity_drop_flag_rollup(spark):
    rows = [
        Row(
            value_date=date(2026, 1, 23),
            currency="THB",
            bucket_start_ts=datetime(2026, 1, 23, 10, 0, 0),
            flag_liquidity_drop=False,
            delta_z=-0.5,
        ),
        Row(
            value_date=date(2026, 1, 23),
            currency="THB",
            bucket_start_ts=datetime(2026, 1, 23, 11, 0, 0),
            flag_liquidity_drop=True,
            delta_z=-4.0,
        ),
    ]
    df = spark.createDataFrame(rows)
    got = daily_liquidity_drop_flag(df)

    exp = spark.createDataFrame(
        [
            Row(
                value_date=date(2026, 1, 23),
                currency="THB",
                flag_liquidity_drop_any=True,
                min_delta_z=-4.0,
            )
        ]
    )
    assert_df_equality(got, exp, ignore_nullable=True, ignore_row_order=True)

