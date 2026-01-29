from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F


@dataclass(frozen=True)
class DqResult:
    check_name: str
    passed: bool
    details: str


def check_no_nulls(df: DataFrame, cols: list[str], check_name: str) -> DqResult:
    # compute per-column null counts
    agg_exprs = [F.sum(F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(c) for c in cols]
    row = df.agg(*agg_exprs).collect()[0].asDict()
    bad = {k: int(v) for k, v in row.items() if v and int(v) > 0}
    return DqResult(
        check_name=check_name,
        passed=(len(bad) == 0),
        details=("ok" if len(bad) == 0 else f"null_counts={bad}"),
    )


def check_no_duplicates(df: DataFrame, key_cols: list[str], check_name: str) -> DqResult:
    dup_cnt = (
        df.groupBy(*[F.col(c) for c in key_cols])
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum("count").alias("dup_rows"))
        .collect()[0]["dup_rows"]
    )
    dup_cnt = int(dup_cnt) if dup_cnt is not None else 0
    return DqResult(
        check_name=check_name,
        passed=(dup_cnt == 0),
        details=("ok" if dup_cnt == 0 else f"duplicate_rows={dup_cnt}"),
    )


def check_rowcount_between(df: DataFrame, low: int, high: int, check_name: str) -> DqResult:
    n = df.count()
    return DqResult(
        check_name=check_name,
        passed=(low <= n <= high),
        details=f"rowcount={n}, expected=[{low},{high}]",
    )

