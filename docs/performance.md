# Performance notes (examples)

โปรเจกต์นี้ใส่ตัวอย่าง pattern ที่เจอบ่อยในงาน Spark สำหรับ risk/monitoring:

## Partitioning (Parquet)

ผลลัพธ์ถูกเขียนเป็น Parquet แบบ partitioned เช่น:

- `intraday_snapshots` partitioned by `value_date,currency`
- `daily_snapshots` partitioned by `value_date`

ช่วยให้ query/report เลือกอ่านเฉพาะวันที่/สกุลเงินที่ต้องการได้เร็วขึ้น (partition pruning)

## Repartition before wide aggregations

ใน `jobs/02_liquidity_monitoring.py` มีตัวอย่าง:

- `tx_b = tx_b.repartition(N, "value_date")`

เพื่อกระจายงาน aggregation ให้อยู่บน partition ที่เหมาะสม ลด skew ในบางเคส (โดยเฉพาะถ้า data โตขึ้น)

## Caching intermediate

ใน monitoring job มีการ `cache()` dataset ที่ถูกใช้ซ้ำหลายครั้ง (net cashflow + concentration + limit utilization)
และ `count()` เพื่อ materialize cache (demo pattern)

## Bucketing (optional / advanced)

ถ้ารันบน Spark ที่เปิดใช้งาน Hive metastore/warehouse ได้ คุณสามารถใช้ bucketing สำหรับ join-heavy workloads ได้ เช่น:

```python
# Example only (requires spark.sql.catalogImplementation=hive and a warehouse)
(
  tx_b
  .write
  .mode("overwrite")
  .bucketBy(64, "counterparty_id")
  .sortBy("counterparty_id")
  .saveAsTable("lr_tx_bucketed")
)
```

หมายเหตุ:
- Bucketing จะมีประโยชน์ชัดเจนเมื่อทำซ้ำ ๆ และอ่านผ่าน table เดิมหลายรอบ
- ใน local mode หรือไม่มี Hive catalog จะใช้ pattern นี้ไม่ได้/ไม่คุ้ม

