# Liquidity Risk: Intraday Liquidity & LCR/NSFR (PySpark)

โปรเจกต์ตัวอย่างสำหรับธนาคาร: “Calculator + Monitoring” คำนวณ/ติดตามสภาพคล่องรายวันหรือรายชั่วโมงจาก
ธุรกรรมเข้า-ออก, เงินฝาก, วงเงิน, settlement, cashflow future พร้อม data quality checks และ anomaly flags

## โครงสร้าง

- `src/liquidity_risk/` โค้ดหลัก (datasets, features, dq, utils)
- `jobs/` spark-submit entrypoints
- `configs/` ไฟล์ config (YAML)
- `data/` (local) โฟลเดอร์ output Parquet
- `tests/` unit tests (PyTest)
- `scripts/` helper scripts สำหรับรันตัวอย่าง

## ติดตั้ง

ต้องมี Python 3.10+ และ Spark 3.x (ที่มี `pyspark` ใน environment)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## รัน end-to-end (local)

รันแบบรวดเดียว:

```bash
bash scripts/run_local.sh configs/local.yml
```

1) สร้างข้อมูลจำลอง (transactions/balances/cashflows/collateral/limits) เป็น Parquet

```bash
spark-submit \
  --master "local[*]" \
  jobs/01_generate_synthetic.py \
  --config configs/local.yml
```

2) สร้าง daily/hourly liquidity snapshots + anomaly flags + DQ checks

```bash
spark-submit \
  --master "local[*]" \
  jobs/02_liquidity_monitoring.py \
  --config configs/local.yml
```

ผลลัพธ์จะถูกเขียนลง `data/out/` เป็น Parquet (partitioned)
โดยหลัก ๆ จะได้:

- `data/out/dq_results/` ผล data quality checks
- `data/out/intraday_snapshots/` intraday bucket snapshots + liquidity-drop flags
- `data/out/daily_snapshots/` daily snapshots + anomaly flags
- `data/out/limit_utilization/` utilization ต่อ counterparty เทียบ settlement limits

## สิ่งที่คำนวณ (หลัก ๆ)

- Net cashflow by time bucket (intraday buckets)
- Concentration risk: top counterparties (สัดส่วนและอันดับ)
- Maturity ladder (กำหนด bucket ตาม maturity_date)
- Daily snapshots + anomaly flags (เช่น drop ผิดปกติ, limit utilization สูงผิดปกติ)
- Data quality checks (schema/key nulls, duplicates, volume sanity)

## Tests

```bash
pytest -q
```

## Performance

ดูตัวอย่าง partitioning/repartition/caching/bucketing ได้ที่ `docs/performance.md`

