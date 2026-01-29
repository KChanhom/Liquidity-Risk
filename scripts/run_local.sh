#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${1:-configs/local.yml}"

spark-submit --master "local[*]" jobs/01_generate_synthetic.py --config "$CONFIG_PATH"
spark-submit --master "local[*]" jobs/02_liquidity_monitoring.py --config "$CONFIG_PATH"

echo "Done. Outputs in data/out/"

