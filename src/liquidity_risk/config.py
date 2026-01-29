from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class AppConfig:
    raw_path: str
    out_path: str
    synthetic: dict[str, Any]
    time_buckets: dict[str, Any]
    monitoring: dict[str, Any]
    performance: dict[str, Any]


def load_config(path: str | Path) -> AppConfig:
    p = Path(path)
    cfg = yaml.safe_load(p.read_text())
    io = cfg.get("io", {})
    return AppConfig(
        raw_path=str(io.get("raw_path", "data/raw")),
        out_path=str(io.get("out_path", "data/out")),
        synthetic=cfg.get("synthetic", {}),
        time_buckets=cfg.get("time_buckets", {}),
        monitoring=cfg.get("monitoring", {}),
        performance=cfg.get("performance", {}),
    )

