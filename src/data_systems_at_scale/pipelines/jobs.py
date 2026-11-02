"""Jobs that turn raw orders into outputs.

`daily_sales` reads one day's raw orders and writes that day's totals per store. Each run replaces the whole
output for that day, and the raw input never changes. So running the job twice gives the same bytes: the job is
idempotent, and a rerun is always safe.

    lake/out/daily_sales/date=2026-10-01/data.jsonl
"""

import json
import logging
from datetime import date
from pathlib import Path

from data_systems_at_scale.pipelines.raw import read_day
from data_systems_at_scale.pipelines.storage import fingerprint, write_atomic

log = logging.getLogger(__name__)


def out_path(root: Path, table: str, partition: str) -> Path:
    return root / "out" / table / partition / "data.jsonl"


def daily_sales(root: Path, day: date, crash_midway: bool = False) -> Path:
    """Orders and amount per store on `day`, from that day's raw orders."""
    orders = read_day(root, day)
    totals: dict[str, dict[str, int]] = {}
    for order in orders:
        store = totals.setdefault(order["store"], {"orders": 0, "amount_cents": 0})
        store["orders"] += 1
        store["amount_cents"] += order["amount_cents"]
    lines = [json.dumps({"date": day.isoformat(), "store": name, **totals[name]}, sort_keys=True) + "\n"
             for name in sorted(totals)]  # by store name, whatever order the orders arrive in
    path = out_path(root, "daily_sales", f"date={day.isoformat()}")
    before = fingerprint(path)
    log.info("daily_sales %s: read %d raw orders; output was %s", day, len(orders), before or "missing")
    write_atomic(path, "".join(lines), crash_midway)
    after = fingerprint(path)
    log.info("daily_sales %s: output is %s, %s", day, after,
             "the same bytes as before" if after == before else "new" if before is None else "changed")
    return path
