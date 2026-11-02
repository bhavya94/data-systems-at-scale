"""Jobs that turn raw orders into outputs.

`daily_sales` reads one day's raw orders and writes that day's totals per store. Each run replaces the whole
output for that day, and the raw input never changes. So running the job twice gives the same bytes: the job is
idempotent, and a rerun is always safe. `weekly_sales` adds up the daily outputs of one ISO week.

    lake/out/daily_sales/date=2026-10-01/data.jsonl
    lake/out/weekly_sales/week=2026-W40/data.jsonl

Each job also records its inputs beside its output, for lineage.
"""

import json
import logging
from datetime import date
from pathlib import Path

from data_systems_at_scale.pipelines.lineage import record
from data_systems_at_scale.pipelines.raw import day_folder, read_day
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
    record(root, path, "daily_sales", sorted(day_folder(root, day).glob("part-*.jsonl")))
    after = fingerprint(path)
    log.info("daily_sales %s: output is %s, %s", day, after,
             "the same bytes as before" if after == before else "new" if before is None else "changed")
    return path


def week_of(day: date) -> str:
    year, week, _ = day.isocalendar()
    return f"{year}-W{week:02d}"


def weekly_sales(root: Path, week: str) -> Path:
    """Orders and amount per store in one ISO week, from the daily outputs of the days in that week."""
    dailies = sorted(path for path in (root / "out" / "daily_sales").glob("date=*/data.jsonl")
                     if week_of(date.fromisoformat(path.parent.name.removeprefix("date="))) == week)
    totals: dict[str, dict[str, int]] = {}
    for daily in dailies:
        for row in map(json.loads, daily.read_text().splitlines()):
            store = totals.setdefault(row["store"], {"orders": 0, "amount_cents": 0})
            store["orders"] += row["orders"]
            store["amount_cents"] += row["amount_cents"]
    lines = [json.dumps({"week": week, "store": name, **totals[name]}, sort_keys=True) + "\n"
             for name in sorted(totals)]
    path = out_path(root, "weekly_sales", f"week={week}")
    log.info("weekly_sales %s: adding up %d daily outputs", week, len(dailies))
    write_atomic(path, "".join(lines))
    record(root, path, "weekly_sales", dailies)
    return path
