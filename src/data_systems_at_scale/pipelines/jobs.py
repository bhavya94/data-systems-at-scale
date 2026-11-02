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


def _replace(root: Path, path: Path, lines: list[str], job: str, inputs: list[Path], label: str,
             crash_midway: bool = False) -> Path:
    """Write a job's output in one step, record its lineage, and log whether the bytes changed."""
    before = fingerprint(path)
    write_atomic(path, "".join(lines), crash_midway)
    record(root, path, job, inputs)
    after = fingerprint(path)
    log.info("%s %s: output is %s, %s", job, label, after,
             "the same bytes as before" if after == before else "new" if before is None else "changed")
    return path


def daily_sales(root: Path, day: date, crash_midway: bool = False) -> Path:
    """Orders and amount per store on `day`, from that day's raw orders. A correction carries an order's id with a
    higher `version`; only the latest version of each order counts."""
    orders = read_day(root, day)
    latest: dict[str, dict] = {}
    for order in sorted(orders, key=lambda order: order.get("version", 1)):  # a later version replaces an earlier
        latest[order["order_id"]] = order
    totals: dict[str, dict[str, int]] = {}
    for order in latest.values():
        store = totals.setdefault(order["store"], {"orders": 0, "amount_cents": 0})
        store["orders"] += 1
        store["amount_cents"] += order["amount_cents"]
    lines = [json.dumps({"date": day.isoformat(), "store": name, **totals[name]}, sort_keys=True) + "\n"
             for name in sorted(totals)]  # by store name, whatever order the orders arrive in
    path = out_path(root, "daily_sales", f"date={day.isoformat()}")
    log.info("daily_sales %s: read %d raw orders; output was %s", day, len(orders), fingerprint(path) or "missing")
    if len(orders) > len(latest):
        log.info("daily_sales %s: %d of them correct earlier orders; the latest version of each counts",
                 day, len(orders) - len(latest))
    return _replace(root, path, lines, "daily_sales", sorted(day_folder(root, day).glob("part-*.jsonl")),
                    day.isoformat(), crash_midway)


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
    return _replace(root, path, lines, "weekly_sales", dailies, week)
