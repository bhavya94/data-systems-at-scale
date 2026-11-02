"""Jobs that turn raw orders into outputs.

`daily_sales` reads one day's raw orders and writes that day's totals per store. Each run replaces the whole
output for that day, and the raw input never changes. So running the job twice gives the same bytes: the job is
idempotent, and a rerun is always safe. `weekly_sales` adds up the daily outputs of one ISO week, and
`customer_sales` totals each customer's orders in one ISO week, from raw orders.

    lake/out/daily_sales/date=2026-10-01/data.jsonl
    lake/out/weekly_sales/week=2026-W40/data.jsonl
    lake/out/customer_sales/week=2026-W40/data.jsonl

Each job also records its inputs beside its output, for lineage. `inputs` says which files a job reads, so the
job, its lineage and the backfill's cost all agree.
"""

import json
import logging
from datetime import date
from pathlib import Path

from data_systems_at_scale.pipelines.lineage import record
from data_systems_at_scale.pipelines.raw import day_folder, days
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


def week_of(day: date) -> str:
    year, week, _ = day.isocalendar()
    return f"{year}-W{week:02d}"


def _day(partition: str) -> date:
    return date.fromisoformat(partition.removeprefix("date="))


def inputs(root: Path, job: str, partition: str) -> list[Path]:
    """The files `job` reads for `partition`, as they are now."""
    if job == "daily_sales":
        return sorted(day_folder(root, _day(partition)).glob("part-*.jsonl"))
    week = partition.removeprefix("week=")
    if job == "weekly_sales":
        return sorted(path for path in (root / "out" / "daily_sales").glob("date=*/data.jsonl")
                      if week_of(_day(path.parent.name)) == week)
    return sorted(path for day in days(root) if week_of(day) == week  # customer_sales reads raw orders
                  for path in day_folder(root, day).glob("part-*.jsonl"))


def latest_orders(paths: list[Path]) -> tuple[list[dict], int]:
    """The latest version of each order in the files, and how many orders the files held in all. A correction
    carries an order's id with a higher `version`."""
    orders = [json.loads(line) for path in paths for line in path.read_text().splitlines()]
    latest: dict[str, dict] = {}
    for order in sorted(orders, key=lambda order: order.get("version", 1)):  # a later version replaces an earlier
        latest[order["order_id"]] = order
    return list(latest.values()), len(orders)


def _add_up(rows: list[dict], key: str, count: str) -> dict[str, dict[str, int]]:
    """Orders and amount per `key`. `count` is the field that holds each row's number of orders, or None for 1."""
    totals: dict[str, dict[str, int]] = {}
    for row in rows:
        total = totals.setdefault(row[key], {"orders": 0, "amount_cents": 0})
        total["orders"] += row[count] if count else 1
        total["amount_cents"] += row["amount_cents"]
    return totals


def _lines(fixed: dict, totals: dict[str, dict[str, int]], key: str) -> list[str]:
    """One JSON line per key, sorted by key, so the output depends on the rows, not on their order."""
    return [json.dumps({**fixed, key: name, **totals[name]}, sort_keys=True) + "\n" for name in sorted(totals)]


def daily_sales(root: Path, day: date, crash_midway: bool = False) -> Path:
    """Orders and amount per store on `day`, from that day's raw orders. Only the latest version of each order
    counts."""
    files = inputs(root, "daily_sales", f"date={day.isoformat()}")
    latest, read = latest_orders(files)
    path = out_path(root, "daily_sales", f"date={day.isoformat()}")
    log.info("daily_sales %s: read %d raw orders; output was %s", day, read, fingerprint(path) or "missing")
    if read > len(latest):
        log.info("daily_sales %s: %d of them correct earlier orders; the latest version of each counts",
                 day, read - len(latest))
    lines = _lines({"date": day.isoformat()}, _add_up(latest, "store", None), "store")
    return _replace(root, path, lines, "daily_sales", files, day.isoformat(), crash_midway)


def weekly_sales(root: Path, week: str) -> Path:
    """Orders and amount per store in one ISO week, from the daily outputs of the days in that week."""
    dailies = inputs(root, "weekly_sales", f"week={week}")
    rows = [json.loads(line) for daily in dailies for line in daily.read_text().splitlines()]
    path = out_path(root, "weekly_sales", f"week={week}")
    log.info("weekly_sales %s: adding up %d daily outputs", week, len(dailies))
    return _replace(root, path, _lines({"week": week}, _add_up(rows, "store", "orders"), "store"), "weekly_sales",
                    dailies, week)


def customer_sales(root: Path, week: str) -> Path:
    """Orders and amount per customer in one ISO week, from raw orders. It keeps each customer's id: a copy of
    personal data, which a delete must reach."""
    files = inputs(root, "customer_sales", f"week={week}")
    latest, _ = latest_orders(files)
    path = out_path(root, "customer_sales", f"week={week}")
    log.info("customer_sales %s: %d orders from %d raw files", week, len(latest), len(files))
    return _replace(root, path, _lines({"week": week}, _add_up(latest, "customer", None), "customer"),
                    "customer_sales", files, week)


def rebuild(root: Path, job: str, partition: str) -> Path:
    """Run `job` for one partition, by name: how a backfill reruns any output."""
    if job == "daily_sales":
        return daily_sales(root, _day(partition))
    return {"weekly_sales": weekly_sales, "customer_sales": customer_sales}[job](root, partition.removeprefix("week="))
