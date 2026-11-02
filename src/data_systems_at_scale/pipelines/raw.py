"""Raw data: seeded, made-up order events, one folder per day.

Raw data is the input every job reads, and it is never edited. Each file is created once, in mode "x", which
fails if the file already exists. Data that arrives later, such as a late order or a correction, lands as a new
file in its day's folder. So a job can always be rerun on exactly the input it read before.

    lake/raw/orders/date=2026-10-01/part-0.jsonl
"""

import json
import logging
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)
STORES = ("s-1", "s-2", "s-3")
CUSTOMERS = 40


def day_folder(root: Path, day: date) -> Path:
    return root / "raw" / "orders" / f"date={day.isoformat()}"


def orders(day: date, rows: int, seed: int) -> list[dict]:
    """`rows` made-up orders on `day`, in time order. The same day and seed always give the same orders."""
    rng = random.Random(f"{seed}-{day.isoformat()}")
    midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    made = [{"order_id": f"{day:%m%d}-{i:04d}",
             "customer": f"c-{rng.randrange(CUSTOMERS):02d}",
             "store": rng.choice(STORES),
             "amount_cents": rng.randrange(500, 20_000),
             "event_time": (midnight + timedelta(seconds=rng.randrange(86_400))).strftime("%Y-%m-%dT%H:%M:%SZ")}
            for i in range(rows)]
    return sorted(made, key=lambda order: order["event_time"])


def write_part(root: Path, day: date, rows: list[dict], part: int = 0) -> Path | None:
    """Create one file in the day's folder. Returns None, and changes nothing, if that file already exists."""
    path = day_folder(root, day) / f"part-{part}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x") as file:  # "x": create, or fail if the file exists
            file.writelines(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    except FileExistsError:
        log.info("%s exists; raw data is written once, so it stays as it is", path.relative_to(root))
        return None
    log.info("wrote %s: %d orders", path.relative_to(root), len(rows))
    return path


def read_day(root: Path, day: date) -> list[dict]:
    """Every order in the day's folder, from every part file, in part order."""
    return [json.loads(line) for path in sorted(day_folder(root, day).glob("part-*.jsonl"))
            for line in path.read_text().splitlines()]
