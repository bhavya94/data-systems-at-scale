"""Backfill: rebuild every output that bad inputs reached, in dependency order, a few at a time.

1. Scope: lineage gives every output the bad days reached, and nothing else.
2. Order: an output can be rebuilt only after the outputs it reads, so the plan goes level by level. Every daily
   output comes before any weekly output.
3. Cost: each step shows the rows it reads, so you see the bill before you pay it.
4. Throttle: at most `parallel` steps run at once, so a backfill doesn't starve the jobs that run every day.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from data_systems_at_scale.pipelines.jobs import daily_sales, weekly_sales
from data_systems_at_scale.pipelines.lineage import downstream, manifests
from data_systems_at_scale.pipelines.raw import day_folder

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Step:
    level: int
    """1 for outputs that read raw data, 2 for outputs that read those, and so on."""
    job: str
    partition: str
    reads: int
    """Rows the step reads: its cost."""


def plan(root: Path, bad_days: list[date]) -> list[Step]:
    """Every output the bad days reached, as steps in the order they must run."""
    starts = [str(p.relative_to(root)) for day in bad_days for p in sorted(day_folder(root, day).glob("part-*.jsonl"))]
    level: dict[str, int] = {}
    for depth, path in downstream(root, starts):
        if depth:  # depth 0 is the raw file itself
            level[path] = max(depth, level.get(path, 0))  # reached twice: it must wait for the deeper path
    inputs = {entry["output"]: entry["inputs"] for entry in manifests(root)}
    steps = []
    for path, depth in level.items():
        _, job, partition, _ = path.split("/")
        reads = sum(len((root / source["path"]).read_text().splitlines()) for source in inputs[path])
        steps.append(Step(depth, job, partition, reads))
    return sorted(steps, key=lambda step: (step.level, step.job, step.partition))


@dataclass(frozen=True)
class Done:
    step: Step
    start: float
    end: float


def run(root: Path, steps: list[Step], parallel: int, step_ms: int = 0) -> tuple[list[Done], int]:
    """Rebuild each step, a level at a time, with at most `parallel` running at once. Returns each step's times
    and the most steps that ran at once."""
    running, peak, lock, done = 0, 0, threading.Lock(), []

    def rebuild(step: Step) -> Done:
        nonlocal running, peak
        with lock:  # the counter is shared by every backfill thread
            running += 1
            peak = max(peak, running)
            log.info("rebuilding %s %s (%d running, limit %d)", step.job, step.partition, running, parallel)
        start = time.time()
        time.sleep(step_ms / 1000)  # stands in for a job over a real day of data
        if step.job == "daily_sales":
            daily_sales(root, date.fromisoformat(step.partition.removeprefix("date=")))
        else:
            weekly_sales(root, step.partition.removeprefix("week="))
        with lock:
            running -= 1
        return Done(step, start, time.time())

    for level in sorted({step.level for step in steps}):
        batch = [step for step in steps if step.level == level]
        with ThreadPoolExecutor(max_workers=parallel, thread_name_prefix="backfill") as pool:
            done += list(pool.map(rebuild, batch))
        log.info("level %d done: rebuilt %d, so the next level can read them", level, len(batch))
    return done, peak
