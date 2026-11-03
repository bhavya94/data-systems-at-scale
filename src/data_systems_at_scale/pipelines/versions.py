"""Verify, then swap: rebuild into a new version of the outputs, check it, and only then make it the live one.

`lake/out` is a symbolic link to one version folder: `lake/out-v1`, `lake/out-v2` and so on. Readers always go
through `lake/out`, so they see one whole version at a time.

1. Stage: copy the live version to the next folder, and make a staging root whose `out` points at it.
2. Rebuild there, while readers keep reading the live version.
3. Verify: each rebuilt output adds up to what it read, and every other output has exactly its old bytes.
4. Swap: replace the link in one rename. If a check fails, nothing is swapped, and the live version stays.

The old version stays on disk, so going back is one more rename.
"""

import json
import logging
import os
import shutil
from pathlib import Path

from data_systems_at_scale.pipelines.backfill import Step
from data_systems_at_scale.pipelines.jobs import inputs, latest_orders, out_path

log = logging.getLogger(__name__)


def live(root: Path) -> Path:
    """The live version folder. The first time, the plain `out` folder becomes version 1, behind a link."""
    out = root / "out"
    if not out.is_symlink():
        out.rename(root / "out-v1")
        out.symlink_to("out-v1")
        log.info("out is now a link to out-v1")
    return root / os.readlink(out)


def stage(root: Path) -> tuple[Path, str]:
    """Copy the live version to the next folder. Returns a staging root that reads raw data from `root` and writes
    outputs to the copy, and the copy's name."""
    current = live(root)
    version = f"out-v{max(int(p.name.removeprefix('out-v')) for p in root.glob('out-v*')) + 1}"
    shutil.copytree(current, root / version)
    staging = root / f"staging-{version}"
    staging.mkdir()
    (staging / "raw").symlink_to(Path("..") / "raw")
    (staging / "out").symlink_to(Path("..") / version)
    log.info("staged %s as a copy of %s; readers still read %s", version, current.name, current.name)
    return staging, version


def _totals(path: Path) -> tuple[int, int]:
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    return sum(row["orders"] for row in rows), sum(row["amount_cents"] for row in rows)


def verify(root: Path, staging: Path, steps: list[Step]) -> list[str]:
    """Problems with the staged version; an empty list means it may go live."""
    problems = []
    for step in steps:
        path = out_path(staging, step.job, step.partition)
        if step.job == "weekly_sales":  # a week must add up to its days
            days = [_totals(day) for day in inputs(staging, step.job, step.partition)]
            expected = (sum(d[0] for d in days), sum(d[1] for d in days))
        else:  # a daily or customer output must add up to the latest version of its raw orders
            latest, _ = latest_orders(inputs(staging, step.job, step.partition))
            expected = (len(latest), sum(order["amount_cents"] for order in latest))
        got = _totals(path)
        if got != expected:
            problems.append(f"{step.job} {step.partition}: {got[0]} orders, {got[1]} cents; "
                            f"its inputs say {expected[0]} orders, {expected[1]} cents")
        else:
            log.info("check: %s %s adds up to its inputs: %d orders, %d cents", step.job, step.partition, *got)
    rebuilt = {f"{step.job}/{step.partition}" for step in steps}
    old = live(root)
    for path in sorted(old.rglob("data.jsonl")):
        name = str(path.parent.relative_to(old))
        if name not in rebuilt and (staging / "out" / name / "data.jsonl").read_bytes() != path.read_bytes():
            problems.append(f"{name}: changed, but it wasn't in the plan")
    log.info("check: every output outside the plan has its old bytes" if not any("plan" in p for p in problems)
             else "check: an output outside the plan changed")
    return problems


def swap(root: Path, version: str) -> None:
    """Point `out` at `version` in one step: make a new link beside it, then rename it over the old link."""
    next_link = root / "out.next"
    next_link.symlink_to(version)
    os.replace(next_link, root / "out")
    log.info("swapped: out now points to %s", version)
