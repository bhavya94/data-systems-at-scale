"""Lineage: each output records which inputs it read, so a bad input can be traced to everything it reached.

Beside each output, a job writes `_lineage.json`: the job, and each input file with its fingerprint. Following
those records forward, from a raw file to the outputs that read it, then to the outputs that read those, gives
every output a bad day reached.

    raw/orders/date=2026-10-03/part-0.jsonl
    └─ out/daily_sales/date=2026-10-03/data.jsonl
       └─ out/weekly_sales/week=2026-W40/data.jsonl
"""

import json
import logging
from collections import defaultdict
from pathlib import Path

from data_systems_at_scale.pipelines.storage import fingerprint, write_atomic

log = logging.getLogger(__name__)
MANIFEST = "_lineage.json"


def record(root: Path, output: Path, job: str, inputs: list[Path]) -> None:
    """Write the lineage of `output`: the job that wrote it, and each input with its fingerprint."""
    entry = {"job": job, "output": str(output.relative_to(root)),
             "inputs": [{"path": str(path.relative_to(root)), "fingerprint": fingerprint(path)} for path in inputs]}
    write_atomic(output.with_name(MANIFEST), json.dumps(entry, indent=1, sort_keys=True) + "\n")
    log.info("lineage: %s read %d inputs: %s", entry["output"], len(inputs),
             ", ".join(source["path"] for source in entry["inputs"]))


def manifests(root: Path) -> list[dict]:
    return [json.loads(path.read_text()) for path in sorted((root / "out").rglob(MANIFEST))]


def downstream(root: Path, starts: list[str]) -> list[tuple[int, str]]:
    """Every output that read one of `starts`, directly or through other outputs, as (depth, path), depth-first."""
    readers: dict[str, list[str]] = defaultdict(list)
    for entry in manifests(root):
        for source in entry["inputs"]:
            readers[source["path"]].append(entry["output"])
    found: list[tuple[int, str]] = []

    def walk(path: str, depth: int) -> None:
        found.append((depth, path))
        for reader in sorted(readers[path]):
            walk(reader, depth + 1)

    for start in starts:
        walk(start, 0)
    return found
