"""Deletes that reach every copy: remove one customer's orders from raw data and from everything built from it.

1. Find: scan the raw files for the customer's orders.
2. Rewrite raw: the one exception to "raw is written once". Each file that holds the customer is rewritten
   without them, in one step, so no reader sees a half-deleted file.
3. Rebuild: the rewritten files have new fingerprints, so their days are stale. The stale backfill rebuilds them
   and every output they reached, including `customer_sales`, which keeps customer ids.
4. Verify: scan every file in the lake, raw data and outputs, for the customer's id. There must be none.
"""

import json
import logging
from pathlib import Path

from data_systems_at_scale.pipelines.storage import write_atomic

log = logging.getLogger(__name__)


def holders(root: Path, customer: str) -> dict[Path, int]:
    """Each raw file that holds the customer's orders, with how many."""
    found = {}
    for path in sorted((root / "raw").rglob("part-*.jsonl")):
        count = sum(json.loads(line)["customer"] == customer for line in path.read_text().splitlines())
        if count:
            found[path] = count
    return found


def erase(root: Path, customer: str) -> dict[Path, int]:
    """Rewrite each raw file that holds the customer, without them. Returns the files and the orders removed."""
    found = holders(root, customer)
    for path, count in found.items():
        kept = [line + "\n" for line in path.read_text().splitlines() if json.loads(line)["customer"] != customer]
        write_atomic(path, "".join(kept))
        log.info("erased %d orders of %s from %s; %d remain", count, customer, path.relative_to(root), len(kept))
    return found


def copies(root: Path, customer: str) -> list[Path]:
    """Every file in the lake that still mentions the customer's id."""
    needle = json.dumps(customer)  # with its quotes, so c-17 doesn't match c-170
    return sorted(path for path in root.rglob("*") if path.is_file() and needle in path.read_text())
