"""Writing an output so that a rerun or a crash never leaves it half-written.

`write_atomic` writes the new content to a temp file beside the output, then renames the temp file over the
output with `os.replace`. On one filesystem a rename is atomic: a reader sees the old file or the new one, never
a mix. A crash before the rename leaves the old output whole, and only a temp file that nothing reads.
"""

import hashlib
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)


def write_atomic(path: Path, text: str, crash_midway: bool = False) -> None:
    """Replace `path` with `text` in one step. `crash_midway` stops halfway through the temp file, as a crash would."""
    temp = path.with_name(path.name + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with temp.open("w") as file:
        if crash_midway:
            file.write(text[: len(text) // 2])
            raise RuntimeError(f"simulated crash after writing half of {temp.name}")
        file.write(text)
    log.info("wrote %d bytes to %s; renaming it over %s in one step", len(text), temp.name, path.name)
    os.replace(temp, path)


def fingerprint(path: Path) -> str | None:
    """The first 12 hex digits of the file's SHA-256, or None if there is no file. Same bytes, same fingerprint."""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12] if path.exists() else None
