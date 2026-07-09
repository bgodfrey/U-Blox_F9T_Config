"""Small JSONL writer with size rotation and periodic fsync.

The GNSS agent and server both write one JSON record per line. This helper
keeps the active file separate from completed files so a later compressor can
ignore in-progress telemetry safely.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


class RotatingJsonlWriter:
    """Append JSONL records, rotating when the active file grows too large.

    Records are written to ``<name>.jsonl.active``. When the file rotates or the
    writer closes cleanly, the active file is renamed to ``<name>.jsonl``. The
    class opens the file for each write, so Python buffers are flushed every
    record; ``fsync`` is throttled by ``fsync_seconds`` to limit disk load.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        max_bytes: int = 0,
        fsync_seconds: float = 5.0,
    ) -> None:
        self.path = Path(path)
        self.max_bytes = max(0, int(max_bytes or 0))
        self.fsync_seconds = max(0.0, float(fsync_seconds))
        self._part = 0
        self._last_fsync = 0.0

    @property
    def active_path(self) -> Path:
        """Return the currently active filename."""

        return self.path.with_name(self.path.name + ".active")

    def set_path(self, path: str | os.PathLike[str]) -> None:
        """Change the base path, carrying an active file along when possible."""

        new_path = Path(path)
        if new_path == self.path:
            return

        old_active = self.active_path
        new_active = new_path.with_name(new_path.name + ".active")
        new_active.parent.mkdir(parents=True, exist_ok=True)
        if old_active.exists():
            os.replace(old_active, new_active)

        self.path = new_path
        self._part = 0

    def write(self, record: dict[str, Any]) -> None:
        """Serialize and append one JSON record."""

        line = json.dumps(record, separators=(",", ":")) + "\n"
        self._rotate_if_needed(len(line.encode("utf-8")))

        active = self.active_path
        active.parent.mkdir(parents=True, exist_ok=True)
        with active.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            self._fsync_if_due(handle)

    def close(self) -> None:
        """Finalize the active file so it is eligible for compression."""

        self._finalize_active()

    def _rotate_if_needed(self, next_bytes: int) -> None:
        if self.max_bytes <= 0:
            return

        active = self.active_path
        if not active.exists():
            return

        if active.stat().st_size + next_bytes <= self.max_bytes:
            return

        self._finalize_active()
        self._part += 1
        self.path = self._part_path(self._part)

    def _part_path(self, part: int) -> Path:
        stem = self.path.stem
        if "_part" in stem:
            stem = stem.rsplit("_part", 1)[0]
        return self.path.with_name(f"{stem}_part{part:03d}{self.path.suffix}")

    def _finalize_active(self) -> None:
        active = self.active_path
        if not active.exists():
            return

        target = self.path
        if target.exists():
            target = self._available_path(target)
        os.replace(active, target)

    def _available_path(self, path: Path) -> Path:
        for idx in range(1, 10000):
            candidate = path.with_name(f"{path.stem}_recovered{idx:03d}{path.suffix}")
            if not candidate.exists():
                return candidate
        raise RuntimeError(f"could not find available telemetry filename for {path}")

    def _fsync_if_due(self, handle: Any) -> None:
        if self.fsync_seconds <= 0:
            return

        now = time.monotonic()
        if now - self._last_fsync < self.fsync_seconds:
            return

        os.fsync(handle.fileno())
        self._last_fsync = now
