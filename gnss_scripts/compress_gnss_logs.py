#!/usr/bin/env python3
"""Compress completed GNSS log/telemetry files.

This script is intended for cron/systemd maintenance jobs. It deliberately
skips active files, symlinks, recent files, and already-compressed files so it
can run while the GNSS agent/server continue writing telemetry.
"""

from __future__ import annotations

import argparse
import gzip
import os
import shutil
import sys
import time
from pathlib import Path


COMPRESS_SUFFIXES = (".jsonl", ".log", ".txt")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line options for the maintenance compressor."""

    parser = argparse.ArgumentParser(description="Compress completed GNSS log and telemetry files")
    parser.add_argument("--dirs", nargs="+", required=True, help="directories to scan")
    parser.add_argument(
        "--older-than-minutes",
        type=float,
        default=60.0,
        help="only compress files older than this many minutes",
    )
    parser.add_argument(
        "--gzip-level",
        type=int,
        default=3,
        choices=range(1, 10),
        metavar="1-9",
        help="gzip compression level; lower levels reduce DAQ-node CPU load",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=20,
        help="maximum files to compress in one run; <=0 means no limit",
    )
    parser.add_argument("--dry-run", action="store_true", help="show eligible files without compressing")
    return parser.parse_args(argv)


def compression_candidates(
    dirs: list[str],
    *,
    older_than_seconds: float,
    now: float | None = None,
) -> list[Path]:
    """Return closed, old-enough files that are safe to compress."""

    now = time.time() if now is None else now
    candidates: list[Path] = []

    for raw_dir in dirs:
        directory = Path(raw_dir).expanduser()
        if not directory.is_dir():
            continue

        for path in directory.iterdir():
            if not path.is_file() or path.is_symlink():
                continue
            if path.name.endswith(".active") or path.suffix == ".gz":
                continue
            if path.suffix not in COMPRESS_SUFFIXES:
                continue
            if now - path.stat().st_mtime < older_than_seconds:
                continue
            candidates.append(path)

    return sorted(candidates, key=lambda p: (p.stat().st_mtime, str(p)))


def gzip_file(path: Path, *, level: int) -> Path:
    """Compress one file to ``.gz`` and remove the uncompressed source."""

    gz_path = path.with_name(path.name + ".gz")
    if gz_path.exists():
        raise FileExistsError(f"refusing to overwrite existing file: {gz_path}")

    with path.open("rb") as src, gzip.open(gz_path, "wb", compresslevel=level) as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    os.unlink(path)
    return gz_path


def main(argv: list[str] | None = None) -> int:
    """Compress eligible files and print a concise summary."""

    args = parse_args(argv)
    older_than_seconds = max(0.0, args.older_than_minutes * 60.0)
    candidates = compression_candidates(args.dirs, older_than_seconds=older_than_seconds)

    if args.max_files > 0:
        candidates = candidates[: args.max_files]

    if not candidates:
        print("No eligible files to compress.")
        return 0

    compressed = 0
    for path in candidates:
        if args.dry_run:
            print(f"dry-run: would compress {path}")
            continue
        try:
            gz_path = gzip_file(path, level=args.gzip_level)
        except Exception as exc:
            print(f"ERROR: failed to compress {path}: {exc}", file=sys.stderr)
            return 1
        compressed += 1
        print(f"compressed: {path} -> {gz_path}")

    if args.dry_run:
        print(f"Dry run complete: {len(candidates)} eligible file(s).")
    else:
        print(f"Compressed {compressed} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
