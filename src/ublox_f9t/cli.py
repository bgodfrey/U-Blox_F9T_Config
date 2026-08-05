"""Public command-line entry point for U-Blox F9T operations."""

from __future__ import annotations

import sys

from gnss_scripts.gnss_orchestrator import main as orchestrator_main


def main(argv: list[str] | None = None) -> int:
    """Run the GNSS CLI."""

    return orchestrator_main(sys.argv[1:] if argv is None else argv)
