# logging_setup_min.py
from __future__ import annotations
import logging, sys

_LEVELS = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG}

def setup_logging(verbosity: int = 2, log_file: str | None = None):
	"""Configure a console logger + optional file, based on a simple verbosity int (0..3)."""
	lvl = _LEVELS.get(max(0, min(verbosity, 3)), logging.INFO)

	# root logger
	root = logging.getLogger()
	root.handlers.clear()
	root.setLevel(lvl)

	fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s",
							datefmt="%H:%M:%S")

	# console
	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(lvl); ch.setFormatter(fmt)
	root.addHandler(ch)

	# optional file
	if log_file:
		fh = logging.FileHandler(log_file, encoding="utf-8")
		fh.setLevel(lvl); fh.setFormatter(fmt)
		root.addHandler(fh)

	# keep 3rd-party noise down unless we’re on DEBUG
	if lvl != logging.DEBUG:
		logging.getLogger("grpc").setLevel(logging.WARNING)
		logging.getLogger("asyncio").setLevel(logging.WARNING)
