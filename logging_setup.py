# logging_setup.py
import logging, sys, os
from typing import Optional

_LEVELS = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG}

def setup_logging(
	verbosity: int = 2,
	log_file: Optional[str] = None,
	console: bool = True,             # control console output
	redirect_std: bool = False,       # redirect print()/stderr into logging
) -> None:
	lvl = _LEVELS.get(max(0, min(verbosity, 3)), logging.INFO)

	root = logging.getLogger()
	root.handlers.clear()
	root.setLevel(lvl)

	fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s",
							datefmt="%H:%M:%S")

	# Console handler (only if you want it)
	if console:
		ch = logging.StreamHandler(sys.stdout)
		ch.setLevel(lvl); ch.setFormatter(fmt)
		root.addHandler(ch)

	# File handler (optional)
	if log_file:
		os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
		fh = logging.FileHandler(log_file, encoding="utf-8")
		fh.setLevel(lvl); fh.setFormatter(fmt)
		root.addHandler(fh)

	# Quiet down noisy libs unless debugging
	if lvl != logging.DEBUG:
		logging.getLogger("grpc").setLevel(logging.WARNING)
		logging.getLogger("asyncio").setLevel(logging.WARNING)

	# Optionally funnel stray prints to logging
	if redirect_std:
		class _StreamToLogger:
			def __init__(self, name, level):
				self._log = logging.getLogger(name); self._lvl = level
			def write(self, buf):
				for line in str(buf).rstrip().splitlines():
					if line:
						self._log.log(self._lvl, line)
			def flush(self): pass
		sys.stdout = _StreamToLogger("stdout", logging.INFO)
		sys.stderr = _StreamToLogger("stderr", logging.ERROR)