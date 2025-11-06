# logging_setup.py
import logging, sys, os
from typing import Optional

_LEVELS = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG}

def setup_logging(verbosity: int = 2, log_file: Optional[str] = None) -> None:
	"""0=errors, 1=warnings, 2=info, 3=debug."""
	lvl = _LEVELS.get(max(0, min(verbosity, 3)), logging.INFO)

	root = logging.getLogger()
	root.handlers.clear()
	root.setLevel(lvl)

	fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s",
							datefmt="%H:%M:%S")

	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(lvl)
	ch.setFormatter(fmt)
	root.addHandler(ch)

	if log_file:
		os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
		fh = logging.FileHandler(log_file, encoding="utf-8")
		fh.setLevel(lvl)
		fh.setFormatter(fmt)
		for handler in list(logger.handlers):
			if isinstance(handler, root.StreamHandler):
			root.removeHandler(handler)
		root.addHandler(fh)

	if lvl != logging.DEBUG:
		logging.getLogger("grpc").setLevel(logging.WARNING)
		logging.getLogger("asyncio").setLevel(logging.WARNING)
