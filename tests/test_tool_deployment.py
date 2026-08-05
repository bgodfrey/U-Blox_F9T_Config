"""Regression tests for checkout-free deployment configuration."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from gnss_scripts.gnss_orchestrator import _bodnar_configure_script, status_gnss


class ToolDeploymentTest(unittest.TestCase):
    def test_status_resolves_installed_tool_paths_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {
                "defaults": {
                    "tool_command": [sys.executable, "-m", "ublox_f9t"],
                    "detect_command": [sys.executable, "-m", "ublox_f9t", "detect"],
                    "working_directory": directory,
                    "logdir": directory,
                    "telem_dir": directory,
                    "cast_addr": "localhost:50054",
                    "ctrl_addr": "localhost:50054",
                    "verbosity": 2,
                    "bodnar": {
                        "present": True,
                        "tool_command": "lbe-1420",
                        "working_directory": directory,
                        "out1_enabled": True,
                        "frequency_hz": 10_000_000,
                        "gnss": "recommended",
                    },
                },
                "modes": {
                    "differential": {
                        "timing_mode": "differential",
                        "receiver_manifest": "package:manifest_f9t.json5",
                    }
                },
                "server": {
                    "daq_name": "test-server",
                    "screen": "gnss_server",
                    "bind_addr": "localhost:50054",
                },
                "nodes": {
                    "daq": {
                        "daq_name": "DAQ",
                        "host": "daq.example",
                        "ssh_user": "test",
                    }
                },
            }
            config_path = root / "deployment.json5"
            config_path.write_text(json.dumps(config), encoding="utf-8")

            report = status_gnss(config_path, local_only=True)

        server, node = report["results"]
        self.assertTrue(server["resolved"]["receiver_manifest"].endswith("manifest_f9t.json5"))
        self.assertTrue(Path(server["resolved"]["receiver_manifest"]).is_file())
        self.assertEqual(node["resolved"]["logdir"], directory)
        self.assertEqual(node["resolved"]["telem_dir"], directory)
        self.assertEqual(node["resolved"]["detect_command"][-1], "detect")
        self.assertEqual(node["resolved"]["bodnar"]["tool_command"], ["lbe-1420"])

    def test_bodnar_tool_command_does_not_require_checkout_paths(self) -> None:
        script = _bodnar_configure_script(
            {
                "tool_command": ["lbe-1420"],
                "working_directory": "/home/test",
                "python": "",
                "script": "",
            },
            {
                "out1_enabled": True,
                "frequency_hz": 10_000_000,
                "gnss": "recommended",
            },
        )

        self.assertIn("lbe-1420 --enable 1", script)
        self.assertIn("lbe-1420 --f1 10000000", script)
        self.assertIn("lbe-1420 --gnss recommended", script)
        self.assertNotIn("lbe-1420-conf.py", script)
