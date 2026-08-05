"""Tests for installed receiver discovery."""

from __future__ import annotations

import contextlib
import io
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ublox_f9t import detect


class DetectTest(unittest.TestCase):
    def test_prints_only_ublox_ports(self) -> None:
        ports = [
            SimpleNamespace(
                vid=0x1546,
                pid=0x01A9,
                device="/dev/ttyACM0",
                manufacturer="u-blox",
                product="ZED-F9T",
            ),
            SimpleNamespace(
                vid=0x1234,
                pid=1,
                device="/dev/ttyUSB0",
                manufacturer=None,
                product=None,
            ),
        ]
        output = io.StringIO()
        with patch.object(detect.list_ports, "comports", return_value=ports):
            with contextlib.redirect_stdout(output):
                result = detect.main()

        self.assertEqual(result, 0)
        self.assertIn("/dev/ttyACM0", output.getvalue())
        self.assertNotIn("/dev/ttyUSB0", output.getvalue())


if __name__ == "__main__":
    unittest.main()
