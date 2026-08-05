"""Smoke tests for the installed command-line facade."""

from __future__ import annotations

import contextlib
import io
import unittest
from unittest.mock import patch

from ublox_f9t.cli import main


class CliTest(unittest.TestCase):
    def test_help_uses_public_command_name(self) -> None:
        output = io.StringIO()
        with patch("sys.argv", ["ublox-f9t", "--help"]):
            with contextlib.redirect_stdout(output):
                with self.assertRaises(SystemExit) as raised:
                    main()

        self.assertEqual(raised.exception.code, 0)
        self.assertIn("usage: ublox-f9t", output.getvalue())
        self.assertIn("install-service", output.getvalue())
        self.assertIn("agent", output.getvalue())
        self.assertIn("server", output.getvalue())

    def test_agent_help_is_delegated(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as raised:
                main(["agent", "--help"])

        self.assertEqual(raised.exception.code, 0)
        self.assertIn("--cast_addr", output.getvalue())

    def test_server_help_is_delegated(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as raised:
                main(["server", "--help"])

        self.assertEqual(raised.exception.code, 0)
        self.assertIn("--timing-mode", output.getvalue())

    def test_detect_help(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as raised:
                main(["detect", "--help"])

        self.assertEqual(raised.exception.code, 0)
        self.assertIn("connected U-Blox", output.getvalue())


if __name__ == "__main__":
    unittest.main()
