"""Tests for the optional GNSS status-publisher boundary."""

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from ublox_f9t.status import BestEffortStatusPublisher, NullStatusPublisher, create_status_publisher


class StatusPublisherTest(unittest.TestCase):
    def test_disabled_status_uses_null_publisher(self) -> None:
        with patch.dict("os.environ", {"GNSS_REDIS_STATUS_ENABLED": "0"}, clear=True):
            publisher = create_status_publisher("agent")

        self.assertIsInstance(publisher, NullStatusPublisher)
        self.assertFalse(publisher.enabled)

    def test_unselected_target_uses_null_publisher(self) -> None:
        env = {
            "GNSS_REDIS_STATUS_ENABLED": "1",
            "GNSS_REDIS_STATUS_PUBLISHER": "server",
        }
        with patch.dict("os.environ", env, clear=True):
            publisher = create_status_publisher("agent")

        self.assertIsInstance(publisher, NullStatusPublisher)
        self.assertFalse(publisher.enabled)

    def test_missing_enabled_plugin_is_configuration_error(self) -> None:
        env = {
            "GNSS_REDIS_STATUS_ENABLED": "1",
            "GNSS_REDIS_STATUS_PUBLISHER": "agent",
            "GNSS_STATUS_BACKEND": "missing-test-backend",
        }
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaisesRegex(RuntimeError, "no publisher plugin is installed"):
                create_status_publisher("agent")

    def test_enabled_plugin_uses_best_effort_wrapper(self) -> None:
        entry_point = Mock(name="entry_point")
        entry_point.name = "test"
        entry_point.load.return_value = lambda config: Mock(config=config)
        env = {
            "GNSS_REDIS_STATUS_ENABLED": "1",
            "GNSS_REDIS_STATUS_PUBLISHER": "agent",
            "GNSS_STATUS_BACKEND": "test",
        }
        with patch.dict("os.environ", env, clear=True):
            with patch("ublox_f9t.status.entry_points", return_value=[entry_point]):
                publisher = create_status_publisher("agent")

        self.assertIsInstance(publisher, BestEffortStatusPublisher)
        self.assertTrue(publisher.enabled)


if __name__ == "__main__":
    unittest.main()
