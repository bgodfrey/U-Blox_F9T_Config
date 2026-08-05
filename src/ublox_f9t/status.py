"""Optional status-publishing boundary for GNSS runtime telemetry."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any, Protocol


PUBLISHER_GROUP = "ublox_f9t.status_publishers"
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublisherConfig:
    """Configuration passed to an external status-publisher factory."""

    endpoint: str
    device_type: str = "gnss"


class StatusPublisher(Protocol):
    """Destination for the latest status from one GNSS receiver."""

    enabled: bool

    def publish(self, device_id: str, alias: str, record: dict[str, Any]) -> None: ...

    def close(self) -> None: ...


class NullStatusPublisher:
    """Publisher used when external latest-status reporting is disabled."""

    enabled = False

    def publish(self, device_id: str, alias: str, record: dict[str, Any]) -> None:
        del device_id, alias, record

    def close(self) -> None:
        return None


class BestEffortStatusPublisher:
    """Keep receiver operation alive when an external status service is down."""

    enabled = True

    def __init__(self, publisher: StatusPublisher) -> None:
        self.publisher = publisher
        self.warned_unavailable = False

    def publish(self, device_id: str, alias: str, record: dict[str, Any]) -> None:
        if not device_id:
            return
        try:
            self.publisher.publish(device_id, alias, record)
            self.warned_unavailable = False
        except Exception as exc:
            if not self.warned_unavailable:
                log.warning("status publish failed: %s", exc)
                self.warned_unavailable = True

    def close(self) -> None:
        try:
            self.publisher.close()
        except Exception as exc:
            log.warning("status publisher close failed: %s", exc)


def create_status_publisher(target: str) -> StatusPublisher:
    """Create the configured publisher for ``agent`` or ``server`` telemetry."""

    enabled = os.getenv("GNSS_REDIS_STATUS_ENABLED", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    selected_target = os.getenv("GNSS_REDIS_STATUS_PUBLISHER", "agent").strip().lower()
    if not enabled or selected_target != target:
        return NullStatusPublisher()

    backend = os.getenv("GNSS_STATUS_BACKEND", "panoseti").strip().lower()
    endpoint = os.getenv(
        "GNSS_REDIS_STATUS_GRPC_ADDR",
        os.getenv("TELEM_SVC_ADDR", "127.0.0.1:50051"),
    )
    device_type = os.getenv("GNSS_REDIS_STATUS_DEVICE_TYPE", "gnss")
    matches = [item for item in entry_points(group=PUBLISHER_GROUP) if item.name == backend]
    if not matches:
        raise RuntimeError(
            f"GNSS status backend {backend!r} is enabled but no publisher plugin is installed; "
            "install the relevant integration package or disable GNSS_REDIS_STATUS_ENABLED"
        )

    factory = matches[0].load()
    publisher = factory(PublisherConfig(endpoint=endpoint, device_type=device_type))
    return BestEffortStatusPublisher(publisher)
