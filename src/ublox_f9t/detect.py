"""Discover connected U-Blox USB serial receivers."""

from __future__ import annotations

from serial.tools import list_ports


UBLOX_VENDOR_ID = 0x1546


def main() -> int:
    """Print U-Blox serial ports and return whether at least one was found."""

    found = False
    for port in list_ports.comports():
        if port.vid != UBLOX_VENDOR_ID:
            continue
        found = True
        vid = f"{port.vid:04x}" if port.vid is not None else "unknown"
        pid = f"{port.pid:04x}" if port.pid is not None else "unknown"
        print(
            f"{port.device}  vid:pid={vid}:{pid}  "
            f"mfg='{port.manufacturer or ''}'  prod='{port.product or ''}'"
        )
    return 0 if found else 1
