#!/usr/bin/env python3
"""Verify receiver CFG registers against a manifest.

This is intentionally a small standalone CLI for operator/status checks. It
loads the same manifest format used by server_v1.py, expands global/role/device
settings for the attached receiver, reads current CFG values with UBX-CFG-VALGET,
and reports mismatches.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import json5
import serial
from serial.tools import list_ports
from pyubx2 import POLL, UBX_CONFIG_DATABASE as CFGDB, UBX_PROTOCOL, UBXMessage, UBXReader
from pyubx2.exceptions import UBXMessageError, UBXParseError, UBXStreamError, UBXTypeError


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import caster_setup_pb2 as pb  # noqa: E402
import server_v1  # noqa: E402


HEX10 = re.compile(r"[0-9A-F]{10}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify u-blox CFG registers against a manifest")
    parser.add_argument("--manifest", required=True, help="path to manifest JSON5 file")
    parser.add_argument(
        "--role",
        default="auto",
        choices=["auto", "base", "receiver", "timing_only"],
        help="manifest role config to verify; default reads the attached device's manifest role",
    )
    parser.add_argument("--port", default="auto", help="serial port, or 'auto' to probe")
    parser.add_argument("--baud", type=int, default=115200, help="serial baud rate")
    parser.add_argument("--layer", default=None, help="VALGET layer to read; default is manifest verify_layer or RAM")
    parser.add_argument("--timeout", type=float, default=3.0, help="VALGET timeout per chunk in seconds")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    return parser.parse_args()


def _norm(key: str) -> str:
    return key.strip().upper().replace("-", "_")


def _name_to_keyid(name: str) -> int:
    rec = CFGDB.get(name.upper())
    if rec is None:
        raise KeyError(f"unknown CFG name: {name}")
    if isinstance(rec, dict):
        return int(rec["keyID"])
    if isinstance(rec, (tuple, list)) and len(rec) >= 1:
        return int(rec[0])
    raise KeyError(f"unsupported CFGDB entry type for {name}: {type(rec)}")


def _merge_cfg_one(results_by_id: dict[int, Any], key: Any, value: Any) -> None:
    if isinstance(key, int):
        key_id = key
    elif isinstance(key, str):
        text = key.strip()
        if text.lower().startswith("0x"):
            key_id = int(text, 16)
        else:
            match = re.match(r"^CFG_0X([0-9A-Fa-f]{8})$", text.upper())
            if match:
                key_id = int(match.group(1), 16)
            else:
                try:
                    key_id = _name_to_keyid(_norm(text))
                except Exception:
                    return
    else:
        return
    results_by_id[key_id] = value


def _merge_cfg_results(results_by_id: dict[int, Any], parsed: Any) -> None:
    cfg = getattr(parsed, "cfgData", None) or getattr(parsed, "cfgdata", None)
    if isinstance(cfg, dict):
        for key, value in cfg.items():
            _merge_cfg_one(results_by_id, key, value)
        return
    if isinstance(cfg, list):
        for item in cfg:
            if isinstance(item, (tuple, list)) and len(item) >= 2:
                _merge_cfg_one(results_by_id, item[0], item[1])
        return

    attrs = getattr(parsed, "__dict__", {})
    for name, value in attrs.items():
        if isinstance(name, str) and name.startswith("CFG_"):
            _merge_cfg_one(results_by_id, name, value)

    for name, key_id in attrs.items():
        match = re.match(r"^(?:keyID|key|cfgKey|keyid)_?(\d+)$", name)
        if not match:
            continue
        idx = match.group(1)
        for value_name in (
            f"val_{idx}",
            f"val{idx}",
            f"value_{idx}",
            f"value{idx}",
            f"valU1_{idx}",
            f"valU2_{idx}",
            f"valU4_{idx}",
            f"valU8_{idx}",
            f"valI1_{idx}",
            f"valI2_{idx}",
            f"valI4_{idx}",
            f"valI8_{idx}",
            f"valR4_{idx}",
            f"valR8_{idx}",
        ):
            if value_name in attrs:
                _merge_cfg_one(results_by_id, key_id, attrs[value_name])
                break


def _values_equal(expected: Any, actual: Any) -> bool:
    if isinstance(expected, bool):
        return bool(actual) == expected
    if isinstance(expected, (int, bool)) and isinstance(actual, (int, bool)):
        return int(actual) == int(expected)
    if isinstance(expected, float):
        try:
            return abs(float(actual) - expected) <= 1e-9
        except Exception:
            return False
    return str(actual) == str(expected)


def get_uniqid(port: str, baud: int, timeout: float = 0.5, tries: int = 3) -> str:
    with serial.Serial(port, baud, timeout=timeout) as ser:
        reader = UBXReader(ser, protfilter=UBX_PROTOCOL)
        poll = UBXMessage("SEC", "SEC-UNIQID", POLL)
        for _ in range(tries):
            ser.reset_input_buffer()
            ser.write(poll.serialize())
            ser.flush()
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    _, msg = reader.read()
                except Exception:
                    break
                if getattr(msg, "identity", "") == "SEC-UNIQID":
                    uid = getattr(msg, "uniqueId", None) or msg.payload[4:9]
                    uid_hex = f"{uid:010X}" if isinstance(uid, int) else bytes(uid).hex().upper()
                    if HEX10.fullmatch(uid_hex):
                        return uid_hex
        raise TimeoutError("No SEC-UNIQID response")


def _candidate_ports() -> list[str]:
    ports = []
    for info in list_ports.comports():
        text = " ".join(str(getattr(info, attr, "") or "") for attr in ("device", "description", "manufacturer", "product", "hwid"))
        score = 0 if "u-blox" in text.lower() else 1
        ports.append((score, str(info.device)))
    ports.sort()
    return [port for _, port in ports]


def discover_receiver(port: str, baud: int) -> tuple[str, str]:
    if port != "auto":
        return port, get_uniqid(port, baud)

    errors = []
    for candidate in _candidate_ports():
        try:
            return candidate, get_uniqid(candidate, baud)
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")
    detail = "; ".join(errors) if errors else "no serial ports found"
    raise RuntimeError(f"could not discover u-blox receiver: {detail}")


def _role_enum(role: str) -> int:
    if role == "base":
        return pb.Role.BASE
    if role == "receiver":
        return pb.Role.RECEIVER
    if role == "timing_only":
        return pb.Role.TIMING_ONLY
    raise ValueError(f"unsupported role {role!r}")


def _manifest_role(manifest: dict[str, Any], device_id: str) -> str:
    device = (manifest.get("devices") or {}).get(device_id.strip().upper())
    if not device:
        raise KeyError(f"device {device_id} not found in manifest devices")
    role = str(device.get("role") or "").strip().lower()
    if role not in {"base", "receiver", "timing_only"}:
        raise ValueError(f"device {device_id} has no valid manifest role")
    return role


async def expected_cfg_data(manifest: dict[str, Any], device_id: str, role: str) -> tuple[list[str], list[tuple[str, Any]]]:
    layers, items = await server_v1.cfg_from_manifest_for_device(manifest, device_id, _role_enum(role))
    cfg_data: list[tuple[str, Any]] = []
    for item in items:
        key = item.get("key")
        if key:
            cfg_data.append((_norm(str(key)), item.get("value")))
    return layers, cfg_data


def verify_cfg_data(
    ser: serial.Serial,
    cfg_data: list[tuple[str, Any]],
    *,
    layer: str,
    timeout_per_chunk: float,
) -> dict[str, Any]:
    want_by_id: dict[int, tuple[str, Any]] = {}
    want_ids: list[int] = []
    skipped = []

    for name, value in cfg_data:
        try:
            key_id = _name_to_keyid(name)
        except KeyError as exc:
            skipped.append({"key": name, "reason": str(exc)})
            continue
        if key_id not in want_by_id:
            want_ids.append(key_id)
        want_by_id[key_id] = (name, value)

    poll_layer = {"RAM": 0, "BBR": 1, "FLASH": 2, "DEFAULT": 7}.get(layer.upper(), 0)
    got_by_id: dict[int, Any] = {}

    for i in range(0, len(want_ids), 32):
        chunk_ids = want_ids[i : i + 32]
        msg_get = UBXMessage.config_poll(layer=poll_layer, position=0, keys=chunk_ids)
        ser.reset_input_buffer()
        ser.write(msg_get.serialize())
        ser.flush()

        reader = UBXReader(ser, protfilter=UBX_PROTOCOL)
        deadline = time.time() + timeout_per_chunk
        last_valget_time = None

        while time.time() < deadline:
            try:
                _, parsed = reader.read()
            except (UBXMessageError, UBXParseError, UBXStreamError, UBXTypeError):
                continue
            if not parsed:
                continue
            if getattr(parsed, "identity", "") != "CFG-VALGET":
                continue
            _merge_cfg_results(got_by_id, parsed)
            last_valget_time = time.time()
            if all(key_id in got_by_id for key_id in chunk_ids):
                break
            if last_valget_time and (time.time() - last_valget_time) > 0.3:
                break

    mismatches = []
    matched = 0
    for key_id in want_ids:
        name, expected = want_by_id[key_id]
        if key_id not in got_by_id:
            mismatches.append({"key": name, "expected": expected, "actual": None, "status": "missing"})
            continue
        actual = got_by_id[key_id]
        if _values_equal(expected, actual):
            matched += 1
        else:
            mismatches.append({"key": name, "expected": expected, "actual": actual, "status": "mismatch"})

    checked = len(want_ids)
    return {
        "ok": not mismatches,
        "checked": checked,
        "matched": matched,
        "mismatches": mismatches,
        "skipped": skipped,
    }


def print_human(report: dict[str, Any]) -> None:
    status = "OK" if report["ok"] else "FAIL"
    print(f"{status} {report.get('device_id', 'UNKNOWN')} role={report['role']} port={report.get('port', 'unknown')}")
    print(f"  manifest: {report['manifest']}")
    print(f"  layer:    {report['layer']}")
    print(f"  matched:  {report['matched']}/{report['checked']}")
    if report["skipped"]:
        print(f"  skipped unsupported keys: {len(report['skipped'])}")
    for mismatch in report["mismatches"]:
        print(
            "  FAIL {key}: expected {expected!r}, actual {actual!r} ({status})".format(
                **mismatch
            )
        )


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest).resolve()
    manifest = json5.load(open(manifest_path, "r", encoding="utf-8"))
    layer = (args.layer or manifest.get("global", {}).get("verify_layer") or "RAM").upper()

    report: dict[str, Any] = {
        "ok": False,
        "manifest": str(manifest_path),
        "role": args.role,
        "layer": layer,
    }
    try:
        port, device_id = discover_receiver(args.port, args.baud)
        role = _manifest_role(manifest, device_id) if args.role == "auto" else args.role
        report["role"] = role
        _, cfg_data = asyncio.run(expected_cfg_data(manifest, device_id, role))
        with serial.Serial(port, args.baud, timeout=0.15, write_timeout=0.5) as ser:
            result = verify_cfg_data(ser, cfg_data, layer=layer, timeout_per_chunk=args.timeout)
        report.update(
            {
                "port": port,
                "device_id": device_id,
                "expected": len(cfg_data),
                **result,
            }
        )
    except Exception as exc:
        report.update({"error": str(exc), "mismatches": [], "skipped": [], "checked": 0, "matched": 0})

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_human(report)
        if report.get("error"):
            print(f"  error: {report['error']}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
