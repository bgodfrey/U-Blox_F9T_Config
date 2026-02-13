#!/usr/bin/env python3
"""
Filename: conf_gnss.py
Author: Ben G. + ChatGPT
Date: August 2025
Version: 1.0

Purpose
-------
Single script that:
  1) Configures u-blox ZED-F9P / ZED-F9T using UBX-CFG-VALSET from JSON/JSON5.
  2) Optionally verifies config via UBX-CFG-VALGET.
  3) Supports a "manifest" format: global + role + devices.
  4) Provides a high-fidelity local logger:
       - Raw stream capture (binary)
       - JSONL ("jlog") with per-frame base64
       - Decoded UBX summaries
       - 1 Hz (or configurable) telemetry snapshots
       - Heartbeat counters
       - File rotation at 10 MiB
"""

import argparse, base64, csv, io, os, pyubx2, re, serial, signal, sys, time
import logging
import json as stdjson          # Used for JSONL logging (stable standard json)
import json5 as json            # Used for config/manifest parsing (comments/trailing commas allowed)
from typing import List, Tuple, Optional, Dict, Any

# Import pyubx2 primitives:
# - UBXReader: streaming parser (serial/file-like)
# - UBXMessage: message builder
# - *Error: exceptions thrown by UBXReader / parsing
# - POLL: constant indicating a poll message for specific UBX messages
from pyubx2 import (
    UBXMessage,
    UBXReader,
    UBXStreamError,
    UBXMessageError,
    UBXTypeError,
    UBXParseError,
    POLL
)

# NOTE: redundant typing import (already imported above). Not harmful, but could be removed.
from typing import List, Tuple, Optional, Dict, Any

# Optional: if pyubx2 provides a config DB, build a keyid->dtype map for nicer formatting.
try:
    from pyubx2 import UBX_CONFIG_DATABASE
    DTYPE_BY_ID = {kid: dtype for (_name, (kid, dtype)) in UBX_CONFIG_DATABASE.items()}
except Exception:
    DTYPE_BY_ID = {}

from pyubx2.ubxhelpers import cfgname2key, process_monver  # cfgname2key resolves CFG_* to numeric keyID; MON-VER helper
# from resources import make_rich_logger

# confg_gnss_logger = make_rich_logger("confg_gnss")

# Bundle of exceptions we treat as "non-fatal read errors" when streaming.
UBX_EXC = (UBXStreamError, UBXMessageError, UBXTypeError, UBXParseError)

# -----------------------------------------------------------------------------
# CFG-VALSET and CFG-VALGET layer handling
# -----------------------------------------------------------------------------
# For VALSET: 'layers' is a bitmask of where to apply settings.
SET_LAYER = {"RAM": 0x01, "BBR": 0x02, "FLASH": 0x04}

# For VALGET: 'layer' is a selector enum (0=RAM, 1=BBR, 2=FLASH, 7=DEFAULT).
POLL_LAYER = {"RAM": 0, "BBR": 1, "FLASH": 2, "DEFAULT": 7}

# Order used if you ever want to search across layers; FLASH often NAKs.
LAYER_ORDER = ["RAM", "BBR", "FLASH", "DEFAULT"]

# Regex for parsing names like "CFG_0x10520007" into key IDs.
_HEXKEY_RE = re.compile(r"^CFG_0x([0-9A-Fa-f]{8})$")

# -----------------------------------------------------------------------------
# Logging rotation metadata hook
# -----------------------------------------------------------------------------
def maybe_write_meta(jlog_out, *, port: str, baud: int, protfilter: int) -> None:
    """
    Write a 'meta' record at the start of each rotated JSONL log part.

    The RotatingFile sets `just_rotated=True` whenever a new part is opened.
    We then emit one meta JSON line describing the session context.
    """
    if getattr(jlog_out, "just_rotated", False):
        jlog_out.just_rotated = False
        log_record(jlog_out, {
            "type": "meta",
            "port": port,
            "baud": baud,
            "protfilter": protfilter,
            "start_unix_ms": int(time.time() * 1000),
            "file_part": getattr(jlog_out, "part", None),
        })
        jlog_out.flush()


def _ts() -> str:
    """Timestamp used in rotated filenames."""
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


class RotatingFile:
    """
    Simple size-based rotating file writer.

    Creates:
      <base>.<timestamp>.partNNNN

    Works for:
      - binary files (mode="ab") for raw stream
      - text files   (mode="a")  for JSONL records

    Rotation occurs BEFORE writing if (bytes_written + len(data)) > max_bytes.
    """
    def __init__(self, base_path: str, mode: str, max_bytes: int, buffering: int = 1):
        self.base_path = base_path
        self.mode = mode
        self.max_bytes = int(max_bytes)
        self.buffering = buffering
        self.session_ts = _ts()
        self.part = 0
        self.f = None
        self.bytes_written = 0
        self.just_rotated = True
        self._open_new()

    def _make_name(self) -> str:
        return f"{self.base_path}.{self.session_ts}.part{self.part:04d}"

    def _open_new(self):
        """Close old part (if any) and open a new one."""
        if self.f:
            try:
                self.f.flush()
            except Exception:
                pass
            try:
                self.f.close()
            except Exception:
                pass

        self.part += 1
        path = self._make_name()
        _ensure_dir(path)
        self.f = open(path, self.mode, buffering=self.buffering)

        # If file already exists (unlikely with timestamp+part), sync counters.
        try:
            self.f.seek(0, os.SEEK_END)
            self.bytes_written = self.f.tell()
            self.just_rotated = True
        except Exception:
            self.bytes_written = 0

    def write(self, data):
        """
        Write bytes/str to the underlying file, rotating as needed.

        Note: len(data) works for both bytes and str.
        """
        n = len(data)
        if (self.bytes_written + n) > self.max_bytes:
            self._open_new()
        self.f.write(data)
        self.bytes_written += n

    def flush(self):
        if self.f:
            self.f.flush()

    def close(self):
        if self.f:
            try:
                self.f.flush()
            except Exception:
                pass
            try:
                self.f.close()
            except Exception:
                pass
            self.f = None

# ----------------------------------------------------------------------------
# Telemetry aggregator
# ----------------------------------------------------------------------------
class TelemetryAgg:
    """
    TelemetryAgg consumes individual UBX frames and keeps "latest values" for a
    compact monitoring snapshot.

    It is purposely *not* a complete decode of every UBX message—it's a curated
    set of timing + health fields that you want in `type="telemetry"` records.
    """
    __slots__ = (
        "temp_c", "qerr_ps", "utc_ok", "num_vis", "num_used",
        "gps_used", "gal_used", "bds_used", "glo_used", "avg_cno", "pdop"
    )

    def __init__(self) -> None:
        # Set defaults such that "no data yet" is clearly visible.
        self.temp_c = None
        self.qerr_ps = None
        self.utc_ok = False
        self.num_vis = 0
        self.num_used = 0
        self.gps_used = 0
        self.gal_used = 0
        self.bds_used = 0
        self.glo_used = 0
        self.avg_cno = 0.0
        self.pdop = 0.0

    def feed_ubx(self, frame: bytes) -> None:
        """
        Update aggregator fields from a raw UBX frame.

        Implementation note:
        - Uses UBXReader(io.BytesIO(frame)) to parse "one message" from the bytes.
        - For NAV-SAT and TIM-TP, we also do manual parsing / fallback parsing
          to guard against pyubx2 version differences.
        """
        try:
            _, msg = UBXReader(io.BytesIO(frame)).read()
        except Exception:
            return

        ident = getattr(msg, "identity", "")
        if ident == "TIM-TP":
            # Primary source: pyubx2 attribute
            qe = getattr(msg, "qErr", None)

            # Fallback: parse raw UBX-TIM-TP payload (length 16 typically)
            # Layout: towMS U4, towSubMS U4, qErr I4 (ps), week U2, flags U1, reserved U1
            if qe is None:
                try:
                    payload = frame[6:-2]
                    if len(payload) >= 12:
                        qe = int.from_bytes(payload[8:12], "little", signed=True)
                except Exception:
                    qe = None

            self.qerr_ps = qe
            self.utc_ok = bool(getattr(msg, "utc", 0))

        elif ident == "MON-SYS":
            # Temperature etc.
            tv = getattr(msg, "tempValue", None)
            if tv is not None:
                self.temp_c = float(tv)

        elif ident == "NAV-SAT":
            # Fast manual parse from raw UBX frame.
            # This avoids heavy pyubx2 group parsing and is stable.
            payload = frame[6:-2]  # strip UBX header (6) + checksum (2)
            if len(payload) < 8:
                return

            numSvs = payload[5]
            # Each SV block is 12 bytes, after 8-byte header.
            n = min(int(numSvs), max(0, (len(payload) - 8) // 12))

            used = 0
            cno_sum = 0.0
            gps = gal = bds = glo = 0

            off = 8
            for _ in range(n):
                gnssId = payload[off + 0]
                cno    = payload[off + 2]
                flags  = int.from_bytes(payload[off + 8:off + 12], "little")
                cno_sum += float(cno)

                # svUsed is bit 3 (0x08) in flags
                if flags & 0x08:
                    used += 1
                    if gnssId == 0: gps += 1
                    elif gnssId == 2: gal += 1
                    elif gnssId == 3: bds += 1
                    elif gnssId == 6: glo += 1

                off += 12

            self.num_vis = n
            self.num_used = used
            self.gps_used, self.gal_used, self.bds_used, self.glo_used = gps, gal, bds, glo
            self.avg_cno = (cno_sum / n) if n else 0.0

        elif ident == "NAV-DOP":
            # pDOP comes as integer scaled by 100.
            pd = getattr(msg, "pDOP", None)
            if pd is not None:
                self.pdop = float(pd / 100.0)

        elif ident == "NAV-TIMEUTC":
            self.utc_ok = bool(getattr(msg, "validUTC", 0))

    def snapshot(self) -> dict:
        """
        Current telemetry snapshot as plain JSON-serializable types.
        This is what gets written to the log at snapshot_hz.
        """
        return {
            "temp_c": self.temp_c,
            "qerr_ps": self.qerr_ps,
            "utc_ok": int(bool(self.utc_ok)),
            "num_vis": int(self.num_vis),
            "num_used": int(self.num_used),
            "gps_used": int(self.gps_used),
            "gal_used": int(self.gal_used),
            "bds_used": int(self.bds_used),
            "glo_used": int(self.glo_used),
            "avg_cno_dbhz": round(float(self.avg_cno),4),
            "pdop": round(float(self.pdop),4),
        }


# -----------------------------------------------------------------------------
# Raw/decoded logging helpers
# -----------------------------------------------------------------------------
def _b64(raw: bytes) -> str:
    """Base64 encode raw bytes for JSONL storage."""
    return base64.b64encode(raw).decode("ascii")

def _safe_scalar(v):
    """
    Convert values into something JSON-serializable.
    - bytes -> hex
    - unknown objects -> str()
    """
    if v is None or isinstance(v, (int, float, str, bool)):
        return v
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return str(v)

def ubx_fields_flat(msg) -> dict:
    """
    Best-effort field extraction from a pyubx2 UBXMessage.
    Skips large list/group payloads (NAV-SAT, NAV-SIG groups, etc.) by default.
    """
    out = {}
    d = getattr(msg, "__dict__", {})
    for k, v in d.items():
        if k.startswith("_"):
            continue
        if k in ("group", "payload"):
            continue
        out[k] = _safe_scalar(v)
    return out

def expand_nav_sat_from_raw(frame: bytes) -> list:
    """
    Parse NAV-SAT payload into a per-satellite list of dicts.
    This complements the summary in TelemetryAgg and is useful for deep debugging.
    """
    payload = frame[6:-2]
    if len(payload) < 8:
        return []
    numSvs = payload[5]
    n = min(int(numSvs), max(0, (len(payload) - 8) // 12))
    svs = []
    off = 8
    for _ in range(n):
        gnssId = payload[off + 0]
        svId   = payload[off + 1]
        cno    = payload[off + 2]
        elev   = int.from_bytes(payload[off + 3:off + 4], "little", signed=True)
        azim   = int.from_bytes(payload[off + 4:off + 6], "little", signed=True)
        prRes  = int.from_bytes(payload[off + 6:off + 8], "little", signed=True)
        flags  = int.from_bytes(payload[off + 8:off + 12], "little", signed=False)
        svs.append({
            "gnssId": gnssId,
            "svId": svId,
            "cno": cno,
            "elev": elev,
            "azim": azim,
            "prRes": prRes,
            "flags": flags,
        })
        off += 12
    return svs

def log_record(out, rec: dict) -> None:
    """
    Write one JSON Lines record.
    out can be:
      - a file handle
      - RotatingFile
    as long as it has a `.write(str)` method.
    """
    line = stdjson.dumps(rec, separators=(",", ":")) + "\n"
    if hasattr(out, "write"):
        out.write(line)
    else:
        raise TypeError("log_record expects a file-like object")


def _ensure_dir(path: str) -> None:
    """Ensure parent directory exists for a file path."""
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def run_logger(port: str, baud: int, raw_path: str, json_path: str,
               snapshot_hz: float = 1.0,
               protfilter: int = 7,
               log_frames: bool = True,
               log_decoded_ubx: bool = True,
               expand_nav_sat: bool = True) -> None:
    """
    Firehose GNSS logger.

    - Writes ALL received frames to:
        raw_path.<ts>.partNNNN   (binary)
      and/or
        json_path.<ts>.partNNNN  (JSONL)

    JSONL record types:
      - meta:    emitted at start of each rotated part
      - frame:   every received frame (UBX/NMEA/RTCM) with raw base64
      - ubx:     decoded UBX fields (best effort)
      - telemetry: periodic snapshot from TelemetryAgg
      - heartbeat: periodic counters/top message IDs
    """
    _ensure_dir(raw_path)
    _ensure_dir(json_path)

    agg = TelemetryAgg()
    snapshot_period = 1.0 / max(0.1, float(snapshot_hz))

    # Simple SIGINT/SIGTERM shutdown flag.
    stop = {"flag": False}
    def _sigint(_signum, _frame):
        stop["flag"] = True
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    # Rotate files at 10 MiB for safety.
    max_bytes = 10 * 1024 * 1024  # 10 MiB

    with serial.Serial(port, baudrate=baud, timeout=0.5) as ser:
        # Rotating raw (binary) and JSONL (text) outputs.
        fraw = RotatingFile(raw_path, mode="ab", max_bytes=max_bytes, buffering=0)
        fj   = RotatingFile(json_path, mode="a",  max_bytes=max_bytes, buffering=1)

        try:
            # protfilter=7 means parse UBX+NMEA+RTCM (pyubx2 mapping)
            rdr = UBXReader(ser, protfilter=protfilter)

            next_snap = time.time()
            last_rx = time.time()
            last_hb = time.time()

            # Frame counters by protocol.
            cnt = {"UBX": 0, "NMEA": 0, "RTCM": 0, "OTHER": 0}

            # Frequency table of UBX identities to show what you're receiving.
            ubx_top = {}

            while not stop["flag"]:
                try:
                    raw, parsed = rdr.read()
                except UBX_EXC:
                    continue

                if raw:
                    # Save exact raw stream to a binary log (replay later).
                    fraw.write(raw)
                    last_rx = time.time()

                    # parsed might be None if pyubx2 couldn't parse a frame (or it’s a protocol you’re not decoding).
                    proto = getattr(parsed, "protocol", None) if parsed else None
                    ident = getattr(parsed, "identity", None) if parsed else None

                    # NOTE: This line is a bit tricky to read; for proto=None it increments OTHER.
                    # If proto is a key in cnt, it increments that.
                    # If proto is not a known key, it will create a new entry (unless proto is None).
                    cnt[proto] = cnt.get(proto, 0) + 1 if proto else cnt["OTHER"] + 1

                    # UBX message stats by identity.
                    if proto == "UBX" and ident:
                        ubx_top[ident] = ubx_top.get(ident, 0) + 1

                    # Log every frame as base64 JSONL.
                    if log_frames:
                        maybe_write_meta(fj, port=port, baud=baud, protfilter=protfilter)
                        log_record(fj, {
                            "type": "frame",
                            "unix_ms": int(time.time() * 1000),
                            "proto": proto,
                            "ident": ident,
                            "n": len(raw),
                            "raw_b64": _b64(raw),
                        })

                    # If it's UBX, log decoded fields + update telemetry aggregator.
                    if raw.startswith(b"\xb5\x62"):
                        if parsed is not None and log_decoded_ubx:
                            rec = {
                                "type": "ubx",
                                "unix_ms": int(time.time() * 1000),
                                "identity": getattr(parsed, "identity", None),
                                "fields": ubx_fields_flat(parsed),
                            }
                            # Optionally expand NAV-SAT into a big SV list.
                            if expand_nav_sat and getattr(parsed, "identity", "") == "NAV-SAT":
                                rec["svs"] = expand_nav_sat_from_raw(raw)

                            maybe_write_meta(fj, port=port, baud=baud, protfilter=protfilter)
                            log_record(fj, rec)

                        # Update downsampled telemetry view.
                        agg.feed_ubx(raw)

                now = time.time()

                # Periodic summary record (compact).
                if now >= next_snap:
                    maybe_write_meta(fj, port=port, baud=baud, protfilter=protfilter)
                    row = {"type": "telemetry", "unix_ms": int(now * 1000)}
                    row.update(agg.snapshot())
                    log_record(fj, row)
                    fj.flush()
                    next_snap = now + snapshot_period

                # Heartbeat record: counters + top message IDs; helps debug whether expected UBX is present.
                if now - last_hb > 2.0:
                    top = sorted(ubx_top.items(), key=lambda kv: kv[1], reverse=True)[:12]
                    maybe_write_meta(fj, port=port, baud=baud, protfilter=protfilter)
                    log_record(fj, {
                        "type": "heartbeat",
                        "unix_ms": int(now * 1000),
                        "counts": cnt,
                        "top_ubx": top,
                        "rx_age_s": round(now - last_rx, 3),
                    })
                    fj.flush()
                    last_hb = now

        finally:
            # Always close outputs cleanly.
            fraw.close()
            fj.close()


# -----------------------------------------------------------------------------
# Register formatting + register description helpers
# -----------------------------------------------------------------------------
def _to_float(x):
    """Best-effort float conversion; returns None if not numeric."""
    try:
        return float(x)
    except Exception:
        return None

def _fmt_val(v, dtype):
    """
    Format a raw value in a type-aware way (used for compare output).
    dtype examples: 'U1','U2','U4','I4','R8','L','E1'
    """
    if isinstance(v, (bytes, bytearray)):
        return v.hex()

    if dtype:
        d = dtype.upper()
        if d == "L":  # boolean/logical
            return "1" if bool(v) else "0"
        if d.startswith("R"):
            try:
                return f"{float(v):.6g}"
            except Exception:
                return str(v)

    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)

def _layers_mask(names: List[str]) -> int:
    """
    Convert ["RAM","BBR"] into a VALSET layer bitmask.
    """
    mask = 0
    for n in names:
        if n.upper() not in SET_LAYER:
            raise ValueError(f"Unknown layer '{n}'. Use RAM, BBR, FLASH.")
        mask |= SET_LAYER[n.upper()]
    return mask

def _merge_cfg_kvmap(dst: dict, kvmap: dict) -> None:
    """
    Legacy helper (not used anymore): merge key/value map into dst.
    """
    for k, v in kvmap.items():
        _merge_cfg_one(dst, k, v)

def _log_plan(cfg_items):
    """Print what you are about to write (useful in verbose mode)."""
    print('Planned writes: ')
    for it in cfg_items:
        print(f"  {it['name']:36s} id={hex(it['id'])} dtype={it['dtype']:>2s} value={_fmt_val(it['value'], it['dtype'])}")

def _await_ack(ser, timeout=3.0) -> bool:
    """
    Wait for UBX-ACK-ACK or UBX-ACK-NAK after a VALSET.
    Returns True if ACK-ACK, False if NAK or timeout.
    """
    rdr = UBXReader(ser, protfilter=2)  # UBX only
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            (raw, parsed) = rdr.read()
        except UBX_EXC:
            continue
        if parsed and parsed.identity in ("ACK-ACK", "ACK-NAK"):
            return parsed.identity == "ACK-ACK"
    return False

def _norm_name(s: str) -> str:
    """Normalize key names: 'cfg-tp-tp1-ena' -> 'CFG_TP_TP1_ENA'."""
    return s.strip().replace("-", "_").upper()

def _fmt_eng(value, dtype, scale_str, unit_str):
    """
    Convert raw values into engineering units using CSV scale/unit metadata.
    Returns (value_str, unit_suffix).
    """
    if isinstance(value, (bytes, bytearray)):
        return value.hex(), ""

    if dtype == "L":
        return ("1" if bool(value) else "0"), ""

    vnum = _to_float(value)
    if vnum is None:
        return str(value), ""

    scale = _to_float(scale_str)
    unit = (unit_str or "").strip()
    if scale is not None:
        eng = vnum * scale
        s = f"{eng:.6g}"
        u = "%" if unit == "%" else (f" {unit}" if unit and unit != "-" else "")
        return s, u
    else:
        u = "%" if unit == "%" else (f" {unit}" if unit and unit != "-" else "")
        return f"{vnum:g}", u

def _split_scaled_llh(lat_deg, lon_deg, h_m):
    """
    Convert LLH into u-blox fixed-mode format:
      - lat/lon: INT part (1e-7 deg) + HP residual (1e-9 deg)
      - height:  cm + HP residual (0.1 mm)
    """
    def ll_to_int_hp(deg):
        tot_1e9 = int(round(deg * 1e9))
        int_1e7 = int(tot_1e9 // 100)
        hp_1e9  = int(tot_1e9 - int_1e7 * 100)
        return int_1e7, hp_1e9

    lat_i, lat_hp = ll_to_int_hp(lat_deg)
    lon_i, lon_hp = ll_to_int_hp(lon_deg)

    tot_0p1mm = int(round(h_m * 10000))
    h_cm      = int(tot_0p1mm // 100)
    h_hp      = int(tot_0p1mm - h_cm * 100)
    return lat_i, lat_hp, lon_i, lon_hp, h_cm, h_hp


def _merge_cfg_one(dst: dict, k, v):
    """
    Merge a single key/value into dst, accepting:
      - numeric keyIDs (int)
      - hex string "0x...."
      - CFG_0xXXXXXXXX
      - CFG_* key names resolvable via cfgname2key()
    """
    if isinstance(k, int):
        kid = k
    elif isinstance(k, str):
        s = k.strip()
        if s.lower().startswith("0x"):
            kid = int(s, 16)
        else:
            m = _HEXKEY_RE.match(s)
            if m:
                kid = int(m.group(1), 16)
            else:
                try:
                    kid, _ = cfgname2key(_norm_name(s))
                except Exception:
                    return
    else:
        return
    dst[kid] = v

def _merge_cfg_results(dst: dict, parsed) -> None:
    """
    Normalize any CFG-VALGET payload into {numeric_keyid: value}.

    pyubx2 has varied how it exposes VALGET results across versions:
      - cfgData dict
      - cfgData list of tuples
      - direct attributes like CFG_TP_...
      - keyIDn / valn pairs
    This function attempts to merge all of those into `dst`.
    """
    cfg = getattr(parsed, "cfgData", None) or getattr(parsed, "cfgdata", None)
    if isinstance(cfg, dict):
        for k, v in cfg.items(): _merge_cfg_one(dst, k, v); return
    if isinstance(cfg, list):
        for k, v in cfg: _merge_cfg_one(dst, k, v); return

    attrs = vars(parsed)

    # 1) Direct CFG_* attributes
    for name, val in attrs.items():
        if isinstance(name, str) and name.startswith("CFG_"):
            _merge_cfg_one(dst, name, val)

    # 2) keyID/val pairs
    for name, kval in attrs.items():
        m = re.match(r'^(?:keyID|key|cfgKey|keyid)_?(\d+)$', name)
        if not m:
            continue
        idx = m.group(1)
        for vname in (
            f"val_{idx}", f"val{idx}", f"value_{idx}", f"value{idx}",
            f"valU1_{idx}", f"valU2_{idx}", f"valU4_{idx}", f"valU8_{idx}",
            f"valI1_{idx}", f"valI2_{idx}", f"valI4_{idx}", f"valI8_{idx}",
            f"valR4_{idx}", f"valR8_{idx}",
        ):
            if vname in attrs:
                _merge_cfg_one(dst, kval, attrs[vname])
                break

def _to_cfg_items(entries):
    """
    Convert a JSON config list of {key,value} into a list of items:
      {"name","id","dtype","value"}

    Important: pyubx2 cares about Python type:
      - integers for U*/I*/E*/X*/L
      - floats for R*
    so this function coerces types accordingly.
    """
    out = []
    for e in entries:
        name = _norm_name(str(e["key"]))
        val = e["value"]
        if isinstance(val, str) and val.lower().startswith("0x"):
            val = int(val, 16)

        kid, dtype = cfgname2key(name)
        dtype_char = dtype[0]

        try:
            if dtype_char in ('L', 'U', 'I', 'E', 'X') and not isinstance(val, int):
                val = int(val)
            elif dtype_char == 'R' and not isinstance(val, float):
                val = float(val)
        except (ValueError, TypeError) as exc:
            print(f"Warning: Could not coerce value for {name} to expected type {dtype}: {exc}", file=sys.stderr)

        out.append({"name": name, "id": kid, "dtype": dtype, "value": val})
    return out

def poll_mon_ver(ser, timeout=2.5):
    """
    Poll MON-VER (Class 0x0A, ID 0x04) to identify device and firmware.

    Returns parsed dict of SW/HW/extensions.
    """
    msg = UBXMessage(0x0A, 0x04, 0)  # empty payload poll
    ser.write(msg.serialize())
    rdr = UBXReader(ser, protfilter=2)
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            raw, parsed = rdr.read()
        except UBX_EXC:
            continue
        if parsed and getattr(parsed, "identity", "") == "MON-VER":
            info = {
                "swVersion": getattr(parsed, "swVersion", None).rstrip(b'\x00').decode('ascii'),
                "hwVersion": getattr(parsed, "hwVersion", None).rstrip(b'\x00').decode('ascii'),
                "extensions": [v.rstrip(b'\x00').decode('ascii') for k, v in parsed.__dict__.items() if k.startswith("extension")]
            }
            print(f"MON-VER: {info}")
            return info
    raise RuntimeError("No MON-VER response")

def resolve_keyid(k):
    """Legacy: resolve int/hex/name into numeric keyID."""
    if isinstance(k, int):
        return k
    if isinstance(k, str) and k.lower().startswith("0x"):
        return int(k, 16)
    kid, _dtype = cfgname2key(k)
    return kid

def send_cfg_valset_grouped(ser, cfg_items, layers_mask, verbose=False, sleep_after_signal=0.3):
    """
    Apply VALSET in two groups:
      1) CFG_SIGNAL_* keys first (these can restart tracking)
      2) Everything else (CFG_TMODE, CFG_TP, CFG_MSGOUT, etc.)

    Also chunk into <=64 pairs per UBX message (protocol limit).

    Returns list of ACK booleans, one per chunk.
    """
    def _send_chunks(pairs, transactional=True, tag=""):
        """
        Send config_set in chunks of 64 key-value pairs.
        Use transaction field for multi-chunk atomicity.
        """
        acks = []
        if not pairs:
            return acks
        n = len(pairs)
        for i in range(0, n, 64):
            chunk = pairs[i:i+64]
            tx = 0
            if transactional and n > 64:
                tx = 1 if i == 0 else (3 if i + 64 >= n else 2)  # start/cont/commit
            msg = UBXMessage.config_set(layers=layers_mask, transaction=tx, cfgData=chunk)
            print(f"[SET {tag}] chunk {i//64+1}/{(n+63)//64} tx={tx}, {len(chunk)} keys")
            ser.write(msg.serialize())
            ok = _await_ack(ser, timeout=2.5)
            print(f"[SET {tag}] ACK={'OK' if ok else 'NAK'}")
            acks.append(ok)
        return acks

    def _order_other(items):
        """
        Order remaining keys so that:
          - TMODE (time mode) is applied early
          - TP (time pulse) before msg outputs
          - MSGOUT before misc
        This can reduce weird intermediate states on some firmware.
        """
        prio = lambda n: (0 if n.startswith("CFG_TMODE_")
                        else 1 if n.startswith("CFG_TP_")
                        else 2 if n.startswith("CFG_MSGOUT_")
                        else 3)
        return sorted(items, key=lambda it: prio(it["name"]))

    sig   = [it for it in cfg_items if it["name"].startswith("CFG_SIGNAL_")]
    other = _order_other([it for it in cfg_items if not it["name"].startswith("CFG_SIGNAL_")])

    print(f"[SET] Group CFG_SIGNAL_*: {len(sig)} keys (atomic)")
    acks = _send_chunks([(it["id"], it["value"]) for it in sig], transactional=True, tag="SIGNAL")
    if sig and sleep_after_signal:
        time.sleep(sleep_after_signal)  # allow receiver to settle after signal config

    print(f"[SET] Group other CFG_*: {len(other)} keys (atomic)")
    acks += _send_chunks([(it["id"], it["value"]) for it in other], transactional=True, tag="OTHER")
    return acks

def poll_cfg(ser, key_ids, layer_name: str, position: int = 0):
    """
    Poll a list of key IDs via CFG-VALGET.
    - chunks <=64 keys
    - reads VALGET responses until keys are received or timeout/silence

    Returns dict: {keyID: value}
    """
    layer = POLL_LAYER[layer_name.upper()]
    results = {}
    for i in range(0, len(key_ids), 64):
        chunk = key_ids[i : i + 64]
        poll = UBXMessage.config_poll(layer=layer, position=position, keys=chunk)
        ser.write(poll.serialize())
        rdr = UBXReader(ser, protfilter=2)

        deadline = time.time() + 3.0
        last_rx = None
        while time.time() < deadline:
            try:
                raw, parsed = rdr.read()
            except UBX_EXC:
                continue
            if parsed and getattr(parsed, "identity", "") == "CFG-VALGET":
                _merge_cfg_results(results, parsed)
                last_rx = time.time()
                if all(k in results for k in chunk):
                    break

                # Some pyubx2 versions might expose cfgData differently; this is extra fallback.
                cfg = getattr(parsed, "cfgData", None) or getattr(parsed, "cfgdata", None)
                if isinstance(cfg, dict):
                    _merge_cfg_results(results, cfg)
                else:
                    for attr, val in parsed.__dict__.items():
                        if attr.startswith("keyID"):
                            idx = attr[5:]
                            kval = val
                            v = parsed.__dict__.get("val" + idx) or parsed.__dict__.get("value" + idx)
                            if v is not None:
                                results[kval] = v
                last_rx = time.time()
                if all(k in results for k in chunk):
                    break
            elif last_rx and (time.time() - last_rx) > 0.3:
                break
    return results


def load_regdesc_csv(path: str):
    """
    Load a CSV of register descriptions (name, id_hex, dtype, scale, unit, default, desc)
    into dicts:
      by_id[keyID] and by_name[name]

    This enables describe_plan() to print eng units and documentation.
    """
    by_id, by_name = {}, {}
    with open(path, newline="") as f:
        rdr = csv.reader(f)
        for row in rdr:
            if not row or row[0].strip().startswith("#"):
                continue
            if row[0].upper().startswith("CFG") and len(row) >= 2:
                name = _norm_name(row[0])
                id_hex = row[1].strip()
            else:
                continue
            try:
                kid = int(id_hex, 16)
            except Exception:
                m = _HEXKEY_RE.match(_norm_name(row[0]))
                kid = int(m.group(1), 16) if m else None
            if kid is None:
                try:
                    kid, _ = cfgname2key(name)
                except Exception:
                    continue

            dtype   = row[2].strip() if len(row) > 2 else ""
            scale   = row[3].strip() if len(row) > 3 else ""
            unit    = row[4].strip() if len(row) > 4 else ""
            default = row[5].strip() if len(row) > 5 else ""
            desc    = row[6].strip() if len(row) > 6 else ""

            entry = {"name": name, "id": kid, "dtype": dtype,
                     "scale": scale, "unit": unit, "default": default, "desc": desc}
            by_id[kid] = entry
            by_name[name] = entry
    return by_id, by_name


def build_tmode_fixed_from_json(pos: dict, acc_m: float):
    """
    Build CFG_TMODE_* key/value pairs for fixed-position (F9T).

    NOTE: F9T time-mode needs a fixed position. This helper converts your JSON
    LLH coordinates into the u-blox fixed-mode units.
    """
    fmt = pos.get("format","LLH").upper()
    items = []

    items += [("CFG_TMODE_MODE", 2)]                      # 2 = FIXED
    items += [("CFG_TMODE_POS_TYPE", 1 if fmt=="LLH" else 0)]
    items += [("CFG_TMODE_FIXED_POS_ACC", int(round(acc_m * 10000)))]  # meters -> 0.1 mm (?) (check docs if needed)

    if fmt == "LLH":
        lat_i, lat_hp, lon_i, lon_hp, h_cm, h_hp = _split_scaled_llh(
            float(pos["lat_deg"]), float(pos["lon_deg"]), float(pos["height_m"])
        )
        items += [
          ("CFG_TMODE_LAT",       lat_i),
          ("CFG_TMODE_LON",       lon_i),
          ("CFG_TMODE_HEIGHT",    h_cm),
          ("CFG_TMODE_LAT_HP",    lat_hp),
          ("CFG_TMODE_LON_HP",    lon_hp),
          ("CFG_TMODE_HEIGHT_HP", h_hp),
        ]
    else:
        raise NotImplementedError("ECEF path not shown—LLH is simplest")
    return items


def describe_plan(cfg_items, db_by_id, db_by_name):
    """
    Print a descriptive line per write:
      - key name
      - key id
      - dtype
      - raw value
      - engineering converted value (scale/unit)
      - description
      - default value (if provided)
    """
    # BUG NOTE: `print.info` does not exist; this line will crash if called.
    # Should be: print("Descriptions for planned writes:")
    print.info("Descriptions for planned writes:")
    for it in cfg_items:
        name = _norm_name(it["name"])
        kid  = it["id"]
        val  = it["value"]
        dtype = it.get("dtype","")
        entry = db_by_id.get(kid) or db_by_name.get(name)
        if not entry:
            print(f"  {name:32s} id={hex(kid)} dtype={dtype:>2s}  value={val}  (no CSV description)")
            continue

        eng_val, eng_unit = _fmt_eng(val, dtype, entry.get("scale",""), entry.get("unit",""))
        def_raw = entry.get("default","")
        def_txt = ""
        if def_raw not in ("", "-"):
            dv = _to_float(def_raw)
            ev, eu = _fmt_eng(dv if dv is not None else def_raw,
                              entry.get("dtype",""), entry.get("scale",""), entry.get("unit",""))
            def_txt = f" (default {ev}{eu})"

        desc = entry.get("desc","")
        print(f"  {name:32s} id={hex(kid)} dtype={entry.get('dtype',''):>2s} "
              f"raw={val}  eng={eng_val}{eng_unit} -> {desc}{def_txt}")


def initial_probe(ser, verbose=False):
    """
    Probe / sanity-check:
      - MON-VER
      - demonstrate VALGET wildcard group key polling
      - poll a couple of TP registers
    """
    info = poll_mon_ver(ser)
    if verbose:
        def _clean(b): return b.strip("\x00")
        print(f"[MON-VER] model={_clean(info['extensions'][3])} prot={_clean(info['extensions'][2])} fw={_clean(info['extensions'][1])}")

    # Proof-of-life for VALGET group wildcard keys (UART1 group keys).
    uart_default = poll_one_by_id(ser, 0x4052FFFF, layer=7)
    uart_ram     = poll_one_by_id(ser, 0x4052FFFF, layer=0)
    print(f"[VALGET] UART1 DEFAULT keys={len(uart_default)} RAM keys={len(uart_ram)}")

    # A couple timepulse keys by raw key IDs (example values).
    tp1 = poll_one_by_id(ser, 0x40050024, layer=0)  # CFG_TP_FREQ_TP1
    tp2 = poll_one_by_id(ser, 0x40050026, layer=0)  # CFG_TP_FREQ_TP2
    print(f"[VALGET] TP1 freq: {tp1.get(0x40050024)}, TP2 freq: {tp2.get(0x40050026)}")

def poll_one_by_id(ser, keyid: int, layer: int = 0, pos: int = 0, timeout=3.0):
    """
    Poll a single config keyID using CFG-VALGET.
    """
    msg = UBXMessage.config_poll(layer=layer, position=pos, keys=[keyid])
    # BUG NOTE: f-string missing; this prints literal braces.
    # Should be: print(f"TX: {msg} {msg.serialize().hex()}")
    print("TX: {msg} {msg.serialize().hex()}")
    ser.write(msg.serialize())

    rdr = UBXReader(ser, protfilter=2)
    t0 = time.time()
    out = {}
    while time.time() - t0 < timeout:
        try:
            raw, parsed = rdr.read()
        except UBX_EXC:
            continue
        if not parsed:
            continue
        print(f"RX: {parsed.identity} {getattr(parsed, 'length', None)}")
        if parsed.identity == "CFG-VALGET":
            out = {}
            _merge_cfg_results(out, parsed)
            valget_merged = {hex(k): v for k, v in out.items()}
            print(f"VALGET merged: {valget_merged}")
            return out
        elif parsed.identity == "ACK-NAK":
            raise RuntimeError("Device NAKed CFG-VALGET (bad payload or unsupported key).")
    return out

def detect_model(ser) -> str:
    """
    Derive device model string (e.g., ZED-F9T).
    """
    info = poll_mon_ver(ser)
    print('INFO: ' + str(info))
    mods = [x for x in info["extensions"]]
    mod = "UNKNOWN"
    for val in mods:
        if "MOD=" in val:
            mod = val[4:]
    return mod


def get_f9t_unique_id(ser: serial.Serial, timeout: float = 3.0) -> str:
    """
    Poll UBX-SEC-UNIQID and return the 5-byte UID as 10-hex chars, uppercase.

    Uses a proper UBX poll message:
      Class 0x27, ID 0x03.
    """
    SEC_CLASS = 0x27
    UNIQID_ID = 0x03
    msg = UBXMessage(SEC_CLASS, UNIQID_ID, POLL)

    try:
        ser.reset_input_buffer()
    except Exception:
        pass
    ser.write(msg.serialize())

    rdr = UBXReader(ser, protfilter=2)
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            (_raw, parsed) = rdr.read()
        except UBX_EXC:
            continue

        if not parsed:
            continue

        if getattr(parsed, "identity", "") == "SEC-UNIQID":
            uid = getattr(parsed, "uniqueId", None)
            print('UID: ' + str(uid))

            if isinstance(uid, (bytes, bytearray)) and len(uid) == 5:
                return uid.hex().upper()

            elif isinstance(uid, int):
                return f'{uid:010x}'.upper()

            else:
                raise RuntimeError(
                    "SEC-UNIQID received, but 'uniqueId' attribute was missing, "
                    f"of wrong type, or wrong length. Got: {uid!r}"
                )
    raise RuntimeError("Timeout waiting for SEC-UNIQID response from device.")


def lock_config(ser, layers: list[str]) -> list[bool]:
    """
    Set CFG_SEC_CFG_LOCK=1 on each requested layer.
    This can lock config until power cycle (depending on layer).
    """
    from pyubx2.ubxhelpers import cfgname2key
    kid, _ = cfgname2key("CFG_SEC_CFG_LOCK")
    acks = []
    for lname in layers:
        mask = SET_LAYER[lname.upper()]
        msg = UBXMessage.config_set(layers=mask, transaction=0, cfgData=[(kid, 1)])
        ser.write(msg.serialize())
        acks.append(_await_ack(ser, timeout=2.5))
    return acks

class _HelpFormatter(argparse.ArgumentDefaultsHelpFormatter,
                     argparse.RawDescriptionHelpFormatter):
    """Argparse formatter: shows defaults + preserves formatting."""
    pass

def parse_args():
    """
    CLI args:
      - Supports both a flat config file and a manifest.
      - Can select device by --uid or --alias, or auto-detect via SEC-UNIQID.
      - Can enable logging mode.
    """
    ap = argparse.ArgumentParser(
        prog="conf_gnss",
        description="Configure u-blox GNSS receivers (F9T/F9P) from a JSON plan and verify\n Currently only verifies a single layer",
        epilog=(
            "Examples:\n"
            "  python config_gnss.py conf_gnss.json5\n"
            "  python config_gnss.py -v --verify-layer RAM config.json\n"
            "  python config_gnss.py -vv --desc-csv regs.csv config.json\n"
            "  python config_gnss.py --probe-only\n"
        ),
        formatter_class=_HelpFormatter,
    )

    ap.add_argument("json", nargs="?", help="Path to configuration JSON/JSON5 (flat or manifest)")

    ap.add_argument("--uid", help="Device unique ID (SEC-UNIQID) to select from manifest (hex, e.g. D92EAA4324)")
    ap.add_argument("--alias", help="Device alias to select from manifest (e.g. WINTERS)")
    ap.add_argument("--list-devices", action="store_true",
                    help="If JSON is a manifest, list device IDs/aliases and exit")
    ap.add_argument("--auto-uid", action="store_true",
                    help="If JSON is a manifest, poll SEC-UNIQID on the chosen serial port and auto-select device")

    # NOTE: help strings still mention CSV, but you now write JSONL/jlog + raw.
    ap.add_argument("--logging", action="store_true", help ="Log ZED-F9T UBX raw frames + telemetry snapshots to CSV")
    ap.add_argument("--raw", default="logs/f9t.ubx", help="Binary output of all UBX frames (append).")
    ap.add_argument("--jlog", default="logs/f9t_telemetry.jlog", help="CSV snapshots (append).")
    ap.add_argument("--hz", type=float, default=1.0, help="Telemetry snapshot rate (Hz).")

    ap.add_argument("--port", default="/dev/ttyACM0", help="Serial port override (e.g. /dev/ttyACM0)")
    ap.add_argument("--baud", default=115200, type=int, help="Serial baud override")

    ap.add_argument("-v", "--verbose", action="count", default=0,
                    help="Increase verbosity (-v, -vv)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse & resolve, print plan, do not write")
    ap.add_argument("--probe-only", action="store_true",
                    help="Probe device (MON-VER, sample VALGET) and exit")
    ap.add_argument("--verify-layer", choices=["RAM","BBR","FLASH","DEFAULT"],
                    help="Layer to poll for verification (overrides JSON)")
    ap.add_argument("--desc-csv", help="CSV with register descriptions for -vv")

    if len(sys.argv) == 1:
        ap.print_help()
        sys.exit(0)

    return ap.parse_args()


def get_unique_id(ser, timeout=2.0):
    """
    Alternative SEC-UNIQID polling method using raw class/id match.
    (Looks like older/experimental approach; main flow uses get_f9t_unique_id.)
    """
    msg = UBXMessage(0x27, 0x03, 0)  # SEC-UNIQID
    ser.write(msg.serialize())

    rdr = UBXReader(ser, protfilter=2)
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            raw, parsed = rdr.read()
            print('PARSED: ' + str(raw))
            print(dir(parsed))

        except UBX_EXC:
            continue

        # Match by numeric class/id (robust even without name mapping)
        if getattr(parsed, "_ubxClass", b"") == b"\x27" and getattr(parsed, "_ubxID", b"") == b"\x03":
            uid = getattr(parsed, "uniqueId", None)
            if isinstance(uid, (bytes, bytearray)) and len(uid) >= 5:
                return bytes(uid[:5])

            # Fallback: parse raw payload. Layout: U1 version, U3 reserved, U5 uniqueId
            payload = getattr(parsed, "_payload", b"") or b""
            if len(payload) >= 9:
                print('Parse raw')
                return bytes(payload[4:9])

            for k, v in vars(parsed).items():
                if k.lower().startswith("unique") and isinstance(v, (bytes, bytearray)) and len(v) >= 5:
                    return bytes(v[:5])
            return None
    return None


def _norm_uid(uid: str) -> str:
    """Normalize UID to uppercase hex without separators."""
    s = (uid or "").strip().upper()
    return re.sub(r"[^0-9A-F]", "", s)


def _is_manifest(doc: dict) -> bool:
    """Detect manifest schema by presence of top-level keys."""
    if not isinstance(doc, dict):
        return False
    return any(k in doc for k in ("global", "devices", "role"))


def _list_manifest_devices(doc: dict) -> List[Tuple[str, str, str]]:
    """Return [(uid, alias, role), ...] from manifest for printing."""
    devs = doc.get("devices", {}) or {}
    out = []
    for uid, cfg in devs.items():
        uid_n = _norm_uid(uid)
        alias = str((cfg or {}).get("alias", ""))
        role = str((cfg or {}).get("role", ""))
        out.append((uid_n, alias, role))
    return sorted(out, key=lambda t: (t[1] or t[0]))


def _select_manifest_device(doc: dict, *, uid: Optional[str], alias: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    """
    Select a device entry by UID or alias.
    If neither provided:
      - if only one device: choose it
      - else: raise with choices
    """
    devs = doc.get("devices", {}) or {}
    if not devs:
        raise ValueError("Manifest has no 'devices' entries.")

    if uid:
        uid_n = _norm_uid(uid)
        for k, v in devs.items():
            if _norm_uid(k) == uid_n:
                return uid_n, (v or {})
        raise ValueError(f"No device with uid={uid_n} found in manifest.")

    if alias:
        want = str(alias).strip().lower()
        matches = [(k, v) for k, v in devs.items() if str((v or {}).get("alias", "")).strip().lower() == want]
        if len(matches) == 1:
            k, v = matches[0]
            return _norm_uid(k), (v or {})
        if len(matches) > 1:
            raise ValueError(f"Alias '{alias}' is not unique in manifest.")
        raise ValueError(f"No device with alias='{alias}' found in manifest.")

    if len(devs) == 1:
        k = next(iter(devs.keys()))
        return _norm_uid(k), (devs[k] or {})

    choices = ", ".join([f"{u}({a or 'no-alias'})" for (u, a, _r) in _list_manifest_devices(doc)])
    raise ValueError(f"Multiple devices in manifest; specify --uid or --alias. Choices: {choices}")


def _build_plan_from_manifest(doc: dict, args: argparse.Namespace) -> Dict[str, Any]:
    """
    Flatten manifest -> legacy flat schema:
      {port, baud, apply_to_layers, verify_layer, position, config: [...]}

    Merge order:
      global.config -> role.config -> device.config
    with de-duplication (later values override earlier).
    """
    g = doc.get("global", {}) or {}
    roles = doc.get("role", {}) or {}

    uid = None
    dev_cfg = None

    # auto-uid: talk to the receiver first and pick matching manifest entry.
    if getattr(args, "auto_uid", False):
        tmp_port = args.port or (g.get("port") if isinstance(g, dict) else None) or "/dev/ttyACM0"
        tmp_baud = args.baud or int((g.get("baud") if isinstance(g, dict) else None) or 115200)
        with serial.Serial(tmp_port, baudrate=tmp_baud, timeout=0.5) as ser:
            uid = get_f9t_unique_id(ser)
        uid, dev_cfg = _select_manifest_device(doc, uid=uid, alias=None)
    else:
        uid, dev_cfg = _select_manifest_device(doc, uid=args.uid, alias=args.alias)

    role_name = str(dev_cfg.get("role", ""))
    role_cfg = (roles.get(role_name, {}) or {}) if role_name else {}

    serial_cfg = dev_cfg.get("serial", {}) or {}
    port = args.port or serial_cfg.get("port") or g.get("port") or "/dev/ttyACM0"
    baud = int(args.baud or serial_cfg.get("baud") or g.get("baud") or 115200)

    apply_layers = dev_cfg.get("apply_to_layers") or g.get("apply_to_layers") or ["RAM"]
    verify_layer = dev_cfg.get("verify_layer") or g.get("verify_layer") or "RAM"

    register_csv = g.get("register_csv") or doc.get("register_csv")

    cfg_entries_raw = []
    for src in (g.get("config"), role_cfg.get("config"), dev_cfg.get("config")):
        if isinstance(src, list):
            cfg_entries_raw.extend(src)

    # De-duplicate by key; maintain first-seen order.
    dedup: Dict[str, Any] = {}
    order: List[str] = []
    for e in cfg_entries_raw:
        if not isinstance(e, dict) or "key" not in e:
            continue
        k = _norm_name(str(e["key"]))
        if k not in dedup:
            order.append(k)
        dedup[k] = e.get("value")
    cfg_entries = [{"key": k, "value": dedup[k]} for k in order]

    pos = dev_cfg.get("position") or g.get("position")

    return {
        "_manifest": {"selected_uid": uid, "alias": dev_cfg.get("alias"), "role": role_name},
        "port": port,
        "baud": baud,
        "apply_to_layers": apply_layers,
        "verify_layer": verify_layer,
        "register_csv": register_csv,
        "position": pos,
        "config": cfg_entries,
    }


def main():
    """
    Main flow:
      - parse args
      - load json/json5
      - optionally flatten manifest
      - open serial
      - optionally build fixed-position for F9T
      - optionally probe
      - write configuration (VALSET)
      - verify via VALGET
      - optionally start logger
    """
    args = parse_args()

    with open(args.json, "r") as f:
        doc = json.load(f)

    if _is_manifest(doc):
        if args.list_devices:
            rows = _list_manifest_devices(doc)
            if not rows:
                print("(no devices in manifest)")
            else:
                print("Devices in manifest:")
                for uid, alias, role in rows:
                    print(f"  uid={uid:10s}  alias={alias or '-':10s}  role={role or '-'}")
            return
        doc = _build_plan_from_manifest(doc, args)
        sel = doc.get("_manifest", {})
        print(f"Selected manifest device: uid={sel.get('selected_uid')} alias={sel.get('alias')} role={sel.get('role')}")

    port = doc.get("port", "/dev/ttyACM0")
    print('PORT IS: ' + str(port))
    baud = int(doc.get("baud", 115200))
    apply_layers = doc.get("apply_to_layers", ["RAM"])
    verify_layer = (args.verify_layer or doc.get("verify_layer", "RAM")).upper()
    cfg_entries = doc.get("config") or []
    if not isinstance(cfg_entries, list):
        raise ValueError("'config' must be a list of {key,value} entries")
    register_csv = args.desc_csv or doc.get("register_csv")
    pos = doc.get("position")

    cfg_items = _to_cfg_items(cfg_entries)
    set_layers_mask = _layers_mask(apply_layers)

    # Open serial for configuration phase
    with serial.Serial(port, baudrate=baud, timeout=0.5) as ser:
        print(ser)
        ser.reset_input_buffer()
        ser.reset_output_buffer()

        # Determine model via MON-VER.
        try:
            model = detect_model(ser)
        except Exception:
            model = 'UNKNOWN'

        print('MODEL IS: ' + str(model))

        # For F9T: if position provided, append CFG_TMODE_* fixed position keys.
        if model.startswith("ZED-F9T") and pos:
            if args.verbose:
                print("Setting coordinate positions from JSON")
            tmode_pairs = build_tmode_fixed_from_json(pos, pos.get("acc_m", 0.05))
            tmode_items = _to_cfg_items([{"key": k, "value": v} for k, v in tmode_pairs])
            cfg_items.extend(tmode_items)
        else:
            print('Not setting position likely this is a receiver')

        # Optional: snapshot values before applying (for diff printing in verbose mode).
        before_map = poll_cfg(ser, [it["id"] for it in cfg_items], verify_layer) if args.verbose else {}

        # Extra verbose: show register descriptions in engineering units.
        if args.verbose >= 2 and register_csv:
            try:
                db_by_id, db_by_name = load_regdesc_csv(register_csv)
                describe_plan(cfg_items, db_by_id, db_by_name)
            except Exception as e:
                print(f"[WARN] Could not load descriptions from {register_csv}: {e}")

        if args.verbose or args.probe_only:
            initial_probe(ser, verbose=True)
            if args.probe_only:
                return

        if args.verbose:
            _log_plan(cfg_items)
        if args.dry_run:
            return

        # Apply config and verify ACKs.
        acks = send_cfg_valset_grouped(ser, cfg_items, set_layers_mask, verbose=args.verbose)
        print(f"ACKs per chunk: {acks}")
        if not all(acks):
            print("[WARN] One or more CFG-VALSET batches were NAKed.")

        # Poll for verification (layer specified).
        key_ids = [it["id"] for it in cfg_items]
        reported = poll_cfg(ser, key_ids, verify_layer)

    # Compare desired vs reported (by key ID).
    failures = []
    for it in cfg_items:
        kid   = it["id"]
        want  = it["value"]
        got   = reported.get(kid)
        dtype = DTYPE_BY_ID.get(kid)
        ok    = (got == want)

        if not args.verbose:
            status = "OK" if ok else f"MISMATCH (wanted {want}, got {got})"
            print(f"{it['name']:36s} : {status} [{verify_layer}]")
        else:
            before_val = before_map.get(kid)
            line  = f"{it['name']:36s} : {'OK' if ok else 'MISMATCH'} [{verify_layer}]"
            line += f"  want={_fmt_val(want, dtype)}"
            line += f"  got={_fmt_val(got, dtype)}"
            line += f"  before={_fmt_val(before_val, dtype)}"
            print(line)

        if not ok:
            failures.append((it["name"], want, got))

    if failures:
        sys.exit(2)
    else:
        print("All settings applied and verified.")

    # Start local logging AFTER configuration/verification is complete.
    # (Reopens the serial port inside run_logger.)
    if args.logging:
        run_logger(port, baud, args.raw, args.jlog, snapshot_hz=args.hz)

    # Optional config lock (commented out):
    # layers_to_lock = ["BBR"]
    # acks = lock_config(ser, layers_to_lock)
    # print("Config lock ACKs:", acks)

if __name__ == "__main__":
    main()