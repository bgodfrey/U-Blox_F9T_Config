"""
fake_gnss_generator.py
======================
Standalone module that generates valid UBX and RTCM3 binary frames for
testing without a physical u-blox ZED-F9T receiver.

All frames pass the same checksum/CRC validation used by serial_demux_loop
in agent_v1.py.

UBX Frame layout:
  0xB5 0x62  [cls] [id]  [len_lo] [len_hi]  <payload…>  [ck_a] [ck_b]
  Checksum (Fletcher-8) covers bytes 2 … 6+len-1 (class through last payload byte).

RTCM3 Frame layout:
  0xD3  [0x00 | len_hi_2bit]  [len_lo]  <payload…>  <CRC24Q 3 bytes>
  CRC24Q covers header(3) + payload.
"""

import struct
from typing import List, Tuple


# ---------------------------------------------------------------------------
# Low-level checksum helpers (identical to agent_v1.py internals)
# ---------------------------------------------------------------------------

def _ubx_ck(payload: bytes) -> Tuple[int, int]:
    """Fletcher-8 checksum over *payload*."""
    ck_a = ck_b = 0
    for b in payload:
        ck_a = (ck_a + b) & 0xFF
        ck_b = (ck_b + ck_a) & 0xFF
    return ck_a, ck_b


def _crc24q(b: bytes) -> int:
    """CRC-24Q (Qualcomm) over *b*.  Returns a 24-bit integer."""
    c = 0
    for x in b:
        c ^= (x & 0xFF) << 16
        for _ in range(8):
            c <<= 1
            if c & 0x1000000:
                c ^= 0x1864CFB
            c &= 0xFFFFFF
    return c


# ---------------------------------------------------------------------------
# UBX frame builder
# ---------------------------------------------------------------------------

def make_ubx_frame(cls: int, id_: int, payload: bytes) -> bytes:
    """Wrap *payload* in a valid UBX frame for class *cls*, message ID *id_*."""
    length = len(payload)
    # Checksum covers: cls, id_, len_lo, len_hi, payload
    ck_input = bytes([cls, id_, length & 0xFF, (length >> 8) & 0xFF]) + payload
    ck_a, ck_b = _ubx_ck(ck_input)
    return (
        b"\xB5\x62"
        + bytes([cls, id_])
        + struct.pack("<H", length)
        + payload
        + bytes([ck_a, ck_b])
    )


# ---------------------------------------------------------------------------
# UBX message constructors
# ---------------------------------------------------------------------------

# Class / ID constants (u-blox interface manual)
_CLS_TIM  = 0x0D
_CLS_MON  = 0x0A
_CLS_NAV  = 0x01

_ID_TIM_TP       = 0x01
_ID_MON_SYS      = 0x39   # HPG-only; tempValue field
_ID_NAV_SAT      = 0x35
_ID_NAV_DOP      = 0x04
_ID_NAV_TIMEUTC  = 0x21


def make_tim_tp(
    qerr_ps: int = 0,
    utc_ok: bool = True,
    tow_ms: int = 0,
    week: int = 2300,
) -> bytes:
    """TIM-TP frame.  qerr_ps is the quantisation error in picoseconds (I4).
    utc_ok maps to the UTC bit in the flags byte."""
    # Payload (16 bytes):
    #  towMS(U4)  towSubMS(U4)  qErr(I4)  week(U2)  flags(U1)  refInfo(U1)
    flags = 0x02 if utc_ok else 0x00   # bit 1 = utc
    payload = struct.pack(
        "<IIiHBB",
        tow_ms,   # towMS
        0,        # towSubMS
        qerr_ps,  # qErr (signed)
        week,
        flags,
        0,        # refInfo
    )
    return make_ubx_frame(_CLS_TIM, _ID_TIM_TP, payload)


def make_mon_sys(temp_c: float = 25.0) -> bytes:
    """MON-SYS frame.  tempValue is at byte offset 18 (I1, °C signed integer).

    pyubx2 MON-SYS payload layout (24 bytes):
      msgVer(U1) bootType(U1) cpuLoad(U1) cpuLoadMax(U1)
      memUsage(U1) memUsageMax(U1) ioUsage(U1) ioUsageMax(U1)
      runTime(U4) noticeCount(U2) warnCount(U2) errorCount(U2)
      tempValue(I1) reserved0(U5)
    """
    raw_temp = max(-128, min(127, int(round(temp_c))))
    payload = struct.pack(
        "<BBBBBBBBIHHHb5s",
        0,        # msgVer
        0,        # bootType
        20,       # cpuLoad (%)
        25,       # cpuLoadMax
        30,       # memUsage
        40,       # memUsageMax
        10,       # ioUsage
        15,       # ioUsageMax
        3600,     # runTime (s)
        0,        # noticeCount
        0,        # warnCount
        0,        # errorCount
        raw_temp, # tempValue (signed)
        b"\x00" * 5,  # reserved0
    )
    return make_ubx_frame(_CLS_MON, _ID_MON_SYS, payload)


def make_nav_sat(
    satellites: List[dict] | None = None,
    itow: int = 0,
) -> bytes:
    """NAV-SAT frame.

    *satellites* is a list of dicts with optional keys:
      gnss_id (0=GPS, 2=Galileo, 3=BeiDou, 6=GLONASS)
      sv_id   (satellite vehicle ID)
      cno     (carrier-to-noise, dBHz)
      used    (bool, whether used in solution — sets bit 3 of flags)

    The payload layout matches agent_v1.py's raw-byte parser:
      offset 0-3: iTOW (U4)
      offset 4:   version (U1, 0)
      offset 5:   numSvs (U1)
      offset 6-7: reserved (U2)
      then numSvs × 12-byte blocks:
        [0] gnssId  [1] svId  [2] cno  [3] elev  [4-5] azim  [6-7] prRes
        [8-11] flags (U4 little-endian; bit 3 = svUsed)
    """
    if satellites is None:
        satellites = []

    header = struct.pack("<IBBH", itow, 0, len(satellites), 0)
    blocks = bytearray()
    for s in satellites:
        gnss_id = s.get("gnss_id", 0)
        sv_id   = s.get("sv_id", 1)
        cno     = s.get("cno", 40)
        used    = s.get("used", True)
        flags   = 0x08 if used else 0x00   # bit 3 = svUsed
        blocks += struct.pack("<BBBbhhI", gnss_id, sv_id, cno, 0, 0, 0, flags)

    return make_ubx_frame(_CLS_NAV, _ID_NAV_SAT, header + bytes(blocks))


def make_nav_dop(pdop: float = 1.5, itow: int = 0) -> bytes:
    """NAV-DOP frame.  pDOP is stored as U2 = pdop × 100.

    Payload (18 bytes):
      iTOW(U4) gDOP(U2) pDOP(U2) tDOP(U2) vDOP(U2) hDOP(U2) nDOP(U2) eDOP(U2)
    """
    pdop_raw = int(round(pdop * 100))
    payload = struct.pack(
        "<IHHHHHHH",
        itow,
        100,       # gDOP (1.00)
        pdop_raw,  # pDOP
        80,        # tDOP
        80,        # vDOP
        80,        # hDOP
        80,        # nDOP
        80,        # eDOP
    )
    return make_ubx_frame(_CLS_NAV, _ID_NAV_DOP, payload)


def make_nav_timeutc(utc_ok: bool = True, itow: int = 0) -> bytes:
    """NAV-TIMEUTC frame.  validUTC is bit 2 (0x04) of the valid byte.

    Payload (20 bytes):
      iTOW(U4) tAcc(U4) nano(I4) year(U2) month(U1) day(U1)
      hour(U1) min(U1) sec(U1) valid(X1)
    """
    valid = 0x07 if utc_ok else 0x00   # bits 0,1,2 = validTOW, validWKN, validUTC
    payload = struct.pack(
        "<IIiHBBBBBB",
        itow,
        100,      # tAcc (ns)
        0,        # nano
        2024,     # year
        1,        # month
        1,        # day
        0,        # hour
        0,        # min
        0,        # sec
        valid,
    )
    return make_ubx_frame(_CLS_NAV, _ID_NAV_TIMEUTC, payload)


# ---------------------------------------------------------------------------
# RTCM3 frame builder
# ---------------------------------------------------------------------------

def make_rtcm_frame(msg_num: int, extra_payload: bytes = b"") -> bytes:
    """Build a minimal but CRC-valid RTCM3 frame for message number *msg_num*.

    The first 12 bits of the payload carry the message number.  Any extra
    bytes can be appended.  The resulting frame passes _crc24q validation.
    """
    # 12-bit message number packed into first two bytes (MSB first).
    # Remaining bits in those two bytes + the rest of extra_payload form the payload.
    payload = bytes([(msg_num >> 4) & 0xFF, (msg_num & 0x0F) << 4]) + extra_payload
    L = len(payload)
    header = bytes([0xD3, (L >> 8) & 0x03, L & 0xFF])
    crc = _crc24q(header + payload)
    return header + payload + crc.to_bytes(3, "big")


# ---------------------------------------------------------------------------
# Pre-built scenarios
# ---------------------------------------------------------------------------

def scenario_good_fix(
    qerr_ps: int = 1500,
    temp_c: float = 42.0,
    pdop: float = 1.2,
    num_sats: int = 12,
) -> List[bytes]:
    """Return a list of UBX frames representing a healthy GNSS fix.

    Includes TIM-TP (timing), MON-SYS (temperature), NAV-SAT (satellites),
    NAV-DOP (dilution of precision), and NAV-TIMEUTC (UTC validity).
    """
    sats = [
        {"gnss_id": 0, "sv_id": i + 1, "cno": 42, "used": True}
        for i in range(num_sats)
    ]
    return [
        make_tim_tp(qerr_ps=qerr_ps, utc_ok=True),
        make_mon_sys(temp_c=temp_c),
        make_nav_sat(satellites=sats),
        make_nav_dop(pdop=pdop),
        make_nav_timeutc(utc_ok=True),
    ]


def scenario_no_fix() -> List[bytes]:
    """Return UBX frames representing a receiver that has not acquired a fix."""
    return [
        make_tim_tp(qerr_ps=0, utc_ok=False),
        make_nav_sat(satellites=[]),
        make_nav_dop(pdop=99.0),
        make_nav_timeutc(utc_ok=False),
    ]


def scenario_rtcm_stream(message_numbers: List[int] | None = None) -> List[bytes]:
    """Return a list of RTCM3 frames for the given message numbers.

    Defaults to a typical base-station output: 1005, 1074, 1084, 1094, 1230.
    """
    if message_numbers is None:
        message_numbers = [1005, 1074, 1084, 1094, 1230]
    return [make_rtcm_frame(n) for n in message_numbers]


# ---------------------------------------------------------------------------
# CLI convenience: print hex dumps
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== scenario_good_fix ===")
    for frame in scenario_good_fix():
        print(f"  len={len(frame):4d}  hex={frame[:16].hex()}…")

    print("\n=== scenario_no_fix ===")
    for frame in scenario_no_fix():
        print(f"  len={len(frame):4d}  hex={frame[:16].hex()}…")

    print("\n=== scenario_rtcm_stream ===")
    for frame in scenario_rtcm_stream():
        print(f"  len={len(frame):4d}  hex={frame[:8].hex()}…")
