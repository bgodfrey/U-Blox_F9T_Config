import asyncio, io, json5, re, serial, time
from pathlib import Path
from pyubx2 import UBX_CONFIG_DATABASE as CFGDB
from pyubx2 import POLL, SET, UBXMessage, UBXReader, UBX_PROTOCOL
from pyubx2.exceptions import UBXMessageError, UBXParseError, UBXStreamError, UBXTypeError
from statistics import mean
from typing import Optional, Dict, Any


# ---------------------------------------------------------------------------
# Regular expression used to validate a 10-digit hexadecimal unique ID
# returned by SEC-UNIQID.
# ---------------------------------------------------------------------------
HEX10 = re.compile(r"[0-9A-F]{10}$")


# ---------------------------------------------------------------------------
# Build two helpful lookup structures from the pyubx2 CFG database:
#
# 1) all_kids     -> sorted list of all numeric keyIDs
# 2) kid_to_name  -> mapping of keyID → CFG_NAME string
#
# This lets us:
#   • poll using numeric keyIDs (what the receiver expects)
#   • print human-readable CFG_* names on output
# ---------------------------------------------------------------------------
def _cfgdb_maps(CFGDB: dict):
    kid_to_name: dict[int, str] = {}
    name_to_kid: dict[str, int] = {}
    for k in CFGDB.keys():
        if not isinstance(k, str):
            continue
        name = k.strip().upper().replace("-", "_")
        try:
            kid = _name_to_keyid_from_cfgdb(CFGDB, name)
        except Exception:
            continue
        kid_to_name[kid] = name
        name_to_kid[name] = kid
    all_kids = sorted(kid_to_name.keys())
    return all_kids, kid_to_name, name_to_kid



# ---------------------------------------------------------------------------
# Extract key/value pairs from a parsed CFG-VALGET response.
#
# pyubx2 stores returned configuration entries in a repeating group
# usually called `cfgData`.
#
# This function:
#   • converts keyID → human readable name
#   • inserts results into the `got` dictionary
# ---------------------------------------------------------------------------
def _merge_valget_into_got(got: dict[str, object], parsed, kid_to_name: dict[int, str]) -> int:
    """
    Merge one CFG-VALGET response into `got`.

    Returns number of new keys added.
    Works with:
      - pyubx2 variants that expose repeating group in parsed.cfgData
      - pyubx2 variants that *only* expose named attributes on parsed.__dict__
    """
    before = len(got)

    # --- Path A: repeating group (keyID/value) ---
    cfgdata = getattr(parsed, "cfgData", None) or getattr(parsed, "cfgdata", None)
    if cfgdata:
        for e in cfgdata:
            if isinstance(e, dict):
                kid = int(e.get("keyID"))
                val = e.get("value")
            else:
                kid = int(getattr(e, "keyID"))
                val = getattr(e, "value")

            name = kid_to_name.get(kid)
            if name:
                got[name] = val
            else:
                got[f"KEYID_{kid}"] = val

        return len(got) - before

    # --- Path B: fallback to attribute-based merge (your verify method) ---
    d = getattr(parsed, "__dict__", {}) or {}
    for k, v in d.items():
        if k.startswith("_"):
            continue
        ku = k.upper()

        # Filter non-CFG/meta fields that show up on CFG-VALGET objects
        if ku in ("IDENTITY", "LAYER", "POSITION", "VERSION"):
            continue

        # Keep only CFG_* style names (avoid weird internal/parsed fields)
        if not ku.startswith("CFG_"):
            continue

        got[ku] = v

    return len(got) - before




# ---------------------------------------------------------------------------
# Read manifest JSON5 file and compute the *expected* configuration
# for this device ID.
#
# Merge order:
#   global config
#   role config
#   device-specific config
#
# Returns:
#   expected: {CFG_KEY_NAME: expected_value}
#   role:     role string
# ---------------------------------------------------------------------------
def expected_cfg_from_manifest(manifest_path: str, device_id: str) -> tuple[dict[str, object], str]:

    man = json5.loads(Path(manifest_path).read_text(encoding="utf-8"))

    dev_id = (device_id or "").strip().upper()
    dev = (man.get("devices", {}) or {}).get(dev_id)

    if not dev:
        raise KeyError(f"device_id {dev_id!r} not found in manifest devices[]")

    role = (dev.get("role") or "").strip().lower()

    role_cfg = ((man.get("role", {}) or {}).get(role, {}) or {}).get("config", []) or []
    glob_cfg = ((man.get("global", {}) or {}).get("config", []) or [])
    dev_cfg  = (dev.get("config", []) or [])

    expected: dict[str, object] = {}

    # Helper to merge config blocks
    def apply_list(lst):
        for item in lst:
            if not isinstance(item, dict):
                continue
            k = (item.get("key") or "").strip().upper()
            if not k:
                continue
            expected[k] = item.get("value")

    apply_list(glob_cfg)
    apply_list(role_cfg)
    apply_list(dev_cfg)

    return expected, role


# ---------------------------------------------------------------------------
# Poll SEC-UNIQID from the receiver to get its 10-digit unique ID.
#
# Important:
#   This function temporarily takes ownership of `ser`.
#   It assumes nothing else is reading from the port.
# ---------------------------------------------------------------------------
def get_uniqid(ser, timeout=1, tries=3):

    ubr = UBXReader(ser, protfilter=UBX_PROTOCOL)
    poll = UBXMessage("SEC", "SEC-UNIQID", POLL)

    for _ in range(tries):
        ser.reset_input_buffer()
        ser.write(poll.serialize())

        t0 = time.time()
        while time.time() - t0 < timeout:
            _, msg = ubr.read()
            if getattr(msg, "identity", "") == "SEC-UNIQID":
                uid = getattr(msg, "uniqueId", None) or msg.payload[4:9]
                uid_hex = (
                    f"{uid:010X}" if isinstance(uid, int)
                    else bytes(uid).hex().upper()
                )

                if HEX10.fullmatch(uid_hex):
                    return uid_hex

    raise TimeoutError("No SEC-UNIQID response")


def _all_cfgnames_from_cfgdb(CFGDB) -> list[str]:
    """
    Return all CFG key *names* we can ask for (uppercased, '_' normalized).
    This is intentionally permissive; the receiver will ignore unsupported keys.
    """
    names = []
    for k in CFGDB.keys():
        if not isinstance(k, str):
            continue
        name = k.strip().upper().replace("-", "_")
        if not name.startswith("CFG_"):
            continue
        names.append(name)
    return sorted(set(names))


def _merge_cfg_results_from_dunder(got: dict[str, object], parsed) -> int:
    before = len(got)
    d = getattr(parsed, "__dict__", {}) or {}
    for k, v in d.items():
        if k.startswith("_"):
            continue
        # ignore CFG-VALGET metadata
        if k in ("identity", "layer", "position", "version"):
            continue
        kk = str(k).strip().upper()
        if kk.startswith("CFG_"):
            got[kk] = v
    return len(got) - before


async def valget_dump_all_like_verify(
    ser,
    ser_lock,
    device_id: str,
    expected: Optional[Dict[str, Any]] = None,
    role: str = "",
    CFGDB: dict = None,
    layer: str = "RAM",
    chunk: int = 16,               # initial chunk size
    chunk_min: int = 4,            # smallest window size before splitting to singles
    chunk_max: int = 64,           # largest window size allowed
    timeout_base: float = 1.5,     # fixed overhead per poll (seconds)
    timeout_per_key: float = 0.06, # additional time per requested key
    quiet_after_s: float = 0.35,   # stop draining after this quiet period
    verbose_chunks: bool = True,
):
    """
    Dump all configuration keys from receiver using adaptive chunking + recursive splitting.

    Strategy:
      1. Walk across CFGDB keys in windows.
      2. Poll each window using CFG-VALGET.
      3. If incomplete response, recursively split missing subset.
      4. Only advance after window is fully exhausted.

    This guarantees no keys are skipped.
    """

    if CFGDB is None:
        raise ValueError("CFGDB must be provided")

    expected = expected or {}
    poll_layer = {"RAM": 0, "BBR": 1, "FLASH": 2, "DEFAULT": 7}.get(layer.upper(), 0)

    # Collect all known CFG names
    all_names = _all_cfgnames_from_cfgdb(CFGDB)
    n_attempt = len(all_names)

    got: dict[str, object] = {}
    chunk_times: list[float] = []
    chunk_got: list[int] = []

    t_total0 = time.monotonic()

    # ------------------------------------------------------------------
    # Recursive poller: splits window if partial results returned
    # ------------------------------------------------------------------
    async def _poll_keys_with_split(keys: list[str], depth: int = 0):

        if not keys:
            return

        # Only try keys not already retrieved
        missing = [k for k in keys if k not in got]
        if not missing:
            return

        # If only one key remains, poll directly
        if len(missing) == 1:
            req = missing
        else:
            req = missing

        msg_get = UBXMessage.config_poll(layer=poll_layer, position=0, keys=req)

        # Timeout scales with request size
        timeout_this = timeout_base + timeout_per_key * len(req)
        deadline = time.time() + timeout_this

        saw_valget = False
        last_valget_time = None

        async with ser_lock:
            try:
                ser.reset_input_buffer()
            except Exception:
                pass

            ser.write(msg_get.serialize())
            ser.flush()

            rdr = UBXReader(ser, protfilter=UBX_PROTOCOL)

            while time.time() < deadline:
                try:
                    _, parsed = rdr.read()
                except (UBXMessageError, UBXParseError, UBXStreamError, UBXTypeError):
                    continue

                if not parsed:
                    continue

                if getattr(parsed, "identity", "") == "CFG-VALGET":
                    saw_valget = True
                    last_valget_time = time.time()

                    _merge_cfg_results_from_dunder(got, parsed)

                    # Stop immediately if all requested keys received
                    if all(k in got for k in req):
                        break

                    continue

                # Quiet break: if receiver stops responding, exit drain loop
                if saw_valget and last_valget_time and \
                   (time.time() - last_valget_time) > quiet_after_s:
                    break

        # Check which keys still missing
        still_missing = [k for k in req if k not in got]

        # If partial result and more than one key, split recursively
        if still_missing and len(still_missing) > 1:
            mid = len(still_missing) // 2
            left = still_missing[:mid]
            right = still_missing[mid:]

            if verbose_chunks:
                print("  " * depth + f"Splitting {len(still_missing)} → {len(left)} + {len(right)}")

            await _poll_keys_with_split(left, depth + 1)
            await _poll_keys_with_split(right, depth + 1)

    # ------------------------------------------------------------------
    # Main adaptive window loop
    # ------------------------------------------------------------------
    i = 0
    cur_chunk = chunk

    while i < n_attempt:

        chunk_names = all_names[i : i + cur_chunk]
        before_chunk = len(got)
        t_chunk0 = time.monotonic()

        if verbose_chunks:
            print(f"Window start={i} size={cur_chunk}")

        await _poll_keys_with_split(chunk_names)

        dt_chunk = time.monotonic() - t_chunk0
        newly_chunk = len(got) - before_chunk

        chunk_times.append(dt_chunk)
        chunk_got.append(newly_chunk)

        # Completion ratio
        have_ratio = sum(1 for n in chunk_names if n in got) / float(len(chunk_names))

        # Adaptive resizing
        if have_ratio < 0.50:
            cur_chunk = max(chunk_min, cur_chunk // 2 or 1)
        elif have_ratio > 0.90:
            cur_chunk = min(chunk_max, cur_chunk * 2)

        # Advance only after exhausting this window
        i += len(chunk_names)

    dt_total = time.monotonic() - t_total0

    # ------------------------------------------------------------------
    # Stats and Output
    # ------------------------------------------------------------------
    exp_keys = set(expected.keys())
    got_keys = set(got.keys())
    exp_got = len(exp_keys & got_keys)
    exp_missing = len(exp_keys - got_keys)

    print(
        f"[dump] device_id={device_id} role={role or '?'} layer={layer} "
        f"attempted={n_attempt} got={len(got)} "
        f"expected={len(exp_keys)} expected_got={exp_got} "
        f"expected_missing={exp_missing} "
        f"total_time={dt_total:.3f}s"
    )

    if chunk_times:
        print(
            f"[dump] chunks={len(chunk_times)} "
            f"chunk_dt min/avg/max = "
            f"{min(chunk_times):.3f}/"
            f"{mean(chunk_times):.3f}/"
            f"{max(chunk_times):.3f}s; "
            f"new_keys_per_chunk min/avg/max = "
            f"{min(chunk_got)}/"
            f"{mean(chunk_got):.1f}/"
            f"{max(chunk_got)}"
        )

    # Print values
    for k in sorted(got.keys()):
        v = got[k]
        if k in expected:
            print(f"{k:40s} = {v!r} (set to {expected[k]!r})")
        else:
            print(f"{k:40s} = {v!r}")

    if exp_missing:
        print("\nMissing expected keys:")
        for k in sorted(exp_keys - got_keys):
            print(f"  {k} (set to {expected[k]!r})")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main():
    port = '/dev/ttyACM0'
    ser_lock = asyncio.Lock()

    # Open serial once and reuse
    ser = serial.Serial(port, 115200, timeout=0.02, write_timeout=0.5)

    uid = get_uniqid(ser)
    print(f"UID is {uid}")

    manifest_path = '/home/bgodfrey/Berkeley/Panoseti/Codebase/panoseti_grpc/gnss_config/U-Blox_F9T_Config/manifest_f9t.json5'

    if not ser.isOpen():
        try:
            ser.open()
            print(f"Port reopened successfully")
            time.sleep(1)
        except serial.SerialException as e:
            print(f"Error reopening port: {e}")

    expected, role = expected_cfg_from_manifest(manifest_path, uid)
    expected = {str(k).strip().upper().replace("-", "_"): v for k, v in expected.items() if k}
    #await valget_probe_one(ser, ser_lock)
    await valget_dump_all_like_verify(
    ser=ser,
    ser_lock=ser_lock,
    device_id=uid,
    expected=expected,   
    role=role, 
    CFGDB=CFGDB,
    layer="RAM",
    chunk=16,
    )

if __name__ == "__main__":
    asyncio.run(main())
