from serial import Serial
from pyubx2 import UBXMessage, UBXReader, POLL, UBX_PROTOCOL
import sys, time, re

HEX10 = re.compile(r"[0-9A-F]{10}$")

def get_uniqid(port="/dev/ttyACM0", baud=115200, timeout=0.5, tries=3):
    with Serial(port, baud, timeout=timeout) as ser:
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
                    uid_hex = (f"{uid:010X}" if isinstance(uid, int) else bytes(uid).hex().upper())
                    if HEX10.fullmatch(uid_hex):
                        return uid_hex
        raise TimeoutError("No SEC-UNIQID response")

def _asciiz(b):
    if isinstance(b, (bytes, bytearray, memoryview)):
        return bytes(b).decode("ascii", "ignore").rstrip("\x00 ").strip()
    return str(b or "").strip()

def _parse_mon_ver_payload(payload: bytes):
    """Parse raw MON-VER payload: 30B sw, 10B hw, then N*30B extension strings."""
    if len(payload) < 40:
        return "", "", []
    sw = _asciiz(payload[0:30])
    hw = _asciiz(payload[30:40])
    exts = []
    i = 40
    while i + 30 <= len(payload):
        s = _asciiz(payload[i:i+30])
        if s:
            exts.append(s)
        i += 30
    return sw, hw, exts

def read_mon_ver(port="/dev/ttyACM0", baud=115200, tries=6, read_timeout=0.15, pause=0.08):
    """
    Poll MON-VER a few times and manually parse extensions.
    Returns dict: {model, fwver, protver, hwver, extensions}
    (Model is forced to ZED-F9T)
    """
    out = {"model": "ZED-F9T", "fwver": "", "protver": "", "hwver": "", "extensions": []}
    with Serial(port, baud, timeout=read_timeout) as ser:
        ubr = UBXReader(ser, protfilter=UBX_PROTOCOL)
        poll = UBXMessage("MON", "MON-VER", POLL)

        for _ in range(tries):
            ser.reset_input_buffer()
            ser.write(poll.serialize())
            t0 = time.time()
            msg = None
            # tight read loop (bounded by read_timeout, not your old long timeout)
            while time.time() - t0 < read_timeout:
                try:
                    _, m = ubr.read()
                except Exception:
                    break
                if getattr(m, "identity", "") == "MON-VER":
                    msg = m
                    break

            if not msg:
                time.sleep(pause)
                continue

            # Prefer raw payload parsing so we always get extensions if present
            payload = bytes(getattr(msg, "payload", b""))
            sw, hw, exts = _parse_mon_ver_payload(payload)
            out["hwver"] = hw or out["hwver"]
            if exts:
                out["extensions"] = exts
                for s in exts:
                    if s.startswith("FWVER=") and not out["fwver"]:
                        out["fwver"] = s.split("=", 1)[1]
                    elif s.startswith("PROTVER=") and not out["protver"]:
                        out["protver"] = s.split("=", 1)[1]
            # fallback for fwver when FWVER=... was absent
            if not out["fwver"] and sw:
                out["fwver"] = sw  # e.g., "EXT CORE 1.00 (3fda8e)"

            # stop once we’ve got at least hwver + some fw info; protver may remain empty
            if out["fwver"] and out["hwver"]:
                break

            time.sleep(pause)

    return out

# Optional: map EXT CORE -> TIM version / protocol if you maintain a table.
# Example skeleton; fill in as you collect values from your fleet.
EXT_TO_TIM = {
     "EXT CORE 1.00 (264600)": ("TIM 2.00",       "29.00"),
     "EXT CORE 1.00 (71b20c)": ("TIM 2.01",       "29.00"),
     "EXT CORE 1.00 (5481ba)": ("TIM 2.02B01",    "29.01"),
     "EXT CORE 1.00 (e8e1de)": ("TIM 2.10B00",    "29.10"),
     "EXT CORE 1.00 (092837)": ("TIM 2.10B01",    "29.10"),
     "EXT CORE 1.00 (08d8b4)": ("TIM 2.11",       "29.10"),
     "EXT CORE 1.00 (9db928)": ("TIM 2.13",       "29.11"),
     "EXT CORE 1.00 (3fda8e)": ("TIM 2.20",       "29.20"),
     "EXT CORE 1.00 (7dd75c)": ("TIM 2.22",       "29.22"),
     "EXT CORE 1.00 (241f52)": ("TIM 2.24",       "29.25"),
    "EXT CORE 1.00 (535349)": ("TIM 2.24",       "29.25")
}

def normalize_f9t_versions(info):
    """
    If fwver is an EXT CORE string and we have a table entry,
    rewrite to TIM x.xx and fill protver accordingly.
    """
    fw = info.get("fwver","")
    if fw.startswith("TIM "):
        return info  # already good
    if fw in EXT_TO_TIM:
        tim, prot = EXT_TO_TIM[fw]
        info["fwver"] = tim
        if not info.get("protver"):
            info["protver"] = prot
    return info

def identify_f9t(port="/dev/ttyACM0", baud=115200):
    uid = get_uniqid(port, baud)
    ver = read_mon_ver(port, baud)
    ver = normalize_f9t_versions(ver)
    return {"uniqid": uid, **ver}

if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(identify_f9t(port = sys.argv[1]))
    else:
        print(identify_f9t())