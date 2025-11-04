"""
Overview (global description)
=============================
This program is the agent that runs on a DAQ node connected to a u‑blox ZED‑F9T (or similar) receiver over USB/serial. It talks to a centralserver/headnode using two gRPC services:

1) Control (bidirectional streaming):
   - Agent → Server: hello, result (apply status), telem (telemetry).
   - Server → Agent: ack (role assignment + credentials), cfgset (VALSET configuration), ping (server heartbeat), optional file transfer.
   - The agent also sends periodic telemetry derived from UBX messages.

2) Caster (data plane):
   - If role is BASE: the agent publishes RTCM3 frames read from the GNSS USB port to the server via a client-streaming  RPC. The server counts frames and (optionally) fans them out to rovers.
   - If role is RECEIVER the agent subscribes to RTCM3 frames from the server via a server-streaming RPC and writes them to the GNSS USB port.

Key internal components
-----------------------
• Serial demux: A single reader parses the raw serial byte stream into validated frames: RTCM3 [Qualcomm 24-bit Cyclic Redundancy Checksum algorithm (CRC24Q) verified] and UBX (checksum verified). It pushes RTCM frames to an RTCM queue and UBX frames to a UBX queue.

• Telemetry aggregator: Consumes UBX frames (e.g., TIM‑TP, MON‑SYS, NAV‑SAT, NAV‑DOP, NAV‑TIMEUTC) to build a rolling telemetry snapshot used by the periodic telemetry publisher.

• Role task: Exactly one task is active at a time based on the server’s assignment: either `publish_loop` (BASE) or `subscribe_loop` (RECEIVER).

• Control call writer: Thin wrapper that serializes writes on the Control stream and copes with mid‑stream failures.

• Watchdogs & lifetime: A ping deadline watcher requests a fresh reconfiguration if the server goes silent; start/stop helpers manage the serial demux, telemetry publisher, and role tasks cleanly.

Sentinel & shutdown convention
------------------------------
• None is the only sentinel value used to signal consumers to stop. It is only placed in queues during full serial shutdown. Role switches do not touch the queues (to avoid poisoning them).

• _shutdown_evt is set during whole‑agent shutdown; serial_demux_loop places
  sentinels only in that case.
"""

from __future__ import annotations

# --- Standard / third‑party imports -----------------------------------------
from datetime import datetime, timezone
from grpc.aio import AioRpcError
from serial import Serial
from pyubx2 import POLL, SET, UBXReader, UBXMessage, UBX_PROTOCOL
import asyncio, contextlib, glob, io, json5, os, re, signal, struct, sys, time
import grpc, serial
import caster_setup_pb2 as pb
import caster_setup_pb2_grpc as rpc
import pyubx2.ubxtypes_configdb as cdb
from typing import Optional, Tuple, Dict

# --- Configuration & constants ----------------------------------------------
CTRL_ADDR = "localhost:50051"     # Control service address (bidirectional)
CAST_ADDR = "localhost:50051"     # Caster service address (publish/subscribe)

HEX10 = re.compile(r"[0-9A-F]{10}$")
PRINT_RTCM_IDS = True              # Print RTCM message IDs as they pass
PRINT_UBX_SUMMARY = True           # Print UBX class/id summaries

# GNSS protocol bitmasks (for CFG‑PRT)
UBX_PROTO_UBX   = 0x01
UBX_PROTO_NMEA  = 0x02
UBX_PROTO_RTCM3 = 0x20

SAVE_TELEM_LOCAL  = True          # write JSONL locally
SAVE_TELEM_REMOTE = True          # send to server over Control.Pipe
TELEM_DIR  = "./telem"  # or "/var/log/f9t_telem.jsonl"
os.makedirs(TELEM_DIR, exist_ok=True)
# one fixed timestamp for this agent run
_START_TS  = datetime.now(timezone.utc)
_START_STR = _START_TS.strftime("%Y%m%d_%H%M%SZ")
_TELEM_PATH = os.path.join(TELEM_DIR, f"UNKNOWN_{_START_STR}.jsonl")

# --- Global scope variables -------------------------
_CFGMAP: Dict[str, Tuple[int, str]] = {}            # Config DB map: KEY -> (ID, TYPE)
_rtcm_q: Optional[asyncio.Queue] = None             # Demux → RTCM frames
_ubx_q:  Optional[asyncio.Queue] = None             # Demux → UBX frames
_demux_task: Optional[asyncio.Task] = None          # Serial parser task
_call_writer = None                                 # CallWriter for Control stream
_telem_task: Optional[asyncio.Task] = None          # UBX → Telemetry aggregator task
_telem_pub_task: Optional[asyncio.Task] = None      # Periodic telemetry publisher
_telem_stop_evt: Optional[asyncio.Event] = None     # Stops telemetry publisher
_telem_restart_lock = asyncio.Lock()
_last_cfg_version: Optional[int] = None             # Applied cfg version 
_ping_deadline: float = 0.0                         # Monotonic timestamp watchdog
_ping_task: Optional[asyncio.Task] = None
_cfg_apply_lock = asyncio.Lock()                    # Serialize config application
_serial_stop_evt: Optional[asyncio.Event] = None
_shutdown_evt   = asyncio.Event()                   # Whole‑agent shutdown (global)

_role = {"task": None, "name": None}                # Active role task + label


# Mapping from EXT CORE strings → friendly TIM/PROT versions
# As of November 2025, all F9T modules have the 2.20 firmware released January 2022

EXT_TO_TIM = {
	"EXT CORE 1.00 (264600)": ("TIM 2.00",    "29.00"),
	"EXT CORE 1.00 (71b20c)": ("TIM 2.01",    "29.00"),
	"EXT CORE 1.00 (5481ba)": ("TIM 2.02B01", "29.01"),
	"EXT CORE 1.00 (e8e1de)": ("TIM 2.10B00", "29.10"),
	"EXT CORE 1.00 (092837)": ("TIM 2.10B01", "29.10"),
	"EXT CORE 1.00 (08d8b4)": ("TIM 2.11",    "29.10"),
	"EXT CORE 1.00 (9db928)": ("TIM 2.13",    "29.11"),
	"EXT CORE 1.00 (3fda8e)": ("TIM 2.20",    "29.20"),
	"EXT CORE 1.00 (7dd75c)": ("TIM 2.22",    "29.22"),
	"EXT CORE 1.00 (241f52)": ("TIM 2.24",    "29.25"),
	"EXT CORE 1.00 (535349)": ("TIM 2.24",    "29.25"),
}

# ----------------------------------------------------------------------------
# Utilities / signal handling
# ----------------------------------------------------------------------------

"""Register SIGINT/SIGTERM handlers that set the given event.

This allows asyncio tasks to await stop_event.wait() for cooperative shutdown across platforms that support loop signal handlers.
"""
def install_signal_handlers(stop_event: asyncio.Event) -> None:
	loop = asyncio.get_running_loop()
	for sig in (signal.SIGINT, signal.SIGTERM):
		try:
			loop.add_signal_handler(sig, stop_event.set)
		except NotImplementedError:
			# Pass if this isn't available.
			pass

"""Cancel a task and await its completion, suppressing CancelledError. Useful for orderly teardown when a task may be mid‑await.
"""
async def cancel_and_wait(task: Optional[asyncio.Task]) -> None:

	if not task:
		return
	task.cancel()
	with contextlib.suppress(asyncio.CancelledError):
		await task

# ----------------------------------------------------------------------------
# Telemetry aggregator (consumes UBX frames and extracts fields)
# ----------------------------------------------------------------------------
class TelemetryAgg:
	"""Accumulates telemetry fields from UBX messages.

	The GNSS streams multiple UBX reports. We keep the latest values that are relevant to monitoring timing quality and sky conditions."""
	__slots__ = (
		"temp_c", "qerr_ps", "utc_ok", "num_vis", "num_used",
		"gps_used", "gal_used", "bds_used", "glo_used", "avg_cno", "pdop"
	)

	def __init__(self) -> None:
		# Initialize with sane defaults; fields updated as UBX arrives.
		self.temp_c = None      # Temperature of the chip (maybe can use as a proxy for enclosure temperature)
		self.qerr_ps = None     # Qerr of the current clock tick in ps
		self.utc_ok = False     # Position dilution of
		self.num_vis = 0        # Number of satellites visible
		self.num_used = 0       # Number of satellites used in the navigation solution
		self.gps_used = 0       # GPS satellites; Can get satellite information from the GNSS ID in the UBX-NAV-SAT data satellites)
		self.gal_used = 0       # Galileo satellites
		self.bds_used = 0       # BeiDou satellites
		self.glo_used = 0       # GLONASS satellites 
		self.avg_cno = 0.0      # Carrier-to-noise density ratio (signal strength); Units are dBHz
		self.pdop = 0.0         # Position dilution of precision - gives a measure of satellite distribution; good PDOP < 3 and poor PDOP > 7 (see https://en.wikipedia.org/wiki/Dilution_of_precision)

	#Turn bytes into stream and then returned the raw and parsed data
	def feed_ubx(self, frame: bytes) -> None:
		try:
			_, msg = UBXReader(io.BytesIO(frame)).read()
		except Exception:
			return  # ignore decode errors

		ident = getattr(msg, "identity", "")
		if ident == "TIM-TP":
			self.qerr_ps = getattr(msg, "qErr", None)
			self.utc_ok = bool(getattr(msg, "utc", 0))
		elif ident == "MON-SYS":
			tv = getattr(msg, "tempValue", None)
			if tv is not None:
				self.temp_c = float(tv)
		elif ident == "NAV-SAT":
			# Count visible/used sats and per‑constellation split; compute avg C/N0.
			sats = getattr(msg, "sats", []) or []
			self.num_vis = len(sats)
			used = 0; cno_sum = 0.0
			gps=gal=bds=glo=0
			for s in sats:
				cno = getattr(s, "cno", 0.0) or 0.0
				cno_sum += cno
				if getattr(s, "flags", 0) & 0x08:  # bit indicates used in solution
					used += 1
					gid = getattr(s, "gnssId", 255)
					if gid == 0: gps += 1
					elif gid == 2: gal += 1
					elif gid == 3: bds += 1
					elif gid == 6: glo += 1
			self.num_used = used
			self.gps_used, self.gal_used, self.bds_used, self.glo_used = gps, gal, bds, glo
			self.avg_cno = (cno_sum / self.num_vis) if self.num_vis else 0.0
		elif ident == "NAV-DOP":
			pd = getattr(msg, "pDOP", None)
			if pd is not None:
				self.pdop = float(pd)
		elif ident == "NAV-TIMEUTC":
			self.utc_ok = bool(getattr(msg, "validUTC", 0))


# Stop any existing telemetry publisher and start exactly one new instance.

async def restart_telem_publisher():
	global _telem_stop_evt, _telem_pub_task, _call_writer
	async with _telem_restart_lock:
		if _telem_pub_task and not _telem_pub_task.done():
			_telem_stop_evt.set()
			with contextlib.suppress(Exception):
				await _telem_pub_task
		# start new
		_telem_stop_evt = asyncio.Event()
		_telem_pub_task = asyncio.create_task(telem_publisher(_call_writer, _agg, _telem_stop_evt))
		print("[telem] publisher restarted")

_agg = TelemetryAgg()  # single aggregator instance shared by publisher

# ----------------------------------------------------------------------------
# Control call writer (serializes writes and tracks open/closed state)
# ----------------------------------------------------------------------------


# Serialize writes on the Control stream and detect mid‑stream failures. Some transports can error while writes are in flight; this wrapper keeps a simple open/closed flag and a lock to prevent concurrent writes.

class CallWriter:
	def __init__(self, call):
		self._call = call
		self._open = True
		self._lock = asyncio.Lock()

	# Return True if the underlying stream is considered open for writes.
	def is_open(self) -> bool:
		return self._open

	# Mark the stream closed (future writes then do nothing)
	def close(self) -> None:
		self._open = False

	# Write a message if open; return False on failure (and mark closed).
	async def write(self, msg) -> bool:
		if not self._open:
			return False
		try:
			async with self._lock:
				await self._call.write(msg)
			return True
		except (asyncio.InvalidStateError, grpc.aio.AioRpcError) as e:
			self._open = False
			print(f"[agent] control write failed: {getattr(e,'code',lambda:None)() if hasattr(e,'code') else ''} {getattr(e,'details',lambda:None)() if hasattr(e,'details') else ''}")
			return False

# ----------------------------------------------------------------------------
# Serial demux + telemetry publisher
# ----------------------------------------------------------------------------


# Compute RTCM3 CRC24Q over bytes b. Returns 24‑bit int. See Debian implementation in C here (https://sources.debian.org/src/gpsd/3.16-4/crc24q.c/)
def _crc24q(b: bytes) -> int:
	c = 0
	for x in b:
		c ^= (x & 0xFF) << 16
		for _ in range(8):
			c <<= 1
			if c & 0x1000000:
				c ^= 0x1864CFB
			c &= 0xFFFFFF
	return c


# Extract 12‑bit RTCM message number from payload.
def _rtcm_id(payload: bytes) -> int:
	return ((payload[0] << 4) | (payload[1] >> 4)) & 0x0FFF


# Compute UBX 8‑bit Fletcher checksum (CK_A, CK_B) over payload. Given in the F9T interface manual
def _ubx_ck(payload: bytes):
	ck_a = ck_b = 0
	for b in payload:
		ck_a = (ck_a + b) & 0xFF
		ck_b = (ck_b + ck_a) & 0xFF
	return ck_a, ck_b

'''
Read raw bytes from serial connection, parse frames, and push into RTCM queue / UBX queue
	• Robust to transient serial errors (backs off briefly and keeps going).
	• Validates CRC/CK and only forwards good frames. Perhaps ignore this if too compute intensive
	• On whole‑agent shutdown (_shutdown_evt), places None sentinels so downstream consumers can exit.
'''
async def serial_demux_loop(ser, ser_lock, rtcm_q: asyncio.Queue, ubx_q: asyncio.Queue, stop_evt: asyncio.Event):

	try:
		rx = bytearray()       # rolling receive buffer
		backoff = 0.01         # backoff for serial errors
		while not stop_evt.is_set():
			await asyncio.sleep(0)  # cooperate with scheduler
			try:
				# Serial read is not asynchronous. Guard with a lock so other writers config, publisher, subscriber) don't interleave.
				async with ser_lock:
					chunk = ser.read(4096)
				if not chunk:
					await asyncio.sleep(0.005)
					continue
				backoff = 0.01
			except (serial.SerialException, serial.SerialTimeoutException, OSError, ValueError) as e:
				print(f"[demux] serial read error: {e}")
				# brief retry loop with capped backoff
				for _ in range(10):
					if stop_evt.is_set():
						break
					await asyncio.sleep(backoff)
				backoff = min(backoff * 2.0, 0.5)
				continue

			rx.extend(chunk)

			# Frame parser: scan buffer and peel off complete frames.
			while True:
				if len(rx) < 6:
					break  # not enough for any header

				# --- RTCM3 sync (0xD3) ---
				if rx[0] == 0xD3 and (rx[1] & 0xFC) == 0:
					L = ((rx[1] & 0x03) << 8) | rx[2]
					total = 3 + L + 3
					if len(rx) < total:
						break  # wait for more
					frame = bytes(rx[:total])
					
					# verify CRC24Q over header+payload (exclude 3 CRC bytes)
					if _crc24q(frame[:-3]) == int.from_bytes(frame[-3:], "big"):
						if PRINT_RTCM_IDS:
							try:
								print(f"RTCM {_rtcm_id(frame[3:-3])} len={L}B", flush=True)
							except Exception:
								print(f"RTCM len={L}B", flush=True)
						await rtcm_q.put(frame)
						del rx[:total]
						continue
					else:
						# bad CRC: drop one byte to resync
						del rx[:1]
						continue

				# --- UBX sync (0xB5 0x62) ---
				if rx[:2] ==  b"\xB5\x62":
					if len(rx) < 6:
						break
					length = rx[4] | (rx[5] << 8)
					total = 6 + length + 2
					if len(rx) < total:
						break
					frame = bytes(rx[:total])
					ck_a, ck_b = _ubx_ck(frame[2:6+length])
					if frame[-2] == ck_a and frame[-1] == ck_b:
						if PRINT_UBX_SUMMARY:
							print(f"UBX {frame[2]:02X}-{frame[3]:02X} len={length}B", flush=True)
						await ubx_q.put(frame)
						del rx[:total]
						continue
					else:
						# bad CK: drop one byte to resync
						del rx[:1]
						continue

				# --- NMEA line or junk ---
				if rx[0] == 0x24:  # '$'
					end = rx.find(b"\r\n", 1)
					if end < 0:
						break
					del rx[:end+2]
					continue

				# unknown byte: skip
				del rx[:1]

	except asyncio.CancelledError:
		# Normal on shutdown; just fall through to finally.
		pass
	finally:
		# Only emit sentinels during whole‑agent shutdown; role switches keep queues alive.
		if _shutdown_evt.is_set():
			with contextlib.suppress(Exception):
				await rtcm_q.put(None)
			with contextlib.suppress(Exception):
				await ubx_q.put(None)


# Consume UBX frames from the UBX queue and feed the telemetry aggregator.
async def ubx_telemetry_loop(ubx_q: asyncio.Queue, agg: TelemetryAgg):
	try:
		while True:
			frame = await ubx_q.get()
			if frame is None:
				break  # sentinel: shutdown
			agg.feed_ubx(frame)
	except asyncio.CancelledError:
		pass


'''
	Periodically snapshot telemetry and:
	  - send upstream over Control.Pipe (if SAVE_TELEM_REMOTE),
	  - append locally as JSONL (if SAVE_TELEM_LOCAL).
	Non-blocking: disk writes are offloaded via asyncio.to_thread.
'''
async def telem_publisher(writer: CallWriter, agg: TelemetryAgg, stop_evt: asyncio.Event):

	try:
		while not stop_evt.is_set():
			# --- snapshot current telemetry fields ---
			unix_ms = int(time.time() * 1000)
			temp_c  = float(getattr(agg, "temp_c", 0.0) or 0.0)
			qerr_ps = int(getattr(agg, "qerr_ps", 0) or 0)
			utc_ok  = bool(getattr(agg, "utc_ok", False))
			num_vis  = int(getattr(agg, "num_vis", 0) or 0)
			num_used = int(getattr(agg, "num_used", 0) or 0)
			gps_used = int(getattr(agg, "gps_used", 0) or 0)
			gal_used = int(getattr(agg, "gal_used", 0) or 0)
			bds_used = int(getattr(agg, "bds_used", 0) or 0)
			glo_used = int(getattr(agg, "glo_used", 0) or 0)
			avg_cno  = float(getattr(agg, "avg_cno", 0.0) or 0.0)
			pdop     = float(getattr(agg, "pdop", 0.0) or 0.0)

			# protobuf payload (for remote)
			t = pb.Telemetry(
				unix_ms=unix_ms,
				temp_c=temp_c,
				qerr_ns=qerr_ps // 1000,  # ps → ns
				utc_ok=utc_ok,
				num_vis=num_vis, num_used=num_used,
				gps_used=gps_used, gal_used=gal_used,
				bds_used=bds_used, glo_used=glo_used,
				avg_cno=avg_cno, pdop=pdop,
			)

			# local JSONL record (flat & compact)
			if SAVE_TELEM_LOCAL:
				rec = {
					"ts": unix_ms,            # local write time (ms)
					"unix_ms": unix_ms,       # device-reported epoch (ms)
					"temp_c": temp_c,
					"qerr_ns": qerr_ps // 1000,
					"utc_ok": utc_ok,
					"num_vis": num_vis, "num_used": num_used,
					"gps_used": gps_used, "gal_used": gal_used,
					"bds_used": bds_used, "glo_used": glo_used,
					"avg_cno": avg_cno, "pdop": pdop,
				}
				# fire-and-forget (don’t await if you want even looser coupling)
				await _append_jsonl(rec)

			# remote publish (guarded + optional)
			if SAVE_TELEM_REMOTE:
				if not writer or not writer.is_open():
					break
				try:
					ok = await writer.write(pb.ControlMsg(telem=t))
				except (asyncio.InvalidStateError, grpc.aio.AioRpcError):
					break
				if not ok:
					break

			# cadence ~1 Hz with early-exit
			try:
				await asyncio.wait_for(stop_evt.wait(), timeout=1.0)
			except asyncio.TimeoutError:
				pass
	except asyncio.CancelledError:
		pass

def _sanitize(name: str, fallback: str = "UNKNOWN") -> str:
	# keep letters, numbers, dash, underscore; collapse others to '_'
	s = re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())
	return s or fallback

"""
Set (or rename to) a telemetry filename that includes alias + start time.
Safe to call multiple times; no-op if it already matches target.
"""
def set_telem_log_alias(alias: str, device_id: str = "") -> None:
	"""
	Set (or rename to) a telemetry filename that includes an alias and start time.
	Safe to call multiple times
	"""
	global _TELEM_PATH
	alias_safe = _sanitize(alias, fallback=_sanitize(device_id) or "UNKNOWN")
	
	suffix = f"{alias_safe}-{device_id[-4:]}" if device_id else alias_safe
	# If you don't want the device id in the name too, uncomment:
	#suffix = alias_safe

	new_path = os.path.join(TELEM_DIR, f"{suffix}_{_START_STR}.jsonl")
	if new_path == _TELEM_PATH:
		return

	# If we’ve already written to the old file, rename it. If not, this is just setting the initial path.
	try:
		if os.path.exists(_TELEM_PATH):
			os.replace(_TELEM_PATH, new_path)
	except Exception:
		# If rename fails, leave the old path; next write will still succeed.
		return

	_TELEM_PATH = new_path

# keep letters, numbers, dash, underscore; collapse others to '_'
def _sanitize(name: str, fallback: str = "UNKNOWN") -> str:
	s = re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())
	return s or fallback


# Append one JSON line without blocking the event loop.
async def _append_jsonl(record: dict):
	line = json5.dumps(record, separators=(",", ":")) + "\n"
	path = _TELEM_PATH  # use the global current path
	def _write():
		os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
		with open(path, "a", encoding="utf-8") as f:
			f.write(line)
	await asyncio.to_thread(_write)

# ----------------------------------------------------------------------------
# Configuration helpers (UBX VALSET and message rate)
# ----------------------------------------------------------------------------

# Normalize config keys: uppercase and replace '-' with '_' for DB lookup.
def _norm(k: str) -> str:
	return k.strip().upper().replace("-", "_")


# Build a fast map from pyubx2's config DB: KEY → (keyId, TYPE). Called on demand before applying config blocks.
def _init_cfgmap() -> None:
	global _CFGMAP
	if _CFGMAP:
		return
	for name in dir(cdb):
		obj = getattr(cdb, name)
		if isinstance(obj, dict) and obj:
			sample_key = next(iter(obj))
			val = obj[sample_key]
			if isinstance(sample_key, str) and isinstance(val, (tuple, list)) and len(val) >= 2:
				kid, typ = val[0], val[1]
				if isinstance(kid, int) and isinstance(typ, str) and typ:
					for k, (kid, typ, *_) in obj.items():
						_CFGMAP[_norm(k)] = (kid, typ.upper())
	if not _CFGMAP:
		raise RuntimeError("Could not locate config DB in pyubx2.ubxtypes_configdb")


# Serialize and write a UBX message atomically under `ser_lock`."""
async def _write_ubx(ser, ser_lock, msg: UBXMessage) -> None:
	async with ser_lock:
		ser.write(msg.serialize())
		ser.flush()

# Set CFG‑PRT for USB (portID=3) to control protocol input/output masks
async def cfg_prt_usb(ser, ser_lock, in_mask: int, out_mask: int) -> None:
	msg = UBXMessage(
		"CFG","CFG-PRT", SET,
		portID=3, reserved0=0, txReady=0, baudRate=0,
		inProtoMask=in_mask & 0xFFFF, outProtoMask=out_mask & 0xFFFF,
		flags=0, reserved5=0)
	await _write_ubx(ser, ser_lock, msg)


# Set output rate on USB for a given UBX class/id (0..255 msgs per measurement cycle).
async def _cfg_msg_usb_rate(ser, ser_lock, msg_class: int, msg_id: int, rate_usb: int) -> None:
	msg = UBXMessage(
		"CFG", "CFG-MSG", SET,
		msgClass=msg_class, msgID=msg_id,
		rateDDC=0, rateUART1=0, rateUART2=0,
		rateUSB=max(0, min(255, rate_usb)),
		rateSPI=0, reserved=0
	)
	await _write_ubx(ser, ser_lock, msg)


# Convenience: Configure an RTCM message type's USB rate via CFG‑MSG.
async def _cfg_msg_usb_rate_rtcm(ser, ser_lock, rtcm_type: int, rate_usb: int) -> None:
	await _cfg_msg_usb_rate(ser, ser_lock, 0xF5, rtcm_type & 0xFF, rate_usb)

# Convenience: Configure a UBX class/id message's USB rate via CFG‑MSG.
async def _cfg_msg_usb_rate_ubx(ser, ser_lock, msg_class: int, msg_id: int, rate_usb: int) -> None:
	await _cfg_msg_usb_rate(ser, ser_lock, msg_class, msg_id, rate_usb)

# ----------------------------------------------------------------------------
# Discovery / identity helpers (SEC‑UNIQID, MON‑VER)
# ----------------------------------------------------------------------------

# Poll SEC‑UNIQID to obtain the 5‑byte unique ID as a 10‑hex‑char string.
def get_uniqid(port="/dev/ttyACM0", baud=115200, timeout=0.5, tries=3) -> str:
	with serial.Serial(port, baud, timeout=timeout) as ser:
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


# Decode ASCII null-terminated strings. Fallback to str().
def _asciiz(b) -> str:
	if isinstance(b, (bytes, bytearray, memoryview)):
		return bytes(b).decode("ascii", "ignore").rstrip(" ").strip()
	return str(b or "").strip()


# Manual MON‑VER payload parser
def _parse_mon_ver_payload(payload: bytes):
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

# Read MON‑VER and return a dict with model/fwver/protver/hwver/extensions.
def read_mon_ver(port="/dev/ttyACM0", baud=115200, tries=6, read_timeout=0.15, pause=0.08):
	out = {"model": "ZED-F9T", "fwver": "", "protver": "", "hwver": "", "extensions": []}
	with serial.Serial(port, baud, timeout=read_timeout) as ser:
		ubr = UBXReader(ser, protfilter=UBX_PROTOCOL)
		poll = UBXMessage("MON", "MON-VER", POLL)
		for _ in range(tries):
			ser.reset_input_buffer(); ser.write(poll.serialize())
			t0 = time.time(); msg = None
			while time.time() - t0 < read_timeout:
				try:
					_, m = ubr.read()
				except Exception:
					break
				if getattr(m, "identity", "") == "MON-VER":
					msg = m; break
			if not msg:
				time.sleep(pause); continue
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
			if not out["fwver"] and sw:
				out["fwver"] = sw
			if out["fwver"] and out["hwver"]:
				break
			time.sleep(pause)
	return out


# Map EXT CORE strings to friendlier TIM/PROT versions if recognized. See EXT_TO_TIM above.
def normalize_f9t_versions(info):
	fw = info.get("fwver", "")
	if fw.startswith("TIM "):
		return info
	if fw in EXT_TO_TIM:
		tim, prot = EXT_TO_TIM[fw]
		info["fwver"] = tim
		if not info.get("protver"):
			info["protver"] = prot
	return info

# High‑level identity: SEC‑UNIQID + normalized MON‑VER info.
def identify_f9t(port="/dev/ttyACM0", baud=115200):
	uid = get_uniqid(port, baud)
	ver = normalize_f9t_versions(read_mon_ver(port, baud))
	return {"uniqid": uid, **ver}


# Scan likely serial paths (or a provided port) for a U‑blox device; return (port, uniqid). Allows user
# to explicitly define a location if wanted
def discover_f9x(timeout=0.6, port: Optional[str] = None):
	if port:
		cands = [port]
	else:
		cands = sorted(glob.glob("/dev/ttyACM*"))
		#sorted(glob.glob("/dev/serial/by-id/*")) or \
				#sorted(glob.glob("/dev/ttyACM*") + glob.glob("/dev/ttyUSB*"))
	for dev in cands:
		try:
			with serial.Serial(dev, 115200, timeout=timeout) as ser:
				ubr = UBXReader(ser, protfilter=UBX_PROTOCOL)
				ser.write(UBXMessage("SEC", "SEC-UNIQID", POLL).serialize())
				t0 = time.time()
				while time.time() - t0 < timeout:
					_, msg = ubr.read()
					if getattr(msg, "identity", "") == "SEC-UNIQID":
						uid = getattr(msg, "uniqueId", None)
						uid_hex = (f"{uid:010X}" if isinstance(uid, int) else bytes(uid).hex().upper())
						print(uid_hex)
						return dev, uid_hex
		except Exception:
			pass
	raise RuntimeError("No u-blox device found")

# ----------------------------------------------------------------------------
# Role management and stream loops
# ----------------------------------------------------------------------------

"""Ensure demux and telemetry pipelines are running before entering a role.

Starts (or restarts if needed):
  • serial_demux_loop (raw bytes → RTCM/UBX queues)
  • ubx_telemetry_loop (UBX → aggregator)
  • telem_publisher (aggregator → Control stream)
"""
async def ensure_demux_started(ser, ser_lock, call):
	global _rtcm_q, _ubx_q, _demux_task, _telem_task, _telem_pub_task, _serial_stop_evt, _telem_stop_evt
	if _rtcm_q is None:
		_rtcm_q = asyncio.Queue(maxsize=256)
		_ubx_q  = asyncio.Queue(maxsize=256)
	if _serial_stop_evt is None:
		_serial_stop_evt = asyncio.Event()
	if _demux_task is None or _demux_task.done():
		_demux_task = asyncio.create_task(serial_demux_loop(ser, ser_lock, _rtcm_q, _ubx_q, _serial_stop_evt))
	if _telem_task is None or _telem_task.done():
		_telem_task = asyncio.create_task(ubx_telemetry_loop(_ubx_q, _agg))
	if _telem_pub_task is None or _telem_pub_task.done():
		await restart_telem_publisher()
		#if _telem_stop_evt is None:
		#	_telem_stop_evt = asyncio.Event()
		#_telem_pub_task = asyncio.create_task(telem_publisher(_call_writer, _agg, _telem_stop_evt))

# Stop demux + telemetry tasks and place sentinels to exit cleanly
async def stop_all_serial_tasks():
	global _demux_task, _telem_task, _telem_pub_task, _serial_stop_evt
	if _serial_stop_evt:
		_serial_stop_evt.set()
	for t in (_demux_task, _telem_task, _telem_pub_task):
		if t and not t.done():
			t.cancel()
			with contextlib.suppress(Exception):
				await t
	_demux_task = _telem_task = _telem_pub_task = None
	for q in (_rtcm_q, _ubx_q):
		if q:
			with contextlib.suppress(Exception):
				q.put_nowait(None)

# Cancel the current role task (publish/subscribe) without touching queues.
async def stop_role_task():
	t = _role.get("task")
	print("Stopping role task...")
	if t and not t.done():
		print("Canceling the task")
		t.cancel()
		with contextlib.suppress(asyncio.CancelledError):
			await t
	print("Updating role...")
	_role.update(task=None, name=None)
	print("Role updated...")


# Start the role task selected by the server (`BASE` → publish, `RECEIVER` → subscribe).
async def start_role_task(role_enum: int, ser, ser_lock, mount: str, token: str, rtcm_q=None):
	await stop_role_task()  # ensure only one role runs at a time
	if role_enum == pb.Role.BASE:
		_role["task"] = asyncio.create_task(publish_loop(ser, ser_lock, mount, token, rtcm_q))
		_role["name"] = "BASE"
	elif role_enum == pb.Role.RECEIVER:
		_role["task"] = asyncio.create_task(subscribe_loop(ser, ser_lock, mount, token))
		_role["name"] = "RECEIVER"
	else:
		_role["task"] = None
		_role["name"] = "UNSPECIFIED"
		print("Role unspecified; not starting publish/subscribe.")
		return
	print(f"Set role to {_role['name']} (enum={pb.Role.Name(role_enum)})")

# Set (or refresh) the ping watchdog deadline timeout_s seconds from now.
def _arm_ping_deadline(timeout_s: float = 15.0) -> None:
	global _ping_deadline
	_ping_deadline = time.monotonic() + timeout_s


# If server pings stop for too long, request reconfig next session and stop role.
async def _ping_watchdog():
	global _ping_deadline, _last_cfg_version
	try:
		while True:
			await asyncio.sleep(2.0)
			dl = _ping_deadline
			if dl and time.monotonic() > dl:
				print("[agent] ping watchdog: server silent → forcing reconfig next session")
				_last_cfg_version = None
				await stop_role_task()
				return
	except asyncio.CancelledError:
		pass


"""
Role = BASE: Push RTCM frames upstream to the server (Caster.Publish).

High-level flow:
  1) Open a gRPC channel and start a client-streaming Publish RPC.
  2) Send a single OPEN message containing mount/token/label (acts like a handshake/metadata).
  3) Repeatedly read RTCM frames from the demux queue (RTCM queue):
	   • If we see the sentinel (None), the upstream pipeline is shutting down → exit loop.
	   • Otherwise write each frame as a PublishMsg(frame=RtcmFrame(...)).
  4) On orderly exit (closing=True), half-close the stream (done_writing) and await the server's
	 final PublishAck that reports the total number of frames the server counted.
  5) Robustness:
	   • Periodic timeout on queue get() keeps the loop cancellable.
	   • Separate exception paths annotate exit_reason for clear diagnosis.

Parameters:
  ser       : pyserial Serial (not used here; only for symmetry / future-proofing)
  ser_lock  : asyncio.Lock to serialize serial access (not used here)
  mount     : logical routing key for the RTCM stream (server-side fanout topic)
  token     : optional authorization token for the caster
  rtcm_q    : asyncio.Queue of bytes; if None, uses the module-global _rtcm_q

Concurrency & backpressure:
  - The demux task produces frames into rtcm_q.
  - This loop consumes frames and writes to gRPC. If the network is slow:
	  * call.write() awaits underlying flow control; this naturally throttles.
	  * The queue’s maxsize on the producer side should be set to avoid unbounded memory growth.

Shutdown conventions:
  - The ONLY sentinel is None. Seeing None means: stop consuming (upstream is stopping).
  - We set closing=True to indicate we should half-close and await server’s final acknowledgement.
"""
async def publish_loop(ser, ser_lock, mount, token, rtcm_q=None):
	"""Role=BASE: stream RTCM frames from `_rtcm_q` to server via Caster.Publish."""
	print("Inside publish loop")
	
	# If no explicit queue was provided, use the global that demux fills.
	if rtcm_q is None:
		global _rtcm_q
		rtcm_q = _rtcm_q
	closing = False             # whether we should half-close and await the server acknolwedgement 
	call = None                 # gRPC stream object (await-able for the final acknowledgement)
	exit_reason = 'running'     # human-friendly reason printed on exit
	try:
		# Create a channel with keepalive so the server can detect dead peers and vice-versa.
		async with grpc.aio.insecure_channel(
			CAST_ADDR,
			options=[("grpc.keepalive_time_ms", 20000), ("grpc.keepalive_timeout_ms", 5000)],
		) as ch:
			stub = rpc.CasterStub(ch)
			
			# 1) Open client-streaming RPC (note: this returns an async writer stream object)
			call = stub.Publish()  # open client‑streaming call
			
			try:
				# 2) Send exactly one OPEN message with mount/token/label. This lets the server bind the stream to a mount point, do authentication, etc.
				await call.write(pb.PublishMsg(open=pb.PublishOpen(mount=mount, token=token, label="f9t-agent")))
				print("[publish] OPEN sent")
			except grpc.aio.AioRpcError as e:
				# OPEN failed (e.g., network error, server rejected).
				exit_reason = f"OPEN_write_error:{e.code().name}"
				print(f"[publish] OPEN write failed: {e.code().name} - {e.details()}")
				closing = True
				return

			seq = 0  # monotonically increasing sequence number for visibility/debug
			while True:
				# 3) Main loop: drain RTCM frames from the queue and forward to server
				try:
					# Wait up to 2s for a frame to remain cancellable and to re-check conditions
					frame = await asyncio.wait_for(rtcm_q.get(), timeout=2.0)
					
					# Sentinel: producer signaled shutdown → close out gracefully
					if frame is None:
						# Sentinel means upstream is shutting down → exit
						exit_reason = "rtcm_q:None"
						closing = True
						break
				
				# No data available yet — loop again to stay responsive to cancellation.
				except asyncio.TimeoutError:
					continue

				# Ignore empty blobs (shouldn't happen).
				if not frame:
					continue
				
				try:
				   # Forward one RTCM frame to the server. Attach a locally-generated seq and millisecond timestamp for debugging.
					await call.write(pb.PublishMsg(frame=pb.RtcmFrame(data=frame, seq=seq, unix_ms=int(time.time()*1000))))
					seq += 1
				except grpc.aio.AioRpcError as e:
					# Mid-stream write failed — capture code and exit.
					exit_reason = f"frame_write_error:{e.code().name}"
					print(f"[publish] frame write failed: {e.code().name} - {e.details()}")
					closing = True
					break
	except asyncio.CancelledError:
		# Task externally cancelled (e.g. server shutdown).
		exit_reason = "CancelledError"; raise
	except grpc.aio.AioRpcError as e:
		# Channel-level failure while opening/using the stream (e.g., UNAVAILABLE).
		exit_reason = f"AioRpcError:{e.code().name}"
		print(f"[publish] stream error: {e.code().name} - {e.details()}")
		return
	except Exception as e:
		# Any unexpected exception (keep the agent alive; outer loops can retry).
		exit_reason = f"Exception:{type(e).__name__}"
		print(f"[publish] stream error: {e}")
	finally:
		# 4) Print a single-line reason every time the loop ends (good logging).
		print(f"[publish] exit reason: {exit_reason}")
		
		# On a normal/known-close path, half-close the stream and await final acknowledgement.
		if closing and call is not None:
			# Half‑close the client stream and await final ACK with total frames
			with contextlib.suppress(Exception):
				await call.done_writing()
			# Await the server’s terminal PublishAck (with total frames seen).
			with contextlib.suppress(Exception):
				ack = await call
				if ack:
					print(f"[publish] caster ack: frames={ack.frames}")

	"""
Role = RECEIVER: pull RTCM frames from the server and write them to the GNSS serial port.

High-level flow:
  1) Open a gRPC channel and call the server-streaming Subscribe RPC with (mount, token).
  2) For each received RtcmFrame, write its payload bytes to the GNSS device (USB/serial).
  3) The loop ends when the server closes the stream or an exception/cancel occurs.

Parameters:
  ser      : pyserial Serial object connected to the GNSS receiver
  ser_lock : asyncio.Lock protecting serial access (shared with other tasks)
  mount    : routing key identifying which base-stream to subscribe to
  token    : optional authorization token for the caster

Concurrency & backpressure:
  - This consumer writes directly to serial inside the serial lock to avoid interleaving
	with configuration writes or any other serial access.
  - If the serial port blocks, gRPC backpressure will apply naturally because we only
	pull the next frame once we finish writing the prior one.

Shutdown conventions:
  - When the server ends the stream (normal or error), the asynchronous for loop breaks.
"""



async def subscribe_loop(ser, ser_lock, mount, token):
	"""
	Role = RECEIVER: pull RTCM frames from server and write them to the GNSS serial port.

	Adds:
	  • CRC24Q + length checks (defensive)
	  • per-RTCM-ID histogram for quick visibility
	  • periodic progress logging
	  • graceful handling of stream/serial errors
	"""
	print('Currently in subscribe loop')

	total = 0
	last_log = time.time()
	from collections import Counter
	id_hist = Counter()

	try:
		async with grpc.aio.insecure_channel(
			CAST_ADDR,
			options=[("grpc.keepalive_time_ms", 20000),
					 ("grpc.keepalive_timeout_ms", 5000)],
		) as ch:
			stub = rpc.CasterStub(ch)
			req = pb.SubscribeRequest(mount=mount, token=token, label="f9t-agent")

			# Server-streaming loop; ends when server closes or on error/cancel.
			async for frame in stub.Subscribe(req):
				data = frame.data

				# --- Optional: sanity/CRC for RTCM frames ---
				# Only check if this *looks* like RTCM3 (0xD3 ...).
				if len(data) >= 6 and data[0] == 0xD3:
					L = ((data[1] & 0x03) << 8) | data[2]
					total_len = 3 + L + 3  # header + payload + CRC24Q

					# If length is inconsistent, drop and continue (resync will happen on next frame).
					if len(data) != total_len:
						print(f"[subscribe] BAD LEN (have={len(data)} want={total_len}) — dropping frame")
						continue

					body = data[:3+L]              # header+payload
					crc  = int.from_bytes(data[3+L:3+L+3], 'big')
					if _crc24q(body) != crc:
						print("[subscribe] BAD CRC24Q — dropping frame")
						continue

					# Extract and record the RTCM message number for visibility.
					msgnum = _rtcm_id(body[3:])    # payload starts after 3-byte header
					id_hist[msgnum] += 1

				# --- Write to receiver (guarded by lock to avoid interleaving with config writes) ---
				try:
					async with ser_lock:
						ser.write(data)
						# ser.flush()  # usually unnecessary; OS buffers handle this
				except (serial.SerialException, OSError) as e:
					print(f"[subscribe] serial write error: {e}")
					# Back off briefly; if persistent, outer control should restart us.
					await asyncio.sleep(0.2)
					continue

				total += 1

				# Periodic progress line with a mini histogram snapshot.
				if time.time() - last_log > 5.0:
					# Show top few IDs to avoid spam.
					tops = ", ".join(f"{k}:{v}" for k, v in id_hist.most_common(5))
					print(f"[subscribe] wrote {total} RTCM frames to GNSS"
						  + (f" | top IDs: {tops}" if tops else ""))
					last_log = time.time()

	except asyncio.CancelledError:
		# Normal on role flip/shutdown.
		raise
	except grpc.aio.AioRpcError as e:
		# Stream/network failure — control plane supervises restarts.
		print(f"[subscribe] stream error: {e.code().name} - {e.details()}")
	except Exception as e:
		print(f"[subscribe] unexpected error: {type(e).__name__}: {e}")



# ----------------------------------------------------------------------------
# Control plane (bididirectional Pipe): hello, cfg application, telemetry, file receive
# ----------------------------------------------------------------------------


"""
	Maintain the Control (bidirectional) gRPC stream with the server and orchestrate the agent.

	Responsibilities:
	  • Open a bidirectional Control stream (rpc.Control/Pipe) and keep it alive with HTTP/2 keepalives.
	  • Send a single HELLO (identity/version info) to the server.
	  • Receive control messages:
		  - ack: server assigns role + caster credentials → start demux + role task
		  - cfgset: apply UBX VALSET keys (in batches) + (re)enable telemetry UBX messages
		  - ping: refresh watchdog deadline (server heartbeat)
		  - file_*: optional small file transfer (atomic write to /tmp/caster); possible way of sending non-urgent metadata
	  • Periodically publish Telemetry (in a separate task) while the control stream is up.
	  • Run a ping watchdog: if pings stop for too long, force reconfig next session and stop role.
	  • Cleanly stop telemetry publisher + role task if the stream ends or errors.

	Parameters:
	  ser, ser_lock     : pyserial Serial + asyncio.Lock protecting serial access
	  uid_hex           : SEC-UNIQID (10 hex chars)
	  fwver, protver    : parsed/normalized MON-VER info (e.g., "TIM 2.20", "29.20")
	  hwver             : hardware version string
	  mount_token_holder: Dictionary shared with outer scopes to expose {'mount','token','role'}

	Conventions:
	  - Telemetry publisher is (re)started here; it reads UBX-derived fields from `_agg`.
	  - Role task is started only after an acknolwedgement arrives (so we have mount/token/role).
	  - _arm_ping_deadline() is called on HELLO and refreshed on each server ping.
	  - On exit (cancel/error), watchdog and role are stopped, telemetry publisher is signaled.
	"""
async def control_pipe(ser, ser_lock, uid_hex, fwver, protver, hwver, mount_token_holder):
	try:
		 # Open a gRPC channel to the Control service with keepalives so both ends can detect dead peers (NAT, link flap, etc.) promptly.
		async with grpc.aio.insecure_channel(
			CTRL_ADDR,
			options=[
				("grpc.keepalive_time_ms", 20000),
				("grpc.keepalive_timeout_ms", 5000),
				("grpc.keepalive_permit_without_calls", 1),
			],
		) as ch:
			
			# Create stub and open the bidirectional control stream.
			stub = rpc.ControlStub(ch)
			call = stub.Pipe()  # open bidi stream

			# Prepare a write-serializer for outbound control messages and ensure the telemetry publisher is running while the control stream is up.
			global _call_writer, _ping_task, _telem_stop_evt, _telem_pub_task
			_call_writer = CallWriter(call)      # serializes writes and tracks open/closed
			_telem_stop_evt = asyncio.Event()    # lets us stop the telemetry publisher
			if _telem_pub_task is None or _telem_pub_task.done():
				# Starts periodic Telemetry → server (reads current fields from _agg)
				_telem_pub_task = asyncio.create_task(telem_publisher(_call_writer, _agg, _telem_stop_evt))

			# ---- Outbound HELLO -------------------------------------------------
			# Send our identity & versions exactly once. If this fails, just return and let outer control flow retry the whole session.
			ok = await _call_writer.write(pb.ControlMsg(hello=pb.DeviceHello(
				device_id=uid_hex, model="ZED-F9T",
				fwver=fwver, protver=protver, hwver=hwver,
				label="f9t-agent"
			)))
			print('Sent hello')

			# Arm the ping watchdog immediately; server pings will refresh it.
			_arm_ping_deadline()
			if _ping_task is None or _ping_task.done():
				_ping_task = asyncio.create_task(_ping_watchdog())
			if not ok:
				return

			# ---- Config application helper -------------------------------------

			"""
			Apply a CfgSet atomically:
			  • Toggle RTCM USB output based on role (base emits; rover mutes).
			  • Translate VALSET items (CfgVal) to UBX config_set tuples and push in batches.
			  • Enable the UBX messages needed by our telemetry aggregator (_agg).
			  • Report success/failure back to server via ApplyResult.
			"""
			async def apply_cfgset(cfgset: pb.CfgSet) -> bool:
				global _last_cfg_version, _telem_stop_evt, _telem_pub_task
				# Fast-path: already applied this version.
				if _last_cfg_version == cfgset.version:
					print(f"[cfgset] version {cfgset.version} already applied; skipping")
					if _call_writer and _call_writer.is_open():
						await _call_writer.write(pb.ControlMsg(result=pb.ApplyResult(name="CfgSet", ok=True, error="")))
						await restart_telem_publisher()
					return True
				# Serialize applying the config to avoid interleaving batches.
				async with _cfg_apply_lock:
					# Double-check inside the lock (in case two arrive close together).
					if _last_cfg_version == cfgset.version:
						print(f"[cfgset] version {cfgset.version} already applied (post-lock); skipping")
						if _call_writer and _call_writer.is_open():
							await _call_writer.write(pb.ControlMsg(result=pb.ApplyResult(name="CfgSet", ok=True, error="")))
							await restart_telem_publisher()
						return True
					ok, err = True, ""
					try:
						print(f"[cfgset] v={cfgset.version} (VALSET via config_set)")
						# Role-aware RTCM USB visibility: helpful for local debugging/logging.
						role_enum = mount_token_holder.get("role")
						is_base   = (role_enum == pb.Role.BASE)
						# Enable/disable RTCM over USB based on role (agent side visibility)
						for typ in (1005,1077,1087,1097,1127,1230,4072):
							# If we're a base, emit these over USB; if a receiver, mute them.
							await _cfg_msg_usb_rate_rtcm(ser, ser_lock, typ, 1 if is_base else 0)

						# Convert protobuf CfgVal items into (KEY, value) pairs expected by UBXMessage.config_set(..., cfgData=[...]).
						cfgData = []
						for cv in cfgset.items:
							if   cv.HasField("b"):   v = cv.b
							elif cv.HasField("i64"): v = cv.i64
							elif cv.HasField("u64"): v = cv.u64
							elif cv.HasField("f64"): v = cv.f64
							elif cv.HasField("s"):   v = cv.s
							else:
								continue
							k = cv.key.strip().upper().replace("-", "_")
							cfgData.append((k, v))
						print(f"[cfgset] applying {len(cfgData)} keys via config_set (RAM only)")

						# Apply in small batches to avoid overflowing device I/O buffers.
						CHUNK = 30
						applied = 0
						for i in range(0, len(cfgData), CHUNK):
							batch = cfgData[i:i+CHUNK]
							try:
								# Use pyubx library to build a VALSET message.
								vs = UBXMessage.config_set(layers=1, transaction=0, cfgData=batch)
								# Serialize write with the serial lock to prevent interleaving.
								async with ser_lock:
									ser.write(vs.serialize()); ser.flush()
								applied += len(batch)
								await asyncio.sleep(0.1)  # let device breathe
							except Exception as e:
								# Non-fatal: we continue with remaining batches but report in logs.
								print(f"[valset] batch {i//CHUNK} failed: {e}")
						print(f"[valset] applied {applied}/{len(cfgData)} items via config_set")

						# Ensure the UBX messages required for our TelemetryAgg are enabled: TIM-TP (qErr + utc flag), MON-SYS (temp), NAV-SAT (counts/CN0), NAV-DOP (PDOP). USB rates are given per measurement cycle
						await _cfg_msg_usb_rate_ubx(ser, ser_lock, 0x0D, 0x01, 1)  # TIM-TP
						await _cfg_msg_usb_rate_ubx(ser, ser_lock, 0x0A, 0x39, 1)  # MON-SYS
						await _cfg_msg_usb_rate_ubx(ser, ser_lock, 0x01, 0x35, 1)  # NAV-SAT
						await _cfg_msg_usb_rate_ubx(ser, ser_lock, 0x01, 0x04, 1)  # NAV-DOP

						if not is_base:
							# Device-level proof it’s ingesting RTCM:
							await _cfg_msg_usb_rate_ubx(ser, ser_lock, 0x02, 0x32, 1)  # UBX-RXM-RTCM @ USB

							# Port-level RX counters (pick one of these; MON-COMMS if supported):
							await _cfg_msg_usb_rate_ubx(ser, ser_lock, 0x0A, 0x36, 1)  # UBX-MON-COMMS @ USB

					except Exception as e:
						ok, err = False, str(e)
						print(f"[cfgset] error applying config: {e}")
					
					# Report result upstream so the server can log/act.
					if _call_writer and _call_writer.is_open():
						await _call_writer.write(pb.ControlMsg(result=pb.ApplyResult(name="CfgSet", ok=ok, error=("" if ok else err))))
						if ok:
							# If config changed output rates, restart telemetry publisher so it re-seeds its timing and respects the new cadence.
							await restart_telem_publisher()
							#_telem_stop_evt = asyncio.Event()
							#_telem_pub_task = asyncio.create_task(telem_publisher(_call_writer, _agg, _telem_stop_evt))
					if ok:
						_last_cfg_version = cfgset.version
					return ok

			# ---------------- Control stream receive loop ------------------------
			# Optional small file receive state.
			file_fp = None; file_tmp = None; file_name = None
			file_dir = "/tmp/caster"; os.makedirs(file_dir, exist_ok=True)
			
			# Flags used in finally{} to decide what to stop.
			stream_broke = False; stream_ended = False
			try:
				# Iterate over inbound messages from the server until the stream ends.
				async for m in call:
					which = m.WhichOneof("msg")
					print("[agent] control recv:", which)
					if m.HasField("ack"):
						# Server assigns: role, mount, token → start the bidirectional stream.
						role_enum = m.ack.role
						print(f"Received ack. Role={pb.Role.Name(role_enum)}")
						mount_token_holder["mount"] = m.ack.mount
						mount_token_holder["token"] = m.ack.token
						mount_token_holder["role"]  = role_enum
						
						# Ensure serial demux + telemetry consumer/publisher are alive.
						await ensure_demux_started(ser, ser_lock, call)
						
						# Start exactly one role task (publish if BASE, subscribe if RECEIVER).
						await start_role_task(role_enum, ser, ser_lock, m.ack.mount, m.ack.token, rtcm_q=_rtcm_q)

						set_telem_log_alias(getattr(m.ack, "alias", ""), device_id=uid_hex)
					
					elif m.HasField("cfgset"):
						print("Configuring device…")
						await apply_cfgset(m.cfgset)
					elif m.HasField("ping"):
						_arm_ping_deadline()  # refresh watchdog deadline
					elif m.HasField("file_begin"):
						# Prepare to receive a small file into /tmp/caster atomically.                        
						file_name = m.file_begin.name or "config.bin"
						file_tmp  = os.path.join(file_dir, f".{file_name}.part")
						try:
							if file_fp and not file_fp.closed:
								file_fp.close()
						except Exception:
							pass
						file_fp = open(file_tmp, "wb")
					elif m.HasField("file_chunk"):
						# Append chunk to the temp file if one is open.
						if file_fp:
							file_fp.write(m.file_chunk.data)
					elif m.HasField("file_end"):
						# Close+fsync temp file and atomically rename it into place.
						ok = True
						try:
							if file_fp:
								file_fp.flush(); os.fsync(file_fp.fileno()); file_fp.close()
								final_path = os.path.join(file_dir, file_name or "config.bin")
								os.replace(file_tmp, final_path)
							else:
								# Best-effort cleanup on failure.
								ok = False
						except Exception:
							ok = False
							with contextlib.suppress(Exception):
								if file_fp and not file_fp.closed:
									file_fp.close()
							with contextlib.suppress(Exception):
								if file_tmp and os.path.exists(file_tmp):
									os.remove(file_tmp)
						# Tell the server if the file transfer worked.
						if _call_writer.is_open():
							await _call_writer.write(pb.ControlMsg(result=pb.ApplyResult(name=f"file:{file_name or ''}", ok=ok)))
						# Reset file state for next transfer.
						file_fp = None; file_tmp = None; file_name = None
			except asyncio.CancelledError:
				# Upstream told us to exit
				stream_broke = True; raise
			except grpc.aio.AioRpcError as e:
				# Stream terminated with an RPC-level error (e.g., UNAVAILABLE).
				stream_broke = True; print(f"[control] stream closed: {e.code().name} - {e.details()}")
			except Exception as e:
				# Unexpected local error while handling messages.
				stream_broke = True; print(f"[control] stream error: {type(e).__name__}: {e}")
			finally:
				# Regardless of how the loop ended, stop the watchdog and bidirectional data stream.
				if _ping_task:
					_ping_task.cancel()
					with contextlib.suppress(Exception):
						await _ping_task
				if stream_ended or stream_broke:
					# Signal telemetry publisher to finish and stop role.
					if _telem_stop_evt and not _telem_stop_evt.is_set():
						_telem_stop_evt.set()
					await stop_role_task()
			print('Made it out of the loop…')
	except asyncio.CancelledError:
		# The entire control_pipe task was cancelled by our supervisor.
		print('Shutting down…'); return

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

"""
Entry point: discover device, open control pipe, and manage lifetime.

• Discovers a u‑blox serial device (or uses argv[1] path) and reads identity.
• Starts `control_pipe`, waits for either stop signal or control exit.
• On stop: shuts down role tasks and serial pipelines; closes serial port.
• On error: logs and retries after a short delay.
"""
async def main():
	stop = asyncio.Event()
	install_signal_handlers(stop)
	while True:
		try:
			# Discover device and identity (optionally use provided port)
			if len(sys.argv) > 1:
				port, uid = discover_f9x(port = sys.argv[1])
			else:
				port, uid = discover_f9x()


			# --- Read MON-VER and normalize (TIM/PROT pretty mapping) ---
			try:
				ver = normalize_f9t_versions(read_mon_ver(port, 115200))
			except Exception as e:
				print(f"[identify] MON-VER read failed: {e} — using fallbacks")
				ver = {"fwver": "", "protver": "", "hwver": ""}

			# Open serial and spin up control plane
			ser = serial.Serial(port, 115200, timeout=0.2, write_timeout=0.5)
			ser_lock = asyncio.Lock()
			creds = {"mount": None, "token": None, "role": None}
			ctrl_task = asyncio.create_task(
				control_pipe(ser, ser_lock, uid, ver['fwver'], ver['protver'], ver['hwver'], creds)
			)

			# Wait until either we have role+creds or a stop signal arrives
			while not stop.is_set() and not (creds["mount"] and creds["token"] and creds["role"]):
				await asyncio.sleep(0.1)
			stop_task = asyncio.create_task(stop.wait())
			done, _ = await asyncio.wait({ctrl_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

			if stop_task in done:
				# Cooperative shutdown
				await stop_role_task()
				await stop_all_serial_tasks()
				await cancel_and_wait(ctrl_task)
				with contextlib.suppress(Exception):
					ser.close()
				break

			# Control exited: stop role, close serial, and retry after a short pause.
			await stop_role_task()
			with contextlib.suppress(Exception):
				ser.close()
			await asyncio.sleep(2.0)
		except Exception as e:
			print("agent error:", e)
			await asyncio.sleep(2.0)


if __name__ == "__main__":
	# Run the agent; allow Ctrl+C to exit cleanly via installed signal handlers
	asyncio.run(main())