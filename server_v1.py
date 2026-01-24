#!/usr/bin/env python3
"""
GNSS caster/control server
This code is set up to control a network of Ublox Zed-F9T GNSS receivers for the PANOSETI time distribution
system. There are two roles in the system: Server and agent. Agent is further subdivided into two roles:
Base and receiver to allow for precise differential timing. This code is for the server. The server does the
following:

- Discover all F9T devices on a network
- Configure all F9T devices using a config file
- Receive RTCM messages from a base station and forward them on to any receivers in the network
- Log telemetry data coming from both base stations and receivers (if wanted). This includes data like qerr, UTC timestamps, and information about satellites in view. These data can also be logged locally.
- Ping the devices so the F9T devices know if the headnode can talk with them
"""
from __future__ import annotations
from datetime import datetime, timezone
from logging_setup import setup_logging

import argparse
import asyncio
import contextlib
import logging

import os
import shutil
import signal
import time
from pathlib import Path

import grpc
import json5

import caster_setup_pb2 as pb
import caster_setup_pb2_grpc as rpc

# ----------------------------- basic config ---------------------------------
REPO_ROOT = Path(__file__).resolve().parent
_START_TS  = datetime.now(timezone.utc)
_START_STR = _START_TS.strftime("%Y%m%d_%H%M%SZ")

TELEM_DIR   = REPO_ROOT / "telemetry"
LOGGING_DIR = REPO_ROOT / "logging"

TELEM_DIR.mkdir(parents=True, exist_ok=True)
LOGGING_DIR.mkdir(parents=True, exist_ok=True)


_LOG_PATH_TELEM   = str(TELEM_DIR / f"telemetry_{_START_STR}.jsonl")
_LOG_PATH_LOGGING = str(LOGGING_DIR / f"SERVER_{_START_STR}.txt")

# Setup for a connection is given by
#	. Role:  Controls whether a device sends out (base) or takes in (receiver) RTCM messages
#	. Mount: Controls routing - in principle this allows for abtirary configurations of base / receivers
#	. Token: Authorization (not really used for this)
# The default policy just gives default config values in case something necessary isn't defined in the 
# the config file. The config version allows for multiple config files in principle. And it's also used 
# to check if a device needs to be reconfigured or not.

DEFAULT_POLICY = {
	"manifest": str(REPO_ROOT / "manifest_f9t.json5"),
	"publish_mount": "BASE",
	"publish_token": "PUBTOKEN",
	"subscribe_mount": "BASE",
	"subscribe_token": "SUBTOKEN",
	"config_version": 1,
}

# Per-device state shared across sessions (simple in-memory variant)
PUSHED_CFG: dict[str, int] = {}        # device_id -> last pushed manifest version
PUSH_INFLIGHT: set[tuple[str, int]] = set()  # (device_id, version) during async push
LATEST_TELEM: dict[str, dict] = {}     # device_id -> last Telemetry dict
LAST_SEEN = {}

# ------------------------------ helpers -------------------------------------


# Exit cleanly on SIGINT/SIGTERM (Unix).
def install_signal_handlers(stop_evt: asyncio.Event) -> None:
	loop = asyncio.get_running_loop()
	for sig in (signal.SIGINT, signal.SIGTERM):
		with contextlib.suppress(NotImplementedError):
			loop.add_signal_handler(sig, stop_evt.set)


# Append a compact JSONL record (easy to ingest/grep).
def jlog(kind: str, device_id: str, alias: str = "", **payload) -> None:
	rec = {"ts": int(time.time() * 1000), "kind": kind, "device_id": device_id, "alias": alias, **payload}
	with open(_LOG_PATH_TELEM, "a", encoding="utf-8") as f:
		# Remember that you will have to load this using JSON5 instead of JSON (if you using Python)
		f.write(json5.dumps(rec, separators=(",", ":")) + "\n")



def parse_args():
	p = argparse.ArgumentParser()
	p.add_argument("--config", default = None, help = "optional path to config file")
	p.add_argument("--ip", default = "0.0.0.0:50051", help = "IP address to bind to default is 0.0.0.0:50051")
	p.add_argument("--log-file", default="", help="optional file path")
	p.add_argument("-v", "--verbosity", type=int, default=2, help="0=errors, 1=warn, 2=info, 3=debug")
	return p.parse_args()


# ----------------------------- hub (fanout) ---------------------------------
class Hub:
	"""Simple in-process pub/sub fanout per mount.

	- attach() returns an asyncio.Queue for a subscriber
	- publish() pushes frames to all queues (non-blocking; drops if full)
	- shutdown() unblocks all subscribers by sending None
	"""

	def __init__(self) -> None:
		self.subs: dict[str, set[asyncio.Queue]] = {}

	def attach(self, mount: str) -> asyncio.Queue:
		q = asyncio.Queue(maxsize=1024)
		self.subs.setdefault(mount, set()).add(q)
		return q

	def detach(self, mount: str, q: asyncio.Queue) -> None:
		self.subs.get(mount, set()).discard(q)

	async def publish(self, mount: str, frame: bytes) -> None:
		log.debug("[hub] mount=%s -> %d subs, frame len=%d", mount, len(self.subs.get(mount, [])), len(frame))
		for q in list(self.subs.get(mount, set())):
			try:
				q.put_nowait(frame)
			except asyncio.QueueFull:
				# drop if slow consumer; production systems may buffer/debounce
				pass

	async def shutdown(self) -> None:
		for qs in list(self.subs.values()):
			for q in list(qs):
				with contextlib.suppress(Exception):
					q.put_nowait(None)


# ----------------------- manifest → CfgSet helpers --------------------------

# Converts a config val from what is given in the manifest. The agent figures out how to convert this
# into a register value that is written to the device

def _to_cfgval(key: str, value) -> pb.CfgVal | None:
	"""Build a typed CfgVal from a Python value (bool/int/float/str)."""
	cv = pb.CfgVal(key=key)
	if isinstance(value, bool):
		cv.b = value
	elif isinstance(value, int):
		# Use u64 for non-negative, i64 for negative
		if value >= 0:
			cv.u64 = value
		else:
			cv.i64 = value
	elif isinstance(value, float):
		cv.f64 = float(value)
	elif value is None:
		return None
	else:
		cv.s = str(value)
	return cv


def build_cfgvals(items: list[dict]) -> list[pb.CfgVal]:
	"""Convert manifest {key,value} dicts into typed CfgVal messages."""
	out: list[pb.CfgVal] = []
	for it in (items or []):
		k = it.get("key")
		if not k:
			continue
		v = it.get("value")
		cv = _to_cfgval(k, v)
		if cv is not None:
			out.append(cv)
	return out


def _llh_to_cfg_tmode_llh(lat_deg: float, lon_deg: float, h_m: float, acc_m: float) -> list[dict]:
	"""Helper: convert LLH into UBX-CFG-TMODE* VALSET fields (1e-7 deg using high precision mode).
	   Returns an array of dictionaries. There are more efficient ways to do this."""

	def split_deg(val_deg: float) -> tuple[int, int]:
		nano = int(round(val_deg * 1e9))  # 1e-9 deg
		coarse = nano // 100                        # 1e-7 deg
		hp = nano - coarse * 100                    # remainder (−99..+99)
		if hp < -99:
			coarse -= 1
			hp += 100
		elif hp > 99:
			coarse += 1
			hp -= 100
		return int(coarse), int(hp)

	lat_1e7, lat_hp = split_deg(lat_deg)
	lon_1e7, lon_hp = split_deg(lon_deg)
	height_cm_total = int(round(h_m * 100.0))
	acc_cm = max(0, int(round(acc_m * 100.0)))

	# Required CFG keys to get position. See Section 6.8.21 CFG-TMODE in the interface description
	return [
		{"key": "CFG_TMODE_MODE", "value": 2},           # fixed mode
		{"key": "CFG_TMODE_POS_TYPE", "value": 1},       # Latitude/longitude/height
		{"key": "CFG_TMODE_LAT", "value": lat_1e7},
		{"key": "CFG_TMODE_LON", "value": lon_1e7},
		{"key": "CFG_TMODE_HEIGHT", "value": height_cm_total},
		{"key": "CFG_TMODE_LAT_HP", "value": lat_hp},
		{"key": "CFG_TMODE_LON_HP", "value": lon_hp},
		{"key": "CFG_TMODE_HEIGHT_HP", "value": 0},
		{"key": "CFG_TMODE_FIXED_POS_ACC", "value": acc_cm},
	]


# Get role (base/receiver) as well as device ID from the manifest.

def decide_role_and_creds_for_device(man: dict, device_id: str) -> tuple[int, str, str, str]:
	"""Return (role_enum, mount, token, alias) using DEFAULT_POLICY creds."""
	dev_key = device_id.strip().upper()
	dev = (man.get("devices") or {}).get(dev_key)
	if not dev:
		raise KeyError(f"device {dev_key} not found in manifest devices")

	role_str = (dev.get("role") or "").strip().lower()
	if role_str not in ("base", "receiver"):
		raise ValueError(f"device {dev_key} has no valid role ('base'|'receiver')")

	role_enum = pb.Role.BASE if role_str == "base" else pb.Role.RECEIVER
	log.info("Role enum: %s", role_enum)
	if role_enum == pb.Role.BASE:
		mount = DEFAULT_POLICY["publish_mount"]
		token = DEFAULT_POLICY.get("publish_token", "")
	else:
		mount = DEFAULT_POLICY["subscribe_mount"]
		token = DEFAULT_POLICY.get("subscribe_token", "")

	alias = dev.get("alias", "")
	return role_enum, mount, token, alias

# Read through the manifest and get all the various settings given in the manifest. Settings are given as:
# Global settings (all devices), role settings (base/receiver), and device settings (device ID). Note that
# there are no F9P settings in here. It is used so irregularly that adding these settings to the global manifest
# seemed extra. But, in principle, you could.
async def cfg_from_manifest_for_device(man: dict, device_id: str, role_enum: int) -> tuple[list[str], list[dict]]:

	role_str = "base" if role_enum == pb.Role.BASE else "receiver"
	layers = (man.get("global", {}).get("apply_to_layers")) or ["RAM"]

	items: list[dict] = []
	items += (man.get("global", {}).get("config") or [])
	items += (man.get("role", {}).get(role_str, {}).get("config") or [])

	dev = man.get("devices", {}).get(device_id.strip().upper(), {})
	items += (dev.get("config") or [])

	pos = (dev or {}).get("position", {})
	if (pos.get("format", "").upper() == "LLH" and
			all(k in pos for k in ("lat_deg", "lon_deg", "height_m"))):
		items += _llh_to_cfg_tmode_llh(
			float(pos["lat_deg"]), float(pos["lon_deg"]), float(pos["height_m"]), float(pos.get("acc_m", 0.02))
		)

	return layers, items


# ---------------------------- heartbeats/ping -------------------------------
# Send a ping ever 5 seconds. This may be too frequent. You can figure it to whatever you want.
async def heartbeat(out_q: asyncio.Queue, period: float = 5.0) -> None:
	"""Server → agent Ping messages at fixed period (agent need not respond)."""
	try:
		while True:
			await asyncio.sleep(period)
			await out_q.put(pb.ControlMsg(ping=pb.Ping(unix_ms=int(time.time() * 1000))))
	except asyncio.CancelledError:
		pass


# ------------------------------ RPC handlers --------------------------------

'''
 There are two services: One for RTCM messages and one for everything else (e.g. hello/acknowledgement, configuration, pinging, and telemetry). The Caster service has two RPCs: One client streaming and one server streaming. The client streaming service is for the base [one (or more) DAQ node(s)], which sends RTCM messages back to the server (headnode). The server  streaming service then takes these messages and sends them to the receiver [one (or more) DAQ node(s)]. The 'everything else service' has a bidirectional stream which is responsible for any other communication that might need to be done between head node and DAQ node(s). The benefit of doing things this way is thatthings are very flexible. All configuration is set up in a single config file that is then parsed by the head node. DAQ nodes discover any devices connected to them and send info back to the head node which is then used to configure devices. Changing the base/subscriber receivers, adding new receivers, or modifying setup parameters (e.g. changing cable delays) can all be done in a single config file. Heavy use of the gRPC AsyncIO (https://grpc.github.io/grpc/python/grpc_asyncio.html) for concurrent operation across multiple devices is utilised.
'''

"""Data service: base agents publish RTCM; rovers subscribe."""
class CasterServicer(rpc.CasterServicer):
   

	def __init__(self, hub: Hub) -> None:
		self.hub = hub

	# Base publish 
	async def Publish(self, request_iterator, context):
		# Logs that the handler is active and initializes a running counter of RTCM frames (frames). This counter is used only to report back in the final ACK.        
		log.info("[server] Publish: handler started")
		frames = 0
		current_mount = None  # remember mount from the OPEN
		try:
			# Consume the client's stream
			async for m in request_iterator:
				if m.HasField("open"):
					# First message
					current_mount = (m.open.mount or "").strip()
					log.info("[server] Publish: OPEN mount=%s label=%s", m.open.mount, m.open.label)
					# You can validate token here if desired.

				# RTCM frame message; increment frames and display log messages    
				elif m.HasField("frame"):
					frames += 1
					if current_mount:
						await self.hub.publish(mount=current_mount, frame=m.frame.data)
					# Fanout is optional here; wire to Hub if you want server-side relay
					# await self.hub.publish(mount=m.open.mount, frame=m.frame.data)
					if (frames % 100) == 0:
						log.info("[server] Publish: frames=%d", frames)
				else:
					log.warning("[server] Publish: unknown oneof; ignoring")
			# client half-closed: return a single ack
			# Client is no longer sending message
			log.info("[server] Publish: client closed, ack frames=%d", frames)
			return pb.PublishAck(frames=frames)
		# Error handling
		except asyncio.CancelledError:
			log.info("[server] Publish: cancelled")
			raise
		except grpc.aio.AioRpcError as e:
			log.warning("[server] Publish: rpc error %s - %s", e.code().name, e.details())
			return pb.PublishAck(frames=frames)
		except Exception as e:
			log.error("[server] Publish: exception %s: %s", type(e).__name__, e)
			return pb.PublishAck(frames=frames)

	# Receiver subscribe
	async def Subscribe(self, request, context):
		log.info("Creating a subscribe queue for mount=%r ...", request.mount)		
		q = self.hub.attach(request.mount)
		try:
			while True:
				frame = await q.get()
				if frame is None:
					break
				yield  pb.RtcmFrame(data=frame)
		finally:
			self.hub.detach(request.mount, q)


# Bidirectional stream allowing for configuration of devices, ping-ing, and telemetry data
class ControlServicer(rpc.ControlServicer):
	"""Control plane: hello/ack, config push, ping, telemetry."""

	async def Pipe(self, request_iterator, context):
		log.info("[server] Control.Pipe: opened")
		# Create a queue with some back pressure to prevent a possible memory leak (too many RTCM) messages being produced / the headnode is bogged down for some reason
		out_q: asyncio.Queue = asyncio.Queue(maxsize=128)
		alias = ""
		# Unique device ID used to figure out device-specific config options
		device_id: str | None = None

		# Heartbeat task
		hb_task: asyncio.Task | None = None

		# Heartbeat period (ping regularity)
		HB_PERIOD_S = 5.0

		# How long before a device is announced stale and is reconfigured when it reconnects
		PING_STALE_MS = int(2 * HB_PERIOD_S * 1000)


		"""
		Server-side receive loop for Control.Pipe (bidirectional streaming).

		Responsibilities:
		  - Consume ControlMsg messages from the agent.
		  - On HELLO:
			  * extract/normalize device_id
			  * load manifest (off the event loop via to_thread)
			  * decide role/mount/token/alias and ACK back to agent
			  * start a periodic server→agent heartbeat task if not running
			  * Push config derived from manifest to this agent
		  - On RESULT:
			  * Log the agent's ack/err for previously issued commands
		  - On TELEMETRY:
			  * (pdate in-memory "latest telemetry" and append a JSONL log record
		  - On PING:
			  * Pings are server to agent so nothing is done)

		Concurrency notes:
		  - Uses a shared output queue to send messages back to the agent [handled by a sibling tx() task].
		  - Heartbeat is its own `asyncio.Task` (created once).
		  - Configuration push is spawned as a fire-and-forget task (`push_cfg_once`), guarded by
			`PUSHED_CFG` and `PUSH_INFLIGHT` to ensure at-most-once per (device, version).
		"""
		async def rx():
			nonlocal device_id, hb_task
			try:
				# Iterate over inbound ControlMsg messages from the agent until the client half-closes
				async for m in request_iterator:
					try:
						# If you want "last seen" to update on any inbound msg, uncomment the next line
						#last_seen_ms = int(time.time() * 1000)  # bump on any inbound
						if m.HasField("hello"):
							# HELLO handshake from agent (establish identity and return role/credentials) 
							h = m.hello
							# Normalize device_id (upper-case, no surrounding whitespace); used as dict key
							device_id = (h.device_id or "").strip().upper()
							log.info("[server] Control.Pipe: HELLO %s", device_id)
							
							# Update a global "last seen" map (ms since epoch) for observability/health checks
							LAST_SEEN[device_id] = time.time()*1000 	
							
							# Load manifest off the event loop to avoid blocking asyncio receive thread
							# (Path.read_text with encoding is CPU+IO-bound; to_thread keeps loop snappy)
							text = await asyncio.to_thread(
								Path(DEFAULT_POLICY["manifest"]).read_text, encoding="utf-8"
							)
							man = json5.loads(text)

							# Decide role/mount/token/alias for this device using manifest + defaults
							role_enum, mount, token, alias = decide_role_and_creds_for_device(man, device_id)
							
							# Send hello acknowledgement to the agent with role/mount and config version
							# (This unblocks the agent to configure its publish/subscribe and behavior.)
							await out_q.put(pb.ControlMsg(ack=pb.HelloAck(
								ok=True,
								role=role_enum,
								alias=alias,
								mount=mount,
								config_version=DEFAULT_POLICY.get("config_version", 1),
								token=token,
								label=h.label or "agent",
							)))

							# Ensure a periodic server→agent heartbeat is running							
							if hb_task is None or hb_task.done():
								hb_task = asyncio.create_task(heartbeat(out_q, period=HB_PERIOD_S))

							# Config push (per device, per manifest version)
							# Version is read from manifest (fallback to 1).
							cfg_version = (man.get("global", {}) or {}).get("version") or 1
							verify_layer = (man.get("global", {}) or {}).get("verify_layer") or "RAM"
							log.debug("verify_layer set to %s", verify_layer)
							# Allows for per-device layer checking

							key = (device_id, cfg_version)

							# Only push if this device hasn't been pushed to at this version, and if there's no push already inflight for the same (device, version).
							if PUSHED_CFG.get(device_id) != cfg_version and key not in PUSH_INFLIGHT:
								PUSH_INFLIGHT.add(key)

								"""
								Build and send CfgSet messages derived from the manifest, in chunks,
								then mark PUSHED_CFG upon success; on error, emit an ApplyResult with details.
								"""
								async def push_cfg_once():
									try:
										# Derive target config: merge global/role/device layers + TMODE if LLH present
										layers, items = await cfg_from_manifest_for_device(man, device_id, role_enum)
										
										# Chunk the config to keep each CfgSet message bounded (e.g., 64 items)
										for i in range(0, len(items), 64):
											chunk = items[i:i + 64]
											cfgvals = build_cfgvals(chunk)
											
											# Send a CfgSet request to the GNSS receiver
											await out_q.put(pb.ControlMsg(cfgset=pb.CfgSet(
												items=cfgvals, apply_to_layers=layers, verify_layer = verify_layer, version=cfg_version
											)))

											
											# Yield so the TX task can run and move data out promptly
											await asyncio.sleep(0)  # let tx run
										
										# Mark success for this device/version so we don't re-push
										PUSHED_CFG[device_id] = cfg_version
										log.info("[server] Control.Pipe: pushed cfg v%s to %s", cfg_version, device_id)
									except Exception as e:
										# Send failure notification back to the agent/operator via an ApplyResult
										await out_q.put(pb.ControlMsg(result=pb.ApplyResult(
											name="push_cfg", ok=False, error=str(e)
										)))
									finally:
										# Always clear inflight guard even on exceptions
										PUSH_INFLIGHT.discard(key)

								# Spawn the push as a background task; rx() continues to process inbound msgs
								asyncio.create_task(push_cfg_once())

						elif m.HasField("result"):
							# --- RESULT: reporting outcome of a prior command (e.g., cfg apply) ---
							r = m.result
							log.info("[server] Control.Pipe: result %s ok=%s err=%r", r.name, r.ok, r.error)

						elif m.HasField("telem"):
							# --- TELEMETRY: status/health snapshot from agent/GNSS ---
							t = m.telem
							# Build a dictionary with types normalized (int/float/bool)
							rec = {
								"unix_ms": int(getattr(t, "unix_ms", 0)),
								"temp_c": float(getattr(t, "temp_c", 0.0)),
								"qerr_ns": round(float(getattr(t, "qerr_ns", 0)),3),
								"utc_ok": bool(getattr(t, "utc_ok", False)),
								"num_vis": int(getattr(t, "num_vis", 0)),
								"num_used": int(getattr(t, "num_used", 0)),
								"gps_used": int(getattr(t, "gps_used", 0)),
								"gal_used": int(getattr(t, "gal_used", 0)),
								"bds_used": int(getattr(t, "bds_used", 0)),
								"glo_used": int(getattr(t, "glo_used", 0)),
								"avg_cno": round(float(getattr(t, "avg_cno", 0.0)),4),
								"pdop": round(float(getattr(t, "pdop", 0.0)),4),
							}
							
							# Update in-memory cache and append a JSONL log row (if we know which device)
							if device_id:
								LATEST_TELEM[device_id] = rec
								#alias = getattr(m.ack, "alias", "") if hasattr(m, "ack") else ""
								jlog("telem", device_id, alias = alias, **rec)

						elif m.HasField("ping"):
							 # --- PING: currently a no-op (pings are server→agent in this design) ---
							 # If you later switch to agent→server heartbeat, update last-seen here.
							 # LAST_SEEN[device_id] = time.time() * 1000
							pass

						# Ignore unknown/unused fields

					except Exception as e:
						# Per-message guard: one bad payload shouldn't tear down the entire stream.
						log.warning("[server] Control.Pipe: per-msg err: %s", e)
						continue

			except asyncio.CancelledError:
			# Stream/task was cancelled (e.g., server shutdown or client aborted)
				log.info("[server] Control.Pipe: rx cancelled")
			finally:
			 # The client half-closed its send side or the loop is exiting for another reason: signal the TX task by placing a sentinel (None) so it can finish cleanly.
				log.info("[server] Control.Pipe: rx ended (client half-closed)")
				await out_q.put(None)  # sentinel for tx()

		"""
		Server-side transmit loop for Control.Pipe (bidi streaming).

		Reads ControlMsg objects from the shared queue (populated by rx(), heartbeat(),
		and any background tasks like push_cfg_once()) and yields them to the gRPC
		response stream.

		Contract & flow:
		  - Blocks on queue.get() until a message is available.
		  - A None value is used as a sentinel to signal shutdown [e.g., rx() saw client
			half-close or server is tearing down]. On sentinel, break and let gRPC close the
			server→client direction.
		  - Uses yield msg to stream messages to the client as they become available.

		Concurrency notes:
		  - This coroutine should run concurrently with the receive loop. They communicate
			via a queue. All producers must agree on None as the sentinel.
		  - Backpressure is naturally handled by the queue’s maxsize (set when the queue is created).
			If the queue is bounded and the client/network is slow, producers will block on put().
		"""
		async def tx():
			try:
				while True:
					# Wait for the next outbound message from any producer.
					msg = await out_q.get()
					
					# Sentinel protocol: None means "shut down the TX loop".
					if msg is None:
						break
					
					# Stream this message to the client. In gRPC AsyncIO, yield inside the handler’s async generator sends one message on the server→client stream.

					yield msg

			except asyncio.CancelledError:
				# Normal during shutdown (e.g., server stopping or client cancelling the call). Swallow to avoid noisy logs; gRPC will handle the cancellation semantics.
				pass

		
		# Kick off the RX side (reader) as a background task; it will consume inbound messages from the agent and populate `out_q` with responses (ACKs, CfgSet, heartbeats, etc.).
		rx_task = asyncio.create_task(rx())


		try:
		# Drive the TX side (writer) directly from this handler by iterating the async generator tx(). Each yield out sends one ControlMsg to the client. This runs until tx() sees a sentinel (None) or the stream is cancelled.
			async for out in tx():
				yield out
		finally:
		# --- Cooperative teardown path (runs on normal exit or on exceptions/cancellation) ---
			if hb_task is not None:
			# Stop the heartbeat task if it was started. Suppress exceptions so shutdown is clean.
				with contextlib.suppress(Exception):
					hb_task.cancel(); await hb_task
			
			# Stop the RX loop task. It normally exits after we send the sentinel into the queue (done in rx()'s finally), but if we’re here for any reason, make sure it’s cancelled.

			with contextlib.suppress(Exception):
				rx_task.cancel(); await rx_task
			log.info("[server] Control.Pipe: closed")

			# If the session ended uncleanly (no recent pings), clear PUSHED_CFG so the next hello will get config again.
			if device_id:
				# Milliseconds since we last saw any message tagged with this device_id
				age = int(time.time() * 1000) - LAST_SEEN[device_id]
				log.debug("age since last discovery %d ms", age)
				
				# If the age exceeds twice the heartbeat period, consider the connection stale. Clearing the PUSHED_CFG entry forces a re-push next time this device connects.

				if age > 2 * HB_PERIOD_S * 1000:
					removed = PUSHED_CFG.pop(device_id, None)
					if removed is not None:
						log.info("[server] Control.Pipe: ping stale (%d ms) → clearing PUSHED_CFG[%s]", age, device_id)


# ------------------------------ server boot ---------------------------------

"""
Create and run the gRPC server with both services:
  - Caster: Publish (client-stream) & Subscribe (server-stream) for RTCM data fanout
  - Control: Pipe (bidirectional stream) for control-plane messages (hello/acknolwedge, cfg, telemetry, ping)

Lifecycle:
  1) Build server and register servicers.
  2) Bind and start listening on addr.
  3) Wait until either:
	   - a termination signal (SIGINT/SIGTERM) is received, or
	   - the server terminates for another reason.
  4) On shutdown request, gracefully:
	   - notify Hub subscribers (unblock) and
	   - stop the gRPC server with a grace period.
"""
async def serve(addr: str = "0.0.0.0:50051") -> None:
	stop = asyncio.Event() # will be set on SIGINT/SIGTERM
	install_signal_handlers(stop) # attach signal handlers to set `stop`

	hub = Hub() # in-process fanout manager for RTCM frames
	
	# Configure gRPC AsyncIO server with keepalive settings suitable for long-lived streams.
	#server = grpc.aio.server(options=[
	#	("grpc.keepalive_time_ms", 20000),  # send HTTP/2 PING every 20s
	#	("grpc.keepalive_timeout_ms", 5000), # consider dead if no ACK in 5s
	#	("grpc.keepalive_permit_without_calls", 1), # allow PINGs without active calls
	#])
	server = grpc.aio.server()
	rpc.add_CasterServicer_to_server(CasterServicer(hub), server)
	rpc.add_ControlServicer_to_server(ControlServicer(), server)


	# Bind to the requested address/port (plaintext for now)
	server.add_insecure_port(addr)
	print("listening on", addr)
	log.info("listening on %s", addr)
	
	# Start accepting RPCs
	await server.start()

	# --- Wait for shutdown condition ---

	# `wait_for_termination()` completes when the server stops for any reason.
	wait_task = asyncio.create_task(server.wait_for_termination())
	
	# `stop.wait()` completes when SIGINT/SIGTERM arrives (see install_signal_handlers).
	stop_task = asyncio.create_task(stop.wait())
	
	# Wait for whichever happens first: a stop signal or server termination
	done, _ = await asyncio.wait({wait_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

	if stop_task in done:
		# External shutdown requested (Ctrl+C / SIGTERM)
		print("shutdown requested, stopping gRPC...")
		log.info("shutdown requested, stopping gRPC…")
		# Unblock any subscribers waiting on Hub queues; this helps active Subscribe RPCs finish.
		await hub.shutdown()
	
		# Ask gRPC to stop accepting new calls and gracefully drain existing ones for up to 3s.
		await server.stop(grace=3.0)

	# Await server termination task (ignore exceptions during final cleanup)
	with contextlib.suppress(Exception):
		await wait_task


if __name__ == "__main__":
	# Entrypoint: Run the server’s main function and allow KeyboardInterrupt to exit cleanly.
	try:
		args = parse_args()
		setup_logging(args.verbosity, log_file = _LOG_PATH_LOGGING or None, console = False)
		log = logging.getLogger("server")   # or "server"
		log.info("starting up…")
		if args.config:
			DEFAULT_POLICY['manifest'] = args.config
			log.info(f"set config file to {args.config}")
		try:
			dest_config = os.path.join(_LOG_PATH_LOGGING, f"config_{_START_STR}.json5")
			print(_LOG_PATH_LOGGING_DIR)
			shutil.copyfile(DEFAULT_POLICY['manifest'], dest_config)
			log.info(f"file '{DEFAULT_POLICY['manifest']}' copied to '{dest_config}' successfully.")
		except FileNotFoundError:
			log.error(f"[server] source file '{DEFAULT_POLICY['manifest']}' not found.")
		asyncio.run(serve(args.ip))
	except KeyboardInterrupt:
		pass