#!/usr/bin/env python3
"""
fake_agent.py
=============
Simulated GNSS agent for end-to-end testing of server_v1.py without a
physical u-blox receiver.

Two modes (--mode):
  base     — generates fake RTCM3 frames and publishes them to the server
             via Caster.Publish; also sends fake UBX telemetry via Control.Pipe
  receiver — subscribes to RTCM3 frames from the server via Caster.Subscribe;
             also sends fake UBX telemetry via Control.Pipe

Both modes perform the full Control.Pipe handshake:
  1. Send DeviceHello  →  server decides role
  2. Receive HelloAck  →  extract role / mount / token
  3. Handle CfgSet     →  acknowledge (ok=True) without real hardware
  4. Handle Ping       →  log only (no response required)
  5. Send ~1 Hz Telemetry snapshots via ControlMsg(telem=...)

Telemetry data flows:  fake_agent → Control.Pipe → server_v1 → TELEM_FWD_Q
  → telem_forwarder_loop → Telemetry.ReportStatus → Redis

Usage:
  python fake_agent.py --device-id FAKE_BASE_001 --mode base \\
                       --ctrl-addr gnss_server:50051 --run-secs 30
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import grpc
import grpc.aio
from pyubx2 import UBXReader

# ── path setup so bare `import caster_setup_pb2` resolves ───────────────────
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import caster_setup_pb2 as pb
import caster_setup_pb2_grpc as rpc
import fake_gnss_generator as gen

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("fake_agent")


# ── telemetry aggregator (minimal copy of agent_v1.TelemetryAgg) ─────────────
class TelemetryAgg:
    """Accumulates telemetry from fake UBX frames."""

    __slots__ = (
        "qerr_ps", "utc_ok", "temp_c",
        "num_vis", "num_used",
        "gps_used", "gal_used", "bds_used", "glo_used",
        "avg_cno", "pdop",
    )

    def __init__(self) -> None:
        self.qerr_ps = 0
        self.utc_ok  = False
        self.temp_c  = 25.0
        self.num_vis = self.num_used = 0
        self.gps_used = self.gal_used = self.bds_used = self.glo_used = 0
        self.avg_cno = 0.0
        self.pdop    = 0.0

    def feed_ubx(self, frame: bytes) -> None:
        try:
            _, msg = UBXReader(io.BytesIO(frame)).read()
        except Exception:
            return
        ident = getattr(msg, "identity", "")
        if ident == "TIM-TP":
            self.qerr_ps = int(getattr(msg, "qErr", 0) or 0)
            self.utc_ok  = bool(getattr(msg, "utc", 0))
        elif ident == "MON-SYS":
            tv = getattr(msg, "tempValue", None)
            if tv is not None:
                self.temp_c = float(tv)
        elif ident == "NAV-DOP":
            pd = getattr(msg, "pDOP", None)
            if pd is not None:
                self.pdop = float(pd)
        elif ident == "NAV-TIMEUTC":
            self.utc_ok = bool(getattr(msg, "validUTC", 0))
        elif ident == "NAV-SAT":
            payload = frame[6:-2]
            if len(payload) < 8:
                return
            numSvs = payload[5]
            n = min(int(numSvs), max(0, (len(payload) - 8) // 12))
            used = 0; cno_sum = 0.0; gps = gal = bds = glo = 0
            off = 8
            for _ in range(n):
                gnssId = payload[off]
                cno    = payload[off + 2]
                flags  = int.from_bytes(payload[off + 8:off + 12], "little")
                cno_sum += float(cno)
                if flags & 0x08:
                    used += 1
                    if gnssId == 0:   gps += 1
                    elif gnssId == 2: gal += 1
                    elif gnssId == 3: bds += 1
                    elif gnssId == 6: glo += 1
                off += 12
            self.num_vis  = n
            self.num_used = used
            self.gps_used, self.gal_used, self.bds_used, self.glo_used = (
                gps, gal, bds, glo
            )
            self.avg_cno = (cno_sum / n) if n else 0.0


# ── gRPC stream helpers ─────────────────────────────────────────────────────

async def rx_loop(
    call: grpc.aio.StreamStreamCall,
    out_q: asyncio.Queue,
    stop: asyncio.Event,
    device_id: str,
) -> None:
    """Read ControlMsg messages from the server (response stream).

    - Ping    → log (agent doesn't need to respond)
    - CfgSet  → enqueue ApplyResult(ok=True)
    - EOF     → exit
    """
    try:
        async for msg in call:
            if stop.is_set():
                break
            if msg.HasField("ping"):
                log.debug("[%s] Ping ts=%d", device_id, msg.ping.unix_ms)
            elif msg.HasField("cfgset"):
                log.info(
                    "[%s] CfgSet v%d received (%d items) — acknowledging",
                    device_id, msg.cfgset.version, len(msg.cfgset.items),
                )
                await out_q.put(pb.ControlMsg(
                    result=pb.ApplyResult(name="CfgSet", ok=True)
                ))
            elif msg.HasField("ack"):
                log.debug("[%s] unexpected duplicate ACK", device_id)
            else:
                log.debug("[%s] rx: %s", device_id, msg.WhichOneof("body"))
    except grpc.aio.AioRpcError as e:
        log.warning("[%s] rx_loop gRPC error: %s %s", device_id,
                    e.code().name, e.details())
    except Exception as e:
        log.warning("[%s] rx_loop error: %s", device_id, e)
    finally:
        log.info("[%s] rx_loop finished", device_id)


async def telem_loop(
    out_q: asyncio.Queue,
    stop: asyncio.Event,
    device_id: str,
    period: float = 1.0,
) -> None:
    """Generate fake UBX telemetry and emit ControlMsg(telem=...) every ~1 s."""
    agg = TelemetryAgg()
    scenario = gen.scenario_good_fix(qerr_ps=2000, temp_c=38.5, pdop=1.4, num_sats=10)
    # Feed all frames once so the first snapshot has complete data
    for frame in scenario:
        agg.feed_ubx(frame)
    frame_idx = len(scenario)
    try:
        while not stop.is_set():
            t = pb.Telemetry(
                unix_ms  = int(time.time() * 1000),
                temp_c   = agg.temp_c,
                qerr_ns  = round(agg.qerr_ps / 1000.0, 3),
                utc_ok   = agg.utc_ok,
                num_vis  = agg.num_vis,
                num_used = agg.num_used,
                gps_used = agg.gps_used,
                gal_used = agg.gal_used,
                bds_used = agg.bds_used,
                glo_used = agg.glo_used,
                avg_cno  = agg.avg_cno,
                pdop     = agg.pdop,
            )
            await out_q.put(pb.ControlMsg(telem=t))
            log.debug("[%s] telem → qerr_ns=%.1f utc_ok=%s num_used=%d",
                      device_id, t.qerr_ns, t.utc_ok, t.num_used)
            try:
                await asyncio.wait_for(stop.wait(), timeout=period)
            except asyncio.TimeoutError:
                pass
    except Exception as e:
        log.warning("[%s] telem_loop error: %s", device_id, e)


# ── role tasks ────────────────────────────────────────────────────────────────

async def publish_loop(
    cast_addr: str,
    mount: str,
    token: str,
    stop: asyncio.Event,
    device_id: str,
    rtcm_hz: float = 1.0,
) -> None:
    """BASE role: generate RTCM3 frames and publish to server via Caster.Publish.

    Generates one burst of RTCM message types [1005, 1074, 1084, 1094, 1230]
    per second, matching a typical base station output.
    """
    opts = [("grpc.keepalive_time_ms", 30000)]
    rtcm_types = [1005, 1074, 1084, 1094, 1230]
    seq = 0

    log.info("[%s] BASE: connecting to caster at %s mount=%s", device_id, cast_addr, mount)
    async with grpc.aio.insecure_channel(cast_addr, options=opts) as ch:
        caster = rpc.CasterStub(ch)
        call = caster.Publish()

        # Open the publish stream
        await call.write(pb.PublishMsg(
            open=pb.PublishOpen(mount=mount, token=token, label="fake-base"),
        ))

        try:
            while not stop.is_set():
                for msg_num in rtcm_types:
                    if stop.is_set():
                        break
                    frame = gen.make_rtcm_frame(msg_num)
                    await call.write(pb.PublishMsg(
                        frame=pb.RtcmFrame(
                            data=frame,
                            seq=seq,
                            unix_ms=int(time.time() * 1000),
                        )
                    ))
                    seq += 1
                if seq % 50 == 0:
                    log.info("[%s] BASE published %d RTCM frames", device_id, seq)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1.0 / rtcm_hz)
                except asyncio.TimeoutError:
                    pass
        finally:
            with contextlib.suppress(Exception):
                await call.done_writing()
            with contextlib.suppress(Exception):
                ack = await call
                log.info("[%s] BASE done, server counted %d frames", device_id, ack.frames)


async def subscribe_loop(
    cast_addr: str,
    mount: str,
    token: str,
    stop: asyncio.Event,
    device_id: str,
) -> None:
    """RECEIVER role: subscribe to RTCM3 from server and count received frames.

    Since there is no real serial device, received frames are discarded after
    counting them.
    """
    opts = [("grpc.keepalive_time_ms", 30000)]
    total = 0
    log.info("[%s] RECEIVER: subscribing at %s mount=%s", device_id, cast_addr, mount)

    async with grpc.aio.insecure_channel(cast_addr, options=opts) as ch:
        caster = rpc.CasterStub(ch)
        call = caster.Subscribe(pb.SubscribeRequest(
            mount=mount, token=token, label="fake-receiver",
        ))

        async def _drain() -> None:
            nonlocal total
            async for _ in call:
                total += 1
                if total % 25 == 0:
                    log.info("[%s] RECEIVER received %d RTCM frames", device_id, total)
                if stop.is_set():
                    break

        drain_task = asyncio.create_task(_drain())
        stop_task  = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            {drain_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t

    log.info("[%s] RECEIVER done, total frames=%d", device_id, total)


# ── main handshake + orchestration ────────────────────────────────────────────

async def run_agent(
    device_id: str,
    ctrl_addr: str,
    cast_addr: str,
    run_secs: float,
    wait_for_server: float = 30.0,
) -> None:
    """Connect to server_v1, handshake, then run role + telemetry tasks.

    Uses a request async-iterator (instead of call.write) to keep the
    Control.Pipe send-side open for the lifetime of the agent.
    """
    opts = [
        ("grpc.keepalive_time_ms",          30_000),
        ("grpc.keepalive_timeout_ms",        10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.initial_reconnect_backoff_ms",  500),
        ("grpc.max_reconnect_backoff_ms",     5_000),
    ]

    # ── Wait for server to be reachable ──────────────────────────────────────
    deadline = time.monotonic() + wait_for_server
    while time.monotonic() < deadline:
        try:
            ch_test = grpc.aio.insecure_channel(ctrl_addr)
            await asyncio.wait_for(ch_test.channel_ready(), timeout=2.0)
            await ch_test.close()
            break
        except Exception:
            log.info("[%s] waiting for server at %s …", device_id, ctrl_addr)
            await asyncio.sleep(2.0)
    else:
        raise TimeoutError(f"Server {ctrl_addr} not reachable after {wait_for_server}s")

    log.info("[%s] server ready, opening Control.Pipe", device_id)

    stop  = asyncio.Event()
    out_q: asyncio.Queue = asyncio.Queue(maxsize=300)

    # ── Build the request async-iterator ─────────────────────────────────────
    # The iterator first yields HELLO, then drains out_q for telem / results.
    # It stays alive until stop is set and the queue is empty (or None sentinel).
    async def request_iter():
        # 1. Send HELLO
        yield pb.ControlMsg(hello=pb.DeviceHello(
            device_id = device_id,
            model     = "ZED-F9T",
            fwver     = "TIM 2.20",
            protver   = "29.20",
            hwver     = "00190000",
            label     = "fake-agent",
        ))
        log.info("[%s] HELLO sent", device_id)

        # 2. Drain out_q (telem messages, CfgSet acks, etc.)
        while not (stop.is_set() and out_q.empty()):
            try:
                msg = await asyncio.wait_for(out_q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if msg is None:
                break
            yield msg

    async with grpc.aio.insecure_channel(ctrl_addr, options=opts) as ch:
        stub = rpc.ControlStub(ch)
        call = stub.Pipe(request_iter())

        # ── 1. Receive HelloAck (first message from server) ──────────────
        msg = await call.read()
        if msg is grpc.aio.EOF or not msg.HasField("ack"):
            log.error("[%s] expected HelloAck, got %s — aborting",
                      device_id,
                      msg.WhichOneof("body") if msg is not grpc.aio.EOF else "EOF")
            stop.set()
            await out_q.put(None)
            return

        ack   = msg.ack
        role  = ack.role
        mount = ack.mount
        token = ack.token
        alias = ack.alias
        log.info(
            "[%s] ACK  role=%s  mount=%s  alias=%s  cfg_ver=%d",
            device_id,
            "BASE" if role == pb.Role.BASE else "RECEIVER",
            mount, alias, ack.config_version,
        )

        # ── 2. Run concurrent tasks ──────────────────────────────────────
        role_coro = (
            publish_loop(cast_addr, mount, token, stop, device_id)
            if role == pb.Role.BASE
            else subscribe_loop(cast_addr, mount, token, stop, device_id)
        )

        tasks = [
            asyncio.create_task(rx_loop(call, out_q, stop, device_id)),
            asyncio.create_task(telem_loop(out_q, stop, device_id)),
            asyncio.create_task(role_coro),
        ]

        # Run for run_secs, then signal stop
        await asyncio.sleep(run_secs)
        log.info("[%s] run_secs=%s elapsed — shutting down", device_id, run_secs)
        stop.set()
        await out_q.put(None)   # unblock request_iter if it is waiting

        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("[%s] agent finished cleanly", device_id)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fake GNSS agent for testing server_v1")
    p.add_argument("--device-id",  required=True,
                   help="Device ID (must match manifest entry)")
    p.add_argument("--ctrl-addr",  default="localhost:50051",
                   help="Control (and Caster if --cast-addr omitted) gRPC address")
    p.add_argument("--cast-addr",  default=None,
                   help="Caster gRPC address (defaults to --ctrl-addr)")
    p.add_argument("--run-secs",   type=float, default=30.0,
                   help="Seconds to run before clean exit (default: 30)")
    p.add_argument("--wait-secs",  type=float, default=30.0,
                   help="Seconds to wait for server before giving up (default: 30)")
    p.add_argument("-v", "--verbosity", type=int, default=2,
                   choices=[0, 1, 2, 3],
                   help="0=ERROR 1=WARNING 2=INFO 3=DEBUG")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    levels = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO, 3: logging.DEBUG}
    logging.getLogger().setLevel(levels[args.verbosity])

    cast_addr = args.cast_addr or args.ctrl_addr
    asyncio.run(run_agent(
        device_id   = args.device_id,
        ctrl_addr   = args.ctrl_addr,
        cast_addr   = cast_addr,
        run_secs    = args.run_secs,
        wait_for_server = args.wait_secs,
    ))
