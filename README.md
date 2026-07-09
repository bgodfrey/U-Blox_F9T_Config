# U-Blox_F9T_Config

## Introduction

This repository contains scripts for configuring and operating a network of
u-blox ZED-F9T timing receivers for PANOSETI. The normal deployment model is a
headnode running a GNSS server and one GNSS agent per DAQ node. The agents talk
to local F9T receivers over USB serial and communicate with the server over
gRPC.

![Sparkfun Zed-F9T](docs/img/Sparkfun_F9T.png 'Sparkfun Zed-F9T receiver')

*U-blox F9T receiver available from
[Sparkfun](https://web.archive.org/web/20250814191842/https://www.sparkfun.com/sparkfun-gnss-timing-breakout-zed-f9t-qwiic.html).*

The ZED-F9T is useful for this work because it provides:

- Multi-band GNSS support for GPS, Galileo, BeiDou, and GLONASS.
- Timing-focused UBX messages, including `UBX-TIM-TP` quantization error
  (`qerr`) corrections for the 1 PPS output.
- Configurable time-pulse outputs.
- Optional RTCM input/output for differential timing operation.
- USB operation from a DAQ node.

PANOSETI is using these receivers as a practical, lower-cost timing system for
deployment cases where fiber-based timing systems such as White Rabbit are not
required or are not practical.

This repository relies heavily on
[pyubx2](https://github.com/semuconsulting/pyubx2) for reading and writing UBX
messages.

## Current Operating Model

PANOSETI consists of a headnode and a number of data acquisition (DAQ) nodes.
Each telescope/dome has a single DAQ node. Science data are collected by each
DAQ node and then aggregated onto the headnode at the end of a data acquisition
period. That design minimizes extra compute and I/O pressure on the DAQ nodes
during an active run, where bottlenecks or packet loss need to be avoided. The
GNSS receiver software follows the same principle: the DAQ nodes run lightweight
agents, while coordination, routing, and operator control happen from the
headnode.

![Basic Design Setup](docs/img/F9T_BasicCodeSetup.png 'Basic Design Setup')

*Basic setup of the code where the headnode serves as an intermediary between a
base and some number of receivers.*

The diagram shows the differential timing architecture. Each DAQ node has a
local F9T receiver and runs an agent. The headnode runs the GNSS server. A
receiver can be configured as a `base`, a `receiver`, or, in absolute timing
mode, `timing_only`. In differential mode, a base agent streams RTCM correction
frames back to the headnode; the headnode then forwards those corrections to
receiver agents subscribed to the same mount. The receiver register settings and
role assignments live in the manifest files, while machine-specific launch
details live in the deployment config.

This separation matters: the local configuration script can write receiver
registers, but it does not describe or start the runtime communication pattern
between receivers. The orchestrated gRPC setup is what makes the receivers talk
to each other through the headnode and provides the normal logging and telemetry
path.

The intended way to operate the system is through:

```text
gnss_scripts/gnss_orchestrator.py
```

The orchestrator reads a deployment config, starts and stops the local GNSS
server and local or remote agents, checks node readiness, optionally configures
Leo Bodnar LBE-1420
10 MHz references, and can verify receiver registers against a selected
manifest.

The main files are:

- `gnss_scripts/gnss_deployment.json5`: deployment config. This describes the
  headnode, DAQ nodes, SSH hosts, Python environments, repo locations, log
  directories, telemetry directories, verbosity, and optional Bodnar settings.
- `manifest_f9t.json5`: receiver register manifest for differential timing.
- `manifest_f9t_absolute.json5`: receiver register manifest for absolute
  timing.
- `gnss_scripts/gnss_orchestrator.py`: operator-facing script used to check,
  start, stop, and manage the deployment.
- `server_v1.py`: headnode gRPC server.
- `agent_v1.py`: DAQ-node agent that talks to the local F9T receiver.
- `conf_gnss_local.py`: one-off local receiver configuration script for cases
  where you want to write and verify receiver settings without starting the
  gRPC server/agent machinery.

The orchestrator sits above the receiver manifests. The deployment config says
where and how to run things; the manifests say what register settings should be
applied to receivers.

For a more detailed operator guide with additional examples, see
[`gnss_scripts/gnss_orchestrator_guide.md`](gnss_scripts/gnss_orchestrator_guide.md).

## Timing Modes

### Differential Mode

Differential mode is the normal base/receiver timing mode.

In differential mode:

- The server is launched with `--timing-mode differential`.
- The default receiver manifest is `manifest_f9t.json5`.
- Receivers use their manifest roles, typically `base` or `receiver`.
- Base nodes can publish RTCM correction frames to the server.
- Receiver nodes can subscribe to RTCM correction frames from the server.
- Telemetry is still collected from all nodes.

Example:

```bash
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --mode differential
```

### Absolute Timing Mode

Absolute timing mode is for receivers that should provide timing without RTCM
exchange.

In absolute mode:

- The server is launched with `--timing-mode absolute`.
- The default receiver manifest is `manifest_f9t_absolute.json5`.
- Receivers should use the `timing_only` role.
- Agents configure receivers and publish telemetry, but do not publish or
  subscribe to RTCM messages.
- The server does not forward RTCM correction streams.

Example:

```bash
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --mode absolute
```

## Running With the GNSS Orchestrator

Run the orchestrator from the directory containing `gnss_deployment.json5`, or
pass `--config` explicitly. By default, it looks for:

```text
./gnss_deployment.json5
```

Examples from the top-level repo directory should pass the config path:

```bash
python gnss_scripts/gnss_orchestrator.py --config gnss_scripts/gnss_deployment.json5 status
python gnss_scripts/gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5
```

All commands and subcommands have help text:

```bash
python gnss_scripts/gnss_orchestrator.py --help
python gnss_scripts/gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5 --help
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --help
python gnss_scripts/gnss_orchestrator.py stop --config gnss_scripts/gnss_deployment.json5 --help
```

### Status Checks

Check all present nodes:

```bash
python gnss_scripts/gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5
```

Check one node:

```bash
python gnss_scripts/gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5 --node WINTERS
```

Emit JSON:

```bash
python gnss_scripts/gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5 --json
```

Verify receiver registers against the selected manifest:

```bash
python gnss_scripts/gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5 --node WINTERS --mode differential --verify-registers
python gnss_scripts/gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5 --node WINTERS --mode absolute --verify-registers
```

Do not use `--verify-registers` during an active data acquisition run. Register
verification opens the receiver serial device and can temporarily disrupt the
running agent's telemetry reads. Use plain `status` during live runs and reserve
register verification for before `start`, after `stop`, or controlled
debugging.

### Start

Preview launch commands without changing anything:

```bash
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --dry-run
```

Start all present nodes in differential mode:

```bash
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --mode differential
```

Start one node:

```bash
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --node WINTERS
```

Configure Leo Bodnar LBE-1420 devices before starting agents:

```bash
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --bodnar
python gnss_scripts/gnss_orchestrator.py start --config gnss_scripts/gnss_deployment.json5 --node WINTERS --bodnar
```

The Bodnar behavior is controlled by the `bodnar` sections in
`gnss_deployment.json5`. A node can be marked as not present, required or
optional, and configured with output frequency, GNSS constellation setting, and
OUT1 enabled/disabled state.

### Stop

Stop all agents and the server:

```bash
python gnss_scripts/gnss_orchestrator.py stop --config gnss_scripts/gnss_deployment.json5
```

Stop one node:

```bash
python gnss_scripts/gnss_orchestrator.py stop --config gnss_scripts/gnss_deployment.json5 --node WINTERS
```

Stop only agents or only the server:

```bash
python gnss_scripts/gnss_orchestrator.py stop --config gnss_scripts/gnss_deployment.json5 --agents-only
python gnss_scripts/gnss_orchestrator.py stop --config gnss_scripts/gnss_deployment.json5 --server-only
```

Compress completed log and telemetry files after stopping:

```bash
python gnss_scripts/gnss_orchestrator.py stop --config gnss_scripts/gnss_deployment.json5 --compress-logs
```

This runs `gzip -9` on completed `.log`, `.txt`, and `.jsonl` files in the
configured log and telemetry directories. Existing `.gz` files are left alone.

## Deployment Config

The deployment config is separate from the receiver manifests. It is the file
that makes the system scalable to many DAQ nodes.

Important sections:

- `defaults`: shared values inherited by nodes unless overridden.
- `modes`: maps `differential` and `absolute` to the corresponding receiver
  manifests.
- `server`: local headnode server settings. The orchestrator always launches
  this process locally; `bind_addr` controls whether remote agents can connect.
- `nodes`: local or remote DAQ-node settings.

Common settings include:

- `daq_name`: human-readable site or DAQ label, such as `WINTERS`.
- `local`: set to `true` for direct execution on the orchestration host; false
  or omitted uses SSH.
- `host`: SSH hostname for a remote node, such as `panoseti-winter`.
- `ssh_user`: remote user for SSH; not required for a local node.
- `python`: Python executable or conda-environment Python to run.
- `repo`: location of this repo on that host.
- `agent_script` / `server_script`: script paths relative to `repo` unless
  absolute.
- `cast_addr` and `ctrl_addr`: gRPC addresses used by agents to reach the
  server.
- `verbosity`: script logging verbosity.
- `logdir`: process log directory.
- `telem_dir`: local telemetry JSONL directory.
- `telemetry.max_file_mb`: rotate active telemetry files at this approximate
  size; set to `0` or less to disable size rotation.
- `telemetry.fsync_seconds`: flush every JSONL record and call `fsync` at most
  this often; set to `0` or less to skip explicit `fsync`.
- `present`: whether the node is normally included by orchestrator commands.
- `bodnar`: optional LBE-1420 configuration.

Different hosts can use different repo paths, Python environments, log
directories, and telemetry directories. This is expected.

### Local Agents

The GNSS server always runs on the machine where `gnss_orchestrator.py` is
invoked. A node can run its agent on that same machine by setting:

```json5
"local": true
```

Local nodes do not require `host` or `ssh_user`. Status checks, receiver
discovery, register verification, Bodnar configuration, agent start/stop, and
log compression are run directly instead of through SSH.

Example of a fully local server and receiver deployment:

```json5
"server": {
  "daq_name": "RAL",
  "python": "/home/bgodfrey/miniconda3/envs/py314/bin/python",
  "repo": "/home/bgodfrey/U-Blox_F9T_Config",
  "script": "server_v1.py",
  "logdir": "/home/bgodfrey/gnss_logging",
  "telem_dir": "/home/bgodfrey/gnss_telem",
  "screen": "gnss_server",
  "bind_addr": "127.0.0.1:50054",
  "verbosity": 2
},

"nodes": {
  "ral": {
    "daq_name": "RAL",
    "local": true,
    "present": true,
    "required": false,
    "python": "/home/bgodfrey/miniconda3/envs/py314/bin/python",
    "repo": "/home/bgodfrey/U-Blox_F9T_Config",
    "agent_script": "agent_v1.py",
    "find_ublox_script": "gnss_scripts/find_ublox.sh",
    "logdir": "/home/bgodfrey/gnss_logging",
    "telem_dir": "/home/bgodfrey/gnss_telem",
    "cast_addr": "127.0.0.1:50054",
    "ctrl_addr": "127.0.0.1:50054",
    "verbosity": 2
  }
}
```

The server `bind_addr` and the local agent's `cast_addr` and `ctrl_addr` must
use the same port. Binding the server to `127.0.0.1` restricts it to local
agents. Use `0.0.0.0` when remote agents must also connect.

Setting `local: true` does not mean "run on the machine named by `host`." It
always means "run on the current orchestration host." If the agent belongs on
another machine, leave `local` false or omit it and configure `host` and
`ssh_user` for SSH.

## Receiver Manifests

Receiver register settings live in JSON5 manifest files. The current main
manifests are:

- `manifest_f9t.json5` for differential timing.
- `manifest_f9t_absolute.json5` for absolute timing.

The manifest controls global register settings and role-specific register
settings.

Useful role concepts:

- `base`: outputs RTCM corrections and provides telemetry.
- `receiver`: receives RTCM corrections and provides telemetry.
- `timing_only`: provides timing and telemetry without RTCM publish/subscribe.

Receiver identity is based on the u-blox unique hardware ID from
`UBX-SEC-UNIQID`. Site aliases in the manifest make telemetry and logs easier to
read than raw hardware IDs.

## One-Off Local Receiver Configuration

For standalone receiver setup or bench testing, use:

```text
conf_gnss_local.py
```

This script applies receiver register settings directly over the local serial
port. It can read the same manifest-style JSON5 files used by the orchestrated
system, select a device by UID or alias, write the selected register plan, and
verify the requested layer.

Examples:

```bash
python conf_gnss_local.py manifest_f9t.json5 --list-devices
python conf_gnss_local.py manifest_f9t.json5 --alias WINTERS --dry-run
python conf_gnss_local.py manifest_f9t.json5 --alias WINTERS --verify-layer RAM
python conf_gnss_local.py manifest_f9t.json5 --auto-uid --port /dev/ttyACM0 --verify-layer RAM
```

Use this path when you only want to configure the attached receiver. It does not
start `server_v1.py`, does not start `agent_v1.py`, does not open any gRPC
control or caster streams, and does not provide the normal local or remote
telemetry logging used during a GNSS run.

For operations where telemetry, server-side logging, RTCM routing, start/stop
control, or multi-node management matter, use `gnss_orchestrator.py` instead.

## Telemetry

Telemetry is written as JSON Lines (`.jsonl`). Each line is valid JSON and can
be read with Python's built-in `json.loads`. Files may later be compressed with
gzip; the content remains line-oriented JSON after decompression.

While a telemetry file is actively being written, it uses a `.jsonl.active`
suffix. On clean shutdown or rotation, it is finalized to `.jsonl`. This lets
background compression skip live files while still making completed telemetry
segments easy to copy, compress, and inspect.

There are two telemetry locations:

- Agent-local telemetry: one file per agent run, usually named with the site or
  receiver ID and a UTC start timestamp.
- Server telemetry: one file per server run, usually named
  `telemetry_<UTC>.jsonl`, containing records from all connected nodes.

The server telemetry rows add:

- `ts`: server write time in Unix milliseconds.
- `kind`: currently `telem` for telemetry rows.
- `device_id`: receiver hardware ID.
- `alias`: site alias or DAQ name.

Agent-local rows also include `ts`; in the current agent implementation this is
the same receiver-derived millisecond timestamp used for `unix_ms`.

The telemetry payload fields are:

- `unix_ms`: receiver-derived telemetry timestamp in Unix milliseconds.
- `temp_c`: receiver temperature from `UBX-MON-SYS`, in degrees Celsius.
- `qerr_ns`: time-pulse quantization error from `UBX-TIM-TP`, in nanoseconds.
  This is set to `null` when `qerr_valid` is false.
- `qerr_valid`: true when the most recent `UBX-TIM-TP` data is fresh.
- `qerr_age_ms`: age of the latest `UBX-TIM-TP` update in milliseconds.
- `nav_sat_valid`: true when the most recent `UBX-NAV-SAT` data is fresh.
- `nav_sat_age_ms`: age of the latest `UBX-NAV-SAT` update in milliseconds.
- `telemetry_stale`: true when either qerr data or NAV-SAT data is stale.
- `utc_ok`: UTC validity flag derived from receiver timing messages.
- `num_vis`: total satellites visible in `UBX-NAV-SAT`.
- `num_used`: satellites marked as used in the current solution.
- `gps_used`: used GPS satellites (`gnssId=0`).
- `gal_used`: used Galileo satellites (`gnssId=2`).
- `bds_used`: used BeiDou satellites (`gnssId=3`).
- `glo_used`: used GLONASS satellites (`gnssId=6`).
- `avg_cno`: mean carrier-to-noise density ratio across NAV-SAT entries, in
  dB-Hz.
- `pdop`: position dilution of precision from `UBX-NAV-DOP`. In fixed-position
  timing mode this may not be a useful health metric.

Freshness defaults:

- At agent startup, before the first `UBX-TIM-TP` and `UBX-NAV-SAT` messages
  have been read, telemetry starts stale: `qerr_valid: false`,
  `nav_sat_valid: false`, and `telemetry_stale: true`.
- In agent-local JSONL, `qerr_age_ms` and `nav_sat_age_ms` are `null` until the
  corresponding message has been seen at least once.
- In protobuf/server telemetry, missing ages are encoded as `0`, but the
  validity flags still indicate whether the data is actually fresh.
- `qerr_valid` becomes false if the latest `UBX-TIM-TP` update is older than
  3000 ms.
- `nav_sat_valid` becomes false if the latest `UBX-NAV-SAT` update is older
  than 10000 ms.
- `telemetry_stale` is true whenever either `qerr_valid` or `nav_sat_valid` is
  false.

When the agent is run at debug verbosity (`-v 3`), agent-local telemetry can
also include extra diagnostic fields:

- `fix_type`: `UBX-NAV-PVT.fixType`.
- `gnss_fix_ok`: receiver fix-valid flag.
- `diff_soln`: true when differential corrections are being used.
- `carr_soln`: carrier-phase solution status.
- `confirmed_date`: receiver-confirmed date flag.
- `confirmed_time`: receiver-confirmed time flag.
- `nav_pvt_num_sv`: number of satellites used according to `UBX-NAV-PVT`.
- `nav_pvt_lat_deg`, `nav_pvt_lon_deg`: receiver latitude and longitude.
- `nav_pvt_height_m`, `nav_pvt_hmsl_m`: ellipsoid and mean-sea-level heights.
- `nav_pvt_hacc_m`, `nav_pvt_vacc_m`: horizontal and vertical accuracy.
- `nav_sat_top`: top NAV-SAT entries sorted by C/N0. Each entry includes
  `gnssId`, `svId`, `cno`, `elev`, `azim`, `qualityInd`, `health`, and
  `svUsed`.

For timing quality, the most important fields are usually `unix_ms`, `qerr_ns`,
`qerr_valid`, `qerr_age_ms`, `utc_ok`, `num_used`, `avg_cno`, and
`telemetry_stale`.

## Manual Script Usage

The orchestrator is the preferred way to run the deployment. Direct script usage
is mainly useful for debugging.

Install dependencies in the relevant conda environment:

```bash
pip install -r requirements.txt
```

Regenerate gRPC files only when `caster_setup.proto` changes:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. caster_setup.proto
```

Manual server example:

```bash
python server_v1.py \
  --config manifest_f9t.json5 \
  --ip 0.0.0.0:50051 \
  --timing-mode differential \
  --telem-dir telemetry \
  -v 2
```

Manual agent example:

```bash
python agent_v1.py \
  --cast_addr 10.200.146.1:50051 \
  --ctrl_addr 10.200.146.1:50051 \
  --log-dir gnss_logging \
  --telem-dir gnss_telem \
  -v 2
```

Use `--help` on each script for current options.

## Operational Notes

- Avoid `status --verify-registers` during active data acquisition because it
  reads the same serial receiver used by the running agent.
- Plain `status` is intended to be non-disruptive and checks configuration,
  paths, SSH reachability, receiver detection, and optional Bodnar status.
- Verbosity affects process logging detail. The compact telemetry stream is not
  meant to depend on high verbosity, though debug verbosity adds extra
  agent-local diagnostics.
- Data acquisition should take precedence over GNSS diagnostics. During live
  runs, keep checks lightweight and avoid actions that open the receiver serial
  port outside the running agent.
