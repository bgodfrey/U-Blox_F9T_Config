# GNSS Orchestrator Guide

This document describes how to use `gnss_scripts/gnss_orchestrator.py` to check,
start, stop, and install persistent GNSS services across DAQ nodes.

The orchestrator is meant to answer three practical questions:

1. Is the GNSS deployment configured correctly?
2. Are the local and remote DAQ nodes reachable and ready?
3. Can I start, stop, and verify the GNSS server/agents from one place?

It does not replace the receiver manifest files. Instead, it sits one layer
above them.

- `manifest_f9t.json5` and `manifest_f9t_absolute.json5` describe receiver
  register settings.
- `gnss_scripts/gnss_deployment.json5` describes where the DAQ nodes are, which
  Python environment to use, where the repo lives on each machine, where logs
  should go, and how to launch each process.
- `gnss_scripts/gnss_orchestrator.py` uses the deployment config to run status,
  start, and stop actions.

The orchestrator can also manage the temporary Leo Bodnar LBE-1420 10 MHz
reference setup. Bodnar configuration is optional and is enabled explicitly with
`start --bodnar`.

## Quick Reference

Run these commands from the directory containing the deployment config, or pass
`--config` explicitly.

All commands and subcommands have built-in help:

```bash
python gnss_orchestrator.py --help
python gnss_orchestrator.py status --help
python gnss_orchestrator.py start --help
python gnss_orchestrator.py stop --help
python gnss_orchestrator.py install-service --help
```

Use a non-default deployment config:

```bash
python gnss_orchestrator.py --config /path/to/gnss_deployment.json5 status
python gnss_orchestrator.py status --config /path/to/gnss_deployment.json5
```

By default, the orchestrator looks for `./gnss_deployment.json5` in the current
working directory.

Check all present nodes:

```bash
python gnss_orchestrator.py status
```

Check one node:

```bash
python gnss_orchestrator.py status --node WINTERS
```

Check one node and verify receiver registers:

```bash
python gnss_orchestrator.py status --node WINTERS --verify-registers
```

Check one node against the absolute timing manifest:

```bash
python gnss_orchestrator.py status --node WINTERS --mode absolute --verify-registers
```

Preview a start without changing anything:

```bash
python gnss_orchestrator.py start --dry-run
```

Start the server and all present agents in differential mode:

```bash
python gnss_orchestrator.py start --mode differential
```

Start the server and all present agents in absolute timing mode:

```bash
python gnss_orchestrator.py start --mode absolute
```

Start one node only:

```bash
python gnss_orchestrator.py start --node WINTERS
```

Preview a persistent agent service for one node:

```bash
python gnss_orchestrator.py install-service --node PTI --dry-run
```

Install and enable that agent service without starting it:

```bash
python gnss_orchestrator.py install-service --node PTI
```

Install the local server service in differential mode:

```bash
python gnss_orchestrator.py install-service --server --mode differential
```

Configure the Leo Bodnar and then start one node:

```bash
python gnss_orchestrator.py start --node WINTERS --bodnar
```

Stop one node only:

```bash
python gnss_orchestrator.py stop --node WINTERS
```

Stop all agents and the server:

```bash
python gnss_orchestrator.py stop
```

Emit JSON instead of human-readable output:

```bash
python gnss_orchestrator.py status --verify-registers --json
```

Do not use `--verify-registers` during an active data acquisition run. Register
verification opens the same receiver serial device as the running agent and can
temporarily disrupt UBX telemetry reads. Use plain `status --json` during live
runs, and reserve `--verify-registers` for before `start`, after `stop`, or
controlled debugging.

## Timing Modes

The orchestrator currently supports two GNSS timing modes:

- `differential`
- `absolute`

The mode controls which receiver manifest is used and how `server_v1.py` and
`agent_v1.py` are launched.

### Differential Mode

Differential mode is the normal base/receiver mode.

In this mode:

- The server starts with `--timing-mode differential`.
- The differential receiver manifest is used.
- The default manifest is `manifest_f9t.json5`.
- Devices use their manifest roles, usually `base` or `receiver`.
- RTCM publishing/subscribing is expected to be enabled where appropriate.
- A base receiver may output RTCM corrections.
- Receiver nodes may receive RTCM corrections.

Start in differential mode:

```bash
python gnss_orchestrator.py start --mode differential
```

Check register settings in differential mode:

```bash
python gnss_orchestrator.py status --mode differential --verify-registers
```

Example successful register verification:

```text
OK   register verify -- 60/60 matched role=base
```

That means the attached receiver was found in the selected manifest, its role
was resolved as `base`, and all 60 manifest-managed registers matched the live
receiver state.

### Absolute Timing Mode

Absolute timing mode is for receivers that should provide timing without
participating in RTCM correction exchange.

In this mode:

- The server starts with `--timing-mode absolute`.
- The absolute receiver manifest is used.
- The default manifest is `manifest_f9t_absolute.json5`.
- Devices should use the `timing_only` role.
- Agents should not publish RTCM messages.
- Agents should not subscribe to RTCM messages.
- The server should not forward RTCM correction streams.
- UBX messages and normal telemetry are still expected.

Start in absolute mode:

```bash
python gnss_orchestrator.py start --mode absolute
```

Check register settings in absolute mode:

```bash
python gnss_orchestrator.py status --mode absolute --verify-registers
```

Example expected failure when a receiver is still configured for differential
mode:

```text
FAIL register verify -- 53/62 matched role=timing_only; mismatches: CFG_USBOUTPROT_RTCM3X, CFG_MSGOUT_RTCM_3X_TYPE1005_USB, CFG_MSGOUT_RTCM_3X_TYPE1077_USB, +6 more
```

This is useful. It says the absolute manifest was selected, but the receiver
still has differential/RTCM settings enabled.

## Deployment Config

The orchestrator config is usually named:

```text
gnss_deployment.json5
```

This file is separate from the receiver manifests. It describes the machines and
process launch details, not the receiver register settings.

The default lookup is relative to the current working directory. If the config
is somewhere else, pass it explicitly:

```bash
python gnss_orchestrator.py --config gnss_scripts/gnss_deployment.json5 status
python gnss_orchestrator.py status --config gnss_scripts/gnss_deployment.json5
```

Important top-level sections:

- `defaults`: shared settings used by nodes unless they override them.
- `modes`: maps timing modes to receiver manifests.
- `server`: describes the GNSS server process, which always runs locally.
- `nodes`: describes local and remote DAQ nodes.

Bodnar settings can be placed in `defaults` and overridden per node. This is
useful because the four DAQ nodes may keep the Bodnar repo in different paths.

### Defaults

The `defaults` section includes common values such as:

```json5
{
  "ssh_user": "panoseti",
  "ssh_connect_timeout_sec": 3,
  "ssh_batch_mode": true,
  "accept_new_host_keys": true,
  "cast_addr": "10.200.146.1:50051",
  "ctrl_addr": "10.200.146.1:50051",
  "verbosity": 2,
  "agent_screen": "gnss_agent",
  "server_screen": "gnss_server",
  "shutdown_grace_sec": 5,
  "process": {
    "runner": "screen",
    "systemd": {
      "user_service": true,
      "enable_on_install": true,
      "check_linger": true,
      "restart": "always",
      "restart_sec": 10,
      "agent_service_name": "gnss-agent.service",
      "server_service_name": "gnss-server.service"
    }
  },
  "agent_script": "agent_v1.py",
  "server_script": "server_v1.py",
  "logdir": "logging",
  "telem_dir": "telem",
  "telemetry": {
    "max_file_mb": 128,
    "fsync_seconds": 5
  },
  "find_ublox_script": "gnss_scripts/find_ublox.sh",
  "bodnar": {
    "present": true,
    "required": false,
    "repo": "/home/panoseti/lbe1420_panoseti",
    "python": "/home/panoseti/miniconda3/envs/pygnss_312/bin/python",
    "configure_script": "lbe-1420-conf.py",
    "out1_enabled": true,
    "frequency_hz": 10000000,
    "gnss": "recommended"
  },
  "start_only_if_receiver_detected": true,
  "required": false
}
```

Node-specific entries can override these values.

Process settings:

- `process.runner`: `screen` or `systemd`. Defaults may be overridden by the
  server or any individual node.
- `enable_on_install`: enable generated units for boot startup. This does not
  immediately start a unit when using `install-service`.
- `check_linger`: require/report whether the user manager can run without a
  login session.
- `restart` and `restart_sec`: systemd restart policy after an unexpected
  process exit.
- `agent_service_name`, `server_service_name`, and per-target `service_name`:
  filenames used under `~/.config/systemd/user/`.

Telemetry rotation settings are passed through to `agent_v1.py` and
`server_v1.py` when the orchestrator starts them:

- `telemetry.max_file_mb`: rotate active telemetry files when they grow beyond
  this approximate size. Use `0` or less to disable size-based rotation.
- `telemetry.fsync_seconds`: write and flush every JSONL record, then call
  `fsync` at most this often. Use `0` or less to skip explicit `fsync`.

Active telemetry files use a `.jsonl.active` suffix. Clean shutdown and
rotation finalize them to `.jsonl`, which lets compression jobs ignore live
files and operate only on completed telemetry segments.

### Modes

The `modes` section maps the orchestrator mode to the receiver manifest:

```json5
"modes": {
  "differential": {
    "timing_mode": "differential",
    "manifest": "manifest_f9t.json5"
  },
  "absolute": {
    "timing_mode": "absolute",
    "manifest": "manifest_f9t_absolute.json5"
  }
}
```

When you run:

```bash
python gnss_orchestrator.py status --mode absolute --verify-registers
```

the orchestrator uses `manifest_f9t_absolute.json5` for register verification.

When you run:

```bash
python gnss_orchestrator.py start --mode absolute
```

the orchestrator starts the server and agents using absolute timing settings.

### Server Config

The `server` section describes `server_v1.py`. The orchestrator always runs
this process locally; it does not use SSH for server operations.

Example:

```json5
"server": {
  "daq_name": "panoseti-palomar",
  "python": "/home/obs/miniconda3/envs/pygnss_39/bin/python",
  "repo": "/home/obs/U-Blox_F9T_Config",
  "script": "server_v1.py",
  "logdir": "logging",
  "telem_dir": "telemetry",
  "screen": "gnss_server",
  "bind_addr": "0.0.0.0:50051",
  "verbosity": 2,
  "receiver_manifest": null
}
```

Important fields:

- `daq_name`: display name in status output.
- `python`: full path to the Python executable/environment.
- `repo`: repo location on the local orchestration host.
- `script`: server script path, usually `server_v1.py`.
- `logdir`: where server logs go.
- `telem_dir`: where server telemetry JSONL files go.
- `screen`: screen session name.
- `bind_addr`: gRPC bind address, for example `0.0.0.0:50051`. This controls
  which interfaces accept agent connections, not where the server runs.
- `verbosity`: passed to `server_v1.py` as `-v`.
- `receiver_manifest`: optional explicit receiver manifest.

### Node Config

The `nodes` section describes each local or remote DAQ node.

Example:

```json5
"winters": {
  "daq_name": "WINTERS",
  "local": false,
  "host": "panoseti-winter",
  "present": true,
  "required": false,
  "ssh_user": "panoseti",
  "python": "/home/panoseti/miniconda3/envs/pygnss_312/bin/python",
  "repo": "/home/panoseti/U-Blox_F9T_Config",
  "agent_script": "agent_v1.py",
  "find_ublox_script": "/home/panoseti/U-Blox_F9T_Config/gnss_scripts/find_ublox.sh",
  "logdir": "/home/panoseti/gnss_logging",
  "telem_dir": "/home/panoseti/gnss_telem",
  "cast_addr": "10.200.146.1:50051",
  "ctrl_addr": "10.200.146.1:50051",
  "verbosity": 2,
  "bodnar": {
    "present": true,
    "required": false,
    "repo": "/home/panoseti/lbe1420_panoseti",
    "python": "/home/panoseti/miniconda3/envs/pygnss_312/bin/python",
    "configure_script": "lbe-1420-conf.py",
    "out1_enabled": true,
    "frequency_hz": 10000000,
    "gnss": "recommended"
  }
}
```

Important fields:

- Node key, for example `winters`: short inventory key used by `--node`.
- `daq_name`: display name, for example `WINTERS`.
- `local`: set to `true` to execute directly on the orchestration host. It
  defaults to `false`, which uses SSH.
- `host`: SSH hostname, for example `panoseti-winter`.
- `present`: whether the node is included by default.
- `required`: whether status failures should make the CLI exit nonzero.
- `ssh_user`: SSH user for remote commands.
- `python`: full path to the Python executable/environment on that node.
- `repo`: repo location on that node.
- `agent_script`: agent script path, usually `agent_v1.py`.
- `find_ublox_script`: receiver detection script.
- `logdir`: where agent launch logs go.
- `telem_dir`: where GNSS telemetry files go.
- `cast_addr`: caster gRPC address passed to the agent.
- `ctrl_addr`: control gRPC address passed to the agent.
- `verbosity`: passed to `agent_v1.py` as `-v`.
- `bodnar`: optional Leo Bodnar LBE-1420 configuration for the temporary 10 MHz
  reference.

For a local agent, set `local: true` and omit `host` and `ssh_user`:

```json5
"headnode_receiver": {
  "daq_name": "PALOMAR",
  "local": true,
  "present": true,
  "python": "/home/obs/miniconda3/envs/pygnss_39/bin/python",
  "repo": "/home/obs/U-Blox_F9T_Config",
  "agent_script": "agent_v1.py",
  "find_ublox_script": "gnss_scripts/find_ublox.sh",
  "logdir": "/home/obs/gnss_logging",
  "telem_dir": "/home/obs/gnss_telem",
  "cast_addr": "127.0.0.1:50051",
  "ctrl_addr": "127.0.0.1:50051",
  "verbosity": 2
}
```

Status checks, receiver discovery, register verification, Bodnar configuration,
agent start/stop, and log compression all run directly for local nodes. The
same operations use SSH when `local` is false or omitted.

For remote nodes, the node key, `daq_name`, and `host` are related but
different:

- Key: stable config identifier, for example `winters`.
- `daq_name`: display/manifest-facing name, for example `WINTERS`.
- `host`: SSH target, for example `panoseti-winter`.

You can filter with any of these:

```bash
python gnss_orchestrator.py status --node winters
python gnss_orchestrator.py status --node WINTERS
python gnss_orchestrator.py status --node panoseti-winter
```

### Bodnar Config

Each node can include a `bodnar` block:

```json5
"bodnar": {
  "present": true,
  "required": false,
  "repo": "/home/panoseti/lbe1420_panoseti",
  "python": "/home/panoseti/miniconda3/envs/pygnss_312/bin/python",
  "configure_script": "lbe-1420-conf.py",
  "out1_enabled": true,
  "frequency_hz": 10000000,
  "gnss": "recommended"
}
```

Fields:

- `present`: whether this node should include Bodnar status/configuration.
- `required`: whether a Bodnar failure should block a `start --bodnar` node
  launch.
- `repo`: location of the `lbe1420_panoseti` repo on that node.
- `python`: Python executable/environment used to run the Bodnar script.
- `configure_script`: Bodnar configuration script, usually
  `lbe-1420-conf.py`.
- `out1_enabled`: desired OUT1 output state. When true, `start --bodnar` runs
  `lbe-1420-conf.py --enable 1`; when false, it runs
  `lbe-1420-conf.py --enable 0`.
- `frequency_hz`: output frequency for OUT1. For the current 10 MHz stopgap,
  this is `10000000`.
- `gnss`: constellation setting passed to `lbe-1420-conf.py --gnss`.

The current recommended constellation setting is:

```json5
"gnss": "recommended"
```

In `lbe-1420-conf.py`, this means GPS + SBAS + Galileo + BeiDou. The Bodnar
script also supports `default`, `all`, or comma-separated constellation names
such as `gps,galileo,beidou`.

## Status Command

The status command is read-only.

Basic usage:

```bash
python gnss_orchestrator.py status
```

What it checks:

- Deployment config fields are present.
- Server Python executable exists.
- Server repo exists.
- Server script exists.
- Server log directory parent exists.
- Each present node can be reached by SSH.
- Each node's Python executable exists and is executable.
- Each node's repo exists.
- Each node's `agent_v1.py` exists.
- Each node's `find_ublox.sh` exists and is executable.
- Log/telemetry parent directories exist.
- A u-blox GNSS receiver can be detected.
- If Bodnar is present, the Bodnar repo/script/Python paths exist and the
  device can be queried with `lbe-1420-conf.py --status`.
- For targets using the `systemd` runner, the user service is installed,
  enabled, active, and has lingering enabled.
- Optionally, receiver registers match the selected manifest.

### Status Examples

Check everything:

```bash
python gnss_orchestrator.py status
```

Check one node:

```bash
python gnss_orchestrator.py status --node WINTERS
```

Inspect systemd state without changing the deployment config:

```bash
python gnss_orchestrator.py status --node PTI --runner systemd
```

Include disabled nodes:

```bash
python gnss_orchestrator.py status --include-disabled
```

Skip SSH and remote checks:

```bash
python gnss_orchestrator.py status --local-only
```

Use JSON output:

```bash
python gnss_orchestrator.py status --json
```

### Bodnar Status

If `bodnar.present` is true for a node, normal `status` includes Bodnar checks:

```text
OK   bodnar python executable -- /home/panoseti/miniconda3/envs/pygnss_312/bin/python
OK   bodnar repo directory -- /home/panoseti/lbe1420_panoseti
OK   bodnar configure script -- /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py
OK   bodnar detected -- fix 3D fix (valid); satellites 12 used / 34 in view; C/N0 best 51 / avg 42 dB-Hz; GPS lock yes; PLL lock yes; antenna OK; OUT1 10000000 Hz
```

The human output intentionally summarizes the Bodnar status. It reports the
operational facts needed for a quick health check:

- GNSS fix state
- satellite count
- C/N0 best/average
- GPS lock
- PLL lock
- antenna state
- OUT1 frequency

The status check is read-only. It runs:

```bash
python lbe-1420-conf.py --status
```

### Register Verification

Register verification is enabled with:

```bash
python gnss_orchestrator.py status --verify-registers
```

**Important operational warning:** `--verify-registers` is register read-only,
but it is not non-invasive while an agent is running. The verifier opens the
u-blox serial port, polls `SEC-UNIQID`, sends `UBX-CFG-VALGET`, and reads UBX
responses. That can compete with the running agent's serial demux and briefly
starve the agent of `TIM-TP`/`NAV-SAT` telemetry. During active acquisition this
can mark qerr stale, so avoid `--verify-registers` while data are being taken.
Use normal `status` for live health checks.

This causes the orchestrator to SSH to the node and run:

```bash
python gnss_scripts/verify_manifest_registers.py --manifest <selected_manifest> --json
```

The verifier:

1. Finds the attached u-blox receiver.
2. Reads its `SEC-UNIQID`.
3. Looks up the receiver in the selected manifest.
4. Determines the expected role.
5. Expands global, role, and device-specific manifest settings.
6. Polls the receiver using `UBX-CFG-VALGET`.
7. Compares live register values to manifest values.

Differential register check:

```bash
python gnss_orchestrator.py status --node WINTERS --mode differential --verify-registers
```

Absolute register check:

```bash
python gnss_orchestrator.py status --node WINTERS --mode absolute --verify-registers
```

Successful result:

```text
OK   register verify -- 60/60 matched role=base
```

Failure result:

```text
FAIL register verify -- 53/62 matched role=timing_only; mismatches: CFG_USBOUTPROT_RTCM3X, CFG_MSGOUT_RTCM_3X_TYPE1005_USB, CFG_MSGOUT_RTCM_3X_TYPE1077_USB, +6 more
```

The human output intentionally summarizes mismatches. Use `--json` to see full
details.

### Register Verification JSON

Run:

```bash
python gnss_orchestrator.py status --node WINTERS --mode absolute --verify-registers --json
```

In JSON mode, each node can include a `register_verify` block:

```json
"register_verify": {
  "ok": false,
  "role": "timing_only",
  "port": "/dev/ttyACM2",
  "device_id": "9D02FA41BC",
  "manifest": "/home/panoseti/U-Blox_F9T_Config/manifest_f9t_absolute.json5",
  "layer": "RAM",
  "checked": 62,
  "matched": 53,
  "mismatches": [
    {
      "key": "CFG_USBOUTPROT_RTCM3X",
      "expected": false,
      "actual": true,
      "status": "mismatch"
    }
  ],
  "values": [
    {
      "key": "CFG_USBOUTPROT_RTCM3X",
      "expected": false,
      "actual": true,
      "status": "mismatch"
    },
    {
      "key": "CFG_RATE_MEAS",
      "expected": 1000,
      "actual": 1000,
      "status": "matched"
    }
  ],
  "skipped": []
}
```

Fields:

- `ok`: true if all checked registers matched.
- `role`: role used for manifest expansion.
- `port`: detected serial port.
- `device_id`: receiver unique ID.
- `manifest`: manifest used for comparison.
- `layer`: receiver config layer queried, usually `RAM`.
- `checked`: number of manifest-managed registers checked.
- `matched`: number that matched.
- `mismatches`: only failed or missing values.
- `values`: all checked values, including matches.
- `skipped`: keys skipped because the local `pyubx2` version could not resolve
  them.

The `values` list is useful for audits and snapshots. It only includes
manifest-managed registers, not every possible u-blox register.

## Install Service Command

`install-service` renders the repository's systemd templates using
`gnss_deployment.json5`, then installs them as user services under
`~/.config/systemd/user/`. The server unit is installed locally. Agent units use
the same local-or-SSH transport as the other orchestrator commands.

Installation is intentionally separate from process control. It performs
lightweight path checks, writes the unit atomically, runs
`systemctl --user daemon-reload`, and enables the unit when
`enable_on_install` is true. It does not start or restart the service.

Preview one remote agent unit:

```bash
python gnss_orchestrator.py install-service --node PTI --dry-run
```

Install one or more agent units:

```bash
python gnss_orchestrator.py install-service --node PTI
python gnss_orchestrator.py install-service --node PTI --node FERN
python gnss_orchestrator.py install-service --all-nodes
```

Install the local server unit. The selected mode and receiver manifest are
embedded in the generated server `ExecStart` command:

```bash
python gnss_orchestrator.py install-service --server --mode differential
python gnss_orchestrator.py install-service --server --mode absolute
```

The command may install both the server and agents in one invocation:

```bash
python gnss_orchestrator.py install-service --server --all-nodes --mode differential
```

For reliable user-service startup after a reboot, lingering must be enabled once
for the service account on each machine:

```bash
sudo loginctl enable-linger panoseti
```

The installer checks this setting and prints a warning when it is not enabled.
It does not invoke `sudo`. Confirm an installed unit before starting it:

```bash
systemctl --user cat gnss-agent.service
systemctl --user status gnss-agent.service
```

Installing a unit alone does not start it. Set `process.runner` to `systemd` for
the relevant target, or use `--runner systemd`, when the orchestrator should
control it.

## Start Command

The start command launches the GNSS server and agents with the runner selected
in `gnss_deployment.json5`. The default remains `screen`; `systemd` provides
automatic restart and reboot recovery.

Basic usage:

```bash
python gnss_orchestrator.py start
```

By default this starts in differential mode.

### Screen Start Behavior

For the server, the orchestrator:

1. Creates the server log and telemetry directories if needed.
2. Stops any existing server screen session with the same screen name.
3. Starts `server_v1.py` in a detached screen session.
4. Writes server stdout/stderr to a UTC-stamped per-run log file.
5. Checks that the screen session exists after launch.

For each selected node, the orchestrator:

1. SSHs to the node.
2. Creates the configured log and telemetry directories if needed.
3. Stops any existing agent screen session with the same screen name.
4. Changes into the configured repo directory.
5. Starts `agent_v1.py` in a detached screen session.
6. Writes agent stdout/stderr to a UTC-stamped per-run log file.
7. Checks that the screen session exists after launch.
8. If launch fails, it prints recent log output to help diagnose the failure.

The server and all agents launched by a single `start` command share the same
UTC run stamp in their orchestrator log filenames. Each log line is also
prefixed with a UTC timestamp.

If `--bodnar` is used, the orchestrator also configures each selected node's
present Bodnar before starting that node's GNSS agent.

For each selected Bodnar, the orchestrator:

1. SSHs to the node.
2. Checks the configured Bodnar Python executable, repo, and script.
3. Runs `lbe-1420-conf.py --enable 1` or `--enable 0` if `out1_enabled` is set.
4. Runs `lbe-1420-conf.py --f1 <frequency_hz>`.
5. Runs `lbe-1420-conf.py --gnss <gnss>`.

The frequency and GNSS commands are separate because `lbe-1420-conf.py` treats
`--enable`, `--f1`, `--gnss`, and `--status` as mutually exclusive options.
`start --bodnar` does not run `lbe-1420-conf.py --status`; use the orchestrator
`status` command for Bodnar health summaries.

### Systemd Start Behavior

For a target using `systemd`, every `start` intentionally performs a fresh
restart:

1. Renders the service again from the current deployment config.
2. Atomically updates the user unit and runs `daemon-reload`.
3. Enables the unit when `enable_on_install` is true.
4. Stops the service so `Restart=always` cannot race with legacy cleanup.
5. Stops any matching screen session or stray Python process.
6. Starts the service and verifies that it reaches `active`.

Restarting is deliberate: the agent reconnects to the server, reapplies the
RAM-only F9T register configuration, and opens new timestamped log and telemetry
files. The server unit is similarly refreshed with the selected timing mode and
manifest.

Running `start` again is therefore a restart with either runner.

### Start Examples

Preview all launch commands without running them:

```bash
python gnss_orchestrator.py start --dry-run
```

Start all present nodes in differential mode:

```bash
python gnss_orchestrator.py start --mode differential
```

Test systemd without changing `process.runner` in the config:

```bash
python gnss_orchestrator.py start --node PTI --runner systemd --dry-run
python gnss_orchestrator.py start --node PTI --runner systemd
```

For routine operation, set the desired server/node `process.runner` values to
`systemd` and omit the override:

```bash
python gnss_orchestrator.py start --node PTI
```

Start all present nodes in absolute mode:

```bash
python gnss_orchestrator.py start --mode absolute
```

Start only WINTERS:

```bash
python gnss_orchestrator.py start --node WINTERS
```

Configure the WINTERS Bodnar and then start WINTERS:

```bash
python gnss_orchestrator.py start --node WINTERS --bodnar
```

Start only WINTERS in absolute mode:

```bash
python gnss_orchestrator.py start --node WINTERS --mode absolute
```

Configure Bodnars and start all present nodes in differential mode:

```bash
python gnss_orchestrator.py start --mode differential --bodnar
```

Start disabled nodes too:

```bash
python gnss_orchestrator.py start --include-disabled
```

Emit machine-readable start output:

```bash
python gnss_orchestrator.py start --json
```

### Example Dry Run

Dry run output includes the generated local or remote shell script:

```text
dry-run  node   winters      WINTERS            host=panoseti-winter
  detail: would start screen gnss_agent with log /home/panoseti/gnss_logging/gnss_agent_20260701_234002Z.log
  target: ssh panoseti-winter
  remote script:
    set -euo pipefail
    mkdir -p /home/panoseti/gnss_logging /home/panoseti/gnss_telem
    ln -sfn gnss_agent_20260701_234002Z.log /home/panoseti/gnss_logging/gnss_agent.log
    screen -S gnss_agent -X quit >/dev/null 2>&1 || true
    sleep 0.5
    cd /home/panoseti/U-Blox_F9T_Config
    screen -dmS gnss_agent bash -c 'set -o pipefail; /home/panoseti/miniconda3/envs/pygnss_312/bin/python -u /home/panoseti/U-Blox_F9T_Config/agent_v1.py --cast_addr 10.200.146.1:50051 --ctrl_addr 10.200.146.1:50051 -v 2 2>&1 | TZ=UTC awk '"'"'{ print strftime("%Y-%m-%dT%H:%M:%SZ"), $0; fflush(); }'"'"' >> /home/panoseti/gnss_logging/gnss_agent_20260701_234002Z.log'
    sleep 1
    screen -ls | grep -q -- '\.gnss_agent[[:space:]]'
```

Dry run is safe. It does not start or stop anything.

With `--bodnar`, dry run also shows the Bodnar configuration commands:

```text
dry-run  bodnar winters      WINTERS            host=panoseti-winter
  detail: would configure Bodnar 10000000 Hz, gnss=recommended
  target: ssh panoseti-winter
  remote script:
    set -e
    cd /home/panoseti/lbe1420_panoseti
    /home/panoseti/miniconda3/envs/pygnss_312/bin/python /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py --enable 1
    /home/panoseti/miniconda3/envs/pygnss_312/bin/python /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py --f1 10000000
    /home/panoseti/miniconda3/envs/pygnss_312/bin/python /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py --gnss recommended
```

Successful real output looks like:

```text
configured bodnar winters      WINTERS            host=panoseti-winter
  detail: 10000000 Hz, gnss=recommended
```

## Stop Command

The stop command uses the configured runner. In systemd mode it stops the user
service and also cleans up matching legacy screen/process instances. It leaves
the service enabled, so it remains eligible for the next boot; `stop` only
changes the current runtime state.

Basic usage:

```bash
python gnss_orchestrator.py stop
```

### Stop Behavior

By default:

- `python gnss_orchestrator.py stop` stops all selected agents and then the
  local server.
- `python gnss_orchestrator.py stop --node WINTERS` stops only that node's
  agent and does not stop the server.

This behavior is intentional. A single-node maintenance action should not take
down the whole GNSS server.

### Stop Examples

Stop one node:

```bash
python gnss_orchestrator.py stop --node WINTERS
```

Stop PTI through systemd without changing its configured runner:

```bash
python gnss_orchestrator.py stop --node PTI --runner systemd
```

Stop all agents and the server:

```bash
python gnss_orchestrator.py stop
```

Stop only the server:

```bash
python gnss_orchestrator.py stop --server-only
```

Stop only agents, leaving the server running:

```bash
python gnss_orchestrator.py stop --agents-only
```

Stop all agents and the server, then compress completed log and telemetry
files:

```bash
python gnss_orchestrator.py stop --compress-logs
```

Stop one node and compress that node's completed files:

```bash
python gnss_orchestrator.py stop --node WINTERS --compress-logs
```

Preview stop commands:

```bash
python gnss_orchestrator.py stop --dry-run
```

Emit machine-readable stop output:

```bash
python gnss_orchestrator.py stop --json
```

### Stop With Compression

`--compress-logs` runs after each selected process has stopped. It uses
`gzip -9` on completed files in the selected log and telemetry directories.

It compresses regular files matching:

- `*.log`
- `*.txt`
- `*.jsonl`

It skips symlinks such as `gnss_agent.log` and `gnss_server.log`. If those
latest-log symlinks pointed at a file that was compressed, the orchestrator
updates the symlink to point at the new `.gz` file.

Compression follows the same target selection as stop:

- `stop --compress-logs` stops/compresses all selected agents and then the
  server.
- `stop --node WINTERS --compress-logs` stops/compresses only WINTERS.
- `stop --agents-only --compress-logs` stops/compresses agents only.
- `stop --server-only --compress-logs` stops/compresses the server only.

Dry run is safe and shows the generated compression commands:

```bash
python gnss_orchestrator.py stop --compress-logs --dry-run
```

### Daytime Background Compression

For continuous GNSS operation, use `gnss_scripts/compress_gnss_logs.py` as a
separate maintenance task instead of compressing from inside the live agent. The
script skips `.active` files, symlinks, recent files, and existing `.gz` files,
so it can run while telemetry is still being accumulated.

Example one-shot command on a DAQ node:

```bash
ionice -c3 nice -n 19 python /home/panoseti/U-Blox_F9T_Config/gnss_scripts/compress_gnss_logs.py \
  --dirs /home/panoseti/gnss_telem /home/panoseti/gnss_logging \
  --older-than-minutes 60 \
  --gzip-level 3 \
  --max-files 20
```

Systemd service/timer templates are provided in:

```text
gnss_scripts/systemd/gnss-compress.service.example
gnss_scripts/systemd/gnss-compress.timer.example
```

Copy the examples into the local user-systemd directory, then edit the copied
`gnss-compress.service` paths for each DAQ node. Keep the `.example` files in
the repo unchanged so future `git pull` operations do not conflict with
node-local paths.

```bash
mkdir -p ~/.config/systemd/user
cp gnss_scripts/systemd/gnss-compress.service.example ~/.config/systemd/user/gnss-compress.service
cp gnss_scripts/systemd/gnss-compress.timer.example ~/.config/systemd/user/gnss-compress.timer
nano ~/.config/systemd/user/gnss-compress.service
systemctl --user daemon-reload
systemctl --user enable --now gnss-compress.timer
```

Useful checks:

```bash
systemctl --user status gnss-compress.timer
systemctl --user list-timers gnss-compress.timer
journalctl --user -u gnss-compress.service -n 100
```

Use `compress_gnss_logs.py --dry-run` first to confirm what would be compressed.

## Direct Register Verifier

The orchestrator usually runs the register verifier for you, but you can run it
directly on a DAQ node.

Example:

```bash
ssh panoseti-winter
conda activate pygnss_312
cd ~/U-Blox_F9T_Config
python gnss_scripts/verify_manifest_registers.py --manifest manifest_f9t.json5 --role auto --port auto
```

Use JSON output:

```bash
python gnss_scripts/verify_manifest_registers.py --manifest manifest_f9t.json5 --role auto --port auto --json
```

Check the absolute manifest:

```bash
python gnss_scripts/verify_manifest_registers.py --manifest manifest_f9t_absolute.json5 --role timing_only --port auto
```

The verifier is read-only. It polls receiver registers with `UBX-CFG-VALGET` but
does not write receiver registers.

It can still conflict with an actively running agent if both are talking to the
same serial port at the same time. If verification output seems strange, stop
the agent for that node and try again.

## Recommended Workflows

Before running any orchestrator command, activate the Python environment that
contains the orchestrator dependencies, including `json5`. For example:

```bash
conda activate pygnss_312
```

For a `uv`-managed environment, either activate its virtual environment:

```bash
source .venv/bin/activate
```

or run commands through `uv`:

```bash
uv run python gnss_orchestrator.py status --config ./gnss_deployment.json5
```

The environment used to invoke the orchestrator is separate from the `python`
paths in `gnss_deployment.json5`. Those configured paths determine which Python
executables launch the server, agents, register verifier, and Bodnar utility.

### Before Starting a Run

1. Check config and reachability:

   ```bash
   python gnss_orchestrator.py status
   ```

2. Check receiver registers:

   ```bash
   python gnss_orchestrator.py status --verify-registers
   ```

3. Preview launch commands:

   ```bash
   python gnss_orchestrator.py start --dry-run
   ```

4. If using the temporary Bodnar 10 MHz reference, preview the Bodnar commands:

   ```bash
   python gnss_orchestrator.py start --bodnar --dry-run
   ```

5. Start GNSS only:

   ```bash
   python gnss_orchestrator.py start --mode differential
   ```

6. Or configure Bodnars and start GNSS:

   ```bash
   python gnss_orchestrator.py start --mode differential --bodnar
   ```

7. Confirm status:

   ```bash
   python gnss_orchestrator.py status --verify-registers
   ```

### At the End of a Run

Stop all agents and the server, then compress completed process logs and
telemetry files:

```bash
python gnss_orchestrator.py stop \
  --config ./gnss_deployment.json5 \
  --compress-logs
```

To inspect the stop and compression commands without executing them:

```bash
python gnss_orchestrator.py stop \
  --config ./gnss_deployment.json5 \
  --compress-logs \
  --dry-run
```

When `--node` is omitted, the command stops all configured agents followed by
the local server. It then runs `gzip -9` on completed `.log`, `.txt`, and
`.jsonl` files in their configured log and telemetry directories. Existing
`.gz` files are left unchanged.

Stopping a single node does not stop the shared server:

```bash
python gnss_orchestrator.py stop \
  --config ./gnss_deployment.json5 \
  --node WINTERS \
  --compress-logs
```

### Switching to Absolute Timing Mode

1. Stop the current deployment:

   ```bash
   python gnss_orchestrator.py stop
   ```

2. Start absolute timing mode:

   ```bash
   python gnss_orchestrator.py start --mode absolute
   ```

3. Verify absolute timing registers:

   ```bash
   python gnss_orchestrator.py status --mode absolute --verify-registers
   ```

4. Confirm the output shows `role=timing_only` and all registers matched.

### Testing That Modes Are Distinct

If a receiver is configured for differential mode, this should pass:

```bash
python gnss_orchestrator.py status --node WINTERS --mode differential --verify-registers
```

This may fail, and that is useful:

```bash
python gnss_orchestrator.py status --node WINTERS --mode absolute --verify-registers
```

Expected failure details often include RTCM keys such as:

```text
CFG_USBOUTPROT_RTCM3X
CFG_MSGOUT_RTCM_3X_TYPE1005_USB
CFG_MSGOUT_RTCM_3X_TYPE1077_USB
```

That means the receiver is still configured for RTCM/differential behavior.

## Logs and Telemetry

The orchestrator launch logs are separate from telemetry files.

Typical server log:

```text
/home/obs/U-Blox_F9T_Config/logging/gnss_server_20260701_234002Z.log
```

Typical agent log:

```text
/home/panoseti/gnss_logging/gnss_agent_20260701_234002Z.log
```

For convenience, the orchestrator also maintains stable `latest` paths:

```text
/home/obs/U-Blox_F9T_Config/logging/gnss_server.log
/home/panoseti/gnss_logging/gnss_agent.log
```

Those stable paths are symlinks to the most recent UTC-stamped run log. This
keeps `tail -f gnss_agent.log` convenient while preserving one operational log
file per orchestrator start.

Typical telemetry directory:

```text
/home/panoseti/gnss_telem
```

Typical server telemetry directory:

```text
/home/obs/U-Blox_F9T_Config/telemetry
```

The launch logs capture stdout/stderr from `server_v1.py` and `agent_v1.py`.
Each launch-log line starts with a UTC timestamp, for example:

```text
2026-07-01T23:40:02Z [agent] discovering u-blox serial port (auto)...
```

The telemetry directory is where the GNSS telemetry data products go.
Files being written have a `.jsonl.active` suffix; completed telemetry segments
end in `.jsonl` and can be compressed later.

Useful remote checks:

```bash
ssh panoseti-winter
screen -ls
tail -n 100 /home/panoseti/gnss_logging/gnss_agent.log
ls -ltr /home/panoseti/gnss_telem
```

### Redis Latest Status

The JSONL telemetry files remain the durable run record. The GNSS server can
also mirror the latest live GNSS status into the PANOSETI Telemetry service,
which stores it in Redis for dashboards and health checks.

This is controlled by the `redis_status` block in `gnss_deployment.json5`:

```json5
"redis_status": {
  "enabled": true,
  "addr": "127.0.0.1:50051",
  "device_type": "gnss"
}
```

When `start` launches the GNSS server, the orchestrator passes these settings as
environment variables:

```bash
GNSS_REDIS_STATUS_ENABLED=1
GNSS_REDIS_STATUS_GRPC_ADDR=127.0.0.1:50051
GNSS_REDIS_STATUS_DEVICE_TYPE=gnss
```

The GNSS server still writes telemetry JSONL as before. When agent telemetry is
received over `Control.Pipe`, the server also publishes a best-effort latest
status update through PANOSETI Telemetry. If the Telemetry service or Redis is
unavailable, GNSS control, RTCM routing, and JSONL logging continue running.

With the default PANOSETI Telemetry configuration, Redis keys use the GNSS
prefix from `telemetry_config.toml`, for example:

```text
UBLOX_ZED-F9T_D92EAA4324
```

## JSON Output

All commands support `--json`.

Use JSON when:

- integrating with a DAQ start/stop script,
- collecting status snapshots,
- debugging register mismatches,
- checking many nodes programmatically.

Example:

```bash
python gnss_orchestrator.py status --verify-registers --json
```

The JSON includes:

- top-level config path,
- selected mode,
- whether register verification was enabled,
- one result for the server,
- one result per selected node,
- per-check status,
- resolved paths,
- GNSS receiver detection state,
- Bodnar detection state and resolved Bodnar config,
- optional full `register_verify` details.

## Exit Codes

For `status`, the process exit code is nonzero only if:

- the server fails validation, or
- a node marked `required: true` fails validation.

Optional nodes can fail without making the status command return nonzero.

This lets the system distinguish between hard blockers and optional node issues.

For `start` and `stop`, required failures are treated as command failures.

## Troubleshooting

### SSH Fails

Symptom:

```text
FAIL ssh reachable -- exit 255
```

Check:

```bash
ssh panoseti-winter
```

Possible causes:

- wrong `host`,
- wrong `ssh_user`,
- SSH keys unavailable,
- node offline,
- `ssh_batch_mode` preventing password prompts.

### Python Environment Missing

Symptom:

```text
FAIL remote -x /home/panoseti/miniconda3/envs/pygnss_312/bin/python
```

Check the `python` path in `gnss_deployment.json5`.

On the node:

```bash
ls -l /home/panoseti/miniconda3/envs/pygnss_312/bin/python
```

### Missing Python Package

Symptom in agent log:

```text
ModuleNotFoundError: No module named 'grpc'
```

Fix on the node:

```bash
cd ~/U-Blox_F9T_Config
conda activate pygnss_312
pip install -r requirements.txt
```

### Receiver Not Detected

Symptom:

```text
FAIL gnss receiver detected
```

Check on the node:

```bash
ls -l /dev/serial/by-id
python gnss_scripts/verify_manifest_registers.py --manifest manifest_f9t.json5 --port auto
```

Possible causes:

- receiver unplugged,
- wrong USB device permissions,
- another process holding the serial port,
- `find_ublox_script` path is wrong.

### Register Verification Fails

Symptom:

```text
FAIL register verify -- 53/62 matched role=timing_only; mismatches: ...
```

Interpretation:

- The receiver was found.
- The manifest was loaded.
- The role was resolved.
- Some live register values did not match the manifest.

Use JSON to see all values:

```bash
python gnss_orchestrator.py status --node WINTERS --mode absolute --verify-registers --json
```

Look at:

```json
"register_verify": {
  "mismatches": [],
  "values": []
}
```

### Bodnar Not Detected

Symptom:

```text
FAIL bodnar detected -- exit ...
```

or:

```text
FAIL bodnar python executable
FAIL bodnar repo directory
FAIL bodnar configure script
```

Check on the node:

```bash
ssh panoseti-winter
cd /home/panoseti/lbe1420_panoseti
/home/panoseti/miniconda3/envs/pygnss_312/bin/python lbe-1420-conf.py --status
```

Possible causes:

- Bodnar USB device unplugged,
- wrong `bodnar.repo`,
- wrong `bodnar.python`,
- missing Python dependencies for `lbe-1420-conf.py`,
- HID device permissions,
- another program holding the HID interface.

### Bodnar Configuration Fails

Symptom:

```text
failed   bodnar winters      WINTERS            host=panoseti-winter
```

Check the dry-run command first:

```bash
python gnss_orchestrator.py start --node WINTERS --bodnar --dry-run
```

Then run the generated commands manually on the node:

```bash
ssh panoseti-winter
cd /home/panoseti/lbe1420_panoseti
/home/panoseti/miniconda3/envs/pygnss_312/bin/python lbe-1420-conf.py --f1 10000000
/home/panoseti/miniconda3/envs/pygnss_312/bin/python lbe-1420-conf.py --gnss recommended
/home/panoseti/miniconda3/envs/pygnss_312/bin/python lbe-1420-conf.py --status
```

### Screen Session Fails Immediately

Symptom:

```text
FAIL screen gnss_agent not found after launch
```

The orchestrator should include recent log output in the failure detail.

Check manually:

```bash
ssh panoseti-winter
tail -n 100 /home/panoseti/gnss_logging/gnss_agent.log
```

Common causes:

- missing Python package,
- wrong Python environment,
- wrong script path,
- wrong gRPC address,
- receiver serial discovery failure.

### Old Log Output Appears

The agent/server launch logs are written to one UTC-stamped file per
orchestrator `start` run. If `gnss_agent.log` or `gnss_server.log` shows old
output, check whether the path is a symlink to the run you expected:

```bash
ls -l /home/panoseti/gnss_logging/gnss_agent.log
```

## Safety Notes

- `status` is read-only.
- `status` may run `lbe-1420-conf.py --status` for present Bodnars, which is a
  read-only device query.
- `status --verify-registers` polls receiver registers but does not write them;
  however, it opens the receiver serial port and can interfere with a running
  agent's UBX telemetry stream. Do not use it during active data acquisition.
- `start --dry-run` is read-only.
- `stop --dry-run` is read-only.
- `start` restarts matching `screen` sessions before launching new ones.
- `start --bodnar` writes Bodnar settings by running `lbe-1420-conf.py --enable`,
  `lbe-1420-conf.py --f1`, and `lbe-1420-conf.py --gnss` before launching each
  selected node's GNSS agent.
- `stop --node NODE` stops only that node's agent.
- `stop` without `--node` stops all selected agents and the server.

## Adding a New DAQ Node

1. Add a new entry under `nodes` in `gnss_deployment.json5`.
2. Pick a stable key, for example `newdaq`.
3. Set `daq_name` to the DAQ display/manifest name.
4. Set `host` to the SSH target.
5. Set `python` to the node's Python environment.
6. Set `repo` to the repo location on that node.
7. Set `logdir` and `telem_dir`.
8. If the node has a Bodnar, set the `bodnar` block for that node.
9. Confirm the receiver manifest contains the receiver unique ID.
10. Run:

   ```bash
   python gnss_orchestrator.py status --node newdaq
   ```

11. Then run:

   ```bash
   python gnss_orchestrator.py status --node newdaq --verify-registers
   ```

## Mental Model

Think of the GNSS system as three layers:

1. Receiver manifests:

   These define what the receivers should be configured to do.

2. Deployment config:

   This defines where scripts, Python environments, logs, telemetry paths, SSH
   targets, and optional Bodnar settings live.

3. Orchestrator:

   This checks the deployment config, starts/stops processes, optionally
   configures Bodnars, and optionally compares live receiver registers against
   the selected manifest.

The orchestrator does not decide what GNSS settings are scientifically correct.
It makes sure the configured deployment can be run consistently and checked
repeatably across all DAQ nodes.
