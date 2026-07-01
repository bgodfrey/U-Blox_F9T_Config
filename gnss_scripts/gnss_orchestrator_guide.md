# GNSS Orchestrator Guide

This document describes how to use `gnss_scripts/gnss_orchestrator.py` to check,
start, and stop the GNSS server and GNSS agents across DAQ nodes.

The orchestrator is meant to answer three practical questions:

1. Is the GNSS deployment configured correctly?
2. Are the remote DAQ nodes reachable and ready?
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

Run these commands from the repo or from `gnss_scripts/`.

Check all enabled nodes:

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

Start the server and all enabled agents in differential mode:

```bash
python gnss_orchestrator.py start --mode differential
```

Start the server and all enabled agents in absolute timing mode:

```bash
python gnss_orchestrator.py start --mode absolute
```

Start one node only:

```bash
python gnss_orchestrator.py start --node WINTERS
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

The orchestrator config lives at:

```text
gnss_scripts/gnss_deployment.json5
```

This file is separate from the receiver manifests. It describes the machines and
process launch details, not the receiver register settings.

Important top-level sections:

- `defaults`: shared settings used by nodes unless they override them.
- `modes`: maps timing modes to receiver manifests.
- `server`: describes the local GNSS server process.
- `nodes`: describes each remote DAQ node.

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
  "agent_script": "agent_v1.py",
  "server_script": "server_v1.py",
  "logdir": "logging",
  "telem_dir": "telem",
  "find_ublox_script": "gnss_scripts/find_ublox.sh",
  "bodnar": {
    "enabled": true,
    "required": false,
    "repo": "/home/panoseti/lbe1420_panoseti",
    "python": "/home/panoseti/miniconda3/envs/pygnss_312/bin/python",
    "configure_script": "lbe-1420-conf.py",
    "frequency_hz": 10000000,
    "gnss": "recommended"
  },
  "start_only_if_receiver_detected": true,
  "required": false
}
```

Node-specific entries can override these values.

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

The `server` section describes the machine running `server_v1.py`.

Example:

```json5
"server": {
  "daq_name": "panoseti-palomar",
  "host": "localhost",
  "ssh_user": "obs",
  "python": "/home/obs/miniconda3/envs/pygnss_39/bin/python",
  "repo": "/home/obs/U-Blox_F9T_Config",
  "script": "server_v1.py",
  "logdir": "logging",
  "screen": "gnss_server",
  "bind_addr": "0.0.0.0:50051",
  "verbosity": 2,
  "receiver_manifest": null
}
```

Important fields:

- `daq_name`: display name in status output.
- `host`: server host. Usually `localhost`.
- `python`: full path to the Python executable/environment.
- `repo`: repo location on the server host.
- `script`: server script path, usually `server_v1.py`.
- `logdir`: where server logs go.
- `screen`: screen session name.
- `bind_addr`: gRPC bind address, for example `0.0.0.0:50051`.
- `verbosity`: passed to `server_v1.py` as `-v`.
- `receiver_manifest`: optional explicit receiver manifest.

### Node Config

The `nodes` section describes each remote DAQ node.

Example:

```json5
"winters": {
  "daq_name": "WINTERS",
  "host": "panoseti-winter",
  "enabled": true,
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
    "enabled": true,
    "required": false,
    "repo": "/home/panoseti/lbe1420_panoseti",
    "python": "/home/panoseti/miniconda3/envs/pygnss_312/bin/python",
    "configure_script": "lbe-1420-conf.py",
    "frequency_hz": 10000000,
    "gnss": "recommended"
  }
}
```

Important fields:

- Node key, for example `winters`: short inventory key used by `--node`.
- `daq_name`: display name, for example `WINTERS`.
- `host`: SSH hostname, for example `panoseti-winter`.
- `enabled`: whether the node is included by default.
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

The node key, `daq_name`, and `host` are related but different:

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
  "enabled": true,
  "required": false,
  "repo": "/home/panoseti/lbe1420_panoseti",
  "python": "/home/panoseti/miniconda3/envs/pygnss_312/bin/python",
  "configure_script": "lbe-1420-conf.py",
  "frequency_hz": 10000000,
  "gnss": "recommended"
}
```

Fields:

- `enabled`: whether this node should include Bodnar status/configuration.
- `required`: whether a Bodnar failure should block a `start --bodnar` node
  launch.
- `repo`: location of the `lbe1420_panoseti` repo on that node.
- `python`: Python executable/environment used to run the Bodnar script.
- `configure_script`: Bodnar configuration script, usually
  `lbe-1420-conf.py`.
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
- Each enabled node can be reached by SSH.
- Each node's Python executable exists and is executable.
- Each node's repo exists.
- Each node's `agent_v1.py` exists.
- Each node's `find_ublox.sh` exists and is executable.
- Log/telemetry parent directories exist.
- A u-blox GNSS receiver can be detected.
- If Bodnar is enabled, the Bodnar repo/script/Python paths exist and the
  device can be queried with `lbe-1420-conf.py --status`.
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

If `bodnar.enabled` is true for a node, normal `status` includes Bodnar checks:

```text
OK   bodnar python executable -- /home/panoseti/miniconda3/envs/pygnss_312/bin/python
OK   bodnar repo directory -- /home/panoseti/lbe1420_panoseti
OK   bodnar configure script -- /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py
OK   bodnar detected -- fix 3D fix (valid); satellites 12 used / 34 in view; GPS lock yes; PLL lock yes; antenna OK; OUT1 10000000 Hz
```

The human output intentionally summarizes the Bodnar status. It reports the
operational facts needed for a quick health check:

- GNSS fix state
- satellite count
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

## Start Command

The start command launches the GNSS server and GNSS agents in `screen` sessions.

Basic usage:

```bash
python gnss_orchestrator.py start
```

By default this starts in differential mode.

### Start Behavior

For the server, the orchestrator:

1. Creates the server log directory if needed.
2. Stops any existing server screen session with the same screen name.
3. Starts `server_v1.py` in a detached screen session.
4. Writes server stdout/stderr to the configured log file.
5. Checks that the screen session exists after launch.

For each selected node, the orchestrator:

1. SSHs to the node.
2. Creates the configured log and telemetry directories if needed.
3. Stops any existing agent screen session with the same screen name.
4. Changes into the configured repo directory.
5. Starts `agent_v1.py` in a detached screen session.
6. Writes agent stdout/stderr to the configured log file.
7. Checks that the screen session exists after launch.
8. If launch fails, it prints recent log output to help diagnose the failure.

If `--bodnar` is used, the orchestrator also configures each selected node's
enabled Bodnar before starting that node's GNSS agent.

For each selected Bodnar, the orchestrator:

1. SSHs to the node.
2. Checks the configured Bodnar Python executable, repo, and script.
3. Runs `lbe-1420-conf.py --status` as a preflight/device-access check.
4. Runs `lbe-1420-conf.py --f1 <frequency_hz>`.
5. Runs `lbe-1420-conf.py --gnss <gnss>`.
6. Runs `lbe-1420-conf.py --status` again after configuration.

The frequency and GNSS commands are separate because `lbe-1420-conf.py` treats
`--f1`, `--gnss`, and `--status` as mutually exclusive options.

This means running `start` again is a restart for the selected processes. It
will stop the existing matching `screen` session and launch a new one.

### Start Examples

Preview all launch commands without running them:

```bash
python gnss_orchestrator.py start --dry-run
```

Start all enabled nodes in differential mode:

```bash
python gnss_orchestrator.py start --mode differential
```

Start all enabled nodes in absolute mode:

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

Configure Bodnars and start all enabled nodes in differential mode:

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
  detail: would start screen gnss_agent with log /home/panoseti/gnss_logging/gnss_agent.log
  target: ssh panoseti-winter
  remote script:
    set -euo pipefail
    mkdir -p /home/panoseti/gnss_logging /home/panoseti/gnss_telem
    screen -S gnss_agent -X quit >/dev/null 2>&1 || true
    sleep 0.5
    cd /home/panoseti/U-Blox_F9T_Config
    screen -dmS gnss_agent bash -lc 'exec /home/panoseti/miniconda3/envs/pygnss_312/bin/python -u /home/panoseti/U-Blox_F9T_Config/agent_v1.py --cast_addr 10.200.146.1:50051 --ctrl_addr 10.200.146.1:50051 -v 2 >> /home/panoseti/gnss_logging/gnss_agent.log 2>&1'
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
    /home/panoseti/miniconda3/envs/pygnss_312/bin/python /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py --f1 10000000
    /home/panoseti/miniconda3/envs/pygnss_312/bin/python /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py --gnss recommended
    /home/panoseti/miniconda3/envs/pygnss_312/bin/python /home/panoseti/lbe1420_panoseti/lbe-1420-conf.py --status
```

Successful real output looks like:

```text
configured bodnar winters      WINTERS            host=panoseti-winter
  detail: 10000000 Hz, gnss=recommended
```

## Stop Command

The stop command stops GNSS `screen` sessions.

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

Preview stop commands:

```bash
python gnss_orchestrator.py stop --dry-run
```

Emit machine-readable stop output:

```bash
python gnss_orchestrator.py stop --json
```

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
/home/obs/U-Blox_F9T_Config/logging/gnss_server.log
```

Typical agent log:

```text
/home/panoseti/gnss_logging/gnss_agent.log
```

Typical telemetry directory:

```text
/home/panoseti/gnss_telem
```

The launch logs capture stdout/stderr from `server_v1.py` and `agent_v1.py`.
The telemetry directory is where the GNSS telemetry data products go.

Useful remote checks:

```bash
ssh panoseti-winter
screen -ls
tail -n 100 /home/panoseti/gnss_logging/gnss_agent.log
ls -ltr /home/panoseti/gnss_telem
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

The agent/server launch logs append by default. If you see old errors followed
by new startup messages, that is normal.

Use timestamps or clear/archive logs manually if needed.

## Safety Notes

- `status` is read-only.
- `status` may run `lbe-1420-conf.py --status` for enabled Bodnars, which is a
  read-only device query.
- `status --verify-registers` polls receiver registers but does not write them.
- `start --dry-run` is read-only.
- `stop --dry-run` is read-only.
- `start` restarts matching `screen` sessions before launching new ones.
- `start --bodnar` writes Bodnar settings by running `lbe-1420-conf.py --f1`
  and `lbe-1420-conf.py --gnss` before launching each selected node's GNSS
  agent.
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
