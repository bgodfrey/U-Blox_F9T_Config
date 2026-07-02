#!/usr/bin/env python3
"""GNSS deployment orchestration.

The status command is intentionally read-only:

    python gnss_scripts/gnss_orchestrator.py status

It validates the deployment inventory and checks local/remote prerequisites.
The start command uses the same inventory and preflight checks before launching
the GNSS server and agents in screen sessions. The stop command gracefully shuts
those sessions down again.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import json5


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = SCRIPT_DIR / "gnss_deployment.json5"


@dataclass
class Check:
    """One status check result.

    These are intentionally small and serializable so the same status report can
    be printed for humans or returned to a larger DAQ controller.
    """

    name: str
    ok: bool
    detail: str = ""


@dataclass
class CommandResult:
    """Normalized result from a local subprocess command."""

    returncode: int
    stdout: str
    stderr: str


@dataclass
class StartResult:
    """One start-action result for the server or a node."""

    kind: str
    key: str
    daq_name: str
    host: str
    status: str
    detail: str = ""
    required: bool = False
    command: str = ""
    script: str = ""


@dataclass
class StopResult:
    """One stop-action result for the server or a node."""

    kind: str
    key: str
    daq_name: str
    host: str
    status: str
    detail: str = ""
    required: bool = False
    command: str = ""
    script: str = ""


def load_config(path: str | os.PathLike[str] = DEFAULT_CONFIG) -> dict[str, Any]:
    """Load the JSON5 deployment inventory.

    Args:
        path: Path to the deployment config. Defaults to
            gnss_scripts/gnss_deployment.json5.

    Returns:
        The parsed JSON5 object as a nested dictionary/list structure.
    """

    with open(path, "r", encoding="utf-8") as f:
        return json5.load(f)


def _str_bool(value: Any) -> bool:
    """Convert config values into booleans.

    The config currently stores booleans as real JSON5 booleans, but this also
    accepts common string forms so future environment-driven overrides can reuse
    the same helper.
    """

    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _present(item: dict[str, Any], default: bool = True) -> bool:
    """Return whether a config item is present, with legacy enabled fallback."""

    if "present" in item:
        return _str_bool(item["present"])
    if "enabled" in item:
        return _str_bool(item["enabled"])
    return default


def _merge(defaults: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    """Merge a config item with shared defaults.

    Args:
        defaults: Values from the top-level "defaults" config section.
        item: A server or node config dictionary.

    Returns:
        A new dictionary containing defaults plus item-specific overrides.
    """

    merged = dict(defaults)
    merged.update(item)
    return merged


def _resolve_under_repo(repo: str, value: str) -> str:
    """Resolve a configured path relative to a repo.

    Absolute paths are returned unchanged. Relative paths are interpreted as
    being under the node/server repository path, which keeps the config compact
    for common files like agent_v1.py and logging directories.
    """

    if value.startswith("/"):
        return value
    return str(Path(repo) / value)


def _bodnar_config(defaults: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    """Merge default and per-node Leo Bodnar settings."""

    merged = dict(defaults.get("bodnar") or {})
    merged.update(node.get("bodnar") or {})
    return merged


def _bodnar_paths(defaults: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    """Resolve the Bodnar Python, repo, and script paths for one node."""

    bodnar = _bodnar_config(defaults, node)
    repo = str(bodnar.get("repo") or "")
    script = str(bodnar.get("configure_script") or "lbe-1420-conf.py")
    python = str(bodnar.get("python") or node.get("python") or "")
    return {
        "python": python,
        "repo": repo,
        "script": _resolve_under_repo(repo, script) if repo else script,
    }


def _mode_settings(config: dict[str, Any], mode: str) -> dict[str, Any]:
    """Return manifest/timing settings for a start mode."""

    modes = config.get("modes", {})
    if mode in modes:
        settings = dict(modes[mode])
    elif mode == "differential":
        settings = {"timing_mode": "differential"}
    elif mode == "absolute":
        settings = {"timing_mode": "absolute", "manifest": "manifest_f9t_absolute.json5"}
    else:
        raise ValueError(f"unknown GNSS start mode {mode!r}")

    if "receiver_manifest" not in settings and "manifest" in settings:
        settings["receiver_manifest"] = settings["manifest"]
    settings.setdefault("timing_mode", mode)
    return settings


def _run(args: list[str], timeout: float) -> CommandResult:
    """Run a local command and normalize the result.

    This wraps subprocess.run so callers do not need try/except blocks for
    timeouts or missing executables. The command is not run through a local
    shell; args must already be tokenized.
    """

    try:
        proc = subprocess.run(
            args,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        return CommandResult(proc.returncode, proc.stdout.strip(), proc.stderr.strip())
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.strip() if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr.strip() if isinstance(exc.stderr, str) else ""
        return CommandResult(124, stdout, stderr or f"timed out after {timeout:g}s")
    except FileNotFoundError as exc:
        return CommandResult(127, "", str(exc))


def _run_bash(script: str, timeout: float) -> CommandResult:
    """Run a local Bash script through bash -c.

    This is used for local screen orchestration, where shell features like
    redirection are the clearest way to express the launch command.
    """

    return _run(["bash", "-c", script], timeout=timeout)


def _ssh_base(node: dict[str, Any]) -> list[str]:
    """Build the common SSH command prefix for a configured node.

    The prefix includes BatchMode, connection timeout, optional accept-new host
    key behavior, and the configured user@host target. Callers append the remote
    command to this list.
    """

    timeout = int(node.get("ssh_connect_timeout_sec", 3))
    batch_mode = "yes" if _str_bool(node.get("ssh_batch_mode", True)) else "no"
    args = [
        "ssh",
        "-o",
        f"BatchMode={batch_mode}",
        "-o",
        f"ConnectTimeout={timeout}",
    ]
    if _str_bool(node.get("accept_new_host_keys", True)):
        args.extend(["-o", "StrictHostKeyChecking=accept-new"])
    args.append(f"{node['ssh_user']}@{node['host']}")
    return args


def _remote_run(node: dict[str, Any], script: str, timeout: float | None = None) -> CommandResult:
    """Run a Bash snippet on a remote node over SSH.

    Args:
        node: A resolved node config dictionary.
        script: Bash code to execute remotely. It is shell-quoted before being
            passed to `bash -c` on the remote host.
        timeout: Optional total timeout for the SSH command.

    Returns:
        Normalized stdout/stderr/returncode from the SSH command.
    """

    if timeout is None:
        timeout = float(node.get("ssh_connect_timeout_sec", 3)) + 10.0
    return _run(_ssh_base(node) + ["bash -c " + shlex.quote(script)], timeout=timeout)


def _remote_preflight_script(paths: dict[str, str]) -> str:
    """Build one remote Bash script for all node preflight checks."""

    checks = [
        ("remote -x " + paths["python"], "-x", paths["python"], paths["python"]),
        ("remote -d " + paths["repo"], "-d", paths["repo"], paths["repo"]),
        ("remote -f " + paths["agent_script"], "-f", paths["agent_script"], paths["agent_script"]),
        ("remote -x " + paths["find_ublox_script"], "-x", paths["find_ublox_script"], paths["find_ublox_script"]),
        ("remote logdir parent exists", "-d", os.path.dirname(paths["logdir"]), os.path.dirname(paths["logdir"])),
        ("remote telem_dir parent exists", "-d", os.path.dirname(paths["telem_dir"]), os.path.dirname(paths["telem_dir"])),
    ]

    lines = [
        "emit_check() {",
        "  label=$1",
        "  test_arg=$2",
        "  path=$3",
        "  detail=$4",
        "  if test \"$test_arg\" \"$path\"; then",
        "    printf 'CHECK\\tOK\\t%s\\t%s\\n' \"$label\" \"$detail\"",
        "  else",
        "    printf 'CHECK\\tFAIL\\t%s\\t%s\\n' \"$label\" \"$detail\"",
        "  fi",
        "}",
    ]
    for label, test_arg, path, detail in checks:
        lines.append(
            "emit_check "
            + " ".join(shlex.quote(value) for value in [label, test_arg, path, detail])
        )

    find_ublox = shlex.quote(paths["find_ublox_script"])
    lines.extend(
        [
            "gnss_output=$(" + find_ublox + " 2>&1)",
            "gnss_rc=$?",
            "gnss_output=${gnss_output//$'\\n'/; }",
            "if [ \"$gnss_rc\" -eq 0 ]; then",
            "  printf 'GNSS\\tOK\\t%s\\n' \"$gnss_output\"",
            "else",
            "  printf 'GNSS\\tFAIL\\texit %s: %s\\n' \"$gnss_rc\" \"$gnss_output\"",
            "fi",
        ]
    )
    return "\n".join(lines)


def _remote_register_verify_script(node: dict[str, Any], manifest_path: str, role: str | None) -> str:
    """Build a remote command that verifies manifest registers."""

    repo = str(node["repo"])
    verifier = _resolve_under_repo(repo, "gnss_scripts/verify_manifest_registers.py")
    args = [
        str(node["python"]),
        verifier,
        "--manifest",
        manifest_path,
        "--json",
    ]
    if role:
        args.extend(["--role", role])
    return "\n".join(
        [
            "set -euo pipefail",
            f"cd {shlex.quote(repo)}",
            _shell_join(args),
        ]
    )


def _remote_bodnar_status_script(paths: dict[str, str], timeout_sec: float) -> str:
    """Build a remote read-only Bodnar preflight/status script."""

    checks = [
        ("bodnar python executable", "-x", paths["python"], paths["python"]),
        ("bodnar repo directory", "-d", paths["repo"], paths["repo"]),
        ("bodnar configure script", "-f", paths["script"], paths["script"]),
    ]
    lines = [
        "emit_check() {",
        "  label=$1",
        "  test_arg=$2",
        "  path=$3",
        "  detail=$4",
        "  if test \"$test_arg\" \"$path\"; then",
        "    printf 'CHECK\\tOK\\t%s\\t%s\\n' \"$label\" \"$detail\"",
        "  else",
        "    printf 'CHECK\\tFAIL\\t%s\\t%s\\n' \"$label\" \"$detail\"",
        "  fi",
        "}",
        "ok=1",
    ]
    for label, test_arg, path, detail in checks:
        lines.append(
            "emit_check "
            + " ".join(shlex.quote(value) for value in [label, test_arg, path, detail])
        )
        lines.append(f"test {shlex.quote(test_arg)} {shlex.quote(path)} || ok=0")

    cmd = _shell_join([paths["python"], paths["script"], "--status"])
    lines.extend(
        [
            "if [ \"$ok\" -eq 1 ]; then",
            f"  cd {shlex.quote(paths['repo'])}",
            f"  bodnar_output=$(timeout {float(timeout_sec):g}s {cmd} 2>&1)",
            "  bodnar_rc=$?",
            "  bodnar_output=${bodnar_output//$'\\n'/; }",
            "  if [ \"$bodnar_rc\" -eq 0 ]; then",
            "    printf 'BODNAR\\tOK\\t%s\\n' \"$bodnar_output\"",
            "  else",
            "    printf 'BODNAR\\tFAIL\\texit %s: %s\\n' \"$bodnar_rc\" \"$bodnar_output\"",
            "  fi",
            "fi",
        ]
    )
    return "\n".join(lines)


def _parse_remote_preflight(result: CommandResult) -> tuple[list[Check], bool | None]:
    """Parse remote preflight output into checks and GNSS detection state."""

    checks: list[Check] = []
    gnss_detected: bool | None = None
    if result.returncode != 0:
        checks.append(Check("remote preflight", False, _format_cmd_result(result)))
        return checks, gnss_detected

    for line in result.stdout.splitlines():
        parts = line.split("\t", 3)
        if len(parts) < 3:
            checks.append(Check("remote preflight output", False, line))
            continue
        kind = parts[0]
        status = parts[1]
        if kind == "CHECK":
            label = parts[2]
            detail = parts[3] if len(parts) > 3 else ""
            checks.append(Check(label, status == "OK", detail))
        elif kind == "GNSS":
            detail = parts[2]
            gnss_detected = status == "OK"
            checks.append(Check("gnss receiver detected", gnss_detected, detail))
        else:
            checks.append(Check("remote preflight output", False, line))

    return checks, gnss_detected


def _summarize_bodnar_status(detail: str) -> str:
    """Condense verbose lbe-1420-conf.py --status output for human status."""

    if detail.startswith("exit "):
        return detail

    fields: list[str] = []
    patterns = [
        ("fix", r"fix:\s*([^;]+)"),
        ("satellites", r"satellites:\s*([^;]+)"),
        ("GPS lock", r"GPS lock:\s*([^;]+)"),
        ("PLL lock", r"PLL lock:\s*([^;]+)"),
        ("antenna", r"antenna:\s*([^;]+)"),
        ("OUT1", r"OUT1:\s*([^;]+)"),
    ]
    for label, pattern in patterns:
        match = re.search(pattern, detail)
        if match:
            fields.append(f"{label} {match.group(1).strip()}")

    return "; ".join(fields) if fields else detail[:240]


def _parse_bodnar_status(result: CommandResult) -> tuple[list[Check], bool | None]:
    """Parse remote Bodnar status output into checks and detection state."""

    checks: list[Check] = []
    detected: bool | None = None
    if result.returncode != 0:
        checks.append(Check("bodnar preflight", False, _format_cmd_result(result)))
        return checks, detected

    for line in result.stdout.splitlines():
        parts = line.split("\t", 3)
        if len(parts) < 3:
            checks.append(Check("bodnar preflight output", False, line))
            continue
        kind = parts[0]
        status = parts[1]
        if kind == "CHECK":
            label = parts[2]
            detail = parts[3] if len(parts) > 3 else ""
            checks.append(Check(label, status == "OK", detail))
        elif kind == "BODNAR":
            detail = parts[2]
            detected = status == "OK"
            checks.append(Check("bodnar detected", detected, _summarize_bodnar_status(detail)))
        else:
            checks.append(Check("bodnar preflight output", False, line))

    return checks, detected


def _parse_register_verify(result: CommandResult) -> tuple[Check, dict[str, Any] | None]:
    """Parse verify_manifest_registers.py JSON into a compact check and report."""

    if result.returncode != 0 and not result.stdout:
        return Check("register verify", False, _format_cmd_result(result)), None
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError:
        return Check("register verify", False, _format_cmd_result(result)), None

    checked = int(report.get("checked", 0))
    matched = int(report.get("matched", 0))
    mismatches = report.get("mismatches") or []
    skipped = report.get("skipped") or []
    detail = f"{matched}/{checked} matched"
    if report.get("role"):
        detail += f" role={report['role']}"
    if skipped:
        detail += f"; {len(skipped)} unsupported keys skipped"
    if mismatches:
        preview = ", ".join(str(item.get("key", "?")) for item in mismatches[:3])
        detail += f"; mismatches: {preview}"
        if len(mismatches) > 3:
            detail += f", +{len(mismatches) - 3} more"
    if report.get("error"):
        detail += f"; {report['error']}"
    return Check("register verify", bool(report.get("ok")), detail), report


def _format_cmd_result(result: CommandResult) -> str:
    """Create a compact one-line explanation for command output.

    Successful commands usually return stdout. Failed commands include the exit
    code plus stderr/stdout so the human status report has enough context.
    """

    if result.returncode == 0:
        return result.stdout
    parts = [f"exit {result.returncode}"]
    if result.stderr:
        parts.append(result.stderr)
    elif result.stdout:
        parts.append(result.stdout)
    return ": ".join(parts)


def _shell_join(args: list[Any]) -> str:
    """Shell-quote and join command arguments."""

    return " ".join(shlex.quote(str(arg)) for arg in args)


def _utc_run_stamp() -> str:
    """Return a compact UTC timestamp used to group one start command's logs."""

    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def _timestamped_log_path(logdir: str, screen_name: str, run_stamp: str) -> str:
    """Return the per-run log path for a screen-managed process."""

    return str(Path(logdir) / f"{screen_name}_{run_stamp}.log")


def _latest_log_path(logdir: str, screen_name: str) -> str:
    """Return the stable path that points at the latest per-run log."""

    return str(Path(logdir) / f"{screen_name}.log")


def _timestamped_log_command(args: list[Any], log_path: str) -> str:
    """Build a shell command that prefixes stdout/stderr lines with UTC time."""

    awk_program = r'{ print strftime("%Y-%m-%dT%H:%M:%SZ"), $0; fflush(); }'
    return (
        "set -o pipefail; "
        f"{_shell_join(args)} 2>&1 | "
        f"TZ=UTC awk {shlex.quote(awk_program)} >> {shlex.quote(log_path)}"
    )


def _screen_grep_pattern(screen_name: str) -> str:
    """Return a screen -ls grep pattern for an exact screen session name."""

    return rf"\.{screen_name}[[:space:]]"


def _screen_verify_script(screen_name: str, log_path: str) -> list[str]:
    """Return Bash lines that verify a screen session and print diagnostics."""

    pattern = shlex.quote(_screen_grep_pattern(screen_name))
    log = shlex.quote(log_path)
    return [
        f"if ! screen -ls | grep -q -- {pattern}; then",
        f"  echo '[FAIL] screen {screen_name} not found after launch'",
        f"  echo '[INFO] log: {log_path}'",
        f"  tail -n 60 {log} 2>/dev/null || true",
        "  exit 1",
        "fi",
    ]


def _pgrep_safe_pattern(process_match: str) -> str:
    """Return a pgrep regex that does not match its own shell wrapper."""

    if not process_match:
        return process_match
    if process_match.startswith("/"):
        return "[/]" + re.escape(process_match[1:])
    return "[" + re.escape(process_match[0]) + "]" + re.escape(process_match[1:])


def _all_checks_ok(item: dict[str, Any]) -> bool:
    """Return true when every Check in a status item passed."""

    return all(check.ok for check in item.get("checks", []))


def _check_required_fields(name: str, item: dict[str, Any], fields: list[str]) -> list[Check]:
    """Check that a config object has non-empty values for required fields.

    Args:
        name: Human-readable prefix used in the check name, such as
            "server" or "nodes.winters".
        item: Config dictionary being validated.
        fields: Required keys to check.

    Returns:
        One Check per required field.
    """

    checks: list[Check] = []
    for field in fields:
        value = item.get(field)
        ok = value is not None and value != ""
        checks.append(Check(f"{name}.{field}", ok, "" if ok else "missing"))
    return checks


def _server_status(config: dict[str, Any]) -> dict[str, Any]:
    """Validate local server configuration and filesystem prerequisites.

    The GNSS server is expected to run on the orchestration/head node, so these
    checks use local filesystem calls instead of SSH. The returned dictionary has
    the same shape as node reports so CLI and future DAQ callers can process
    server/node results uniformly.
    """

    defaults = config.get("defaults", {})
    server = _merge(defaults, config.get("server", {}))
    if "screen" not in server and "server_screen" in server:
        server["screen"] = server["server_screen"]

    # Server script/logdir paths may be absolute, but the usual case is that
    # they are stored relative to the server repo.
    repo = str(server.get("repo", ""))
    script = str(server.get("script") or server.get("server_script") or "server_v1.py")
    server_script = _resolve_under_repo(repo, script) if repo else script
    logdir = str(server.get("logdir", "logging"))
    logdir_path = _resolve_under_repo(repo, logdir) if repo else logdir
    receiver_manifest = server.get("receiver_manifest")
    receiver_manifest_path = ""
    if receiver_manifest:
        receiver_manifest_path = _resolve_under_repo(repo, str(receiver_manifest)) if repo else str(receiver_manifest)

    # Required-field checks catch malformed inventory entries before we try to
    # use those values in subprocess commands.
    checks = _check_required_fields(
        "server",
        server,
        ["daq_name", "host", "python", "repo", "script", "logdir", "screen", "bind_addr"],
    )

    # These checks are local because the GNSS server is expected to run on the
    # orchestration/head node.
    if server.get("python"):
        checks.append(Check("server python executable", os.access(str(server["python"]), os.X_OK), str(server["python"])))
    if repo:
        checks.append(Check("server repo directory", Path(repo).is_dir(), repo))
    checks.append(Check("server script file", Path(server_script).is_file(), server_script))
    checks.append(Check("server logdir parent", Path(logdir_path).parent.is_dir(), logdir_path))
    if receiver_manifest_path:
        checks.append(Check("server receiver manifest", Path(receiver_manifest_path).is_file(), receiver_manifest_path))

    return {
        "kind": "server",
        "key": "server",
        "daq_name": server.get("daq_name", "server"),
        "host": server.get("host", "localhost"),
        "checks": checks,
        "resolved": {
            "python": server.get("python"),
            "repo": repo,
            "script": server_script,
            "logdir": logdir_path,
            "receiver_manifest": receiver_manifest_path,
        },
    }


def _node_status(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
    *,
    local_only: bool,
    verify_registers: bool = False,
    mode_settings: dict[str, Any] | None = None,
    check_bodnar: bool = True,
) -> dict[str, Any]:
    """Validate one DAQ/GNSS node entry.

    Args:
        key: Inventory key from the "nodes" mapping.
        raw_node: Node-specific config from the inventory.
        defaults: Shared defaults from the inventory.
        local_only: If true, skip all SSH and receiver-detection checks.

    Returns:
        A structured status dictionary containing identity fields, resolved
        paths, check results, and the receiver-detection state.
    """

    node = _merge(defaults, raw_node)
    present = _present(node, True)

    # Resolve the paths that future start/stop commands will use. The status
    # command reports these resolved values so config mistakes are easy to spot.
    repo = str(node.get("repo", ""))
    agent_script = _resolve_under_repo(repo, str(node.get("agent_script", "agent_v1.py"))) if repo else ""
    find_ublox = _resolve_under_repo(repo, str(node.get("find_ublox_script", "gnss_scripts/find_ublox.sh"))) if repo else ""
    logdir = _resolve_under_repo(repo, str(node.get("logdir", "logging"))) if repo else ""
    telem_dir = _resolve_under_repo(repo, str(node.get("telem_dir", "telem"))) if repo else ""
    bodnar = _bodnar_config(defaults, node)
    bodnar_present = _present(bodnar, False)
    bodnar_paths = _bodnar_paths(defaults, node)

    # These fields are the minimum needed to start an agent in the future:
    # SSH target, Python executable, repo/script locations, gRPC addresses, and
    # logging/telemetry destinations.
    checks = _check_required_fields(
        f"nodes.{key}",
        node,
        [
            "daq_name",
            "host",
            "ssh_user",
            "python",
            "repo",
            "agent_script",
            "find_ublox_script",
            "logdir",
            "telem_dir",
            "cast_addr",
            "ctrl_addr",
            "verbosity",
        ],
    )

    gnss_detected: bool | None = None
    bodnar_detected: bool | None = None
    register_verify_report: dict[str, Any] | None = None
    if check_bodnar and bodnar_present:
        checks.extend(
            _check_required_fields(
                f"nodes.{key}.bodnar",
                bodnar,
                ["repo", "python", "configure_script", "out1_enabled", "frequency_hz", "gnss"],
            )
        )
    elif check_bodnar:
        checks.append(Check("bodnar present", True, "not present"))

    if not present:
        checks.append(Check("node present", True, "not present; remote checks skipped"))
    elif local_only:
        # Local-only mode is useful during config editing and CI because it does
        # not require network access or SSH keys.
        checks.append(Check("remote checks", True, "skipped by --local-only"))
        if check_bodnar and bodnar_present:
            checks.append(Check("bodnar remote checks", True, "skipped by --local-only"))
    else:
        # First prove SSH works. If it does not, the remaining remote path checks
        # would all fail for the same reason and make the report noisier.
        checks.append(Check("ssh reachable", False, "not checked"))
        ssh_result = _remote_run(node, "true")
        checks[-1] = Check("ssh reachable", ssh_result.returncode == 0, _format_cmd_result(ssh_result))

        if ssh_result.returncode == 0:
            preflight_script = _remote_preflight_script(
                {
                    "python": str(node["python"]),
                    "repo": repo,
                    "agent_script": agent_script,
                    "find_ublox_script": find_ublox,
                    "logdir": logdir,
                    "telem_dir": telem_dir,
                }
            )
            preflight_result = _remote_run(node, preflight_script, timeout=20.0)
            remote_checks, gnss_detected = _parse_remote_preflight(preflight_result)
            checks.extend(remote_checks)

            if check_bodnar and bodnar_present:
                bodnar_script = _remote_bodnar_status_script(
                    bodnar_paths,
                    float(bodnar.get("timeout_sec", 20.0)),
                )
                bodnar_result = _remote_run(node, bodnar_script, timeout=float(bodnar.get("timeout_sec", 20.0)) + 10.0)
                bodnar_checks, bodnar_detected = _parse_bodnar_status(bodnar_result)
                checks.extend(bodnar_checks)

            if verify_registers:
                settings = mode_settings or {"timing_mode": "differential", "receiver_manifest": "manifest_f9t.json5"}
                manifest = str(settings.get("receiver_manifest") or "manifest_f9t.json5")
                manifest_path = _resolve_under_repo(repo, manifest)
                role = "timing_only" if settings.get("timing_mode") == "absolute" else None
                verify_script = _remote_register_verify_script(node, manifest_path, role)
                verify_result = _remote_run(node, verify_script, timeout=90.0)
                verify_check, register_verify_report = _parse_register_verify(verify_result)
                checks.append(verify_check)

    result = {
        "kind": "node",
        "key": key,
        "daq_name": node.get("daq_name", key),
        "host": node.get("host", key),
        "present": present,
        "required": _str_bool(node.get("required", False)),
        "gnss_detected": gnss_detected,
        "bodnar_detected": bodnar_detected,
        "checks": checks,
        "resolved": {
            "python": node.get("python"),
            "repo": repo,
            "agent_script": agent_script,
            "find_ublox_script": find_ublox,
            "logdir": logdir,
            "telem_dir": telem_dir,
            "cast_addr": node.get("cast_addr"),
            "ctrl_addr": node.get("ctrl_addr"),
            "verbosity": node.get("verbosity"),
            "bodnar": {
                "present": bodnar_present,
                "required": _str_bool(bodnar.get("required", False)),
                "python": bodnar_paths["python"],
                "repo": bodnar_paths["repo"],
                "configure_script": bodnar_paths["script"],
                "out1_enabled": bodnar.get("out1_enabled"),
                "frequency_hz": bodnar.get("frequency_hz"),
                "gnss": bodnar.get("gnss"),
            },
        },
    }
    if register_verify_report is not None:
        result["register_verify"] = register_verify_report
    return result


def status_gnss(
    config_path: str | os.PathLike[str] = DEFAULT_CONFIG,
    *,
    nodes: list[str] | None = None,
    include_disabled: bool = False,
    local_only: bool = False,
    verify_registers: bool = False,
    mode: str = "differential",
) -> dict[str, Any]:
    """Build a read-only GNSS deployment status report.

    This function is importable by a future DAQ controller. The CLI below is
    only a thin presentation layer around this structured report.

    Args:
        config_path: Deployment inventory path.
        nodes: Optional list of node keys, hostnames, or DAQ names to include.
        include_disabled: Include disabled nodes in the report.
        local_only: Skip SSH checks and validate only local/config structure.

    Returns:
        A report dictionary with the config path and per-server/per-node
        structured status entries.
    """

    config = load_config(config_path)
    defaults = config.get("defaults", {})
    raw_nodes = config.get("nodes", {})
    selected = set(nodes or [])
    mode_settings = _mode_settings(config, mode)

    # Always include the server status. Node filtering applies only to DAQ nodes.
    results = [_server_status(config)]
    for key, node in raw_nodes.items():
        # Let users filter by inventory key, SSH host, or DAQ/display name.
        if selected and key not in selected and node.get("host") not in selected and node.get("daq_name") not in selected:
            continue
        present = _present(_merge(defaults, node), True)
        if not present and not include_disabled:
            continue
        results.append(
            _node_status(
                key,
                node,
                defaults,
                local_only=local_only,
                verify_registers=verify_registers,
                mode_settings=mode_settings,
            )
        )

    return {"config_path": str(config_path), "mode": mode, "verify_registers": verify_registers, "results": results}


def _selected_node_items(
    config: dict[str, Any],
    nodes: list[str] | None,
    include_disabled: bool = False,
) -> list[tuple[str, dict[str, Any]]]:
    """Return configured node entries matching optional user filters.

    Filters may match the inventory key, SSH host, or DAQ/display name. This is
    shared by status/start so the CLI behaves consistently.
    """

    defaults = config.get("defaults", {})
    selected = set(nodes or [])
    out: list[tuple[str, dict[str, Any]]] = []
    for key, raw_node in config.get("nodes", {}).items():
        if selected and key not in selected and raw_node.get("host") not in selected and raw_node.get("daq_name") not in selected:
            continue
        node = _merge(defaults, raw_node)
        if not _present(node, True) and not include_disabled:
            continue
        out.append((key, raw_node))
    return out


def _server_launch_script(server_status: dict[str, Any], run_stamp: str) -> tuple[str, str]:
    """Build the local Bash script that starts the GNSS server in screen.

    Returns:
        A tuple of (script, log_path). The script is suitable for bash -c.
    """

    server = server_status["config"]
    resolved = server_status["resolved"]
    screen = str(server.get("screen") or server.get("server_screen") or "gnss_server")
    logdir = str(resolved["logdir"])
    log_path = _timestamped_log_path(logdir, screen, run_stamp)
    latest_log = _latest_log_path(logdir, screen)

    server_args = [
        server["python"],
        "-u",
        resolved["script"],
        "--ip",
        server.get("bind_addr", "0.0.0.0:50051"),
        "--timing-mode",
        server.get("timing_mode", "differential"),
        "-v",
        server.get("verbosity", 2),
    ]
    if resolved.get("receiver_manifest"):
        server_args.extend(["--config", resolved["receiver_manifest"]])

    inner = _timestamped_log_command(server_args, log_path)
    script = "\n".join(
        [
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(logdir)}",
            f"ln -sfn {shlex.quote(Path(log_path).name)} {shlex.quote(latest_log)}",
            f"screen -S {shlex.quote(screen)} -X quit >/dev/null 2>&1 || true",
            "sleep 0.5",
            f"screen -dmS {shlex.quote(screen)} bash -c {shlex.quote(inner)}",
            "sleep 1",
            *_screen_verify_script(screen, log_path),
        ]
    )
    return script, log_path


def _start_server(config: dict[str, Any], *, dry_run: bool, mode: str = "differential", run_stamp: str) -> StartResult:
    """Start the local GNSS server, or describe the command in dry-run mode."""

    mode_config = _mode_settings(config, mode)
    if mode_config.get("receiver_manifest"):
        config = dict(config)
        config["server"] = dict(config.get("server", {}))
        config["server"]["receiver_manifest"] = mode_config["receiver_manifest"]
    config = dict(config)
    config["server"] = dict(config.get("server", {}))
    config["server"]["timing_mode"] = mode_config.get("timing_mode", mode)

    server_status = _server_status(config)
    defaults = config.get("defaults", {})
    server = _merge(defaults, config.get("server", {}))
    if "screen" not in server and "server_screen" in server:
        server["screen"] = server["server_screen"]
    server_status["config"] = server
    script, log_path = _server_launch_script(server_status, run_stamp)
    command = "bash -c " + shlex.quote(script)

    if not _all_checks_ok(server_status):
        return StartResult(
            kind="server",
            key="server",
            daq_name=str(server_status["daq_name"]),
            host=str(server_status["host"]),
            status="failed",
            detail="server preflight failed",
            required=True,
            command=command,
            script=script,
        )

    if dry_run:
        return StartResult(
            kind="server",
            key="server",
            daq_name=str(server_status["daq_name"]),
            host=str(server_status["host"]),
            status="dry-run",
            detail=f"would start screen {server.get('screen')} in {server.get('timing_mode')} mode with log {log_path}",
            required=True,
            command=command,
            script=script,
        )

    result = _run_bash(script, timeout=15.0)
    ok = result.returncode == 0
    return StartResult(
        kind="server",
        key="server",
        daq_name=str(server_status["daq_name"]),
        host=str(server_status["host"]),
        status="started" if ok else "failed",
        detail=f"log {log_path}" if ok else _format_cmd_result(result),
        required=True,
        command=command,
        script=script,
    )


def _remote_agent_launch_script(node_status: dict[str, Any], run_stamp: str) -> tuple[str, str]:
    """Build the remote Bash script that starts one GNSS agent in screen."""

    node = node_status["config"]
    resolved = node_status["resolved"]
    screen = str(node.get("agent_screen", "gnss_agent"))
    logdir = str(resolved["logdir"])
    telem_dir = str(resolved["telem_dir"])
    log_path = _timestamped_log_path(logdir, screen, run_stamp)
    latest_log = _latest_log_path(logdir, screen)

    agent_args = [
        node["python"],
        "-u",
        resolved["agent_script"],
        "--cast_addr",
        resolved["cast_addr"],
        "--ctrl_addr",
        resolved["ctrl_addr"],
        "--log-dir",
        resolved["logdir"],
        "--telem-dir",
        resolved["telem_dir"],
        "-v",
        resolved["verbosity"],
    ]
    inner = _timestamped_log_command(agent_args, log_path)
    script = "\n".join(
        [
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(logdir)} {shlex.quote(telem_dir)}",
            f"ln -sfn {shlex.quote(Path(log_path).name)} {shlex.quote(latest_log)}",
            f"screen -S {shlex.quote(screen)} -X quit >/dev/null 2>&1 || true",
            "sleep 0.5",
            f"cd {shlex.quote(resolved['repo'])}",
            f"screen -dmS {shlex.quote(screen)} bash -c {shlex.quote(inner)}",
            "sleep 1",
            *_screen_verify_script(screen, log_path),
        ]
    )
    return script, log_path


def _start_node(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
    *,
    dry_run: bool,
    run_stamp: str,
) -> StartResult:
    """Start one remote GNSS agent, or describe the action in dry-run mode."""

    node_status = _node_status(key, raw_node, defaults, local_only=False, check_bodnar=False)
    node = _merge(defaults, raw_node)
    node_status["config"] = node
    script, log_path = _remote_agent_launch_script(node_status, run_stamp)
    command = _shell_join(_ssh_base(node) + ["bash -c " + shlex.quote(script)])
    required = _str_bool(node.get("required", False))
    start_only_if_receiver_detected = _str_bool(node.get("start_only_if_receiver_detected", True))

    if start_only_if_receiver_detected and node_status.get("gnss_detected") is False:
        return StartResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="skipped",
            detail="GNSS receiver not detected",
            required=required,
            command=command,
            script=script,
        )

    if not _all_checks_ok(node_status):
        return StartResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="failed",
            detail="node preflight failed",
            required=required,
            command=command,
            script=script,
        )

    if dry_run:
        return StartResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="dry-run",
            detail=f"would start screen {node.get('agent_screen')} with log {log_path}",
            required=required,
            command=command,
            script=script,
        )

    result = _remote_run(node, script, timeout=20.0)
    ok = result.returncode == 0
    return StartResult(
        kind="node",
        key=key,
        daq_name=str(node_status["daq_name"]),
        host=str(node_status["host"]),
        status="started" if ok else "failed",
        detail=f"log {log_path}" if ok else _format_cmd_result(result),
        required=required,
        command=command,
        script=script,
    )


def _bodnar_configure_script(paths: dict[str, str], bodnar: dict[str, Any]) -> str:
    """Build a remote script that configures a Leo Bodnar LBE-1420."""

    commands = ["set -e", f"cd {shlex.quote(paths['repo'])}"]
    out1_enabled = bodnar.get("out1_enabled")
    frequency = bodnar.get("frequency_hz")
    gnss = bodnar.get("gnss")
    if out1_enabled is not None:
        commands.append(_shell_join([paths["python"], paths["script"], "--enable", 1 if _str_bool(out1_enabled) else 0]))
    if frequency is not None and frequency != "":
        commands.append(_shell_join([paths["python"], paths["script"], "--f1", frequency]))
    if gnss:
        commands.append(_shell_join([paths["python"], paths["script"], "--gnss", gnss]))
    commands.append(_shell_join([paths["python"], paths["script"], "--status"]))
    return "\n".join(commands)


def _configure_bodnar(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
    *,
    dry_run: bool,
) -> StartResult:
    """Configure one remote Leo Bodnar receiver, or describe the action."""

    node = _merge(defaults, raw_node)
    bodnar = _bodnar_config(defaults, node)
    paths = _bodnar_paths(defaults, node)
    present = _present(bodnar, False)
    required = _str_bool(bodnar.get("required", False))
    daq_name = str(node.get("daq_name", key))
    host = str(node.get("host", key))

    script = _bodnar_configure_script(paths, bodnar)
    command = _shell_join(_ssh_base(node) + ["bash -c " + shlex.quote(script)])
    out1 = bodnar.get("out1_enabled")
    out1_detail = f", out1={'on' if _str_bool(out1) else 'off'}" if out1 is not None else ""
    detail = f"{bodnar.get('frequency_hz')} Hz, gnss={bodnar.get('gnss')}{out1_detail}"

    if not present:
        return StartResult(
            kind="bodnar",
            key=key,
            daq_name=daq_name,
            host=host,
            status="skipped",
            detail="Bodnar not present",
            required=required,
            command=command,
            script=script,
        )

    preflight_script = _remote_bodnar_status_script(paths, float(bodnar.get("timeout_sec", 20.0)))
    if dry_run:
        return StartResult(
            kind="bodnar",
            key=key,
            daq_name=daq_name,
            host=host,
            status="dry-run",
            detail=f"would configure Bodnar {detail}",
            required=required,
            command=command,
            script=script,
        )

    ssh_result = _remote_run(node, "true")
    if ssh_result.returncode != 0:
        return StartResult(
            kind="bodnar",
            key=key,
            daq_name=daq_name,
            host=host,
            status="failed",
            detail="ssh unreachable: " + _format_cmd_result(ssh_result),
            required=required,
            command=command,
            script=script,
        )

    preflight_result = _remote_run(node, preflight_script, timeout=float(bodnar.get("timeout_sec", 20.0)) + 10.0)
    preflight_checks, detected = _parse_bodnar_status(preflight_result)
    if detected is not True or not all(check.ok for check in preflight_checks):
        failed = next((check for check in preflight_checks if not check.ok), None)
        reason = failed.detail if failed else _format_cmd_result(preflight_result)
        return StartResult(
            kind="bodnar",
            key=key,
            daq_name=daq_name,
            host=host,
            status="failed",
            detail=f"Bodnar preflight failed: {reason}",
            required=required,
            command=command,
            script=script,
        )

    result = _remote_run(node, script, timeout=float(bodnar.get("timeout_sec", 20.0)) + 20.0)
    ok = result.returncode == 0
    return StartResult(
        kind="bodnar",
        key=key,
        daq_name=daq_name,
        host=host,
        status="configured" if ok else "failed",
        detail=detail if ok else _format_cmd_result(result),
        required=required,
        command=command,
        script=script,
    )


def start_gnss(
    config_path: str | os.PathLike[str] = DEFAULT_CONFIG,
    *,
    nodes: list[str] | None = None,
    dry_run: bool = False,
    include_disabled: bool = False,
    mode: str = "differential",
    configure_bodnar: bool = False,
) -> dict[str, Any]:
    """Start the GNSS server and configured remote agents.

    Args:
        config_path: Deployment inventory path.
        nodes: Optional list of node keys, hostnames, or DAQ names to start.
        dry_run: If true, report launch commands without executing them.
        include_disabled: Include disabled nodes when selecting targets.

    Returns:
        A structured start report with one StartResult for the server and one
        for each selected node.
    """

    config = load_config(config_path)
    defaults = config.get("defaults", {})
    run_stamp = _utc_run_stamp()
    results = [_start_server(config, dry_run=dry_run, mode=mode, run_stamp=run_stamp)]

    if results[0].status == "failed":
        return {
            "config_path": str(config_path),
            "dry_run": dry_run,
            "mode": mode,
            "bodnar": configure_bodnar,
            "run_stamp": run_stamp,
            "results": results,
        }

    for key, raw_node in _selected_node_items(config, nodes, include_disabled=include_disabled):
        if configure_bodnar:
            bodnar_result = _configure_bodnar(key, raw_node, defaults, dry_run=dry_run)
            results.append(bodnar_result)
            if bodnar_result.required and bodnar_result.status == "failed":
                node = _merge(defaults, raw_node)
                results.append(
                    StartResult(
                        kind="node",
                        key=key,
                        daq_name=str(node.get("daq_name", key)),
                        host=str(node.get("host", key)),
                        status="skipped",
                        detail="required Bodnar configuration failed",
                        required=_str_bool(node.get("required", False)),
                    )
                )
                continue
        results.append(_start_node(key, raw_node, defaults, dry_run=dry_run, run_stamp=run_stamp))

    return {
        "config_path": str(config_path),
        "dry_run": dry_run,
        "mode": mode,
        "bodnar": configure_bodnar,
        "run_stamp": run_stamp,
        "results": results,
    }


def _stop_script(screen_name: str, process_match: str, grace_sec: int, log_path: str | None) -> str:
    """Build a Bash script that gracefully stops a screen-managed process.

    The script first sends Ctrl+C to matching screen sessions so Python can run
    its normal signal handlers. It then sends SIGTERM to any process still
    matching the configured script path, waits up to shutdown_grace_sec, and
    removes stale screen sessions.
    """

    screen = shlex.quote(screen_name)
    match = shlex.quote(_pgrep_safe_pattern(process_match))
    log = shlex.quote(log_path) if log_path else ""
    lines = [
        "set -euo pipefail",
        f"SCREEN_NAME={screen}",
        f"PROCESS_MATCH={match}",
        f"GRACE_SEC={int(grace_sec)}",
        "screen_sessions() {",
        "  screen -ls 2>/dev/null | awk -v n=\"$SCREEN_NAME\" '$1 ~ (\"\\\\.\" n \"$\") {print $1}'",
        "}",
        "had_session=0",
        "while read -r sid; do",
        "  [ -n \"$sid\" ] || continue",
        "  had_session=1",
        "  screen -S \"$sid\" -X stuff $'\\003' >/dev/null 2>&1 || true",
        "done < <(screen_sessions)",
        "pkill -TERM -f -- \"$PROCESS_MATCH\" >/dev/null 2>&1 || true",
        "for _ in $(seq 1 \"$GRACE_SEC\"); do",
        "  if ! pgrep -f -- \"$PROCESS_MATCH\" >/dev/null 2>&1; then",
        "    break",
        "  fi",
        "  sleep 1",
        "done",
        "while read -r sid; do",
        "  [ -n \"$sid\" ] || continue",
        "  screen -S \"$sid\" -X quit >/dev/null 2>&1 || true",
        "done < <(screen_sessions)",
        "screen -wipe >/dev/null 2>&1 || true",
        "if screen_sessions | grep -q .; then",
        "  echo \"[FAIL] screen $SCREEN_NAME still present after stop\"",
    ]
    if log_path:
        lines.extend(
            [
                f"  echo '[INFO] log: {log_path}'",
                f"  tail -n 60 {log} 2>/dev/null || true",
            ]
        )
    lines.extend(
        [
            "  exit 1",
            "fi",
            "if pgrep -f -- \"$PROCESS_MATCH\" >/dev/null 2>&1; then",
            "  echo '[FAIL] process still running after stop'",
        ]
    )
    if log_path:
        lines.extend(
            [
                f"  echo '[INFO] log: {log_path}'",
                f"  tail -n 60 {log} 2>/dev/null || true",
            ]
        )
    lines.extend(
        [
            "  exit 1",
            "fi",
            "if [ \"$had_session\" -eq 1 ]; then",
            "  echo \"[OK] stopped $SCREEN_NAME\"",
            "else",
            "  echo \"[OK] $SCREEN_NAME was not running\"",
            "fi",
        ]
    )
    return "\n".join(lines)


def _server_stop_script(server_status: dict[str, Any]) -> tuple[str, str]:
    """Build the local Bash script that stops the GNSS server."""

    server = server_status["config"]
    resolved = server_status["resolved"]
    screen = str(server.get("screen") or server.get("server_screen") or "gnss_server")
    log_path = str(Path(str(resolved["logdir"])) / f"{screen}.log")
    grace_sec = int(server.get("shutdown_grace_sec", 5))
    return _stop_script(screen, str(resolved["script"]), grace_sec, log_path), log_path


def _stop_server(config: dict[str, Any], *, dry_run: bool) -> StopResult:
    """Stop the local GNSS server, or describe the command in dry-run mode."""

    server_status = _server_status(config)
    defaults = config.get("defaults", {})
    server = _merge(defaults, config.get("server", {}))
    if "screen" not in server and "server_screen" in server:
        server["screen"] = server["server_screen"]
    server_status["config"] = server
    script, log_path = _server_stop_script(server_status)
    command = "bash -c " + shlex.quote(script)

    if dry_run:
        return StopResult(
            kind="server",
            key="server",
            daq_name=str(server_status["daq_name"]),
            host=str(server_status["host"]),
            status="dry-run",
            detail=f"would stop screen {server.get('screen')} and process {server_status['resolved']['script']}",
            required=True,
            command=command,
            script=script,
        )

    result = _run_bash(script, timeout=float(server.get("shutdown_grace_sec", 5)) + 10.0)
    ok = result.returncode == 0
    return StopResult(
        kind="server",
        key="server",
        daq_name=str(server_status["daq_name"]),
        host=str(server_status["host"]),
        status="stopped" if ok else "failed",
        detail=f"log {log_path}" if ok else _format_cmd_result(result),
        required=True,
        command=command,
        script=script,
    )


def _node_stop_status(key: str, raw_node: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Resolve the node fields needed for stop without running full preflight."""

    node = _merge(defaults, raw_node)
    repo = str(node.get("repo", ""))
    agent_script = _resolve_under_repo(repo, str(node.get("agent_script", "agent_v1.py"))) if repo else ""
    logdir = _resolve_under_repo(repo, str(node.get("logdir", "logging"))) if repo else ""
    return {
        "kind": "node",
        "key": key,
        "daq_name": node.get("daq_name", key),
        "host": node.get("host", key),
        "required": _str_bool(node.get("required", False)),
        "resolved": {
            "agent_script": agent_script,
            "logdir": logdir,
        },
        "config": node,
    }


def _remote_agent_stop_script(node_status: dict[str, Any]) -> tuple[str, str]:
    """Build the remote Bash script that stops one GNSS agent."""

    node = node_status["config"]
    resolved = node_status["resolved"]
    screen = str(node.get("agent_screen", "gnss_agent"))
    log_path = str(Path(str(resolved["logdir"])) / f"{screen}.log")
    grace_sec = int(node.get("shutdown_grace_sec", 5))
    return _stop_script(screen, str(resolved["agent_script"]), grace_sec, log_path), log_path


def _stop_node(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
    *,
    dry_run: bool,
) -> StopResult:
    """Stop one remote GNSS agent, or describe the action in dry-run mode."""

    node_status = _node_stop_status(key, raw_node, defaults)
    node = node_status["config"]
    script, log_path = _remote_agent_stop_script(node_status)
    command = _shell_join(_ssh_base(node) + ["bash -c " + shlex.quote(script)])
    required = _str_bool(node.get("required", False))

    if dry_run:
        return StopResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="dry-run",
            detail=f"would stop screen {node.get('agent_screen')} and process {node_status['resolved']['agent_script']}",
            required=required,
            command=command,
            script=script,
        )

    ssh_result = _remote_run(node, "true")
    if ssh_result.returncode != 0:
        return StopResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="failed",
            detail="ssh unreachable: " + _format_cmd_result(ssh_result),
            required=required,
            command=command,
            script=script,
        )

    result = _remote_run(node, script, timeout=float(node.get("shutdown_grace_sec", 5)) + 10.0)
    ok = result.returncode == 0
    return StopResult(
        kind="node",
        key=key,
        daq_name=str(node_status["daq_name"]),
        host=str(node_status["host"]),
        status="stopped" if ok else "failed",
        detail=f"log {log_path}" if ok else _format_cmd_result(result),
        required=required,
        command=command,
        script=script,
    )


def stop_gnss(
    config_path: str | os.PathLike[str] = DEFAULT_CONFIG,
    *,
    nodes: list[str] | None = None,
    dry_run: bool = False,
    include_disabled: bool = False,
    server_only: bool = False,
    agents_only: bool = False,
) -> dict[str, Any]:
    """Stop GNSS agents and, when appropriate, the local GNSS server.

    By default, stopping the full deployment stops all selected agents first and
    then stops the local server. If one or more --node filters are supplied, the
    default is intentionally narrower: only those agents are stopped, so a
    single-node maintenance action does not accidentally take down the server.
    """

    config = load_config(config_path)
    defaults = config.get("defaults", {})
    results: list[StopResult] = []

    stop_agents = not server_only
    stop_server = server_only or (not agents_only and not nodes)

    if stop_agents:
        for key, raw_node in _selected_node_items(config, nodes, include_disabled=include_disabled):
            results.append(_stop_node(key, raw_node, defaults, dry_run=dry_run))

    if stop_server:
        results.append(_stop_server(config, dry_run=dry_run))

    return {
        "config_path": str(config_path),
        "dry_run": dry_run,
        "server_only": server_only,
        "agents_only": agents_only,
        "results": results,
    }


def _check_status(checks: list[Check]) -> str:
    """Collapse a list of checks into a human-readable overall state.

    Returns "OK" only when every check passed; otherwise returns "FAIL".
    """

    return "OK" if all(c.ok for c in checks) else "FAIL"


def _print_status(report: dict[str, Any]) -> None:
    """Print the status report in a compact human-readable format.

    This is for direct terminal use. Higher-level automation should prefer
    status_gnss() or the CLI's --json output.
    """

    print(f"Config: {report['config_path']}")
    if report.get("verify_registers"):
        print(f"Timing: {report.get('mode', 'differential')}")
        print("Register verify: enabled")
    print()
    for item in report["results"]:
        label = item["daq_name"]
        host = item["host"]
        status = _check_status(item["checks"])
        print(f"{status:4} {item['kind']:6} {item['key']:12} {label:18} host={host}")
        for check in item["checks"]:
            mark = "OK" if check.ok else "FAIL"
            detail = f" -- {check.detail}" if check.detail else ""
            print(f"  {mark:4} {check.name}{detail}")
        print()


def _print_start(report: dict[str, Any]) -> None:
    """Print a start report in a compact human-readable format."""

    mode = "dry-run" if report.get("dry_run") else "start"
    print(f"Config: {report['config_path']}")
    print(f"Mode:   {mode}")
    if report.get("mode"):
        print(f"Timing: {report['mode']}")
    print()
    for item in report["results"]:
        print(f"{item.status:8} {item.kind:6} {item.key:12} {item.daq_name:18} host={item.host}")
        if item.detail:
            print(f"  detail: {item.detail}")
        if report.get("dry_run") and item.script:
            if item.kind in {"node", "bodnar"}:
                print(f"  target: ssh {item.host}")
                print("  remote script:")
            else:
                print("  local script:")
            for line in item.script.splitlines():
                print(f"    {line}")
        print()


def _print_stop(report: dict[str, Any]) -> None:
    """Print a stop report in a compact human-readable format."""

    mode = "dry-run" if report.get("dry_run") else "stop"
    print(f"Config: {report['config_path']}")
    print(f"Mode:   {mode}")
    print()
    for item in report["results"]:
        print(f"{item.status:8} {item.kind:6} {item.key:12} {item.daq_name:18} host={item.host}")
        if item.detail:
            print(f"  detail: {item.detail}")
        if report.get("dry_run") and item.script:
            if item.kind == "node":
                print(f"  target: ssh {item.host}")
                print("  remote script:")
            else:
                print("  local script:")
            for line in item.script.splitlines():
                print(f"    {line}")
        print()


def _jsonable(report: dict[str, Any]) -> dict[str, Any]:
    """Convert dataclass values to plain dicts for JSON output.

    Status reports contain Check dataclass instances. Start/stop reports contain
    action dataclass instances. json.dumps cannot serialize those directly, so
    this creates an equivalent plain-Python object.
    """

    out = {k: v for k, v in report.items() if k != "results"}
    out["results"] = []
    for item in report["results"]:
        if isinstance(item, (StartResult, StopResult)):
            copied = item.__dict__
        else:
            copied = dict(item)
            copied["checks"] = [check.__dict__ for check in item["checks"]]
        out["results"].append(copied)
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the orchestration CLI.

    Args:
        argv: Optional argument list for tests or programmatic invocation. If
            omitted, argparse reads sys.argv.
    """

    parser = argparse.ArgumentParser(description="GNSS deployment orchestrator")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help=f"path to deployment JSON5 config (default: {DEFAULT_CONFIG})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="validate config and check GNSS deployment prerequisites")
    status.add_argument("--node", action="append", default=[], help="limit status to a node key, host, or DAQ name")
    status.add_argument("--mode", choices=["differential", "absolute"], default="differential", help="GNSS timing mode for register verification")
    status.add_argument("--verify-registers", action="store_true", help="read receiver CFG registers and compare against the selected manifest")
    status.add_argument("--include-disabled", action="store_true", help="include nodes marked present=false")
    status.add_argument("--local-only", action="store_true", help="skip SSH and remote checks")
    status.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    start = sub.add_parser("start", help="start the GNSS server and selected agents")
    start.add_argument("--node", action="append", default=[], help="limit start to a node key, host, or DAQ name")
    start.add_argument("--mode", choices=["differential", "absolute"], default="differential", help="GNSS timing mode")
    start.add_argument("--bodnar", action="store_true", help="configure Leo Bodnar LBE-1420 devices before starting agents")
    start.add_argument("--include-disabled", action="store_true", help="include nodes marked present=false")
    start.add_argument("--dry-run", action="store_true", help="show launch commands without running them")
    start.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    stop = sub.add_parser("stop", help="stop selected GNSS agents and optionally the server")
    stop.add_argument("--node", action="append", default=[], help="limit stop to a node key, host, or DAQ name")
    stop.add_argument("--include-disabled", action="store_true", help="include nodes marked present=false")
    stop.add_argument("--server-only", action="store_true", help="stop only the local GNSS server")
    stop.add_argument("--agents-only", action="store_true", help="stop agents without stopping the local GNSS server")
    stop.add_argument("--dry-run", action="store_true", help="show stop commands without running them")
    stop.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Returns:
        Process exit code. For status, the code is nonzero only if the server or
        a node marked required fails validation.
    """

    args = parse_args(argv)
    if args.command == "status":
        report = status_gnss(
            args.config,
            nodes=args.node,
            include_disabled=args.include_disabled,
            local_only=args.local_only,
            verify_registers=args.verify_registers,
            mode=args.mode,
        )
        if args.json:
            print(json.dumps(_jsonable(report), indent=2, sort_keys=True))
        else:
            _print_status(report)

        # For now, only the server and required nodes affect the process exit
        # code. Optional nodes can fail status checks without making the command
        # unusable for a higher-level DAQ controller.
        failed_required = False
        for item in report["results"]:
            if item["kind"] == "server" or item.get("required"):
                failed_required = failed_required or not all(check.ok for check in item["checks"])
        return 1 if failed_required else 0

    if args.command == "start":
        report = start_gnss(
            args.config,
            nodes=args.node,
            dry_run=args.dry_run,
            include_disabled=args.include_disabled,
            mode=args.mode,
            configure_bodnar=args.bodnar,
        )
        if args.json:
            print(json.dumps(_jsonable(report), indent=2, sort_keys=True))
        else:
            _print_start(report)

        failed_required = False
        for item in report["results"]:
            if item.kind == "server" or item.required:
                failed_required = failed_required or item.status == "failed"
        return 1 if failed_required else 0

    if args.command == "stop":
        if args.server_only and args.agents_only:
            raise SystemExit("--server-only and --agents-only cannot be used together")
        if args.server_only and args.node:
            raise SystemExit("--node cannot be used with --server-only")

        report = stop_gnss(
            args.config,
            nodes=args.node,
            dry_run=args.dry_run,
            include_disabled=args.include_disabled,
            server_only=args.server_only,
            agents_only=args.agents_only,
        )
        if args.json:
            print(json.dumps(_jsonable(report), indent=2, sort_keys=True))
        else:
            _print_stop(report)

        failed_required = False
        for item in report["results"]:
            if item.kind == "server" or item.required:
                failed_required = failed_required or item.status == "failed"
        return 1 if failed_required else 0

    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
