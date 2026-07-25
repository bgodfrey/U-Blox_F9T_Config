#!/usr/bin/env python3
"""GNSS deployment orchestration.

The status command is intentionally read-only:

    python gnss_scripts/gnss_orchestrator.py status

It validates the deployment inventory and checks local/remote prerequisites.
The start and stop commands use the same inventory and support screen or
systemd process runners. The install-service command renders and installs
systemd user units without starting them.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shlex
import socket
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import json5


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = Path("gnss_deployment.json5")


class ConfigLoadError(RuntimeError):
    """Raised when the deployment config cannot be loaded cleanly."""


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
    local: bool = False
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
    local: bool = False
    command: str = ""
    script: str = ""


@dataclass
class InstallResult:
    """One systemd user-service installation result."""

    kind: str
    key: str
    daq_name: str
    host: str
    status: str
    service_name: str
    detail: str = ""
    required: bool = False
    local: bool = False
    command: str = ""
    script: str = ""


def load_config(path: str | os.PathLike[str] = DEFAULT_CONFIG) -> dict[str, Any]:
    """Load the JSON5 deployment inventory.

    Args:
        path: Path to the deployment config. Defaults to
            gnss_deployment.json5 in the current working directory.

    Returns:
        The parsed JSON5 object as a nested dictionary/list structure.
    """

    config_path = Path(path).expanduser()
    try:
        with config_path.open("r", encoding="utf-8") as f:
            return json5.load(f)
    except FileNotFoundError as exc:
        raise ConfigLoadError(
            f"deployment config not found: {config_path}\n"
            "Run from the directory containing gnss_deployment.json5, or pass "
            "--config /path/to/gnss_deployment.json5."
        ) from exc
    except OSError as exc:
        raise ConfigLoadError(f"could not read deployment config {config_path}: {exc}") from exc
    except Exception as exc:
        raise ConfigLoadError(f"could not parse deployment config {config_path}: {exc}") from exc


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


def _node_is_local(node: dict[str, Any]) -> bool:
    """Return whether node commands should run on the orchestrator host."""

    return _str_bool(node.get("local", False))


def _node_host(node: dict[str, Any], key: str) -> str:
    """Return the display host for a local or remote node."""

    if _node_is_local(node):
        return str(node.get("host") or socket.gethostname())
    return str(node.get("host") or key)


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


def _node_command(node: dict[str, Any], script: str) -> str:
    """Render a node Bash script as either a local command or an SSH command."""

    if _node_is_local(node):
        return "bash -c " + shlex.quote(script)
    return _shell_join(_ssh_base(node) + ["bash -c " + shlex.quote(script)])


def _node_run(node: dict[str, Any], script: str, timeout: float | None = None) -> CommandResult:
    """Run a node Bash script locally or over SSH according to node.local."""

    if _node_is_local(node):
        return _run_bash(script, timeout=timeout or 10.0)
    return _remote_run(node, script, timeout=timeout)


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


def _node_preflight_script(paths: dict[str, str], *, local: bool) -> str:
    """Build one Bash script for all local or remote node preflight checks."""

    prefix = "local" if local else "remote"
    checks = [
        (f"{prefix} -x " + paths["python"], "-x", paths["python"], paths["python"]),
        (f"{prefix} -d " + paths["repo"], "-d", paths["repo"], paths["repo"]),
        (f"{prefix} -f " + paths["agent_script"], "-f", paths["agent_script"], paths["agent_script"]),
        (f"{prefix} -x " + paths["find_ublox_script"], "-x", paths["find_ublox_script"], paths["find_ublox_script"]),
        (f"{prefix} logdir parent exists", "-d", os.path.dirname(paths["logdir"]), os.path.dirname(paths["logdir"])),
        (f"{prefix} telem_dir parent exists", "-d", os.path.dirname(paths["telem_dir"]), os.path.dirname(paths["telem_dir"])),
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


def _register_verify_script(node: dict[str, Any], manifest_path: str, role: str | None, port: str | None = None) -> str:
    """Build a node command that verifies manifest registers."""

    repo = str(node["repo"])
    verifier = _resolve_under_repo(repo, "gnss_scripts/verify_manifest_registers.py")
    args = [
        str(node["python"]),
        verifier,
        "--manifest",
        manifest_path,
        "--json",
    ]
    if port:
        args.extend(["--port", port])
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


def _parse_node_preflight(result: CommandResult) -> tuple[list[Check], bool | None, str | None]:
    """Parse node preflight output into checks and GNSS detection state."""

    checks: list[Check] = []
    gnss_detected: bool | None = None
    gnss_port: str | None = None
    if result.returncode != 0:
        checks.append(Check("remote preflight", False, _format_cmd_result(result)))
        return checks, gnss_detected, gnss_port

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
            if gnss_detected:
                candidate = detail.split(maxsplit=1)[0] if detail else ""
                if candidate.startswith("/dev/"):
                    gnss_port = candidate
            checks.append(Check("gnss receiver detected", gnss_detected, detail))
        else:
            checks.append(Check("remote preflight output", False, line))

    return checks, gnss_detected, gnss_port


def _summarize_bodnar_status(detail: str) -> str:
    """Condense verbose lbe-1420-conf.py --status output for human status."""

    if detail.startswith("exit "):
        return detail

    fields: list[str] = []
    cno_matches = [
        (int(count), int(best), int(avg))
        for count, best, avg in re.findall(r"(\d+)\s+sats,\s*C/N0 best\s+(\d+)\s*/\s*avg\s+(\d+)", detail)
    ]
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
        if label == "satellites" and cno_matches:
            best = max(match[1] for match in cno_matches)
            total_sats = sum(match[0] for match in cno_matches)
            weighted_avg = round(sum(count * avg for count, _, avg in cno_matches) / total_sats) if total_sats else 0
            fields.append(f"C/N0 best {best} / avg {weighted_avg} dB-Hz")

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


def _server_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return server config with shared defaults and server-specific defaults."""

    defaults = config.get("defaults", {})
    raw_server = config.get("server", {})
    server = _merge(defaults, raw_server)
    if "screen" not in server and "server_screen" in server:
        server["screen"] = server["server_screen"]
    if "telem_dir" not in raw_server:
        server["telem_dir"] = "telemetry"
    return server


def _telemetry_runtime_args(config: dict[str, Any]) -> list[str]:
    """Return telemetry rotation/flush CLI args for agent_v1.py/server_v1.py."""

    telemetry = config.get("telemetry", {})
    if not isinstance(telemetry, dict):
        telemetry = {}

    max_file_mb = telemetry.get("max_file_mb", config.get("telem_max_file_mb", 128))
    fsync_seconds = telemetry.get("fsync_seconds", config.get("telem_fsync_seconds", 5))
    return [
        "--telem-max-file-mb",
        str(max_file_mb),
        "--telem-fsync-seconds",
        str(fsync_seconds),
    ]


def _process_config(defaults: dict[str, Any], item: dict[str, Any], kind: str) -> dict[str, Any]:
    """Resolve nested process/systemd settings for a server or agent."""

    default_process = defaults.get("process") or {}
    item_process = item.get("process") or {}
    process = dict(default_process)
    process.update(item_process)

    systemd = dict(default_process.get("systemd") or {})
    systemd.update(item_process.get("systemd") or {})
    default_name = systemd.get(f"{kind}_service_name", f"gnss-{kind}.service")
    systemd.setdefault("service_name", default_name)
    systemd.setdefault("user_service", True)
    systemd.setdefault("enable_on_install", True)
    systemd.setdefault("check_linger", True)
    systemd.setdefault("restart", "always")
    systemd.setdefault("restart_sec", 10)
    process["systemd"] = systemd
    process.setdefault("runner", "screen")
    return process


def _systemd_service_name(value: Any) -> str:
    """Validate and normalize a configured systemd service filename."""

    name = str(value or "")
    if not re.fullmatch(r"[A-Za-z0-9_.:@-]+\.service", name):
        raise ValueError(f"invalid systemd service name {name!r}")
    return name


def _systemd_exec(args: list[Any]) -> str:
    """Render argv using systemd's ExecStart quoting rules."""

    quoted: list[str] = []
    for value in args:
        text = str(value).replace("%", "%%").replace("\\", "\\\\").replace('"', '\\"')
        quoted.append(f'"{text}"')
    return " ".join(quoted)


def _require_config_values(label: str, values: dict[str, Any], names: list[str]) -> None:
    """Raise a concise error when service rendering lacks required config."""

    missing = [name for name in names if values.get(name) in {None, ""}]
    if missing:
        raise ValueError(f"{label} missing required service field(s): {', '.join(missing)}")


def _systemd_restart_values(systemd: dict[str, Any]) -> tuple[str, float]:
    """Validate the restart policy values inserted into a generated unit."""

    restart = str(systemd.get("restart", "always"))
    allowed = {
        "no",
        "on-success",
        "on-failure",
        "on-abnormal",
        "on-watchdog",
        "on-abort",
        "always",
    }
    if restart not in allowed:
        raise ValueError(f"invalid systemd restart policy {restart!r}")
    try:
        restart_sec = float(systemd.get("restart_sec", 10))
    except (TypeError, ValueError) as exc:
        raise ValueError("systemd restart_sec must be a non-negative number") from exc
    if restart_sec < 0:
        raise ValueError("systemd restart_sec must be a non-negative number")
    return restart, restart_sec


def _render_systemd_template(template_name: str, values: dict[str, Any]) -> str:
    """Render a repository-managed systemd unit template."""

    template_path = SCRIPT_DIR / "systemd" / template_name
    try:
        template = template_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"could not read systemd template {template_path}: {exc}") from exc
    return template.format(**values)


def _telemetry_runtime_values(config: dict[str, Any]) -> tuple[Any, Any]:
    """Return configured telemetry size and fsync values."""

    args = _telemetry_runtime_args(config)
    return args[1], args[3]


def _render_agent_service(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
) -> tuple[dict[str, Any], str, str]:
    """Resolve one node and render its GNSS agent systemd unit."""

    node = _merge(defaults, raw_node)
    process = _process_config(defaults, raw_node, "agent")
    systemd = process["systemd"]
    if not _str_bool(systemd.get("user_service", True)):
        raise ValueError("install-service currently supports only systemd user services")

    repo = str(node.get("repo") or "")
    agent_script = _resolve_under_repo(repo, str(node.get("agent_script") or "agent_v1.py"))
    logdir = _resolve_under_repo(repo, str(node.get("logdir") or "logging"))
    telem_dir = _resolve_under_repo(repo, str(node.get("telem_dir") or "telem"))
    max_file_mb, fsync_seconds = _telemetry_runtime_values(node)
    required_values = {
        "python": node.get("python"),
        "repo": repo,
        "agent_script": agent_script,
        "cast_addr": node.get("cast_addr"),
        "ctrl_addr": node.get("ctrl_addr"),
        "logdir": logdir,
        "telem_dir": telem_dir,
    }
    _require_config_values(
        f"node {key}",
        required_values,
        list(required_values),
    )
    restart, restart_sec = _systemd_restart_values(systemd)
    args = [
        node.get("python"),
        "-u",
        agent_script,
        "--cast_addr",
        node.get("cast_addr"),
        "--ctrl_addr",
        node.get("ctrl_addr"),
        "--log-dir",
        logdir,
        "--telem-dir",
        telem_dir,
        "--telem-max-file-mb",
        max_file_mb,
        "--telem-fsync-seconds",
        fsync_seconds,
        "-v",
        node.get("verbosity", 2),
    ]
    service_name = _systemd_service_name(systemd.get("service_name"))
    unit = _render_systemd_template(
        "gnss-agent.service.template",
        {
            "daq_name": node.get("daq_name", key),
            "repo": repo,
            "exec_start": _systemd_exec(args),
            "restart": restart,
            "restart_sec": f"{restart_sec:g}",
        },
    )
    resolved = {
        **node,
        "repo": repo,
        "agent_script": agent_script,
        "logdir": logdir,
        "telem_dir": telem_dir,
        "process": process,
    }
    return resolved, service_name, unit


def _render_server_service(
    config: dict[str, Any],
    mode: str,
) -> tuple[dict[str, Any], str, str]:
    """Resolve the local server and render its systemd unit."""

    defaults = config.get("defaults", {})
    raw_server = config.get("server", {})
    server = _server_config(config)
    process = _process_config(defaults, raw_server, "server")
    systemd = process["systemd"]
    if not _str_bool(systemd.get("user_service", True)):
        raise ValueError("install-service currently supports only systemd user services")

    mode_config = _mode_settings(config, mode)
    repo = str(server.get("repo") or "")
    script_value = str(server.get("script") or server.get("server_script") or "server_v1.py")
    server_script = _resolve_under_repo(repo, script_value)
    telem_dir = _resolve_under_repo(repo, str(server.get("telem_dir") or "telemetry"))
    receiver_manifest = mode_config.get("receiver_manifest") or server.get("receiver_manifest")
    receiver_manifest_path = (
        _resolve_under_repo(repo, str(receiver_manifest)) if receiver_manifest else ""
    )
    max_file_mb, fsync_seconds = _telemetry_runtime_values(server)
    required_values = {
        "python": server.get("python"),
        "repo": repo,
        "script": server_script,
        "bind_addr": server.get("bind_addr"),
        "telem_dir": telem_dir,
    }
    _require_config_values("server", required_values, list(required_values))
    restart, restart_sec = _systemd_restart_values(systemd)
    args = [
        server.get("python"),
        "-u",
        server_script,
        "--ip",
        server.get("bind_addr", "0.0.0.0:50051"),
        "--timing-mode",
        mode_config.get("timing_mode", mode),
        "--telem-dir",
        telem_dir,
        "--telem-max-file-mb",
        max_file_mb,
        "--telem-fsync-seconds",
        fsync_seconds,
        "-v",
        server.get("verbosity", 2),
    ]
    if receiver_manifest_path:
        args.extend(["--config", receiver_manifest_path])

    service_name = _systemd_service_name(systemd.get("service_name"))
    unit = _render_systemd_template(
        "gnss-server.service.template",
        {
            "repo": repo,
            "exec_start": _systemd_exec(args),
            "restart": restart,
            "restart_sec": f"{restart_sec:g}",
        },
    )
    resolved = {
        **server,
        "repo": repo,
        "script": server_script,
        "telem_dir": telem_dir,
        "receiver_manifest": receiver_manifest_path,
        "timing_mode": mode_config.get("timing_mode", mode),
        "process": process,
    }
    return resolved, service_name, unit


def _systemd_install_script(
    unit: str,
    service_name: str,
    *,
    enable: bool,
    check_linger: bool,
) -> str:
    """Build a node script that atomically installs one systemd user unit."""

    encoded = base64.b64encode(unit.encode("utf-8")).decode("ascii")
    enable_lines = (
        [
            'systemctl --user enable "$UNIT_NAME"',
            'ENABLED="yes"',
        ]
        if enable
        else ['ENABLED="not requested"']
    )
    linger_lines = (
        [
            'LINGER="$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null || true)"',
            '[ -n "$LINGER" ] || LINGER="unknown"',
        ]
        if check_linger
        else ['LINGER="not checked"']
    )
    return "\n".join(
        [
            "set -euo pipefail",
            f"UNIT_NAME={shlex.quote(service_name)}",
            'UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"',
            'UNIT_PATH="$UNIT_DIR/$UNIT_NAME"',
            'mkdir -p "$UNIT_DIR"',
            'TEMP_UNIT="$(mktemp "$UNIT_DIR/.${UNIT_NAME}.XXXXXX")"',
            'trap \'rm -f "$TEMP_UNIT"\' EXIT',
            f"printf '%s' {shlex.quote(encoded)} | base64 --decode > \"$TEMP_UNIT\"",
            'chmod 0644 "$TEMP_UNIT"',
            'mv -f "$TEMP_UNIT" "$UNIT_PATH"',
            "trap - EXIT",
            "systemctl --user daemon-reload",
            *enable_lines,
            *linger_lines,
            'printf "GNSS_UNIT_PATH=%s\\n" "$UNIT_PATH"',
            'printf "GNSS_ENABLED=%s\\n" "$ENABLED"',
            'printf "GNSS_LINGER=%s\\n" "$LINGER"',
            'printf "GNSS_USER=%s\\n" "$(id -un)"',
        ]
    )


def _install_result_detail(result: CommandResult, check_linger: bool) -> str:
    """Summarize installer markers and add a useful linger warning."""

    markers: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if line.startswith("GNSS_") and "=" in line:
            name, value = line.split("=", 1)
            markers[name] = value

    detail = markers.get("GNSS_UNIT_PATH", "systemd user unit installed")
    enabled = markers.get("GNSS_ENABLED")
    if enabled:
        detail += f"; enabled={enabled}"
    linger = markers.get("GNSS_LINGER")
    if check_linger and linger != "yes":
        user = markers.get("GNSS_USER", "USER")
        detail += (
            f"; linger={linger or 'unknown'}: boot startup may require "
            f"'sudo loginctl enable-linger {shlex.quote(user)}'"
        )
    return detail


def _runner_config(
    defaults: dict[str, Any],
    item: dict[str, Any],
    kind: str,
    override: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Resolve and validate the process runner for one target."""

    process = _process_config(defaults, item, kind)
    runner = str(override or process.get("runner") or "screen").lower()
    if runner not in {"screen", "systemd"}:
        raise ValueError(f"invalid process runner {runner!r}; expected screen or systemd")
    return runner, process


def _systemd_status_script(service_name: str, check_linger: bool) -> str:
    """Build a read-only script that reports one systemd user service."""

    lines = [
        "set -u",
        f"UNIT_NAME={shlex.quote(service_name)}",
        'LOAD="$(systemctl --user show "$UNIT_NAME" -p LoadState --value 2>/dev/null || true)"',
        'ACTIVE="$(systemctl --user is-active "$UNIT_NAME" 2>/dev/null || true)"',
        'ENABLED="$(systemctl --user is-enabled "$UNIT_NAME" 2>/dev/null || true)"',
        '[ -n "$LOAD" ] || LOAD="not-found"',
        '[ -n "$ACTIVE" ] || ACTIVE="unknown"',
        '[ -n "$ENABLED" ] || ENABLED="unknown"',
    ]
    if check_linger:
        lines.extend(
            [
                'LINGER="$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null || true)"',
                '[ -n "$LINGER" ] || LINGER="unknown"',
            ]
        )
    else:
        lines.append('LINGER="not checked"')
    lines.extend(
        [
            'printf "GNSS_LOAD=%s\\n" "$LOAD"',
            'printf "GNSS_ACTIVE=%s\\n" "$ACTIVE"',
            'printf "GNSS_ENABLED=%s\\n" "$ENABLED"',
            'printf "GNSS_LINGER=%s\\n" "$LINGER"',
        ]
    )
    return "\n".join(lines)


def _parse_systemd_checks(result: CommandResult, service_name: str, check_linger: bool) -> list[Check]:
    """Convert systemd status markers into normal orchestrator checks."""

    if result.returncode != 0:
        return [Check("systemd process status", False, _format_cmd_result(result))]

    markers: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if line.startswith("GNSS_") and "=" in line:
            name, value = line.split("=", 1)
            markers[name] = value

    load = markers.get("GNSS_LOAD", "unknown")
    active = markers.get("GNSS_ACTIVE", "unknown")
    enabled = markers.get("GNSS_ENABLED", "unknown")
    checks = [
        Check("systemd service installed", load == "loaded", f"{service_name} load={load}"),
        Check("systemd service enabled", enabled == "enabled", f"{service_name} enabled={enabled}"),
        Check("systemd service active", active == "active", f"{service_name} active={active}"),
    ]
    if check_linger:
        linger = markers.get("GNSS_LINGER", "unknown")
        checks.append(Check("systemd user linger", linger == "yes", f"linger={linger}"))
    return checks


def _parse_systemd_conflict_check(result: CommandResult, service_name: str) -> Check | None:
    """Return a warning only when systemd is active under the screen runner."""

    active = "unknown"
    for line in result.stdout.splitlines():
        if line.startswith("GNSS_ACTIVE="):
            active = line.split("=", 1)[1]
            break
    if active != "active":
        return None
    return Check("runner conflict", False, f"runner=screen but {service_name} is active")


def _server_status(
    config: dict[str, Any],
    *,
    check_process: bool = False,
    runner_override: str | None = None,
) -> dict[str, Any]:
    """Validate local server configuration and filesystem prerequisites.

    The GNSS server is expected to run on the orchestration/head node, so these
    checks use local filesystem calls instead of SSH. The returned dictionary has
    the same shape as node reports so CLI and future DAQ callers can process
    server/node results uniformly.
    """

    defaults = config.get("defaults", {})
    raw_server = config.get("server", {})
    server = _server_config(config)
    runner, process = _runner_config(defaults, raw_server, "server", runner_override)

    # Server script/logdir paths may be absolute, but the usual case is that
    # they are stored relative to the server repo.
    repo = str(server.get("repo", ""))
    script = str(server.get("script") or server.get("server_script") or "server_v1.py")
    server_script = _resolve_under_repo(repo, script) if repo else script
    logdir = str(server.get("logdir", "logging"))
    logdir_path = _resolve_under_repo(repo, logdir) if repo else logdir
    telem_dir = str(server.get("telem_dir", "telemetry"))
    telem_dir_path = _resolve_under_repo(repo, telem_dir) if repo else telem_dir
    receiver_manifest = server.get("receiver_manifest")
    receiver_manifest_path = ""
    if receiver_manifest:
        receiver_manifest_path = _resolve_under_repo(repo, str(receiver_manifest)) if repo else str(receiver_manifest)

    # Required-field checks catch malformed inventory entries before we try to
    # use those values in subprocess commands.
    checks = _check_required_fields(
        "server",
        server,
        ["daq_name", "python", "repo", "script", "logdir", "telem_dir", "screen", "bind_addr"],
    )

    # These checks are local because the GNSS server is expected to run on the
    # orchestration/head node.
    if server.get("python"):
        checks.append(Check("server python executable", os.access(str(server["python"]), os.X_OK), str(server["python"])))
    if repo:
        checks.append(Check("server repo directory", Path(repo).is_dir(), repo))
    checks.append(Check("server script file", Path(server_script).is_file(), server_script))
    checks.append(Check("server logdir parent", Path(logdir_path).parent.is_dir(), logdir_path))
    checks.append(Check("server telem_dir parent", Path(telem_dir_path).parent.is_dir(), telem_dir_path))
    if receiver_manifest_path:
        checks.append(Check("server receiver manifest", Path(receiver_manifest_path).is_file(), receiver_manifest_path))
    if check_process:
        systemd = process["systemd"]
        service_name = _systemd_service_name(systemd.get("service_name"))
        status_result = _run_bash(
            _systemd_status_script(
                service_name,
                _str_bool(systemd.get("check_linger", True)),
            ),
            timeout=10.0,
        )
        if runner == "systemd":
            checks.extend(
                _parse_systemd_checks(
                    status_result,
                    service_name,
                    _str_bool(systemd.get("check_linger", True)),
                )
            )
        else:
            conflict = _parse_systemd_conflict_check(status_result, service_name)
            if conflict is not None:
                checks.append(conflict)

    return {
        "kind": "server",
        "key": "server",
        "daq_name": server.get("daq_name", "server"),
        "host": socket.gethostname(),
        "local": True,
        "runner": runner,
        "checks": checks,
        "resolved": {
            "python": server.get("python"),
            "repo": repo,
            "script": server_script,
            "logdir": logdir_path,
            "telem_dir": telem_dir_path,
            "receiver_manifest": receiver_manifest_path,
            "service_name": process["systemd"].get("service_name"),
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
    check_process: bool = False,
    runner_override: str | None = None,
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
    is_local = _node_is_local(node)
    runner, process = _runner_config(defaults, raw_node, "agent", runner_override)

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

    # These fields are the minimum needed to start an agent. Remote nodes also
    # require an SSH target; local nodes use paths on the orchestrator host.
    required_fields = [
        "daq_name",
        "python",
        "repo",
        "agent_script",
        "find_ublox_script",
        "logdir",
        "telem_dir",
        "cast_addr",
        "ctrl_addr",
        "verbosity",
    ]
    if not is_local:
        required_fields[1:1] = ["host", "ssh_user"]
    checks = _check_required_fields(
        f"nodes.{key}",
        node,
        required_fields,
    )

    gnss_detected: bool | None = None
    gnss_port: str | None = None
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
    elif local_only and not is_local:
        # Local-only mode is useful during config editing and CI because it does
        # not require network access or SSH keys.
        checks.append(Check("remote checks", True, "skipped by --local-only"))
        if check_bodnar and bodnar_present:
            checks.append(Check("bodnar remote checks", True, "skipped by --local-only"))
    else:
        # Prove the execution transport works before running the combined
        # filesystem and hardware preflight.
        transport_name = "local execution" if is_local else "ssh reachable"
        transport_result = _node_run(node, "true")
        checks.append(Check(transport_name, transport_result.returncode == 0, _format_cmd_result(transport_result)))

        if transport_result.returncode == 0:
            preflight_script = _node_preflight_script(
                {
                    "python": str(node["python"]),
                    "repo": repo,
                    "agent_script": agent_script,
                    "find_ublox_script": find_ublox,
                    "logdir": logdir,
                    "telem_dir": telem_dir,
                },
                local=is_local,
            )
            preflight_result = _node_run(node, preflight_script, timeout=20.0)
            node_checks, gnss_detected, gnss_port = _parse_node_preflight(preflight_result)
            checks.extend(node_checks)

            if check_bodnar and bodnar_present:
                bodnar_script = _remote_bodnar_status_script(
                    bodnar_paths,
                    float(bodnar.get("timeout_sec", 20.0)),
                )
                bodnar_result = _node_run(node, bodnar_script, timeout=float(bodnar.get("timeout_sec", 20.0)) + 10.0)
                bodnar_checks, bodnar_detected = _parse_bodnar_status(bodnar_result)
                checks.extend(bodnar_checks)

            if verify_registers:
                settings = mode_settings or {"timing_mode": "differential", "receiver_manifest": "manifest_f9t.json5"}
                manifest = str(settings.get("receiver_manifest") or "manifest_f9t.json5")
                manifest_path = _resolve_under_repo(repo, manifest)
                role = "timing_only" if settings.get("timing_mode") == "absolute" else None
                verify_script = _register_verify_script(node, manifest_path, role, gnss_port)
                verify_result = _node_run(node, verify_script, timeout=90.0)
                verify_check, register_verify_report = _parse_register_verify(verify_result)
                checks.append(verify_check)

            if check_process:
                systemd = process["systemd"]
                service_name = _systemd_service_name(systemd.get("service_name"))
                status_result = _node_run(
                    node,
                    _systemd_status_script(
                        service_name,
                        _str_bool(systemd.get("check_linger", True)),
                    ),
                    timeout=10.0,
                )
                if runner == "systemd":
                    checks.extend(
                        _parse_systemd_checks(
                            status_result,
                            service_name,
                            _str_bool(systemd.get("check_linger", True)),
                        )
                    )
                else:
                    conflict = _parse_systemd_conflict_check(status_result, service_name)
                    if conflict is not None:
                        checks.append(conflict)

    result = {
        "kind": "node",
        "key": key,
        "daq_name": node.get("daq_name", key),
        "host": _node_host(node, key),
        "local": is_local,
        "present": present,
        "required": _str_bool(node.get("required", False)),
        "runner": runner,
        "gnss_detected": gnss_detected,
        "gnss_port": gnss_port,
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
            "local": is_local,
            "service_name": process["systemd"].get("service_name"),
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
    runner: str | None = None,
) -> dict[str, Any]:
    """Build a read-only GNSS deployment status report.

    This function is importable by a future DAQ controller. The CLI below is
    only a thin presentation layer around this structured report.

    Args:
        config_path: Deployment inventory path.
        nodes: Optional list of node keys, hostnames, or DAQ names to include.
        include_disabled: Include disabled nodes in the report.
        local_only: Skip SSH checks and validate only local/config structure.
        runner: Optional screen/systemd override for process-state checks.

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
    results = [_server_status(config, check_process=True, runner_override=runner)]
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
                check_process=True,
                runner_override=runner,
            )
        )

    return {
        "config_path": str(config_path),
        "mode": mode,
        "runner": runner,
        "verify_registers": verify_registers,
        "results": results,
    }


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
    telem_dir = str(resolved["telem_dir"])
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
        "--telem-dir",
        resolved["telem_dir"],
        "-v",
        server.get("verbosity", 2),
    ]
    server_args.extend(_telemetry_runtime_args(server))
    if resolved.get("receiver_manifest"):
        server_args.extend(["--config", resolved["receiver_manifest"]])

    inner = _timestamped_log_command(server_args, log_path)
    script = "\n".join(
        [
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(logdir)} {shlex.quote(telem_dir)}",
            f"ln -sfn {shlex.quote(Path(log_path).name)} {shlex.quote(latest_log)}",
            f"screen -S {shlex.quote(screen)} -X quit >/dev/null 2>&1 || true",
            "sleep 0.5",
            f"screen -dmS {shlex.quote(screen)} bash -c {shlex.quote(inner)}",
            "sleep 1",
            *_screen_verify_script(screen, log_path),
        ]
    )
    return script, log_path


def _systemd_start_script(
    resolved: dict[str, Any],
    kind: str,
    unit: str,
    service_name: str,
    legacy_stop_script: str,
) -> tuple[str, str]:
    """Build actual and human-readable scripts for a systemd restart."""

    systemd = resolved["process"]["systemd"]
    install_script = _systemd_install_script(
        unit,
        service_name,
        enable=_str_bool(systemd.get("enable_on_install", True)),
        check_linger=_str_bool(systemd.get("check_linger", True)),
    )
    service = shlex.quote(service_name)
    control_lines = [
        f"systemctl --user stop {service} >/dev/null 2>&1 || true",
        legacy_stop_script,
        f"systemctl --user start {service}",
        "sleep 1",
        f"if ! systemctl --user is-active --quiet {service}; then",
        f"  echo '[FAIL] systemd service {service_name} is not active'",
        f"  journalctl --user -u {service} -n 60 --no-pager 2>/dev/null || true",
        "  exit 1",
        "fi",
        f"echo '[OK] systemd service {service_name} is active'",
    ]
    actual = "\n".join(
        [
            _service_preflight_script(resolved, kind),
            install_script,
            *control_lines,
        ]
    )
    display = "\n".join(
        [
            f"# install/update ~/.config/systemd/user/{service_name} from the rendered template",
            "systemctl --user daemon-reload",
            (
                f"systemctl --user enable {service}"
                if _str_bool(systemd.get("enable_on_install", True))
                else "# service enablement not requested"
            ),
            f"systemctl --user stop {service}  # suppress Restart=always during cleanup",
            "# stop any legacy screen session or matching Python process",
            f"systemctl --user start {service}",
            f"systemctl --user is-active --quiet {service}",
        ]
    )
    return actual, display


def _stop_systemd_before_screen(script: str, service_name: str) -> str:
    """Prevent a screen launch/stop from leaving a duplicate systemd process."""

    return "\n".join(
        [
            "set -euo pipefail",
            f"systemctl --user stop {shlex.quote(service_name)} >/dev/null 2>&1 || true",
            script,
        ]
    )


def _start_server(
    config: dict[str, Any],
    *,
    dry_run: bool,
    mode: str = "differential",
    run_stamp: str,
    runner_override: str | None = None,
) -> StartResult:
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
    server = _server_config(config)
    server_status["config"] = server
    runner, process = _runner_config(
        config.get("defaults", {}),
        config.get("server", {}),
        "server",
        runner_override,
    )

    if runner == "systemd":
        resolved, service_name, unit = _render_server_service(config, mode)
        legacy_stop_script, _ = _server_stop_script(server_status)
        script, display_script = _systemd_start_script(
            resolved,
            "server",
            unit,
            service_name,
            legacy_stop_script,
        )
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
                local=True,
                command=command,
                script=display_script,
            )
        if dry_run:
            return StartResult(
                kind="server",
                key="server",
                daq_name=str(server_status["daq_name"]),
                host=str(server_status["host"]),
                status="dry-run",
                detail=f"would refresh and restart systemd service {service_name} in {mode} mode",
                required=True,
                local=True,
                command=command,
                script=display_script,
            )

        result = _run_bash(
            script,
            timeout=float(server.get("shutdown_grace_sec", 5)) + 25.0,
        )
        ok = result.returncode == 0
        return StartResult(
            kind="server",
            key="server",
            daq_name=str(server_status["daq_name"]),
            host=str(server_status["host"]),
            status="started" if ok else "failed",
            detail=(
                f"systemd service {service_name} active; "
                f"{_install_result_detail(result, _str_bool(resolved['process']['systemd'].get('check_linger', True)))}"
                if ok
                else _format_cmd_result(result)
            ),
            required=True,
            local=True,
            command=command,
            script=display_script,
        )

    script, log_path = _server_launch_script(server_status, run_stamp)
    script = _stop_systemd_before_screen(
        script,
        _systemd_service_name(process["systemd"].get("service_name")),
    )
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
            local=True,
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
            local=True,
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
        local=True,
        command=command,
        script=script,
    )


def _agent_launch_script(node_status: dict[str, Any], run_stamp: str) -> tuple[str, str]:
    """Build the Bash script that starts one GNSS agent in screen."""

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
    agent_args.extend(_telemetry_runtime_args(node))
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
    runner_override: str | None = None,
) -> StartResult:
    """Start one local or remote GNSS agent, or describe a dry run."""

    node_status = _node_status(key, raw_node, defaults, local_only=False, check_bodnar=False)
    node = _merge(defaults, raw_node)
    is_local = _node_is_local(node)
    node_status["config"] = node
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
            local=is_local,
            command="",
            script="",
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
            local=is_local,
            command="",
            script="",
        )

    runner, process = _runner_config(defaults, raw_node, "agent", runner_override)
    if runner == "systemd":
        resolved, service_name, unit = _render_agent_service(key, raw_node, defaults)
        stop_status = _node_stop_status(key, raw_node, defaults)
        legacy_stop_script, _ = _agent_stop_script(stop_status)
        script, display_script = _systemd_start_script(
            resolved,
            "agent",
            unit,
            service_name,
            legacy_stop_script,
        )
        command = _node_command(node, script)
        if dry_run:
            return StartResult(
                kind="node",
                key=key,
                daq_name=str(node_status["daq_name"]),
                host=str(node_status["host"]),
                status="dry-run",
                detail=f"would refresh and restart systemd service {service_name}",
                required=required,
                local=is_local,
                command=command,
                script=display_script,
            )

        result = _node_run(
            node,
            script,
            timeout=float(node.get("shutdown_grace_sec", 5)) + 25.0,
        )
        ok = result.returncode == 0
        return StartResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="started" if ok else "failed",
            detail=(
                f"systemd service {service_name} active; "
                f"{_install_result_detail(result, _str_bool(resolved['process']['systemd'].get('check_linger', True)))}"
                if ok
                else _format_cmd_result(result)
            ),
            required=required,
            local=is_local,
            command=command,
            script=display_script,
        )

    script, log_path = _agent_launch_script(node_status, run_stamp)
    script = _stop_systemd_before_screen(
        script,
        _systemd_service_name(process["systemd"].get("service_name")),
    )
    command = _node_command(node, script)
    if dry_run:
        return StartResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="dry-run",
            detail=f"would start screen {node.get('agent_screen')} with log {log_path}",
            required=required,
            local=is_local,
            command=command,
            script=script,
        )

    result = _node_run(node, script, timeout=20.0)
    ok = result.returncode == 0
    return StartResult(
        kind="node",
        key=key,
        daq_name=str(node_status["daq_name"]),
        host=str(node_status["host"]),
        status="started" if ok else "failed",
        detail=f"log {log_path}" if ok else _format_cmd_result(result),
        required=required,
        local=is_local,
        command=command,
        script=script,
    )


def _bodnar_configure_script(paths: dict[str, str], bodnar: dict[str, Any]) -> str:
    """Build a node script that configures a Leo Bodnar LBE-1420."""

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
    return "\n".join(commands)


def _configure_bodnar(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
    *,
    dry_run: bool,
) -> StartResult:
    """Configure one local or remote Leo Bodnar receiver."""

    node = _merge(defaults, raw_node)
    bodnar = _bodnar_config(defaults, node)
    paths = _bodnar_paths(defaults, node)
    present = _present(bodnar, False)
    required = _str_bool(bodnar.get("required", False))
    is_local = _node_is_local(node)
    daq_name = str(node.get("daq_name", key))
    host = _node_host(node, key)

    script = _bodnar_configure_script(paths, bodnar)
    command = _node_command(node, script)
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
            local=is_local,
            command=command,
            script=script,
        )

    if dry_run:
        return StartResult(
            kind="bodnar",
            key=key,
            daq_name=daq_name,
            host=host,
            status="dry-run",
            detail=f"would configure Bodnar {detail}",
            required=required,
            local=is_local,
            command=command,
            script=script,
        )

    transport_result = _node_run(node, "true")
    if transport_result.returncode != 0:
        return StartResult(
            kind="bodnar",
            key=key,
            daq_name=daq_name,
            host=host,
            status="failed",
            detail=("local execution failed: " if is_local else "ssh unreachable: ")
            + _format_cmd_result(transport_result),
            required=required,
            local=is_local,
            command=command,
            script=script,
        )

    result = _node_run(node, script, timeout=float(bodnar.get("timeout_sec", 20.0)) + 20.0)
    ok = result.returncode == 0
    return StartResult(
        kind="bodnar",
        key=key,
        daq_name=daq_name,
        host=host,
        status="configured" if ok else "failed",
        detail=detail if ok else _format_cmd_result(result),
        required=required,
        local=is_local,
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
    runner: str | None = None,
) -> dict[str, Any]:
    """Start the local GNSS server and configured local/remote agents.

    Args:
        config_path: Deployment inventory path.
        nodes: Optional list of node keys, hostnames, or DAQ names to start.
        dry_run: If true, report launch commands without executing them.
        include_disabled: Include disabled nodes when selecting targets.
        runner: Optional screen/systemd override for all selected processes.

    Returns:
        A structured start report with one StartResult for the server and one
        for each selected node.
    """

    config = load_config(config_path)
    defaults = config.get("defaults", {})
    run_stamp = _utc_run_stamp()
    results = [
        _start_server(
            config,
            dry_run=dry_run,
            mode=mode,
            run_stamp=run_stamp,
            runner_override=runner,
        )
    ]

    if results[0].status == "failed":
        return {
            "config_path": str(config_path),
            "dry_run": dry_run,
            "mode": mode,
            "runner": runner,
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
                        host=_node_host(node, key),
                        status="skipped",
                        detail="required Bodnar configuration failed",
                        required=_str_bool(node.get("required", False)),
                        local=_node_is_local(node),
                    )
                )
                continue
        results.append(
            _start_node(
                key,
                raw_node,
                defaults,
                dry_run=dry_run,
                run_stamp=run_stamp,
                runner_override=runner,
            )
        )

    return {
        "config_path": str(config_path),
        "dry_run": dry_run,
        "mode": mode,
        "runner": runner,
        "bodnar": configure_bodnar,
        "run_stamp": run_stamp,
        "results": results,
    }


def _service_preflight_script(resolved: dict[str, Any], kind: str) -> str:
    """Build lightweight path checks used before installing a service."""

    script_key = "agent_script" if kind == "agent" else "script"
    return "\n".join(
        [
            "set -euo pipefail",
            f"test -x {shlex.quote(str(resolved['python']))}",
            f"test -d {shlex.quote(str(resolved['repo']))}",
            f"test -f {shlex.quote(str(resolved[script_key]))}",
            f"mkdir -p {shlex.quote(str(resolved.get('logdir', '')))} "
            f"{shlex.quote(str(resolved['telem_dir']))}",
        ]
    )


def _install_one_service(
    *,
    kind: str,
    key: str,
    resolved: dict[str, Any],
    service_name: str,
    unit: str,
    node: dict[str, Any] | None,
    dry_run: bool,
) -> InstallResult:
    """Install one rendered service locally or through a node's SSH transport."""

    systemd = resolved["process"]["systemd"]
    enable = _str_bool(systemd.get("enable_on_install", True))
    check_linger = _str_bool(systemd.get("check_linger", True))
    is_local = node is None or _node_is_local(node)
    host = socket.gethostname() if node is None else _node_host(node, key)
    daq_name = str(resolved.get("daq_name", key))
    required = kind == "server" or _str_bool(resolved.get("required", False))
    install_script = _systemd_install_script(
        unit,
        service_name,
        enable=enable,
        check_linger=check_linger,
    )
    action_script = _service_preflight_script(resolved, kind) + "\n" + install_script
    command = (
        "bash -c " + shlex.quote(action_script)
        if node is None
        else _node_command(node, action_script)
    )

    if dry_run:
        return InstallResult(
            kind=kind,
            key=key,
            daq_name=daq_name,
            host=host,
            status="dry-run",
            service_name=service_name,
            detail=(
                f"would install ~/.config/systemd/user/{service_name}; "
                f"enable={'yes' if enable else 'no'}"
            ),
            required=required,
            local=is_local,
            command=command,
            script=unit,
        )

    result = (
        _run_bash(action_script, timeout=20.0)
        if node is None
        else _node_run(node, action_script, timeout=20.0)
    )
    ok = result.returncode == 0
    return InstallResult(
        kind=kind,
        key=key,
        daq_name=daq_name,
        host=host,
        status="installed" if ok else "failed",
        service_name=service_name,
        detail=(
            _install_result_detail(result, check_linger)
            if ok
            else _format_cmd_result(result)
        ),
        required=required,
        local=is_local,
        command=command,
        script=unit,
    )


def install_services(
    config_path: str | os.PathLike[str] = DEFAULT_CONFIG,
    *,
    nodes: list[str] | None = None,
    install_server: bool = False,
    all_nodes: bool = False,
    include_disabled: bool = False,
    mode: str = "differential",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Render and install selected GNSS systemd user services."""

    if not install_server and not nodes and not all_nodes:
        raise ValueError("select --server, one or more --node values, or --all-nodes")
    if nodes and all_nodes:
        raise ValueError("--node and --all-nodes cannot be used together")

    config = load_config(config_path)
    defaults = config.get("defaults", {})
    results: list[InstallResult] = []

    if install_server:
        resolved, service_name, unit = _render_server_service(config, mode)
        results.append(
            _install_one_service(
                kind="server",
                key="server",
                resolved=resolved,
                service_name=service_name,
                unit=unit,
                node=None,
                dry_run=dry_run,
            )
        )

    selectors = [] if all_nodes else list(nodes or [])
    selected_items = (
        _selected_node_items(
            config,
            selectors,
            include_disabled=include_disabled,
        )
        if all_nodes or selectors
        else []
    )
    if selectors:
        matched = {
            selector
            for selector in selectors
            for key, raw_node in config.get("nodes", {}).items()
            if selector in {key, raw_node.get("host"), raw_node.get("daq_name")}
        }
        unmatched = [selector for selector in selectors if selector not in matched]
        if unmatched:
            raise ValueError(f"unknown node selector(s): {', '.join(unmatched)}")

        hidden = []
        selected_keys = {key for key, _ in selected_items}
        for key, raw_node in config.get("nodes", {}).items():
            if key in selected_keys:
                continue
            if any(selector in {key, raw_node.get("host"), raw_node.get("daq_name")} for selector in selectors):
                hidden.append(key)
        if hidden:
            raise ValueError(
                f"selected node(s) marked present=false: {', '.join(hidden)}; "
                "pass --include-disabled to install them"
            )

    for key, raw_node in selected_items:
        resolved, service_name, unit = _render_agent_service(key, raw_node, defaults)
        node = _merge(defaults, raw_node)
        results.append(
            _install_one_service(
                kind="agent",
                key=key,
                resolved=resolved,
                service_name=service_name,
                unit=unit,
                node=node,
                dry_run=dry_run,
            )
        )

    return {
        "config_path": str(config_path),
        "dry_run": dry_run,
        "mode": mode,
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


def _systemd_stop_script(service_name: str, legacy_stop_script: str) -> tuple[str, str]:
    """Build actual and display scripts that stop systemd and legacy runners."""

    service = shlex.quote(service_name)
    actual = "\n".join(
        [
            "set -euo pipefail",
            f"systemctl --user stop {service} >/dev/null 2>&1 || true",
            legacy_stop_script,
            f"if systemctl --user is-active --quiet {service}; then",
            f"  echo '[FAIL] systemd service {service_name} is still active'",
            "  exit 1",
            "fi",
            f"echo '[OK] systemd service {service_name} is inactive'",
        ]
    )
    display = "\n".join(
        [
            f"systemctl --user stop {service}",
            "# stop any legacy screen session or matching Python process",
            f"systemctl --user is-active --quiet {service}  # expected to be inactive",
        ]
    )
    return actual, display


def _server_stop_script(server_status: dict[str, Any]) -> tuple[str, str]:
    """Build the local Bash script that stops the GNSS server."""

    server = server_status["config"]
    resolved = server_status["resolved"]
    screen = str(server.get("screen") or server.get("server_screen") or "gnss_server")
    log_path = str(Path(str(resolved["logdir"])) / f"{screen}.log")
    grace_sec = int(server.get("shutdown_grace_sec", 5))
    return _stop_script(screen, str(resolved["script"]), grace_sec, log_path), log_path


def _stop_server(
    config: dict[str, Any],
    *,
    dry_run: bool,
    runner_override: str | None = None,
) -> StopResult:
    """Stop the local GNSS server, or describe the command in dry-run mode."""

    server_status = _server_status(config)
    server = _server_config(config)
    server_status["config"] = server
    script, log_path = _server_stop_script(server_status)
    runner, process = _runner_config(
        config.get("defaults", {}),
        config.get("server", {}),
        "server",
        runner_override,
    )
    if runner == "systemd":
        service_name = _systemd_service_name(process["systemd"].get("service_name"))
        script, display_script = _systemd_stop_script(service_name, script)
        command = "bash -c " + shlex.quote(script)
        if dry_run:
            return StopResult(
                kind="server",
                key="server",
                daq_name=str(server_status["daq_name"]),
                host=str(server_status["host"]),
                status="dry-run",
                detail=f"would stop systemd service {service_name} and legacy processes",
                required=True,
                local=True,
                command=command,
                script=display_script,
            )

        result = _run_bash(
            script,
            timeout=float(server.get("shutdown_grace_sec", 5)) + 15.0,
        )
        ok = result.returncode == 0
        return StopResult(
            kind="server",
            key="server",
            daq_name=str(server_status["daq_name"]),
            host=str(server_status["host"]),
            status="stopped" if ok else "failed",
            detail=f"systemd service {service_name} inactive" if ok else _format_cmd_result(result),
            required=True,
            local=True,
            command=command,
            script=display_script,
        )

    script = _stop_systemd_before_screen(
        script,
        _systemd_service_name(process["systemd"].get("service_name")),
    )
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
            local=True,
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
        local=True,
        command=command,
        script=script,
    )


def _node_stop_status(key: str, raw_node: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    """Resolve the node fields needed for stop without running full preflight."""

    node = _merge(defaults, raw_node)
    repo = str(node.get("repo", ""))
    agent_script = _resolve_under_repo(repo, str(node.get("agent_script", "agent_v1.py"))) if repo else ""
    logdir = _resolve_under_repo(repo, str(node.get("logdir", "logging"))) if repo else ""
    telem_dir = _resolve_under_repo(repo, str(node.get("telem_dir", "telem"))) if repo else ""
    return {
        "kind": "node",
        "key": key,
        "daq_name": node.get("daq_name", key),
        "host": _node_host(node, key),
        "local": _node_is_local(node),
        "required": _str_bool(node.get("required", False)),
        "resolved": {
            "agent_script": agent_script,
            "logdir": logdir,
            "telem_dir": telem_dir,
        },
        "config": node,
    }


def _agent_stop_script(node_status: dict[str, Any]) -> tuple[str, str]:
    """Build the Bash script that stops one GNSS agent."""

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
    runner_override: str | None = None,
) -> StopResult:
    """Stop one local or remote GNSS agent, or describe a dry run."""

    node_status = _node_stop_status(key, raw_node, defaults)
    node = node_status["config"]
    is_local = _node_is_local(node)
    script, log_path = _agent_stop_script(node_status)
    required = _str_bool(node.get("required", False))
    runner, process = _runner_config(defaults, raw_node, "agent", runner_override)
    if runner == "systemd":
        service_name = _systemd_service_name(process["systemd"].get("service_name"))
        script, display_script = _systemd_stop_script(service_name, script)
        command = _node_command(node, script)
        if dry_run:
            return StopResult(
                kind="node",
                key=key,
                daq_name=str(node_status["daq_name"]),
                host=str(node_status["host"]),
                status="dry-run",
                detail=f"would stop systemd service {service_name} and legacy processes",
                required=required,
                local=is_local,
                command=command,
                script=display_script,
            )

        result = _node_run(
            node,
            script,
            timeout=float(node.get("shutdown_grace_sec", 5)) + 15.0,
        )
        ok = result.returncode == 0
        return StopResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="stopped" if ok else "failed",
            detail=f"systemd service {service_name} inactive" if ok else _format_cmd_result(result),
            required=required,
            local=is_local,
            command=command,
            script=display_script,
        )

    script = _stop_systemd_before_screen(
        script,
        _systemd_service_name(process["systemd"].get("service_name")),
    )
    command = _node_command(node, script)
    if dry_run:
        return StopResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="dry-run",
            detail=f"would stop screen {node.get('agent_screen')} and process {node_status['resolved']['agent_script']}",
            required=required,
            local=is_local,
            command=command,
            script=script,
        )

    transport_result = _node_run(node, "true")
    if transport_result.returncode != 0:
        return StopResult(
            kind="node",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="failed",
            detail=("local execution failed: " if is_local else "ssh unreachable: ")
            + _format_cmd_result(transport_result),
            required=required,
            local=is_local,
            command=command,
            script=script,
        )

    result = _node_run(node, script, timeout=float(node.get("shutdown_grace_sec", 5)) + 10.0)
    ok = result.returncode == 0
    return StopResult(
        kind="node",
        key=key,
        daq_name=str(node_status["daq_name"]),
        host=str(node_status["host"]),
        status="stopped" if ok else "failed",
        detail=f"log {log_path}" if ok else _format_cmd_result(result),
        required=required,
        local=is_local,
        command=command,
        script=script,
    )


def _compress_logs_script(paths: list[str]) -> str:
    """Build a Bash script that gzip-compresses completed log files.

    The find expression uses -type f so latest-log symlinks such as
    gnss_agent.log and gnss_server.log are not replaced by .gz files.
    """

    quoted_paths = " ".join(shlex.quote(path) for path in paths if path)
    return "\n".join(
        [
            "set -euo pipefail",
            f"paths=({quoted_paths})",
            "files=()",
            "links=()",
            'for dir in "${paths[@]}"; do',
            '  [ -d "$dir" ] || continue',
            '  while IFS= read -r -d \'\' link; do',
            '    target="$(readlink "$link" 2>/dev/null || true)"',
            '    [ -n "$target" ] || continue',
            '    if [[ "$target" = /* ]]; then',
            '      compressed_target="${target}.gz"',
            '      link_target="${target}.gz"',
            "    else",
            '      compressed_target="${dir}/${target}.gz"',
            '      link_target="${target}.gz"',
            "    fi",
            '    links+=("${link}|${compressed_target}|${link_target}")',
            "  done < <(find \"$dir\" -maxdepth 1 -type l \\( -name '*.log' -o -name '*.txt' -o -name '*.jsonl' \\) -print0)",
            '  while IFS= read -r -d \'\' file; do',
            '    files+=("$file")',
            "  done < <(find \"$dir\" -maxdepth 1 -type f \\( -name '*.log' -o -name '*.txt' -o -name '*.jsonl' \\) -print0)",
            "done",
            'if [ "${#files[@]}" -eq 0 ]; then',
            "  echo '[OK] no files to compress'",
            "  exit 0",
            "fi",
            'gzip -9 -- "${files[@]}"',
            'for item in "${links[@]}"; do',
            "  IFS='|' read -r link compressed_target link_target <<< \"$item\"",
            '  [ -f "$compressed_target" ] || continue',
            '  ln -sfn "$link_target" "$link"',
            "done",
            'echo "[OK] compressed ${#files[@]} files"',
        ]
    )


def _server_compress_logs(config: dict[str, Any], *, dry_run: bool) -> StopResult:
    """Compress completed local server log/telemetry files."""

    server_status = _server_status(config)
    server = _server_config(config)
    resolved = server_status["resolved"]
    paths = [str(resolved["logdir"]), str(resolved["telem_dir"])]
    script = _compress_logs_script(paths)
    command = "bash -c " + shlex.quote(script)

    if dry_run:
        return StopResult(
            kind="compress",
            key="server",
            daq_name=str(server_status["daq_name"]),
            host=str(server_status["host"]),
            status="dry-run",
            detail="would gzip -9 server log and telemetry files",
            required=True,
            local=True,
            command=command,
            script=script,
        )

    result = _run_bash(script, timeout=float(server.get("compress_timeout_sec", 300.0)))
    ok = result.returncode == 0
    return StopResult(
        kind="compress",
        key="server",
        daq_name=str(server_status["daq_name"]),
        host=str(server_status["host"]),
        status="compressed" if ok else "failed",
        detail=_format_cmd_result(result),
        required=True,
        local=True,
        command=command,
        script=script,
    )


def _node_compress_logs(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
    *,
    dry_run: bool,
) -> StopResult:
    """Compress completed local or remote agent log/telemetry files."""

    node_status = _node_stop_status(key, raw_node, defaults)
    node = node_status["config"]
    resolved = node_status["resolved"]
    paths = [str(resolved["logdir"]), str(resolved["telem_dir"])]
    script = _compress_logs_script(paths)
    is_local = _node_is_local(node)
    command = _node_command(node, script)
    required = _str_bool(node.get("required", False))

    if dry_run:
        return StopResult(
            kind="compress",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="dry-run",
            detail="would gzip -9 agent log and telemetry files",
            required=required,
            local=is_local,
            command=command,
            script=script,
        )

    transport_result = _node_run(node, "true")
    if transport_result.returncode != 0:
        return StopResult(
            kind="compress",
            key=key,
            daq_name=str(node_status["daq_name"]),
            host=str(node_status["host"]),
            status="failed",
            detail=("local execution failed: " if is_local else "ssh unreachable: ")
            + _format_cmd_result(transport_result),
            required=required,
            local=is_local,
            command=command,
            script=script,
        )

    result = _node_run(node, script, timeout=float(node.get("compress_timeout_sec", 300.0)))
    ok = result.returncode == 0
    return StopResult(
        kind="compress",
        key=key,
        daq_name=str(node_status["daq_name"]),
        host=str(node_status["host"]),
        status="compressed" if ok else "failed",
        detail=_format_cmd_result(result),
        required=required,
        local=is_local,
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
    compress_logs: bool = False,
    runner: str | None = None,
) -> dict[str, Any]:
    """Stop GNSS agents and, when appropriate, the local GNSS server.

    By default, stopping the full deployment stops all selected agents first and
    then stops the local server. If one or more --node filters are supplied, the
    default is intentionally narrower: only those agents are stopped, so a
    single-node maintenance action does not accidentally take down the server.
    The configured runner is used unless runner supplies an explicit override.
    """

    config = load_config(config_path)
    defaults = config.get("defaults", {})
    results: list[StopResult] = []

    stop_agents = not server_only
    stop_server = server_only or (not agents_only and not nodes)

    if stop_agents:
        for key, raw_node in _selected_node_items(config, nodes, include_disabled=include_disabled):
            stop_result = _stop_node(
                key,
                raw_node,
                defaults,
                dry_run=dry_run,
                runner_override=runner,
            )
            results.append(stop_result)
            if compress_logs and stop_result.status in {"stopped", "dry-run"}:
                results.append(_node_compress_logs(key, raw_node, defaults, dry_run=dry_run))

    if stop_server:
        stop_result = _stop_server(
            config,
            dry_run=dry_run,
            runner_override=runner,
        )
        results.append(stop_result)
        if compress_logs and stop_result.status in {"stopped", "dry-run"}:
            results.append(_server_compress_logs(config, dry_run=dry_run))

    return {
        "config_path": str(config_path),
        "dry_run": dry_run,
        "server_only": server_only,
        "agents_only": agents_only,
        "compress_logs": compress_logs,
        "runner": runner,
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
    if report.get("runner"):
        print(f"Runner: {report['runner']} (override)")
    if report.get("verify_registers"):
        print(f"Timing: {report.get('mode', 'differential')}")
        print("Register verify: enabled")
    print()
    for item in report["results"]:
        label = item["daq_name"]
        host = item["host"]
        status = _check_status(item["checks"])
        print(
            f"{status:4} {item['kind']:6} {item['key']:12} {label:18} "
            f"host={host} runner={item.get('runner', 'screen')}"
        )
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
    if report.get("runner"):
        print(f"Runner: {report['runner']} (override)")
    print()
    for item in report["results"]:
        print(f"{item.status:8} {item.kind:6} {item.key:12} {item.daq_name:18} host={item.host}")
        if item.detail:
            print(f"  detail: {item.detail}")
        if report.get("dry_run") and item.script:
            if item.kind in {"node", "bodnar"}:
                if item.local:
                    print("  target: local")
                    print("  local script:")
                else:
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
    if report.get("runner"):
        print(f"Runner: {report['runner']} (override)")
    if report.get("compress_logs"):
        print("Compress logs: enabled")
    print()
    for item in report["results"]:
        print(f"{item.status:8} {item.kind:6} {item.key:12} {item.daq_name:18} host={item.host}")
        if item.detail:
            print(f"  detail: {item.detail}")
        if report.get("dry_run") and item.script:
            if item.kind == "node" or (item.kind == "compress" and item.key != "server"):
                if item.local:
                    print("  target: local")
                    print("  local script:")
                else:
                    print(f"  target: ssh {item.host}")
                    print("  remote script:")
            else:
                print("  local script:")
            for line in item.script.splitlines():
                print(f"    {line}")
        print()


def _print_install(report: dict[str, Any]) -> None:
    """Print a systemd service installation report."""

    mode = "dry-run" if report.get("dry_run") else "install-service"
    print(f"Config: {report['config_path']}")
    print(f"Mode:   {mode}")
    print(f"Timing: {report['mode']}")
    print()
    for item in report["results"]:
        print(
            f"{item.status:9} {item.kind:6} {item.key:12} "
            f"{item.daq_name:18} host={item.host}"
        )
        print(f"  service: {item.service_name}")
        if item.detail:
            print(f"  detail: {item.detail}")
        if report.get("dry_run") and item.script:
            print("  rendered unit:")
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
        if isinstance(item, (StartResult, StopResult, InstallResult)):
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

    def add_config_argument(arg_parser: argparse.ArgumentParser, *, default: str | Any = argparse.SUPPRESS) -> None:
        """Add the deployment config option to the root parser or a subcommand."""
        arg_parser.add_argument(
            "--config",
            default=default,
            help=f"path to deployment JSON5 config (default: {DEFAULT_CONFIG})",
        )

    def add_runner_argument(arg_parser: argparse.ArgumentParser) -> None:
        """Add an optional process-runner override."""
        arg_parser.add_argument(
            "--runner",
            choices=["screen", "systemd"],
            default=None,
            help="override process runner from deployment config",
        )

    parser = argparse.ArgumentParser(description="GNSS deployment orchestrator")
    add_config_argument(parser, default=str(DEFAULT_CONFIG))
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="validate config and check GNSS deployment prerequisites")
    add_config_argument(status)
    add_runner_argument(status)
    status.add_argument("--node", action="append", default=[], help="limit status to a node key, host, or DAQ name")
    status.add_argument("--mode", choices=["differential", "absolute"], default="differential", help="GNSS timing mode for register verification")
    status.add_argument("--verify-registers", action="store_true", help="read receiver CFG registers and compare against the selected manifest")
    status.add_argument("--include-disabled", action="store_true", help="include nodes marked present=false")
    status.add_argument("--local-only", action="store_true", help="skip SSH and remote checks")
    status.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    start = sub.add_parser("start", help="start the GNSS server and selected agents")
    add_config_argument(start)
    add_runner_argument(start)
    start.add_argument("--node", action="append", default=[], help="limit start to a node key, host, or DAQ name")
    start.add_argument("--mode", choices=["differential", "absolute"], default="differential", help="GNSS timing mode")
    start.add_argument("--bodnar", action="store_true", help="configure Leo Bodnar LBE-1420 devices before starting agents")
    start.add_argument("--include-disabled", action="store_true", help="include nodes marked present=false")
    start.add_argument("--dry-run", action="store_true", help="show launch commands without running them")
    start.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    stop = sub.add_parser("stop", help="stop selected GNSS agents and optionally the server")
    add_config_argument(stop)
    add_runner_argument(stop)
    stop.add_argument("--node", action="append", default=[], help="limit stop to a node key, host, or DAQ name")
    stop.add_argument("--include-disabled", action="store_true", help="include nodes marked present=false")
    stop.add_argument("--server-only", action="store_true", help="stop only the local GNSS server")
    stop.add_argument("--agents-only", action="store_true", help="stop agents without stopping the local GNSS server")
    stop.add_argument("--compress-logs", action="store_true", help="gzip -9 completed log and telemetry files after stopping")
    stop.add_argument("--dry-run", action="store_true", help="show stop commands without running them")
    stop.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    install = sub.add_parser(
        "install-service",
        help="install or update systemd user services without starting them",
    )
    add_config_argument(install)
    install.add_argument(
        "--node",
        action="append",
        default=[],
        help="install an agent service by node key, host, or DAQ name; repeatable",
    )
    install.add_argument("--all-nodes", action="store_true", help="install agent services on all present nodes")
    install.add_argument("--server", action="store_true", help="install the local GNSS server service")
    install.add_argument(
        "--mode",
        choices=["differential", "absolute"],
        default="differential",
        help="timing mode embedded in the server service",
    )
    install.add_argument("--include-disabled", action="store_true", help="include nodes marked present=false")
    install.add_argument("--dry-run", action="store_true", help="render units without installing them")
    install.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Returns:
        Process exit code. For status, the code is nonzero only if the server or
        a node marked required fails validation.
    """

    args = parse_args(argv)
    try:
        if args.command == "status":
            report = status_gnss(
                args.config,
                nodes=args.node,
                include_disabled=args.include_disabled,
                local_only=args.local_only,
                verify_registers=args.verify_registers,
                mode=args.mode,
                runner=args.runner,
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
                runner=args.runner,
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
                compress_logs=args.compress_logs,
                runner=args.runner,
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

        if args.command == "install-service":
            report = install_services(
                args.config,
                nodes=args.node,
                install_server=args.server,
                all_nodes=args.all_nodes,
                include_disabled=args.include_disabled,
                mode=args.mode,
                dry_run=args.dry_run,
            )
            if args.json:
                print(json.dumps(_jsonable(report), indent=2, sort_keys=True))
            else:
                _print_install(report)

            return 1 if any(item.status == "failed" for item in report["results"]) else 0
    except (ConfigLoadError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
