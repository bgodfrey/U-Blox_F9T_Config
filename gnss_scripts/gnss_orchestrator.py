#!/usr/bin/env python3
"""GNSS deployment orchestration.

The first implemented command is intentionally read-only:

    python gnss_scripts/gnss_orchestrator.py status

It validates the deployment inventory and checks local/remote prerequisites
without starting or stopping GNSS processes.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
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
            passed to `bash -lc` on the remote host.
        timeout: Optional total timeout for the SSH command.

    Returns:
        Normalized stdout/stderr/returncode from the SSH command.
    """

    if timeout is None:
        timeout = float(node.get("ssh_connect_timeout_sec", 3)) + 10.0
    return _run(_ssh_base(node) + ["bash -lc " + shlex.quote(script)], timeout=timeout)


def _remote_test(node: dict[str, Any], test_arg: str, path: str, timeout: float | None = None) -> Check:
    """Run a remote POSIX file test and return it as a status Check.

    Examples:
        _remote_test(node, "-x", "/path/to/python") checks executability.
        _remote_test(node, "-d", "/path/to/repo") checks directory existence.
    """

    result = _remote_run(node, f"test {test_arg} {shlex.quote(path)}", timeout=timeout)
    return Check(f"remote {test_arg} {path}", result.returncode == 0, _format_cmd_result(result))


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

    # Server script/logdir paths may be absolute, but the usual case is that
    # they are stored relative to the server repo.
    repo = str(server.get("repo", ""))
    script = str(server.get("script") or server.get("server_script") or "server_v1.py")
    server_script = _resolve_under_repo(repo, script) if repo else script
    logdir = str(server.get("logdir", "logging"))
    logdir_path = _resolve_under_repo(repo, logdir) if repo else logdir

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
        },
    }


def _node_status(
    key: str,
    raw_node: dict[str, Any],
    defaults: dict[str, Any],
    *,
    local_only: bool,
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
    enabled = _str_bool(node.get("enabled", True))

    # Resolve the paths that future start/stop commands will use. The status
    # command reports these resolved values so config mistakes are easy to spot.
    repo = str(node.get("repo", ""))
    agent_script = _resolve_under_repo(repo, str(node.get("agent_script", "agent_v1.py"))) if repo else ""
    find_ublox = _resolve_under_repo(repo, str(node.get("find_ublox_script", "gnss_scripts/find_ublox.sh"))) if repo else ""
    logdir = _resolve_under_repo(repo, str(node.get("logdir", "logging"))) if repo else ""
    telem_dir = _resolve_under_repo(repo, str(node.get("telem_dir", "telem"))) if repo else ""

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
    if not enabled:
        checks.append(Check("node enabled", True, "disabled; remote checks skipped"))
    elif local_only:
        # Local-only mode is useful during config editing and CI because it does
        # not require network access or SSH keys.
        checks.append(Check("remote checks", True, "skipped by --local-only"))
    else:
        # First prove SSH works. If it does not, the remaining remote path checks
        # would all fail for the same reason and make the report noisier.
        checks.append(Check("ssh reachable", False, "not checked"))
        ssh_result = _remote_run(node, "true")
        checks[-1] = Check("ssh reachable", ssh_result.returncode == 0, _format_cmd_result(ssh_result))

        if ssh_result.returncode == 0:
            # Check the remote files/directories that a later `start` command
            # will rely on. Parent-directory checks allow log/telem directories
            # to be created by the startup path later.
            checks.append(_remote_test(node, "-x", str(node["python"])))
            checks.append(_remote_test(node, "-d", repo))
            checks.append(_remote_test(node, "-f", agent_script))
            checks.append(_remote_test(node, "-x", find_ublox))
            checks.append(_remote_test(node, "-d", os.path.dirname(logdir)))
            checks.append(_remote_test(node, "-d", os.path.dirname(telem_dir)))

            # Receiver detection uses the same helper script that the shell
            # startup path used. Exit code 0 means at least one u-blox device was
            # found; nonzero means no receiver or an execution problem.
            find_result = _remote_run(node, shlex.quote(find_ublox), timeout=15.0)
            gnss_detected = find_result.returncode == 0
            detail = find_result.stdout if gnss_detected else _format_cmd_result(find_result)
            checks.append(Check("gnss receiver detected", gnss_detected, detail))

    return {
        "kind": "node",
        "key": key,
        "daq_name": node.get("daq_name", key),
        "host": node.get("host", key),
        "enabled": enabled,
        "required": _str_bool(node.get("required", False)),
        "gnss_detected": gnss_detected,
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
        },
    }


def status_gnss(
    config_path: str | os.PathLike[str] = DEFAULT_CONFIG,
    *,
    nodes: list[str] | None = None,
    include_disabled: bool = False,
    local_only: bool = False,
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

    # Always include the server status. Node filtering applies only to DAQ nodes.
    results = [_server_status(config)]
    for key, node in raw_nodes.items():
        # Let users filter by inventory key, SSH host, or DAQ/display name.
        if selected and key not in selected and node.get("host") not in selected and node.get("daq_name") not in selected:
            continue
        enabled = _str_bool(_merge(defaults, node).get("enabled", True))
        if not enabled and not include_disabled:
            continue
        results.append(_node_status(key, node, defaults, local_only=local_only))

    return {"config_path": str(config_path), "results": results}


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


def _jsonable(report: dict[str, Any]) -> dict[str, Any]:
    """Convert dataclass checks to plain dicts for JSON output.

    The internal report contains Check dataclass instances. json.dumps cannot
    serialize those directly, so this creates an equivalent plain-Python object.
    """

    out = {"config_path": report["config_path"], "results": []}
    for item in report["results"]:
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
    status.add_argument("--include-disabled", action="store_true", help="include disabled nodes")
    status.add_argument("--local-only", action="store_true", help="skip SSH and remote checks")
    status.add_argument("--json", action="store_true", help="emit machine-readable JSON")
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

    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
