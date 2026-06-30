#!/usr/bin/env bash
set -euo pipefail


SERVER_PY="/home/obs/miniconda3/envs/pygnss_39/bin/python"
SERVER_SCRIPT="/home/obs/U-Blox_F9T_Config/server_v1.py"
SERVER_LOGDIR="/home/obs/U-Blox_F9T_Config/logging"
SERVER_SCREEN="gnss_server"

REMOTE_USER="panoseti"
REMOTE_FIND_GNSS="/home/panoseti/gnss_scripts/find_ublox.sh"

# Use absolute interpreter path (conda/venv) on the remote host
REMOTE_PY="/home/panoseti/miniconda3/envs/pygnss_39/bin/python"

# Remote paths
REMOTE_AGENT="/home/panoseti/U-Blox_F9T_Config/agent_v1.py"
REMOTE_LOGDIR="/home/panoseti/U-Blox_F9T_Config/logging"

# Addresses passed to the agent (server IP:port as seen by the remote hosts)
CAST_ADDR="10.200.146.1:50051"
CTRL_ADDR="10.200.146.1:50051"

# Screen session name
SCREEN_NAME="gnss_agent"

# Netcat probe settings
NC_TIMEOUT_SEC=1

# SSH settings (BatchMode prevents password prompts; set to "no" if you want prompts)
SSH_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout=3
  -o StrictHostKeyChecking=accept-new
)

LOCAL_HOSTNAMES=(
  "$(hostname)"
  "$(hostname -s)"
  "$(hostname -f 2>/dev/null || true)"
)

echo "[INFO] Starting GNSS server…"

# Sanity checks
if [[ ! -x "$SERVER_PY" ]]; then
  echo "[FAIL] Python interpreter not executable: $SERVER_PY"
  exit 1
fi

if [[ ! -f "$SERVER_SCRIPT" ]]; then
  echo "[FAIL] Server script not found: $SERVER_SCRIPT"
  exit 1
fi

mkdir -p "$SERVER_LOGDIR"

# Stop existing server if present
if screen -ls | grep -q "\.${SERVER_SCREEN}[[:space:]]"; then
  echo "[INFO] Existing server found — stopping"
  screen -S "$SERVER_SCREEN" -X quit
  sleep 0.5
fi

# Start server in screen
screen -dmS "$SERVER_SCREEN" bash -c "
  set -euo pipefail
  exec \"$SERVER_PY\" -u \"$SERVER_SCRIPT\" \
    >> \"$SERVER_LOGDIR/${SERVER_SCREEN}.log\" 2>&1
"

# Give it a moment to fail fast if broken
sleep 1

# Verify screen session exists
if screen -ls | grep -q "\.${SERVER_SCREEN}[[:space:]]"; then
  echo "[OK]   GNSS server running in screen:${SERVER_SCREEN}"
  echo "       Log: ${SERVER_LOGDIR}/${SERVER_SCREEN}.log"
else
  echo "[FAIL] GNSS server failed to start"
  echo "       Check log:"
  echo "       ${SERVER_LOGDIR}/${SERVER_SCREEN}.log"
  tail -n 20 "${SERVER_LOGDIR}/${SERVER_SCREEN}.log" || true
  exit 1
fi

# Parse /etc/hosts:
# - ignore comments/blank lines
# - ignore IPv6 lines
# - ignore localhost-ish entries
# - emit hostnames
mapfile -t HOSTS < <(
  awk '
    $0 ~ /^[[:space:]]*#/ {next}
    NF < 2 {next}
    $1 ~ /:/ {next}                       # skip IPv6
    {
      for (i=2; i<=NF; i++) {
        h=$i
        # strip inline comments from hostname tokens just in case
        sub(/#.*/, "", h)
        if (h=="" ) continue
        if (h ~ /^(localhost|localhost\.localdomain)$/) continue
        print h
      }
    }
  ' /etc/hosts | sort -u
)

if [[ ${#HOSTS[@]} -eq 0 ]]; then
  echo "No hosts found in /etc/hosts (after filtering)."
  exit 1
fi

echo "Found ${#HOSTS[@]} host(s) in /etc/hosts."

start_on_host() {
  local host="$1"

  # Probe port 22 with netcat
  if ! nc -z -w "${NC_TIMEOUT_SEC}" "${host}" 22 >/dev/null 2>&1; then
    echo "[DOWN] ${host}:22"
    return 0
  fi

  # Check GNSS presence remotely (we only care about exit code)
  if ! ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${host}" "${REMOTE_FIND_GNSS}" >/dev/null 2>&1; then
    echo "[SKIP] ${host}: GNSS receiver not detected"
    return 0
  fi

  echo "[UP]   ${host}:22  -> GNSS present -> starting agent"

  ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${host}" bash -c "'
    set -euo pipefail
    mkdir -p \"${REMOTE_LOGDIR}\"
    screen -S \"${SCREEN_NAME}\" -X quit >/dev/null 2>&1 || true
    cd \"$(dirname "${REMOTE_AGENT}")\"
    screen -dmS \"${SCREEN_NAME}\" \
      \"${REMOTE_PY}\" -u \"${REMOTE_AGENT}\" \
        --cast_addr \"${CAST_ADDR}\" \
        --ctrl_addr \"${CTRL_ADDR}\" \
        -v 3 \
      >> \"${REMOTE_LOGDIR}/${SCREEN_NAME}.log\" 2>&1
    echo \"started ${SCREEN_NAME}\"
  '" || {
    echo "[FAIL] ${host}: ssh/start failed"
    return 0
  }

  ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${host}" \
    "screen -ls | grep -q \"\.${SCREEN_NAME}[[:space:]]\" && echo \"[OK]   ${host}: screen up\" || echo \"[WARN] ${host}: screen not found (agent may have exited)\""
}

for h in "${HOSTS[@]}"; do
  # Skip this machine
  for me in "${LOCAL_HOSTNAMES[@]}"; do
    if [[ -n "$me" && "$h" == "$me" ]]; then
      echo "[SKIP] ${h}: local server host"
      continue 2   # skip to next h
    fi
  done
  # Normal path: start agent on remote host
  start_on_host "$h"
done
