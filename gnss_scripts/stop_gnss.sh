#!/usr/bin/env bash
set -euo pipefail

REMOTE_USER="panoseti"
SCREEN_AGENT="gnss_agent"
SCREEN_SERVER="gnss_server"

SSH_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout=3
)

# Get local hostnames so we can skip SSH to ourselves
LOCAL_HOSTNAMES=(
  "$(hostname)"
  "$(hostname -s)"
)

echo "=== Stopping GNSS network ==="


# ---------------- parse /etc/hosts ----------------
mapfile -t HOSTS < <(
  awk '
    $0 ~ /^[[:space:]]*#/ {next}
    NF < 2 {next}
    $1 ~ /:/ {next}
    {
      for (i=2; i<=NF; i++) {
        h=$i
        sub(/#.*/, "", h)
        if (h=="" ) continue
        if (h ~ /^(localhost|localhost\.localdomain)$/) continue
        print h
      }
    }
  ' /etc/hosts | sort -u
)

# ---------------- stop agents ----------------
for h in "${HOSTS[@]}"; do

  # Skip local machine
  for me in "${LOCAL_HOSTNAMES[@]}"; do
    if [[ "$h" == "$me" ]]; then
      echo "[SKIP] ${h}: local server host"
      continue 2
    fi
  done

  # Check SSH reachability
  if ! ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${h}" "true"> /dev/null 2>&1; then
    echo "[DOWN] ${h}: ssh unreachable"
    continue
  fi

  echo "[INFO] Stopping agent on ${h}"

  ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${h}" "
    screen -S \"$SCREEN_AGENT\" -X quit >/dev/null 2>&1 || true
    sleep 0.5
    if screen -ls | grep -q \"\.${SCREEN_AGENT}[[:space:]]\"; then
      echo \"[FAIL] ${h}: agent still running\"
    else
      echo \"[OK]   ${h}: agent stopped\"
    fi
  "

done

# ---------------- stop local server ----------------
echo "[INFO] Stopping local server (${SCREEN_SERVER})"

screen -S "$SCREEN_SERVER" -X quit >/dev/null 2>&1 || true
sleep 0.5

if screen -ls | grep -q "\.${SCREEN_SERVER}[[:space:]]"; then
  echo "[FAIL] server still running"
else
  echo "[OK]   server stopped"
fi

echo "=== GNSS shutdown complete ==="
