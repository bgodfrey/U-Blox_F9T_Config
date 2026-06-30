#!/usr/bin/env bash
set -euo pipefail

REMOTE_USER="panoseti"

SCREEN_AGENT="gnss_agent"
SCREEN_SERVER="gnss_server"

GRACE_SEC=5

SSH_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout=5
  -o StrictHostKeyChecking=accept-new
)

LOCAL_HOSTNAMES=(
  "$(hostname)"
  "$(hostname -s)"
)

# Parse /etc/hosts
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

echo "=== GNSS shutdown ==="

stop_remote_agent() {
  local host="$1"

  # Run a remote bash script via stdin
  ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${host}" bash -s -- \
    "$SCREEN_AGENT" "$GRACE_SEC" <<'REMOTE'
set -euo pipefail
SCREEN_NAME="$1"
GRACE_SEC="$2"

# 1) Nice stop: send Ctrl+C into screen if it exists
if screen -ls 2>/dev/null | awk -v n="$SCREEN_NAME" '$1 ~ ("\\." n "$") {found=1} END{exit found?0:1}'; then
  # send ^C to each matching session id
  while read -r sid; do
    screen -S "$sid" -X stuff $'\003' || true
  done < <(screen -ls 2>/dev/null | awk -v n="$SCREEN_NAME" '$1 ~ ("\\." n "$") {print $1}')
fi

# 2) Also SIGTERM the python (extra insurance)
pkill -TERM -f 'agent_v1\.py' >/dev/null 2>&1 || true

# 3) Wait a bit
for _ in $(seq 1 "$GRACE_SEC"); do
  if pgrep -f 'agent_v1\.py' >/dev/null 2>&1; then
    sleep 1
  else
    break
  fi
done

# 4) Close any remaining screen sessions and wipe dead sockets
while read -r sid; do
  screen -S "$sid" -X quit >/dev/null 2>&1 || true
done < <(screen -ls 2>/dev/null | awk -v n="$SCREEN_NAME" '$1 ~ ("\\." n "$") {print $1}')

screen -wipe >/dev/null 2>&1 || true

# 5) Verify
if screen -ls 2>/dev/null | awk -v n="$SCREEN_NAME" '$1 ~ ("\\." n "$") {found=1} END{exit found?0:1}'; then
  echo "[FAIL] agent screen still present"
  exit 1
else
  echo "[OK] agent stopped"
fi
REMOTE
}

# -------- stop agents remotely --------
for h in "${HOSTS[@]}"; do
  # skip local server hostnames
  for me in "${LOCAL_HOSTNAMES[@]}"; do
    if [[ -n "$me" && "$h" == "$me" ]]; then
      echo "[SKIP] ${h}: local server host"
      continue 2
    fi
  done

  # ssh exec check (auth + hostkey)
  if ! ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${h}" true >/dev/null 2>&1; then
    echo "[FAIL] ${h}: ssh failed"
    continue
  fi

  echo "[INFO] ${h}: stopping agent"
  if stop_remote_agent "$h"; then
    echo "[OK]   ${h}: agent stopped"
  else
    echo "[FAIL] ${h}: agent stop failed"
  fi
done

# -------- stop local server --------
echo "[INFO] stopping local server"

# Nice stop: Ctrl+C into screen if present
if screen -ls 2>/dev/null | awk -v n="$SCREEN_SERVER" '$1 ~ ("\\." n "$") {found=1} END{exit found?0:1}'; then
  while read -r sid; do
    screen -S "$sid" -X stuff $'\003' || true
  done < <(screen -ls 2>/dev/null | awk -v n="$SCREEN_SERVER" '$1 ~ ("\\." n "$") {print $1}')
fi

# Also SIGTERM python
pkill -TERM -f 'server_v1\.py' >/dev/null 2>&1 || true

# Wait
for _ in $(seq 1 "$GRACE_SEC"); do
  pgrep -f 'server_v1\.py' >/dev/null 2>&1 || break
  sleep 1
done

# Quit any remaining screen and wipe
while read -r sid; do
  screen -S "$sid" -X quit >/dev/null 2>&1 || true
done < <(screen -ls 2>/dev/null | awk -v n="$SCREEN_SERVER" '$1 ~ ("\\." n "$") {print $1}')

screen -wipe >/dev/null 2>&1 || true

if screen -ls 2>/dev/null | awk -v n="$SCREEN_SERVER" '$1 ~ ("\\." n "$") {found=1} END{exit found?0:1}'; then
  echo "[FAIL] local server screen still present"
  exit 1
else
  echo "[OK]   local server stopped"
fi

echo "=== GNSS shutdown complete ==="
