#!/usr/bin/env bash
set -euo pipefail

found=0

for t in /sys/class/tty/ttyACM*; do
  [ -e "$t" ] || continue
  devpath="$(readlink -f "$t/device")"

  # Walk upward until we find idVendor/idProduct
  p="$devpath"
  while [ "$p" != "/" ] && [ ! -f "$p/idVendor" ]; do
    p="$(dirname "$p")"
  done

  if [ -f "$p/idVendor" ]; then
    vid="$(cat "$p/idVendor")"
    pid="$(cat "$p/idProduct" 2>/dev/null || true)"
    prod="$(cat "$p/product" 2>/dev/null || true)"
    mfg="$(cat "$p/manufacturer" 2>/dev/null || true)"

    if [ "$vid" = "1546" ]; then
      echo "/dev/$(basename "$t")  vid:pid=${vid}:${pid}  mfg='${mfg}'  prod='${prod}'"
      found=1
    fi
  fi
done

if [ "$found" -eq 1 ]; then
  exit 0
fi

exit 1
