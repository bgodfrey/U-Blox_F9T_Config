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
    pid="$(cat "$p/idProduct")"
    prod="$(cat "$p/product" 2>/dev/null || true)"
    mfg="$(cat "$p/manufacturer" 2>/dev/null || true)"

    if [ "$vid" = "1546" ]; then
      echo "/dev/$(basename "$t")  vid:pid=${vid}:${pid}  mfg='${mfg}'  prod='${prod}'"
    fi
  fi
done
