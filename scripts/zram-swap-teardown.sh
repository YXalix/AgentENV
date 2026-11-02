#!/usr/bin/env bash
# Tear down the zram swap tier created by scripts/zram-swap-setup.sh:
# swap off every active zram device and reset it. Disk-backed swap areas
# (swapfiles/partitions) are left untouched. Pages still parked in zram are
# dropped back to resident memory as each device is swapped off, so make sure
# the host has room before running under memory pressure.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: this script requires root (run with sudo)" >&2
    exit 1
fi

active=$(swapon --show=NAME --noheadings 2>/dev/null | grep -E '^/dev/zram[0-9]+$' || true)

if [[ -z "$active" ]]; then
    echo "no active zram swap devices"
else
    for dev in $active; do
        echo "zram: swapping off $dev"
        swapoff "$dev"
        disk="${dev#/dev/}"
        echo "zram: resetting $dev"
        echo 1 > "/sys/block/$disk/reset"
    done
fi

echo
echo "remaining swap layout:"
swapon --show || true
