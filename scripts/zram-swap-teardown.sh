#!/usr/bin/env bash
# Tear down the zram swap tier created by scripts/zram-swap-setup.sh: swap off
# every active zram device and reset every initialized one, including idle
# leftovers from aborted runs (setup refuses to configure those otherwise).
# With --remove, devices are additionally deleted via zram-control hot_remove
# instead of being left in place. Disk-backed swap areas (swapfiles/partitions)
# are left untouched. Pages still parked in zram are dropped back to resident
# memory as each device is swapped off, so make sure the host has room before
# running under memory pressure.
#
# Usage:
#   sudo bash scripts/zram-swap-teardown.sh [--remove]

set -euo pipefail

REMOVE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --remove) REMOVE=1; shift ;;
        -h|--help)
            grep '^#' "$0" | tail -n +2 | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1 (see --help)" >&2
            exit 1
            ;;
    esac
done

if [[ $EUID -ne 0 ]]; then
    echo "error: this script requires root (run with sudo)" >&2
    exit 1
fi

# Swap off active zram swaps first: reset and hot_remove refuse while in use.
active=$(swapon --show=NAME --noheadings 2>/dev/null | grep -E '^/dev/zram[0-9]+$' || true)
if [[ -z "$active" ]]; then
    echo "no active zram swap devices"
else
    for dev in $active; do
        echo "zram: swapping off $dev"
        swapoff "$dev"
    done
fi

# Reset every initialized device. Idle ones with a stale disksize count too:
# setup's comp_algorithm/disksize writes fail with EBUSY on those otherwise.
for zdisk in /sys/block/zram*; do
    [[ -e "$zdisk/disksize" ]] || continue
    if [[ "$(cat "$zdisk/disksize")" -ne 0 ]]; then
        echo "zram: resetting ${zdisk##*/}"
        if ! echo 1 > "$zdisk/reset"; then
            echo "warning: could not reset ${zdisk##*/} (held open by another process?)" >&2
        fi
    fi
done

if [[ $REMOVE -eq 1 ]]; then
    if [[ ! -e /sys/class/zram-control/hot_remove ]]; then
        echo "warning: zram-control/hot_remove unavailable; devices left in place" >&2
    else
        for zdisk in /sys/block/zram*; do
            [[ -e "$zdisk/disksize" ]] || continue
            disk="${zdisk##*/}"
            echo "zram: removing $disk"
            if ! echo "${disk#zram}" > /sys/class/zram-control/hot_remove; then
                echo "warning: could not remove $disk (still in use)" >&2
            fi
        done
    fi
fi

echo
echo "remaining swap layout:"
swapon --show || true
