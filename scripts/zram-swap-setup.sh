#!/usr/bin/env bash
# Configure the host swap layout AgentENV's vPause expects: disable zswap and
# set up same-priority zram devices as the primary swap tier, with any
# existing lower-priority swapfile kept as overflow.
#
# Why: vPause funnels `process_madvise(MADV_PAGEOUT)` into the swap subsystem.
# One swap area serializes that traffic on a single lock domain (per-device
# si->lock plus zswap's per-device tree and global pool locks). zram removes
# the zswap locks entirely and more devices shard the remaining per-device
# locks further. zswap must be disabled explicitly: mainline zswap (checked on
# 6.6) happily fronts zram devices and would compress every page twice.
#
# Sizing note: the kernel hard-caps swap areas at MAX_SWAPFILES = 32, and
# every zram device joins every NUMA node's per-priority rotation list
# (swap_avail_heads[] + plist_requeue), so devices cannot be bound to a node.
# The useful lever is the device COUNT: --per-node N creates N devices per
# online NUMA node and the script clamps the total to 31, leaving one slot
# for the disk swapfile. Compressed pages are allocated on the writer's local
# node by zsmalloc, so pageout workers already get node-local memory.
#
# Usage:
#   sudo bash scripts/zram-swap-setup.sh [--per-node N] [--size-mib M]
#                                        [--algorithm lz4] [--priority P]
#
# Defaults: 4 devices per NUMA node x 8192 MiB each, lz4, priority 100.
# Budget = per-node count x nodes x size (e.g. 4 nodes -> 16 devices -> 128 GiB).
#
# Cleanup: scripts/zram-swap-teardown.sh

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "error: this script requires root (run with sudo)" >&2
    exit 1
fi

PER_NODE=4
SIZE_MIB=8192
ALGORITHM=lz4
PRIORITY=100
# MAX_SWAPFILES (5-bit swap type) minus one slot for the disk swapfile.
MAX_AREAS=31

while [[ $# -gt 0 ]]; do
    case "$1" in
        --per-node) PER_NODE="$2"; shift 2 ;;
        --size-mib) SIZE_MIB="$2"; shift 2 ;;
        --algorithm) ALGORITHM="$2"; shift 2 ;;
        --priority) PRIORITY="$2"; shift 2 ;;
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

if ! [[ "$PER_NODE" =~ ^[1-9][0-9]*$ ]] || ! [[ "$SIZE_MIB" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: --per-node and --size-mib must be positive integers" >&2
    exit 1
fi

NODES=$(ls /sys/devices/system/node/ 2>/dev/null | grep -c '^node[0-9]*$')
if [[ "$NODES" -eq 0 ]]; then
    NODES=1
fi

WANT=$((PER_NODE * NODES))
if [[ $WANT -gt $MAX_AREAS ]]; then
    WANT=$MAX_AREAS
    echo "note: clamped to $MAX_AREAS devices (kernel MAX_SWAPFILES=32, one slot"
    echo "      reserved for the disk swapfile)"
fi
EXISTING=$(swapon --show=NAME --noheadings 2>/dev/null | wc -l)
LIMIT=$((MAX_AREAS + 1))
if [[ $((WANT + EXISTING)) -gt $LIMIT ]]; then
    WANT=$((LIMIT - EXISTING))
    echo "note: $EXISTING swap area(s) already active; reduced to $WANT new devices"
fi
if [[ $WANT -le 0 ]]; then
    echo "error: no swap-area slots left for zram devices" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 1. Disable zswap
# ---------------------------------------------------------------------------
if [[ -w /sys/module/zswap/parameters/enabled ]]; then
    echo "zswap: disabling"
    echo 0 > /sys/module/zswap/parameters/enabled
fi
ZSWAP_CMDLINE_NOTE=0
if grep -qE '\bzswap\.enabled=1\b' /proc/cmdline 2>/dev/null; then
    ZSWAP_CMDLINE_NOTE=1
fi

# ---------------------------------------------------------------------------
# 2. Load the zram module with room for the devices
# ---------------------------------------------------------------------------
if [[ ! -d /sys/module/zram ]]; then
    echo "zram: loading module (num_devices=$WANT)"
    modprobe zram num_devices="$WANT"
elif [[ ! -e /sys/class/zram-control/hot_add ]]; then
    echo "error: zram is loaded without zram-control; reboot with the module unloaded first" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 3. Create and activate the devices
# ---------------------------------------------------------------------------
# Reuses already-idle /dev/zram[0..] devices and hot-adds any extras needed.
created=0
index=0
while [[ $created -lt $WANT ]]; do
    dev="/dev/zram$index"
    if [[ ! -e "$dev" ]]; then
        new_index=$(cat /sys/class/zram-control/hot_add)
        dev="/dev/zram$new_index"
        index=$new_index
    fi
    disk="${dev#/dev/}"
    if [[ ! -e "/sys/block/$disk" ]]; then
        index=$((index + 1))
        continue
    fi
    if swapon --show=NAME --noheadings 2>/dev/null | grep -q "^$dev$"; then
        index=$((index + 1))
        continue
    fi
    echo "$ALGORITHM" > "/sys/block/$disk/comp_algorithm"
    echo "$((SIZE_MIB * 1024 * 1024))" > "/sys/block/$disk/disksize"
    mkswap "$dev" > /dev/null
    swapon -p "$PRIORITY" "$dev"
    echo "zram: activated $dev ($SIZE_MIB MiB, $ALGORITHM, priority $PRIORITY)"
    created=$((created + 1))
    index=$((index + 1))
done

# ---------------------------------------------------------------------------
# 4. Report the resulting layout
# ---------------------------------------------------------------------------
echo
echo "swap layout:"
swapon --show
echo
total_mib=$((created * SIZE_MIB))
echo "zram tier: ${created} x ${SIZE_MIB} MiB = ${total_mib} MiB compressed RAM on ${NODES} NUMA node(s)"
if [[ $ZSWAP_CMDLINE_NOTE -eq 1 ]]; then
    echo "warning: zswap.enabled=1 is on the kernel command line; it will re-enable"
    echo "         zswap on the next boot. Remove it from the bootloader config."
fi
if ! swapon --show=PRIO --noheadings 2>/dev/null | grep -qv "^$PRIORITY$"; then
    echo "note: no lower-priority disk swap found. Keep a low-priority swapfile"
    echo "      (e.g. swapon -p 10) so cold pages overflow to disk instead of"
    echo "      failing when the zram tier fills."
fi
