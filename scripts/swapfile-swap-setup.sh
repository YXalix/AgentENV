#!/usr/bin/env bash
# Add a disk-backed swapfile tier for vPause: create --count swapfiles under
# --dir and activate them at one priority. Complements
# scripts/zram-swap-setup.sh: at the zram tier's priority (default 100) the
# files join the same per-NUMA rotation and shard the per-area swap locks
# further; at a lower priority they act as overflow when the zram tier fills.
#
# Why: vPause funnels process_madvise(MADV_PAGEOUT) into the swap subsystem,
# and one swap area serializes that traffic on a single per-area si->lock
# domain. Like zram devices, multiple swapfiles spread pageout across one
# lock domain each; the kernel rotates same-priority areas per NUMA node
# (swap_avail_heads[] + plist_requeue). Files on one disk share that disk's
# block queue, so they shard the locks but not the bandwidth — put each file
# on a different disk to widen the I/O path too. Disk-backed pageout is far
# slower than zram and resume swap-in waits on block I/O, so prefer the zram
# tier for latency-sensitive pauses. For a swapfile-only comparison, tear
# down the zram tier first (scripts/zram-swap-teardown.sh).
#
# Sizing note: the kernel hard-caps all swap areas at MAX_SWAPFILES = 32 and
# this script clamps the file count to the free slots. zswap is disabled the
# same way scripts/zram-swap-setup.sh does it: mainline zswap (checked on
# 6.6) fronts every swap backend, including swapfiles, and would compress
# every page twice. ext4/xfs work as-is; btrfs requires a NOCOW,
# uncompressed file outside any snapshot (swapon refuses otherwise).
#
# Usage:
#   sudo bash scripts/swapfile-swap-setup.sh [--count N] [--size-mib M]
#                                           [--dir DIR] [--priority P]
#
# Defaults: 4 files x 8192 MiB under /var/lib/agentenv-swap, priority 100
# (same rotation as the zram tier).
#
# Cleanup: scripts/swapfile-swap-teardown.sh (--dir must match)

set -euo pipefail

COUNT=4
SIZE_MIB=8192
DIR=/var/lib/agentenv-swap
PRIORITY=100
MAX_AREAS=32

while [[ $# -gt 0 ]]; do
    case "$1" in
        --count) COUNT="$2"; shift 2 ;;
        --size-mib) SIZE_MIB="$2"; shift 2 ;;
        --dir) DIR="$2"; shift 2 ;;
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

if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]] || ! [[ "$SIZE_MIB" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: --count and --size-mib must be positive integers" >&2
    exit 1
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: this script requires root (run with sudo)" >&2
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
# 2. Clamp to the free swap-area slots
# ---------------------------------------------------------------------------
BYTES=$((SIZE_MIB * 1024 * 1024))
EXISTING=$(swapon --show=NAME --noheadings 2>/dev/null | wc -l)
if [[ $((COUNT + EXISTING)) -gt $MAX_AREAS ]]; then
    COUNT=$((MAX_AREAS - EXISTING))
    echo "note: $EXISTING swap area(s) already active; reduced to $COUNT new files"
fi
if [[ $COUNT -le 0 ]]; then
    echo "error: no swap-area slots left (MAX_SWAPFILES=$MAX_AREAS)" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 3. Create and activate the files
# ---------------------------------------------------------------------------
# Idempotent: already-active files are skipped, existing inactive files of the
# matching size are reused, anything else under the same name is replaced.

mkdir -p "$DIR"

created=0
for i in $(seq 1 "$COUNT"); do
    file="$DIR/swapfile-$i"
    if swapon --show=NAME --noheadings 2>/dev/null | grep -qxF -- "$file"; then
        continue
    fi
    if [[ -e "$file" ]] && [[ "$(stat -c %s "$file")" -ne "$BYTES" ]]; then
        echo "swapfile: replacing $file (size mismatch)"
        rm -f -- "$file"
    fi
    if [[ ! -e "$file" ]]; then
        echo "swapfile: allocating $file ($SIZE_MIB MiB)"
        fallocate -l "$BYTES" "$file"
    fi
    chmod 600 "$file"
    mkswap "$file" > /dev/null
    if ! swapon -p "$PRIORITY" "$file"; then
        echo "error: swapon $file failed (on btrfs the file must be NOCOW," >&2
        echo "      uncompressed, and outside any snapshot; see dmesg)" >&2
        exit 1
    fi
    echo "swapfile: activated $file ($SIZE_MIB MiB, priority $PRIORITY)"
    created=$((created + 1))
done

# ---------------------------------------------------------------------------
# 4. Report the resulting layout
# ---------------------------------------------------------------------------
echo
echo "swap layout:"
swapon --show
echo
total_mib=$((created * SIZE_MIB))
echo "swapfile tier: ${created} x ${SIZE_MIB} MiB = ${total_mib} MiB disk swap under $DIR"
if [[ $ZSWAP_CMDLINE_NOTE -eq 1 ]]; then
    echo "warning: zswap.enabled=1 is on the kernel command line; it will re-enable"
    echo "         zswap on the next boot. Remove it from the bootloader config."
fi
