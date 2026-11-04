#!/usr/bin/env bash
# Tear down the disk swapfile tier created by
# scripts/swapfile-swap-setup.sh: swap off every active swapfile under its
# directory and delete the files, including inactive leftovers from aborted
# runs. Swap areas outside that directory (other swapfiles and partitions)
# are left untouched. Pages still parked in each swapfile are returned to
# resident memory as it is swapped off, so make sure the host has room
# before running under memory pressure.
#
# Usage:
#   sudo bash scripts/swapfile-swap-teardown.sh [--dir DIR]

set -euo pipefail

DIR=/var/lib/agentenv-swap
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir) DIR="$2"; shift 2 ;;
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

active=$(swapon --show=NAME --noheadings 2>/dev/null | grep -F -- "$DIR/swapfile-" || true)
if [[ -z "$active" ]]; then
    echo "no active swapfiles under $DIR"
else
    while IFS= read -r file; do
        echo "swapfile: swapping off $file"
        swapoff "$file"
        echo "swapfile: removing $file"
        rm -f -- "$file"
    done <<< "$active"
fi

# Inactive leftovers (allocated by an aborted setup, replaced on size
# mismatch) are ours to clean; -f keeps this quiet when nothing matches.
rm -f -- "$DIR"/swapfile-*

echo
echo "remaining swap layout:"
swapon --show || true
