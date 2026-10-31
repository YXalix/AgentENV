# Snapshots

A snapshot is a reusable checkpoint of a sandbox. It preserves the sandbox's
filesystem and runtime state so that you can later start a new sandbox from the
same point instead of rebuilding the environment and rerunning setup work.

- **Templates** are stored as snapshots. A template build commits one snapshot;
  the template ID is an alias that resolves to it.
- **Warm-started sandboxes** restore the state of a committed template or snapshot.
- **Running sandboxes** can produce new snapshots, capturing their current
  state for later reuse or branching.

## How Pausing Persists Memory

Storm pauses (the resume/exec/pause/sleep cycle) persist guest RAM into a
private per-sandbox file `mem.upper` under the sandbox's artifact tree: RAM is
split into 16KiB blocks whose compressed payloads are packed variable-length
into a single bounded file, indexed by an incremental 8-byte per-block
metadata table, with a free-list recycling space and in-place rewrites
preferred. Each pause writes only the blocks the guest dirtied, so no new
layers appear, resume opens with zero index merging, and the file's pages stay
hot in the page cache across pause/resume cycles. An epoch in the file header
is checked against the paused record so torn generations are discarded instead
of resumed. Fragmented files are compacted in place (vacuum) once
fragmentation or bloat thresholds trip.

Two internal fallbacks derive from sandbox state, not configuration:

- A sandbox paused for the first time after a fresh boot (no parent memory
  stack to splice from) materializes one OverlayBD memory layer for that
  pause, in the historical per-pause format; the varlen lineage starts on the
  next pause after its first resume.
- Capture, fork, and publish never read `mem.upper` directly: they first
  collapse the lower stack plus `mem.upper` into one immutable OverlayBD
  layer, then capture the pages dirtied since the last pause on top of it.

Sandboxes paused by older binaries (legacy layer stacks) migrate to the
varlen lineage on their first storm pause (throttled node-wide; while the
migration budget is saturated they keep materializing per-pause layers).

Both paths require Firecracker dirty-page tracking
(`[memory_snapshot].track_dirty_pages`, enabled by default).

## What a Snapshot Preserves

A snapshot preserves:

- VM state and memory, allowing running processes to continue from the captured point.
- The root filesystem and its changes.
- Attached-drive contents and attachment metadata.
- Mounted-volume contents, mount paths, sizes, and access modes. Starting from
  the snapshot creates new volumes from the captured contents rather than
  reusing the source volume identities.
- CPU, memory, and disk settings, together with command context such as
  environment variables, working directory, user, and startup commands.

---

Where to Go Next:

- [Create a Snapshot](./create.md) — capture a reusable checkpoint from a running sandbox.
- [Manage Snapshots](./manage.md) — list, inspect, and delete snapshots.
- [Rootfs as an OCI Image](./use.md) — publish or export only the captured root filesystem.
- [Optional P2P Visibility](./p2p.md) — accelerate committed snapshot artifact distribution between nodes.
