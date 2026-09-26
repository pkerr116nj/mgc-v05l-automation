# Phase 3D detection-only observability

Three independent sidecars observe the existing workers. They never signal or restart a worker, import a worker module, modify a receipt, request data, change retry behavior, or activate Stage 2.

`config.json` pins each worker's PID, start time, and command. A reused PID is treated as EXITED. The deployed observer PIDs and local diagnostic logs are in `runtime.json`.

Every 120 seconds, each observer atomically replaces its worker JSON and appends one line to its worker log under `/Volumes/Personal-Drive/MGC-research/ndxp-phase3d/status/`. JSON contains progress, PID identity, last successful publication time, active work, failures, interval throughput, accounting for A, status, and status reason. Throughput is null until the second sample; A measures request completions and acquired bytes, B measures session completions and raw bytes represented by completed timing records. Raw market files are never read or hashed by these monitors. Only active file sizes and small existing metadata are inspected. Reused March 28 data count once.

A separate watchdog reads both heartbeats every 300 seconds and atomically writes `overview.txt`. Worker snapshots remain owned by their respective observer. The watchdog reevaluates freshness and PID identity instead of trusting a cached status.

- EXITED: PID missing or process identity changed (takes priority; recorded exceptions remain in JSON).
- STALLED: heartbeat older than 600 seconds, or no completion/file-size progress for the configured threshold (A: 1,200 seconds; B: 1,800 seconds).
- ERROR: reported fatal traceback or an explicit observation error. Handled quarantined request failures are counted but are not fatal worker exceptions.
- IDLE: B is alive in watch mode, with no backlog of completed Stage 1 sessions and no in-progress artifact. No Stage 2 assumptions are made.
- RUNNING: observed progress remains within the configured threshold. Initial classification uses existing file/completion timestamps; subsequent samples compare counters. CPU usage alone does not reset the progress clock.

Sampling may take longer during NAS I/O congestion. A NAS outage can also prevent publication of the NAS-resident overview; observer errors are sent to separate `/tmp/phase3d-*-observer.log` files. No corrective action is automatic.

Only status/log files and sidecar lock/runtime files are written. Sidecar locks are separate from acquisition and processing locks.
