# Data Retention And Archive Policy

## Status

Current operational and architectural policy.

This document is not a runtime configuration file and is not a source of trading authority. It records the current operating direction for logs, runtime artifacts, research artifacts, hot-path storage, archive retention, dashboard access patterns, and future cold-storage design.

## Purpose

Define what stays hot, what is retained briefly, what is archived, what is authoritative, and what must never be used as runtime truth.

## Operational Logs Versus Structured Trading Evidence

Operational logs exist primarily for troubleshooting, incident review, startup diagnostics, and short-term service health analysis.

Ordinary text logs are not the canonical source for trade execution research.

Trade research should use structured durable artifacts such as:

- broker fills
- orders
- Canonical Trade Records
- CTOL
- CTOE
- RA8 finalized path capture
- RA7 canonical paths
- attribution records
- regime/context records
- realized P&L and lifecycle evidence

If a fact matters for future trading research, it should be captured structurally rather than relying on text-log retention.

## Log Retention Philosophy

Short retention is appropriate for most custom operational logs.

The current practical target is approximately 1-2 days for repetitive listener, HTTP, dashboard, and service logs unless evidence shows a longer period is needed.

Logs should rotate daily and also have a size cap where practical. Compression is appropriate for rotated logs.

Retention may be increased later if a specific audit, debugging, or research requirement justifies it.

Do not retain large repetitive logs indefinitely merely because storage is available.

System journal retention may remain bounded under normal journald policy unless measured growth becomes a problem.

Short log retention does not mean discarding structured trade evidence. Structured research artifacts may require much longer retention than operational logs.

## Hot-Path Artifact Policy

Runtime hot-path artifacts must remain bounded.

Runtime services and dashboards should read latest or bounded canonical snapshots, not scan unbounded historical directories.

Generated timestamped artifacts must not accumulate without a retention or archive policy.

Dashboards should not depend on giant historical JSONL, log, or database scans for current status.

Latest-state projections should remain small, fast, and freshness-validated.

Historical analysis should be separated from current operational reads.

## Research-Versus-Runtime Data Boundary

`outputs/track_b_research/` is for research, replay, backtesting, diagnostics, and offline validation.

Research-captured artifacts are not live runtime truth.

Runtime consumers must use explicit runtime/execution-core producers with provenance and freshness validation.

If research artifacts are the only available source, runtime must fail closed.

Archive copies must never be silently reintroduced into live decision paths.

## Archive Policy

Historical research evidence should move from hot operational storage into explicit cold/archive retention.

Archive design should preserve:

- provenance
- source path
- generated_at
- schema version
- instrument
- timeframe
- source category
- mode, such as runtime, research, or replay

Archive storage should be organized so future research can reconstruct source lineage.

Archive storage is not automatically authoritative merely because it is durable. Authority still depends on the source artifact family and documented hierarchy.

## Future Linux Archive Hosts

This section records future direction, not current completed implementation.

Old PCs may be repurposed as Linux storage/archive hosts.

Future archive architecture should favor:

- bounded hot storage on active hosts
- explicit cold/archive storage on dedicated systems
- recoverable structure
- clear retention classes
- no hidden dependency from runtime to cold storage

The archive system should support later backup and replication without forcing the trading runtime to depend on network storage for active decisions.

No final archive host has been selected by this policy.

## Host-Role Implications

### Atlas

Atlas should keep only active runtime state, bounded service logs, current research working sets, and explicitly required local artifacts.

Atlas should not become an unbounded long-term artifact warehouse merely because it has faster storage.

### Jupiter

Jupiter should remain an infrastructure and management host rather than becoming the default dumping ground for historical runtime data.

### NAS Or Future Linux Storage Hosts

NAS or future Linux storage hosts are appropriate candidates for cold/archive retention where provenance and recovery are preserved.

Archive use should not create a live runtime dependency.

### Mac Mini

The Mac mini is the primary trading workstation and current TWS host.

It should not become the canonical archive for Track B runtime or research artifacts merely because development occurs there.

## Artifact Retention Classes

Retention class should be explicit where practical.

### Ephemeral

- Temporary process files.
- Transient lock/PID files.
- Scratch outputs.
- Disposable intermediate artifacts.

### Hot Operational

- Latest runtime truth.
- Latest broker truth.
- Latest managed positions/orders.
- Current dashboards.
- Current health/status artifacts.
- Short-retention logs.

### Durable Research

- Canonical trades.
- CTOL/CTOE.
- Finalized paths.
- Attribution.
- Experiment inputs/outputs.
- Validated datasets.
- Schema/provenance records.

### Cold Archive

- Historical durable research evidence.
- Superseded but still valuable artifacts.
- Old reports.
- Retained validation evidence.
- Compressed historical datasets.

## Dashboard And Monitoring Policy

Monitoring surfaces should answer current-state questions from bounded fresh artifacts.

They should not infer current health from stale historical logs.

Freshness and source timestamps should be visible where useful.

Operational dashboards should show:

- service status
- data freshness
- broker/reconciliation state
- current authority state
- storage pressure where relevant

Dashboards remain diagnostic and do not gain submit authority.

## Storage Pressure And Observability

Giant unmanaged logs or artifacts can degrade system responsiveness through disk pressure, metadata overhead, and I/O contention.

Storage growth should be observable.

Alerts should eventually cover:

- low free space
- abnormal artifact growth
- stale data
- failed rotation
- archive backlog

This is future operational hardening unless already implemented.

## Retention Changes

Retention periods are operational policy, not immutable architecture.

They may be adjusted when evidence shows:

- incident review requires more history
- research depends on currently unstructured evidence
- regulatory or audit requirements change
- storage constraints tighten

Any increase in retention should have a stated purpose.

## Boundaries

This policy records current operating direction.

It does not itself delete, rotate, archive, migrate, or reclassify files.

Implementation changes to retention, storage paths, archive services, or runtime dependencies must follow the appropriate Routine or Architecture Track.

No runtime component may depend on archive storage without an explicitly approved architecture decision.

## Related Documents

- `PROJECT_PRINCIPLES.md`
- `SYSTEM_OVERVIEW.md`
- `docs/architecture/track-b-architectural-invariants.md`
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md`
- `docs/architecture/narratives/AN-004-research-platform-evolution.md`
- `docs/operations/current-infrastructure-baseline.md`
- `docs/track_b_artifact_retention_policy.md`
- `docs/operator_dashboard_log_retention.md`
- `docs/roadmap/PARKING_LOT.md`
