# Track B Artifact Retention / Cold Storage Policy v1

## Purpose

Track B keeps a small set of hot execution_core authority artifacts on the operational path and treats older reports, logs, and diagnostics as evidence, not runtime truth. This policy defines what must remain hot, what may age into warm diagnostics, and what can later be copied into cold storage by an explicit dry-run archive tool.

This policy is PAPER-only. It does not grant live-money authority and does not authorize deletion, move, archive, broker mutation, lifecycle mutation, paper_proof, broad cancel, broad flatten, or runtime restart.

## Authority Rule

Execution_core authority artifacts are the source of truth for active decisions. Dashboard and operator files are projections or displays unless a legacy artifact is explicitly listed as a hot decision artifact during migration.

Runtime, readiness, restart, remediation, and routing code must not consume dashboard projections as authority. Cold archive files must never be read for active runtime decisions.

## Hot-Path Authority Artifacts

Hot-path artifacts are current/latest files required for PAPER runtime safety, proof readiness, remediation safety, or operator supervision. They are protected from archive/delete actions while they are the latest authority artifact.

Core hot authority examples:

- `outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json`
- `outputs/track_b_execution_core/managed_orders/latest_managed_orders.json`
- `outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json`
- `outputs/track_b_execution_core/position_truth/latest_position_truth.json`
- `outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json`
- `outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json`
- `outputs/track_b_execution_core/managed_positions/latest_managed_positions.json`
- `outputs/track_b_execution_core/agent_registry/latest_agent_registry.json`
- `outputs/track_b_execution_core/agent_health/latest_agent_health.json`
- `outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json`
- `outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json`
- `outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json`
- `outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json`
- `outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json`
- current broker lease and canonical readiness latest artifacts while their authority paths are being migrated.

Active lifecycle and ownership artifacts are also hot when they describe unresolved or active state:

- lifecycle reports in `OPEN_MANAGED`, `REVIEW_REQUIRED`, `OPEN_MANAGED_METADATA_INCOMPLETE`, `BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE`, `CLOSE_UNKNOWN`, or close-working states.
- working open-order evidence.
- unresolved submit ownership records.
- current broker reconciliation and broker lease evidence.
- current runtime truth, PID metadata, and stop provenance.

## Warm Diagnostics

Warm diagnostics explain recent behavior but are not routing authority unless they are the current/latest authority artifact listed above.

Examples:

- recent runtime logs.
- recent proof reports.
- recent remediation audits.
- recent managed order adjustment plans.
- recent crash-loop, self-recover, and resume histories.
- recent operator/status projections.
- recent broker/order/reconciliation snapshots used for incident review.

Suggested warm retention:

- routine dashboard/operator snapshots: 7 days.
- routine runtime logs and preflight reports: 7 to 30 days.
- remediation, broker/order, reconciliation, suspicious-order, and review-required evidence: 30 to 90 days locally before archive staging.
- proof/audit artifacts: retain longer, then archive compressed with an index.

## Cold Archive

Cold archive is historical storage for completed proof artifacts, old reports, old runtime logs, historical reconciliation snapshots, stale generated status files, and incident evidence that no active runtime depends on.

The target design is local cold storage or a future Linux storage box with compressed, date-partitioned archives and a manifest/index file. Cold archive is for audit and reproducibility only. Runtime, readiness, restart, remediation, and routing code must not read cold archive for active decisions.

## Never Archive While Active

Do not archive, delete, move, or compress these while they are active or ambiguous:

- active lifecycle open or review reports.
- working open order evidence.
- unresolved submit ownership rows.
- current shared-truth authority latest files.
- current reconciliation, broker lease, canonical readiness, proof readiness, and runtime truth latest files.
- artifacts referenced by an active managed position, managed order, manifest, ownership record, or remediation audit.
- malformed lifecycle/order evidence that could correspond to real broker-backed exposure.

If evidence is ambiguous, classify it as protected and require operator review.

## Inventory Command

The read-only inventory authority is:

```bash
python -m mgc_v05l.execution_core.track_b_artifact_retention_inventory
```

It writes:

```text
outputs/track_b_execution_core/artifact_retention/latest_artifact_retention_inventory.json
```

The inventory is dry-run only. It reports hot protected artifacts, protected active lifecycle reports, warm diagnostics, and cold archive candidates from bounded local scans. Scan-limit warnings mean the inventory is conservative and should not be used as an archive manifest until a future archive planner completes a target-specific dry run. It performs no deletion, move, compression, archive, broker mutation, lifecycle mutation, paper_proof, submit, cancel, close, replace, flatten, or runtime restart.

## Archive Preconditions

A future archive implementation must be explicit and dry-run first. It may proceed only when:

- shared truth refresh is clean enough for artifact maintenance.
- no active broker exposure or unknown open order exists.
- no unresolved ownership or active lifecycle review state depends on the target artifact.
- the dry-run manifest lists every target path and retention reason.
- the archive operation writes a manifest/index before removing any local copy.
- operator authorization is explicit for any destructive local cleanup.

## Prohibited Actions

- No deleting active authority artifacts.
- No archive action without dry-run and clean shared-truth preflight.
- No runtime reads from cold archive for active decisions.
- No dashboard projection as runtime, readiness, restart, remediation, or routing authority.
- No broad cleanup of research datasets under this policy; research/offline datasets need a separate retention policy.

## Future Work

- Add a dry-run archive planner that consumes this inventory and shared truth.
- Add compressed date-partitioned archive bundles with a manifest/index.
- Add cold-storage transfer support for the future Linux storage box.
- Add explicit retention metadata to long-lived report writers where useful.
