# Track B Canonical Truth Snapshot

Status: design proposal, read-only authority aggregation layer

Scope: Track B PAPER operational truth for runtime status, broker facts, lifecycle facts, submit authority, recovery status, contract eligibility, and review-required conflicts.

This layer does not submit, cancel, close, liquidate, restart, or repair anything. It reads existing authority artifacts, normalizes their answers into one snapshot, and emits explicit conflict classifications when sources disagree.

## Goal

Create one read-only authority aggregator that answers the core operator and submit-path questions without forcing downstream code to choose between competing artifacts.

The snapshot is not a dashboard projection. It is an authority summary with source paths, generated timestamps, freshness windows, precedence rules, and conflict evidence.

## Questions Answered

The canonical truth snapshot must answer:

- Is the runtime alive?
- Is the runtime submit-capable?
- Is recovery active?
- Is broker truth fresh?
- Are there open broker orders?
- Are there broker positions?
- Are lifecycle positions open?
- Are broker/lifecycle reconciled?
- Is Safe-State allowing submit?
- Is Control Plane fresh/coherent?
- Is a fill broker-backed?
- Is a contract eligible for new entry, close-only, or blocked?
- Are there truth conflicts requiring review?

## Proposed Data Model

```python
@dataclass(frozen=True)
class TrackBTruthSource:
    source_name: str
    artifact_path: str
    generated_at: datetime | None
    freshness_seconds: int
    fresh: bool
    authority_level: str
    diagnostic_only: bool = False


@dataclass(frozen=True)
class TrackBBrokerTruthSummary:
    fresh: bool
    account_id: str | None
    open_order_count: int
    broker_position_count: int
    positions: tuple[TrackBBrokerPositionTruth, ...]
    open_orders: tuple[TrackBBrokerOrderTruth, ...]
    source: TrackBTruthSource


@dataclass(frozen=True)
class TrackBLifecycleTruthSummary:
    fresh: bool
    open_position_count: int
    managed_order_count: int
    open_positions: tuple[TrackBLifecyclePositionTruth, ...]
    source: TrackBTruthSource


@dataclass(frozen=True)
class TrackBSubmitAuthoritySummary:
    runtime_alive: bool
    runtime_submit_capable: bool
    safe_state_submit_allowed: bool
    control_plane_fresh: bool
    control_plane_coherent: bool
    recovery_active: bool
    broker_lifecycle_reconciled: bool
    duplicate_writer_detected: bool
    blockers: tuple[str, ...]


@dataclass(frozen=True)
class TrackBContractTruth:
    symbol: str
    requested_local_symbol: str | None
    resolved_local_symbol: str | None
    con_id: int | None
    expiry: str | None
    entry_status: Literal["ENTRY_ELIGIBLE", "ENTRY_CLOSE_ONLY", "ENTRY_BLOCKED", "ENTRY_AMBIGUOUS"]
    exit_status: Literal["EXIT_ORIGINAL_CONTRACT_ALLOWED", "EXIT_BLOCKED"]
    reason_codes: tuple[str, ...]
    source: TrackBTruthSource


@dataclass(frozen=True)
class TrackBFillEvidenceTruth:
    broker_backed: bool
    order_id: str | None
    client_id: str | None
    perm_id: str | None
    exec_id: str | None
    source_artifact_path: str
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class TrackBTruthConflict:
    classification: str
    question: str
    authoritative_source: str | None
    conflicting_sources: tuple[str, ...]
    reason_codes: tuple[str, ...]
    review_required: bool


@dataclass(frozen=True)
class TrackBTruthSnapshot:
    schema_version: Literal["track_b_truth_snapshot_v1"]
    generated_at: datetime
    runtime_generation_id: str | None
    snapshot_id: str
    paper_only: bool
    live_money_eligible: bool
    paper_proof_invoked: bool
    broker_truth: TrackBBrokerTruthSummary
    lifecycle_truth: TrackBLifecycleTruthSummary
    submit_authority: TrackBSubmitAuthoritySummary
    contract_truth: tuple[TrackBContractTruth, ...]
    fill_evidence: tuple[TrackBFillEvidenceTruth, ...]
    conflicts: tuple[TrackBTruthConflict, ...]
    operator_summary: dict[str, Any]
```

The first implementation should keep this data model pure: read artifact payloads, produce a snapshot object/report, and never call broker adapters or runtime start/stop paths.

## Source Precedence

| Question | Primary authority | Secondary authority | Diagnostic only | Conflict behavior |
| --- | --- | --- | --- | --- |
| Runtime alive | Paper runtime truth / runtime environment truth for active generation | Process surface hygiene and launchd carrier status | Dashboard/operator status | If process and runtime truth disagree, emit `RUNTIME_TRUTH_CONFLICT_REVIEW_REQUIRED` |
| Runtime submit-capable | Canonical readiness / runtime operability for active generation | Safe-State and Control Plane submit-compatible evidence | Legacy guarded-loop artifacts | Stale or conflicting submit authority emits `SUBMIT_AUTHORITY_CONFLICT_REVIEW_REQUIRED` |
| Recovery active | Standalone recovery launchd/service status | Latest recovery audit artifact if fresh | Stale hourly audit reports | Stale artifact cannot claim active; emit `RECOVERY_STATUS_STALE_REVIEW_REQUIRED` |
| Broker truth fresh | Broker truth refresh artifact populated from broker read-only APIs | Broker truth lease only as freshness/ownership evidence | UI broker panels | Stale broker truth blocks submit and emits `BROKER_TRUTH_STALE` |
| Open broker orders | Broker truth open-order rows | Broker-backed managed order registry only if reconciled | Local submit intents | Broker truth beats local artifacts |
| Broker positions | Broker truth positions | Broker-backed lifecycle rows only if broker truth unavailable for diagnostics | Lifecycle-only rows | Broker truth beats lifecycle projection |
| Lifecycle positions open | Lifecycle managed position registry/ledger | Managed order registry | Dashboard position summary | If lifecycle says open and broker says flat, reconciliation decides; do not infer |
| Broker/lifecycle reconciled | Broker/lifecycle reconciliation artifact | Lifecycle review cleanup report | Dashboard status | Dirty/stale reconciliation blocks submit |
| Safe-State allowing submit | Runtime Safe-State envelope | Runtime operability summary carrying Safe-State id | Runtime-start readiness | Runtime-start allowed does not imply submit allowed |
| Control Plane fresh/coherent | Control Plane Snapshot | Pre-action snapshot validator result | Operator surface projection | Stale/incoherent snapshot cannot authorize submit |
| Fill broker-backed | Broker execution/fill evidence with `perm_id`/`exec_id` where applicable | Broker order/fill ledger with broker identifiers | Local `paper-*` artifacts | Missing broker ids emits `FILL_NOT_BROKER_BACKED` |
| Contract entry status | Contract resolver with fresh broker contractDetails | Product roll policy | Hardcoded localSymbol/conId | Ambiguous/stale details fail closed |
| Contract exit status | Original lifecycle filled contract identity | Broker position contract truth | Entry resolver recommendation | Exits use original filled contract; do not roll exits |
| Aggregate account placeholders | Exact lifecycle owner row plus broker truth | Reconciliation artifact | Aggregate `MULTIPLE` view | `MULTIPLE` is diagnostic if exact row resolves; true mismatch blocks |

## Core Classifications

The aggregator should emit one or more conflict classifications. Conflicts never auto-resolve by inference.

- `TRUTH_SNAPSHOT_OK`
- `TRUTH_CONFLICT_REVIEW_REQUIRED`
- `RUNTIME_TRUTH_CONFLICT_REVIEW_REQUIRED`
- `SUBMIT_AUTHORITY_CONFLICT_REVIEW_REQUIRED`
- `RECOVERY_STATUS_STALE_REVIEW_REQUIRED`
- `BROKER_TRUTH_STALE`
- `BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED`
- `LIFECYCLE_TRUTH_STALE`
- `BROKER_LIFECYCLE_RECONCILIATION_DIRTY`
- `SAFE_STATE_SUBMIT_BLOCKED`
- `SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT`
- `CONTROL_PLANE_STALE`
- `CONTROL_PLANE_INCOHERENT`
- `FILL_NOT_BROKER_BACKED`
- `LOCAL_ARTIFACT_NOT_AUTHORITY`
- `CONTRACT_ENTRY_ELIGIBLE`
- `CONTRACT_ENTRY_CLOSE_ONLY`
- `CONTRACT_ENTRY_BLOCKED`
- `CONTRACT_DETAILS_STALE`
- `CONTRACT_AMBIGUOUS`
- `EXACT_LIFECYCLE_OWNER_RESOLVED`
- `AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY`
- `EXACT_LIFECYCLE_IDENTITY_MISMATCH`
- `DUPLICATE_WRITER_DETECTED`

## Required Invariants

- `paper_only=true`
- `live_money_eligible=false`
- `paper_proof_invoked=false`
- Dashboard/operator artifacts are diagnostic only.
- Stale artifacts cannot authorize submit.
- Broker truth beats local artifacts for broker order and broker position facts.
- Broker-backed fill evidence requires `perm_id` and `exec_id` where applicable.
- Aggregate placeholders such as `MULTIPLE`, `MISSING`, `UNKNOWN`, or blank values are diagnostic unless exact lifecycle identity fails to resolve.
- Entry contract resolver applies to new entries.
- Managed exits use the original filled `conId`, `localSymbol`, expiry, account, side/action, qty, and lifecycle identity.
- Any source conflict that cannot be resolved by explicit precedence emits `TRUTH_CONFLICT_REVIEW_REQUIRED`.

## Snapshot Construction Flow

1. Read runtime truth, canonical readiness, runtime operability, recovery service status, broker truth, lifecycle registry/ledger, broker/lifecycle reconciliation, Safe-State envelope, Control Plane Snapshot, contract resolver reports, and optional fill evidence artifacts.
2. Parse generated timestamps and apply source-specific freshness thresholds.
3. Normalize source summaries into typed truth sections.
4. Apply source precedence per question.
5. Run hard invariant checks: paper-only, no live money, no paper proof, no duplicate writer.
6. Compute conflict classifications without mutating any source.
7. Emit one `TrackBTruthSnapshot` artifact and an operator-friendly summary.

## Migration Plan

1. **Design-only spec:** Land this document and review field names against current artifacts.
2. **Pure model module:** Add `TrackBTruthSnapshot` dataclasses and JSON serialization with no filesystem reads.
3. **Fixture-backed aggregator:** Add a read-only builder that accepts already-loaded payloads. No live broker calls.
4. **Lifecycle simulation tests:** Back the first tests with `track_b_lifecycle_simulation_harness` scenarios:
   - `aggregate_account_multiple_exact_row_valid`
   - `stale_control_plane_snapshot`
   - `safe_state_submit_blocked`
   - `planner_snapshot_mismatch`
   - `local_paper_artifact_without_broker_ids`
   - `contract_close_only_new_entry_blocked_exit_allowed`
   - `managed_exit_close_identity_contract_mismatch`
5. **Read-only artifact reader:** Add artifact path config and freshness checks.
6. **Operator/status adoption:** Have status surfaces read the snapshot for display while preserving existing behavior.
7. **Submit-path adoption:** Only after read-only parity is proven, let governance/bridge consume snapshot sections as authority.
8. **Deprecate competing projections:** Mark legacy dashboard/operator fields diagnostic once consumers migrate.

## Tests Backed By Lifecycle Simulation Harness

| Test | Harness scenario | Expected truth snapshot result |
| --- | --- | --- |
| Clean lifecycle reconciles flat | `clean_full_lifecycle` | `TRUTH_SNAPSHOT_OK`, broker flat, lifecycle flat, fill broker-backed |
| Passive cancel is not fill | `passive_entry_cancel` | open/closed order classified from broker order status; no fill inferred |
| Fill not adopted | `entry_fill_not_adopted` | broker-backed fill present, lifecycle adoption conflict |
| Missing lifecycle id | `managed_exit_due_missing_lifecycle_id` | `EXACT_LIFECYCLE_IDENTITY_MISMATCH` |
| Wrong exit bar count | `managed_exit_policy_wrong_bar_count` | lifecycle policy conflict, submit not authorized for managed exit |
| Lane/thesis mismatch | `lane_id_vs_thesis_strategy_id_mismatch` | identity conflict requiring review |
| Aggregate MULTIPLE exact row valid | `aggregate_account_multiple_exact_row_valid` | `EXACT_LIFECYCLE_OWNER_RESOLVED`, `AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY` |
| Stale Control Plane | `stale_control_plane_snapshot` | `CONTROL_PLANE_STALE`, submit not authorized |
| Safe-State submit blocked | `safe_state_submit_blocked` | `SAFE_STATE_SUBMIT_BLOCKED`, runtime-start authority not treated as submit authority |
| Planner snapshot mismatch | `planner_snapshot_mismatch` | `SUBMIT_AUTHORITY_CONFLICT_REVIEW_REQUIRED` |
| Extra cleanup diagnostics | `scoped_cleanup_extra_diagnostic_fields` | hard identity matches; extra diagnostic fields ignored |
| Local paper artifact only | `local_paper_artifact_without_broker_ids` | `FILL_NOT_BROKER_BACKED`, `LOCAL_ARTIFACT_NOT_AUTHORITY` |
| Contract close-only | `contract_close_only_new_entry_blocked_exit_allowed` | new entry blocked, exit allowed on original filled contract |

## Non-Goals For Initial Implementation

- No broker calls.
- No runtime start/stop.
- No lifecycle cleanup.
- No submit/cancel/close/liquidation behavior.
- No dashboard authority.
- No automatic remediation.
