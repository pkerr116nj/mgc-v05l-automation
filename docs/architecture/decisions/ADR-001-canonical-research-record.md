# ADR-001: Canonical Research Record

## Status

Accepted.

## Decision Date

2026-08-04.

## Context

Track B PAPER trading now produces enough deterministic artifacts to reconstruct and analyze completed autonomous trades, but research consumers still need to rediscover artifact paths, join conventions, and quality limitations for every analysis.

The current platform already contains the necessary authority layers:

- Broker truth for current exposure and open orders.
- Canonical Trade Records for completed-trade identity.
- CTOL and CTOE for completed-trade analytics and enrichment.
- RA8 finalized capture and RA7 canonical paths for path research.
- RA3 deterministic attribution evidence.
- Operational artifacts for safety and provenance.

The architecture baseline and DP-001 proposal conclude that the platform should be consolidated and evolved, not replaced. RR-001 reviewed the proposal and recommended approval with minor revisions. DP-001 has been revised to incorporate those changes.

## Decision

Adopt the Canonical Research Record (CRR) as a derived, deterministic research-facing contract over existing Track B artifacts.

CRR introduces no new production or research authority. It simplifies access to existing evidence, records source provenance and join quality, and provides a stable interface for research scripts, analytics, experiments, investigations, and future research UI.

## Authority Hierarchy Preserved

CRR preserves the following authority hierarchy:

1. Broker truth remains authoritative for current exposure and open orders.
2. Canonical Trade Records remain authoritative for completed-trade identity.
3. CTOL and CTOE remain the completed-trade analytics and enrichment layers.
4. RA8 finalized capture and RA7 canonical paths remain path evidence.
5. RA3 remains subordinate deterministic attribution evidence.
6. CTOE context validity is authoritative only for enrichment validity, not for raw trade identity or fills.
7. Operational artifacts remain safety and provenance evidence.

CRR must not override, replace, or weaken these layers.

## V1 Boundary

CRR v1 is limited to completed-trade research reconstruction.

It may contain:

- Completed-trade identity.
- Entry and exit anchors as references or reconciled cache values.
- Outcome, enrichment, path, attribution, context, and operational provenance references.
- Lightweight attribution, context, and operational status summaries.
- Join-quality classifications.
- Per-source provenance.
- Research guardrails.

It must not contain:

- Broker authority.
- Runtime authority.
- Managed Exit authority.
- Guardian, Safe-State, readiness, reconciliation, or trading-gate authority.
- Full path sample payloads.
- Embedded competing copies of operational truth.
- Inferred or fabricated missing evidence.
- Strategy recommendations or production recommendations.

## Materialization Decision

CRR v1 will be materialized as a versioned JSONL snapshot under:

`outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_records.jsonl`

A companion validation/report artifact is required.

This is a v1 materialization decision. It does not prohibit a future DuckDB or Parquet materialization if scale, query ergonomics, or dashboard usage later justify it.

## Join-Quality And Tolerance-Debt Policy

CRR must distinguish:

- Exact joins.
- Missing joins.
- Broken joins.
- Tolerance joins.

A missing join means required source evidence is absent or unavailable.

A broken join means expected referential evidence exists but cannot be resolved consistently or violates the declared join contract.

Tolerance joins are migration debt. No new tolerance join may be added without explicit documentation of:

- Why exact linkage is unavailable.
- Which fields and tolerances are used.
- Which source artifacts are involved.
- The intended elimination path or roadmap item.

Existing canonical join and resolution logic from CTOL, CTOE, RA8, RA7, RA3, and other established resolvers must be reused wherever available. CRR must not independently reimplement competing join semantics.

## Validation And Reconciliation Requirements

Any materialized authoritative value in CRR is a deterministic cache only. References and authoritative source artifacts remain controlling.

Validation must:

- Regenerate CRR deterministically from source artifacts.
- Compare cached materialized values against authoritative sources.
- Fail validation or explicitly report mismatches.
- Preserve per-source artifact path, schema version, generated timestamp, and fingerprint/hash where available.
- Report exact, tolerance, missing, and broken joins separately.
- Reconstruct a representative completed trade from entry decision through final exit.

## Consequences

Positive consequences:

- One stable research-facing contract.
- Less artifact-path and join rediscovery.
- Explicit reconstruction quality.
- Deterministic provenance.
- Preservation of existing RA, CTOL, CTOE, CAE, REF, and related work.

Costs:

- CRR schema and version maintenance.
- Reconciliation work against upstream artifacts.
- Ongoing tolerance-join debt management.
- Possible future migration to DuckDB or Parquet if JSONL becomes insufficient.

## Risks And Mitigations

Risk: CRR could drift into a new authority layer.

Mitigation: CRR remains explicitly derived, diagnostic, and isolated from production authority.

Risk: Materialized fields could be treated as authoritative.

Mitigation: Materialized fields are reconciled caches only, with source provenance and validation.

Risk: Tolerance joins could hide identity weakness.

Mitigation: Tolerance joins are marked, reported, and tracked as migration debt.

Risk: CRR could duplicate complex upstream semantics.

Mitigation: CRR must reuse established canonical resolution logic rather than creating competing join implementations.

## Rejected Or Deferred Alternatives

Rejected:

- Replacing Canonical Trade Records, CTOL, CTOE, RA7, RA8, RA3, or operational artifacts with CRR.
- Allowing CRR to influence runtime, broker, Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, or trading-gate authority.
- Inferring missing evidence.

Deferred:

- DuckDB or Parquet materialization.
- The elimination schedule for each existing tolerance join.
- Expansion beyond the v1 lightweight-reference boundary.

## Implementation Boundary

This ADR approves the CRR architecture. It does not implement CRR.

Implementation must be a separate bounded phase that:

- Does not change runtime behavior.
- Does not change broker behavior.
- Does not change Managed Exit behavior.
- Does not change strategy logic.
- Does not create trading gates.
- Does not create production recommendations.
- Writes only research artifacts and validation reports.

## Links

- Baseline: `docs/architecture/baselines/BA-001-current-research-platform.md`
- Proposal: `docs/architecture/proposals/DP-001-canonical-research-record.md`
- Review resolution: `docs/architecture/reviews/RR-001-DP-001-canonical-research-record.md`
- Epic: `docs/epics/EPIC-002-research-platform.md`
- Research inventory: `outputs/reports/research_data_inventory/research_data_inventory.md`
- Research inventory JSON: `outputs/reports/research_data_inventory/research_data_inventory.json`
- Data-flow diagram: `outputs/reports/research_data_inventory/research_data_flow_diagram.md`
