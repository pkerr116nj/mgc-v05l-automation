# DP-001: Canonical Research Record

## Status

Accepted; superseded as the active decision record by `ADR-001-canonical-research-record.md` for implementation governance.

This proposal records the accepted design basis. Implementation governance now flows through ADR-001.

## Summary

Introduce a lightweight Canonical Research Record (CRR) as a stable, deterministic, research-facing contract over existing Track B artifacts.

CRR simplifies access to existing evidence. It does not replace Canonical Trade Records, CTOL, CTOE, RA8, RA7, RA3, broker truth, Managed Exit, Guardian, Safe-State, or any other established authority layer. It introduces no new production authority.

The record should make one completed trade reconstructable from entry decision through final exit while preserving the established authority hierarchy:

1. Broker truth for current exposure and open orders.
2. Canonical Trade Records for completed trade identity.
3. CTOL and CTOE for completed-trade analytics and enrichment.
4. RA8 finalized capture and RA7 canonical paths for path research.
5. RA3 as deterministic attribution evidence, subordinate to Canonical Trade Records and CTOL/CTOE identity/outcome authority.
6. CTOE context validity as authority for whether enrichment fields are valid, not for raw trade identity or fills.
7. Operational artifacts for safety and provenance.

## Design Principles

- CRR is derived and never authoritative.
- Every field traces to an authoritative source.
- Missing evidence remains missing.
- Tolerance joins are explicitly marked and treated as migration debt.
- Research artifacts do not influence production authority.
- CRR simplifies access to existing evidence rather than replacing or duplicating authority layers.
- Existing canonical resolution logic should be reused wherever available.
- Materialized values are deterministic caches only; source artifacts remain controlling.

## Problem

Track B now has rich artifacts, but research scripts still need to know too much about artifact paths and join conventions.

Current reconstruction requires:

- Canonical Trade Records for trade identity and anchors.
- RA8 `path_capture.capture_id` for path accumulation/finalization.
- RA7 `canonical_trade_path_id` for normalized path summaries.
- CTOL `trade_outcome_id` for completed-trade metrics.
- CTOE for VIX, GRE, CRFD, VWAP, AVWAP, and context validity.
- RA3 attribution for deterministic entry/exit reason labels.
- Operational artifacts for safety and supervision provenance.

The system needs a clear research envelope that references these artifacts, records join quality, and exposes missing or broken joins explicitly.

## Non-Goals

- Do not replace Canonical Trade Records.
- Do not replace CTOL or CTOE.
- Do not replace RA8, RA7, or RA3.
- Do not move full path samples into the Canonical Trade Record or CRR.
- Do not create a broker authority surface.
- Do not alter runtime, Managed Exit, strategy logic, readiness, reconciliation, or gates.
- Do not infer missing values.
- Do not embed large upstream payloads or duplicate operational truth.

## Proposed V1 Contract

A Canonical Research Record should be a derived research object with these sections.

### Identity

- `research_record_id`
- `schema_version`
- `generated_at`
- `trade_identity`
  - `trade_id`
  - `source_trade_id`
  - `lifecycle_id`
  - `instrument`
  - `contract`
  - `con_id`
  - `side`
  - `quantity`

### Entry And Exit Anchors

Entry and exit fields may be selectively materialized for analytical performance, but only as deterministic, reconciled cache values with explicit source provenance.

- `entry_anchor`
  - `entry_time`
  - `entry_price`
  - `entry_order_id`
  - `entry_perm_id`
  - `entry_exec_id`
  - `strategy_id`
  - `lane_id`
- `exit_anchor`
  - `exit_time`
  - `exit_price`
  - `exit_order_id`
  - `exit_perm_id`
  - `exit_exec_id`
  - `exit_policy`
  - `exit_reason`

References and authoritative source artifacts remain controlling.

### Outcome And Path References

- `outcome_ref`
  - `trade_outcome_id`
  - `ctol_path`
  - `ctoe_path`
  - `ctol_fingerprint`
  - `ctoe_fingerprint`
- `path_ref`
  - `capture_id`
  - `canonical_trade_path_id`
  - `path_status`
  - `path_fingerprint`
  - `ra8_finalized_capture_path`
  - `ra7_canonical_path_path`

### Lightweight Attribution, Context, And Operational References

CRR v1 keeps these sections only as lightweight references and validity/status summaries. It must not embed large upstream payloads or duplicate operational truth.

- `attribution_ref`
  - `decision_attribution_id`
  - `ra3_path`
  - `entry_attribution_status`
  - `exit_attribution_status`
- `context_summary`
  - `vix_valid`
  - `gre_valid`
  - `crfd_valid`
  - `vwap_avwap_valid`
  - `ctoe_context_validity_ref`
- `operational_provenance_refs`
  - broker truth snapshot
  - open-order truth snapshot
  - managed positions/orders
  - Guardian
  - Safe-State
  - runtime truth

### Join Quality

- `join_quality`
  - exact joins
  - tolerance joins
  - missing joins
  - broken joins
  - migration debt items

### Source Provenance

Every source used by a CRR row should include:

- `source_name`
- `source_artifact_path`
- `source_schema_version`
- `source_generated_at`
- `source_fingerprint` or `source_hash` where available
- `resolution_method`
- `resolution_quality`

CRR schema version is not enough by itself because CRR derives from multiple upstream schemas.

### Guardrails

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`

## Join Contract

Preferred joins:

1. `canonical_trade_records.trade_id` or `source_trade_id` identifies the completed trade.
2. `canonical_trade_records.path_capture.capture_id` joins to RA8 open/finalized path artifacts.
3. RA8 finalized capture joins to RA7 canonical paths by established canonical resolution logic, using `capture_id`, `source_trade_id`, or `path_fingerprint` only where those joins are already supported.
4. CTOL joins to Canonical Trade Records by explicit trade ids when present, falling back to instrument plus entry/exit anchors only when reported as a tolerance join.
5. CTOE joins to CTOL by `trade_outcome_id`.
6. RA3 joins to CTOL/Canonical Trade Records by `source_trade_id` or `trade_outcome_id` when those fields are available.

CRR must reuse existing canonical resolution logic from CTOL, RA8, RA7, or another established resolver wherever available. It must not independently reimplement competing join semantics.

Every tolerance join must be marked. Every missing join must remain missing rather than inferred.

## Missing Join And Broken Join Definitions

### Missing Join

A missing join means required source evidence is absent or unavailable.

Worked example: a completed Canonical Trade Record has no `path_capture.capture_id` because the trade predates path-capture deployment. The RA8/RA7 path join is missing. CRR records the missing join and does not infer a path from rotating candle snapshots.

### Broken Join

A broken join means expected referential evidence exists but cannot be resolved consistently or violates the declared join contract.

Worked example: a Canonical Trade Record includes `path_capture.capture_id=trade_path_capture_abc`, and RA8 finalized capture contains that capture id, but RA7 canonical paths resolve the same capture id to a different `source_trade_id` or incompatible contract. The join is broken and must fail validation or be explicitly reported.

## Research Authority Rules

- Current exposure and open orders come only from fresh broker truth.
- Completed trade identity comes from Canonical Trade Records.
- Realized P&L proxy and completed-trade metrics come from CTOL.
- Context values and context validity come from CTOE.
- CTOE context validity is authoritative only for enrichment validity, not for raw trade identity or fills.
- MFE, MAE, giveback, and counterfactual readiness come from RA8/RA7 path evidence.
- RA3 provides deterministic attribution evidence, subordinate to Canonical Trade Records and CTOL/CTOE identity/outcome authority.
- Operational artifacts explain safety state and provenance but do not author completed-trade metrics.
- Research artifacts do not influence production authority.

## Tolerance-Join Debt Policy

Tolerance joins are tracked migration debt.

Rules:

- No new tolerance join without explicit documentation.
- Each tolerance join must record why exact linkage is unavailable.
- Each tolerance join must identify the intended elimination path or roadmap item.
- Research reports must distinguish tolerance-joined evidence from exact-joined evidence.
- The roadmap should seek to reduce tolerance joins over time.

## Validation And Reconciliation

Materialized authoritative fields are caches only.

Validation requirements:

- Every cached value must be reproducible from its authoritative source.
- Validation must compare materialized values against authoritative artifacts.
- Mismatches must fail validation or be explicitly reported.
- Missing joins and broken joins must be separately counted.
- Source schema versions and source fingerprints/hashes must be retained where available.
- CRR generation must be deterministic for the same source artifact set.

Examples of reconciled cache fields may include `entry_time`, `exit_time`, `entry_price`, `exit_price`, `lane_id`, `strategy_id`, and `realized_pnl_proxy`. These fields remain controlled by their source artifacts.

## Materialization And Location

CRR v1 should be materialized as a versioned JSONL snapshot under:

`outputs/track_b_execution_core/research_analytics/canonical_research_record/`

Proposed filename:

`canonical_research_records.jsonl`

The materialized snapshot should have a companion validation/report artifact in the same directory.

This is a v1 design choice. It is not a permanent prohibition against future DuckDB or Parquet materialization if scale or query needs justify that later.

## Compatibility And Versioning

- CRR rows carry their own `schema_version`.
- Each source artifact reference carries source schema version and source fingerprint/hash where available.
- Additive fields are preferred for compatibility.
- Breaking schema changes require a new CRR schema version and migration notes.
- Existing RA1-RA8, CTOL, CTOE, CAE, REF, Investigation, Evidence, Claim, and Conclusion artifacts remain valid.

## V1 Boundary

CRR v1 is a deterministic research access layer over completed trades.

In v1:

- Keep identity, entry/exit anchors, outcome/path references, join quality, source provenance, and guardrails.
- Keep attribution, context, and operational provenance as lightweight references/status summaries only.
- Allow selective materialized cache fields only when reconciled to authoritative sources.
- Do not embed full path samples.
- Do not duplicate broker truth, open-order truth, managed positions, or Safe-State payloads.
- Do not create production recommendations, trading gates, broker authority, runtime authority, or Managed Exit authority.

## Initial Consumers

- Empirical entry/exit research scripts.
- REF offline experiments.
- CAE saved queries and future dashboards.
- Investigation/Evidence records when a completed trade is attached as evidence.

## Resolved Review Questions

- `attribution_ref`, `context_summary`, and `operational_provenance_refs` remain in v1 only as lightweight references and validity/status summaries.
- Entry/exit and other authoritative values may be selectively materialized only as deterministic, reconciled cache fields with explicit source provenance.
- Initial materialization is a versioned JSONL snapshot under `outputs/track_b_execution_core/research_analytics/canonical_research_record/` with a companion validation/report artifact.

## Acceptance Criteria

- A representative completed trade can be reconstructed through the proposed envelope.
- CRR can be regenerated deterministically from the same source artifact set.
- Materialized cache fields reconcile against authoritative sources.
- Missing joins, broken joins, exact joins, and tolerance joins appear explicitly in `join_quality`.
- Per-source artifact path, schema version, generated timestamp, and fingerprint/hash where available are preserved.
- No existing authority layer is weakened or bypassed.
- CRR introduces no new production authority.
- Existing RA1-RA8, CTOL, CTOE, CAE, and REF outputs remain valid consumers/producers.
