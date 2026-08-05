# BA-001: Current Research Platform

## Status

Baseline assessment complete for design planning.

Claude's review of the broader direction may be acknowledged as having occurred, but this baseline records the pre-design factual state. It does not incorporate review recommendations as baseline facts.

## Purpose

Document the current Track B PAPER research platform before proceeding with `DP-001-canonical-research-record.md`.

This baseline uses factual evidence from:

- `outputs/reports/research_data_inventory/research_data_inventory.md`
- `outputs/reports/research_data_inventory/research_data_inventory.json`
- `outputs/reports/research_data_inventory/research_data_flow_diagram.md`
- `docs/epics/EPIC-002-research-platform.md`
- `docs/architecture/proposals/DP-001-canonical-research-record.md`

## Current Platform Architecture

Track B already has a layered research architecture around PAPER trading artifacts:

1. Phase-1 and Databento candle artifacts feed runtime strategy lanes.
2. Runtime strategy lanes create entry decisions and order intents.
3. IBKR broker acknowledgements and fills are captured into canonical trade evidence.
4. Canonical Trade Records preserve completed-trade identity and anchors.
5. RA8 accumulates and finalizes trade paths out of process.
6. RA7 normalizes path summaries into Canonical Trade Paths.
7. CTOL converts completed canonical trades into outcome rows.
8. CTOE enriches CTOL rows with market context, regime, and path telemetry.
9. CAE, REF, RA reports, and other research consumers analyze completed trades.
10. Operational artifacts provide safety, supervision, and provenance context.

The platform is therefore not a blank slate. It already contains the major pieces needed for deterministic completed-trade research.

## What Works Well

- Canonical Trade Records provide a durable completed-trade identity spine.
- CTOL and CTOE provide reusable completed-trade analytics and enrichment layers.
- RA8 and RA7 separate full path capture from normalized path summaries.
- CAE provides reusable grouped analytics, saved queries, execution provenance, diffs, and insights.
- REF provides an offline experiment framework for diagnostic research.
- Operational artifacts preserve safety and provenance without needing to become research metrics.
- The current architecture already distinguishes broker authority from research evidence.

## Established Authority Hierarchy

The established authority hierarchy is:

1. Broker truth is authoritative for current exposure and open orders.
2. Canonical Trade Records are authoritative for completed-trade identity.
3. CTOL and CTOE provide completed-trade analytics and enrichment.
4. RA8 finalized capture and RA7 canonical paths provide path research evidence.
5. Operational artifacts provide safety and provenance.

Research artifacts must not override broker truth, order truth, Managed Exit authority, Guardian, Safe-State, or strategy/runtime authority.

## Existing Research Capabilities

The existing platform supports:

- Completed-trade population analysis through CTOL.
- Contextual enrichment through CTOE.
- Strategy/lane/instrument/session analytics through CAE.
- Saved analytics queries, execution records, result diffs, and deterministic insights.
- Offline research experiments through REF.
- Trade decision attribution through RA3.
- Exit counterfactual design and diagnostics through RA4.
- Trade path reconstruction and live path capture through RA5-RA8.
- Winner-versus-loser feature discovery through RA9.
- Research Workflow and Investigation/Evidence/Claim/Conclusion infrastructure.

These capabilities should be consolidated and evolved, not replaced.

## Evidence And Source Artifacts

Primary source artifacts:

- `outputs/track_b_execution_core/strategy_performance/canonical_trade_records.jsonl`
- `outputs/track_b_execution_core/trade_outcome_layer/canonical_trade_outcomes.jsonl`
- `outputs/track_b_execution_core/trade_outcome_enrichment/canonical_trade_outcome_enrichment.jsonl`
- `outputs/track_b_execution_core/research_analytics/live_trade_path_accumulator/finalized_trade_path_capture.jsonl`
- `outputs/track_b_execution_core/research_analytics/canonical_trade_path_layer/canonical_trade_paths.jsonl`
- `outputs/track_b_execution_core/research_analytics/ra3_trade_decision_attribution/canonical_trade_decision_attribution.jsonl`

Operational provenance artifacts:

- `outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json`
- `outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json`
- `outputs/track_b_execution_core/managed_positions/latest_managed_positions.json`
- `outputs/track_b_execution_core/managed_orders/latest_managed_orders.json`
- `outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json`
- `outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json`
- `outputs/track_b_execution_core/*truth*/latest_*.json`

Research and planning evidence:

- `outputs/reports/research_data_inventory/research_data_inventory.md`
- `outputs/reports/research_data_inventory/research_data_inventory.json`
- `outputs/reports/research_data_inventory/research_data_flow_diagram.md`
- `docs/epics/EPIC-002-research-platform.md`
- `docs/architecture/proposals/DP-001-canonical-research-record.md`

## Current Friction And Known Gaps

Known gaps include:

- Immutable identifier propagation is not complete across all layers.
- Some joins still rely on timestamp, contract, or tolerance matching.
- Tolerance joins must be exposed rather than silently treated as exact.
- Legacy trade-path coverage is incomplete.
- Partial-fill, scale-in, and scale-out sequencing needs one deterministic representation.
- Opening-range position is not captured directly for every completed trade.
- GRE/CRFD coverage is incomplete.
- VWAP/AVWAP relation and distance are incomplete.
- Structured setup labels are incomplete.
- Exit attribution is still coarse for many trades.

These gaps are research-data issues. They should not be patched by weakening broker, runtime, Managed Exit, Guardian, Safe-State, or order authority.

## Risks Of Doing Nothing

- Research scripts will continue rediscovering artifact paths and join rules.
- New analyses may accidentally treat tolerance joins as exact joins.
- Missing fields may be interpreted inconsistently across reports.
- Path coverage status may be overlooked before MFE, MAE, or counterfactual metrics are used.
- Existing RA1-RA8, CAE, REF, CTOL, and CTOE work may drift into parallel conventions.
- Future UI or AI-facing research summaries may lack deterministic traceability.

## Desired Future State

The future state should make one completed trade reconstructable through a small, documented research-facing envelope.

That future state should:

- Preserve Canonical Trade Records as the completed-trade identity spine.
- Preserve CTOL and CTOE as analytics and enrichment layers.
- Preserve RA8 and RA7 as path evidence layers.
- Preserve operational artifacts as safety and provenance evidence.
- Mark exact joins, tolerance joins, missing joins, and broken joins explicitly.
- Keep full bar samples outside the Canonical Trade Record.
- Keep research outputs diagnostic-only unless explicitly promoted through a separate approved process.

This section describes the desired state without prescribing implementation details.

## Recommendation On DP-001

Proceed with `DP-001-canonical-research-record.md`.

The baseline supports a small canonical research record or envelope because the platform already has the necessary component artifacts. The design should focus on consolidation, join quality, and provenance rather than replacement or broad refactoring.

## Consolidation Statement

The current platform should be consolidated and evolved, not replaced.

Existing RA1-RA8, CAE, REF, CTOL, and CTOE work must be preserved. DP-001 should create a research-facing connective layer over those artifacts, not a competing data model that weakens established authority boundaries.
