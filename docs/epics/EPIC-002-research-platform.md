# EPIC-002: Research Platform

## Status

Active planning. `ADR-001-canonical-research-record.md` accepts CRR as the research-facing architecture decision; the next milestone is bounded CRR v1 implementation planning.

## Purpose

Build a coherent research platform on top of the existing Track B PAPER trading artifacts so completed autonomous trades can be reconstructed, analyzed, compared, and used as deterministic research evidence.

This epic consolidates existing RA1-RA8, CTOL, CTOE, CAE, REF, and related work. It does not replace those layers.

The long-term research analytics platform vision is recorded in `docs/architecture/research-analytics-platform-vision.md`.

## Factual Basis

The current research spine is:

1. Broker truth is authoritative for current exposure and open orders.
2. Canonical Trade Records are authoritative for completed trade identity.
3. CTOL and CTOE provide completed-trade analytics and enrichment.
4. RA8 finalized capture and RA7 canonical paths provide path research evidence.
5. RA3 provides subordinate deterministic attribution evidence.
6. Operational artifacts provide safety and provenance.

The current data-flow inventory lives at:

- `outputs/reports/research_data_inventory/research_data_inventory.md`
- `outputs/reports/research_data_inventory/research_data_inventory.json`
- `outputs/reports/research_data_inventory/research_data_flow_diagram.md`

## Scope

### In Scope

- Define a small canonical research record contract that ties together existing artifacts.
- Preserve Canonical Trade Records as the completed-trade identity spine.
- Preserve CTOL and CTOE as the analytics/enrichment layers.
- Preserve RA8 and RA7 as the path-capture/path-summary layers.
- Preserve CAE and REF as consumers of completed-trade analytics.
- Make broken joins and missing research fields explicit.
- Improve reproducibility of empirical entry/exit research.

### Out Of Scope

- Runtime strategy changes.
- Broker authority changes.
- Managed Exit authority changes.
- Trading gates.
- Production recommendations.
- Replacement of CTOL, CTOE, CAE, RA7, RA8, or REF.
- Reorganization of generated outputs.

## Success Criteria

- A completed trade can be reconstructed from entry decision through final exit using documented joins.
- Missing fields are represented explicitly instead of inferred.
- Path coverage status is visible and trusted before MFE/MAE or counterfactual metrics are used.
- Research scripts can consume one documented spine instead of rediscovering artifact paths.
- Derived research outputs remain traceable to canonical identities and source artifacts.

## Open Problems

- Some joins still rely on timestamp/contract tolerance rather than immutable identifiers.
- Opening-range-position is not present on every completed trade.
- GRE/CRFD and VWAP/AVWAP coverage remain incomplete.
- Legacy path captures may be partial because rolling candle snapshots were not durable enough.
- Partial fill and scale-in/scale-out semantics need a single research-facing representation.

## Related Work

- RA1-RA2: autonomous runtime trade review and contextual expectancy.
- RA3: canonical trade decision attribution.
- RA4-RA8: exit counterfactual design, path reconstruction, canonical paths, live path accumulation.
- CTOL/CTOE: completed-trade outcome and enrichment layers.
- CAE: canonical analytics engine and saved-query/provenance/diff/insight layers.
- REF: offline research experiment framework.

## Next Milestone

Create the bounded CRR v1 implementation plan from `ADR-001-canonical-research-record.md`.
