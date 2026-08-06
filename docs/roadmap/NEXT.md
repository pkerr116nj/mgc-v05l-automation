# Next

This page holds the next tranche of planning work after `NOW.md`. Items here should be small enough to move into `NOW.md` without re-opening the whole architecture.

## Research Data Spine

- Plan and implement the accepted Canonical Research Record v1 contract from ADR-001.
- Add a documented join contract for `trade_id`, `source_trade_id`, `trade_outcome_id`, `capture_id`, and `canonical_trade_path_id`.
- Identify which current joins still require timestamp/contract tolerance and prioritize converting them to explicit identifiers.
- Document the difference between raw truth, canonical derived truth, and research-derived evidence.
- Keep future advisory research summaries aligned with `docs/architecture/decision-intelligence-vision.md`.
- Keep CRR and analytics-surface planning aligned with `docs/architecture/research-analytics-platform-vision.md`.

## Trade Reconstruction

- Create a checklist for reconstructing one completed trade from entry decision through final exit.
- Add a small sample reconstruction dossier for one completed trade with a valid path capture.
- Separate legacy partial path captures from post-extension captures in research reports.
- Document how partial fills, scale-ins, and scale-outs should appear in the research record without creating duplicate canonical trades.

## Research Coverage

- Track missing opening-range-position coverage for completed trades.
- Track missing GRE/CRFD coverage by instrument/session.
- Track missing VWAP/AVWAP relation and distance at entry and exit.
- Track path coverage status by deployment era: legacy, post-RA8, post-path-capture-extension.
- Use `docs/research/pattern-engine-current-state.md` as the current Pattern Engine and operator-baseline terminology reference.

## Operations Documentation

- Create short operator-facing docs for:
  - broker truth authority,
  - Managed Exit supervision evidence,
  - RA8 path accumulator health,
  - research artifact freshness.

## Decision Records

- Promote durable architecture decisions into `docs/architecture/decisions/` once accepted through the governance lifecycle.
- Keep speculative work in `docs/architecture/proposals/` until accepted.
