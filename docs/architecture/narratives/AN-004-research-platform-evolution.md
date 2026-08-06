# AN-004: Research Platform Evolution

## Purpose

Describe how research evolved from isolated reports and independent artifacts into a coherent deterministic research platform.

## Where We Were

Research began as individual analyses over available outputs. Reports answered useful questions, but each one often had to rediscover artifact paths, join conventions, data-quality gaps, and authority boundaries.

That was enough for early exploration. It became insufficient once autonomous PAPER trading produced a larger body of completed trades and the project needed repeatable evidence for entry, exit, context, path, and experiment questions.

## What We Decided

The project chose consolidation rather than replacement.

Key decisions were:

- Preserve Canonical Trade Records as completed-trade identity.
- Use CTOL for completed-trade outcomes.
- Use CTOE for enrichment and context validity.
- Preserve RA architecture for attribution, path capture, path normalization, and feature discovery.
- Use CAE for reusable analytics.
- Use REF for offline experiments.
- Adopt CRR as a derived, deterministic research-facing contract over existing artifacts.
- Use engineering governance to make research architecture decisions durable.

Authority became explicit: research layers can organize evidence, but they do not override broker, runtime, Managed Exit, Guardian, Safe-State, or trading authority.

## What Changed

Research became deterministic.

Instead of every analysis being a one-off traversal through generated artifacts, the platform now has named layers with clear responsibilities. Missing joins, broken joins, tolerance joins, sparse path coverage, and context validity are treated as first-class research quality signals rather than incidental annoyances.

The research platform now preserves existing RA, CTOL, CTOE, CAE, and REF work while giving future tools one stable way to reason about completed trades.

## Where We Are Going

The direction is an integrated quantitative research platform.

Near-term evolution should focus on:

- Implementing CRR v1 without runtime changes.
- Validating deterministic regeneration and reconciliation.
- Reducing tolerance-join debt.
- Improving path, context, setup, and exit-attribution completeness.
- Keeping research diagnostic until explicitly approved otherwise.

## Related Documents

- `docs/architecture/track-b-architectural-invariants.md`
- `docs/architecture/baselines/BA-001-current-research-platform.md`
- `docs/architecture/proposals/DP-001-canonical-research-record.md`
- `docs/architecture/reviews/RR-001-DP-001-canonical-research-record.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
- `docs/architecture/research-analytics-platform-vision.md`
- `docs/architecture/decision-intelligence-vision.md`
- `docs/epics/EPIC-002-research-platform.md`
- `docs/research/pattern-engine-current-state.md`
- `docs/operations/data-retention-and-archive-policy.md`
- `outputs/reports/research_data_inventory/research_data_inventory.md`
- `outputs/reports/research_data_inventory/research_data_flow_diagram.md`
