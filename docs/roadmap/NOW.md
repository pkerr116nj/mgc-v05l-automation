# Now

This page is the small, Git-backed working set for what matters now. It is not a runtime gate, service manifest, or trading configuration.

## Operating Principle

Keep the Track B PAPER runtime trading normally while improving the research record around what happened. Do not let research artifacts override live safety authority.

Authority hierarchy remains:

1. Broker truth for current exposure and open orders.
2. Canonical Trade Records for completed trade identity.
3. CTOL and CTOE for completed-trade analytics and enrichment.
4. RA8 finalized capture and RA7 canonical paths for path research.
5. Operational artifacts for safety and provenance.

## Active Focus

- Create the bounded CRR implementation plan.
- Implement CRR v1 without runtime changes.
- Validate deterministic regeneration, reconciliation, and join-quality reporting.
- Close out the first ADR implementation cycle.
- Stabilize the research data spine around completed PAPER trades.
- Treat `outputs/track_b_execution_core/strategy_performance/canonical_trade_records.jsonl` as the completed-trade identity spine.
- Preserve `path_capture.capture_id` as the join from Canonical Trade Records into RA8 path capture and RA7 canonical paths.
- Use CTOL and CTOE as the completed-trade analytics layer, not as raw fill authority.
- Keep RA8 path accumulation out of the execution hot path and failure-isolated.
- Continue evaluating entry and exit research questions using completed trades only.

## Current Research Questions

- Which entry-context fields are missing from Canonical Trade Records and should be captured at decision time?
- Which completed trades have enough RA8/RA7 path coverage to support MFE, MAE, giveback, and counterfactual exit analysis?
- Where do CTOL/CTOE joins still depend on timestamp tolerance instead of immutable identifiers?
- Which regime/context enrichments are missing often enough to block stable research conclusions?

## Current Non-Goals

- Do not reorganize application code.
- Do not change runtime configuration or services from roadmap work.
- Do not replace CTOL, CTOE, CAE, RA7, RA8, or REF with a parallel system.
- Do not turn research findings into production recommendations or trading gates.
