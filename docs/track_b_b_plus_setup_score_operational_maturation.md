# Track B B_PLUS Setup Score Operational Maturation Overlay

Status: PAPER_ONLY / OPERATIONAL_MATURATION / NON_PRODUCTION / NOT_PROMOTION_ELIGIBLE.

This overlay allows active Track B PAPER lanes to consume the generic Entry Acceptance Layer at the B_PLUS level when exact predicates do not fire. It is designed to create bounded paper execution flow for operational learning without changing session policy, live-money eligibility, broker reconciliation, or canonical strategy promotion criteria.

## Existing Scoring Surface

The canonical near-candidate scoring surface already exists in src/mgc_v05l/execution_core/track_b_entry_acceptance.py and is documented in docs/track_b_entry_acceptance_layer_v1_design.md. It provides advisory acceptance classes, deterministic dimension weights, freshness/provenance failure modes, and the exact/near/degraded/invalid taxonomy.

The existing implementation is research/advisory only. It deliberately emits QUALITY_CONTEXT_ONLY, sets runtime authority flags false, and its writer is limited to tmp/test paths. It also has a concrete adapter only for the Asia early normal breakout/retest/hold family, so it cannot be directly dropped into every active forced-session and Asia/London lane without building family-specific payload adapters.

## Generic Levels

The Entry Acceptance Layer now exposes generic consumption levels that upstream strategy documents or overlays can declare:

- EXACT: exact structural match, score >= 0.85.
- B_PLUS: upper near-match, default score >= 0.80 and structural similarity >= 0.72; the active PAPER-only operational maturation overlay may declare a lower non-production threshold for bounded execution-stack exercise.
- NEAR: canonical near-match, score >= 0.70.
- DEGRADED: degraded-but-valid advisory context, score >= 0.50.
- INVALID and LOW_CONFIDENCE: fail-closed/non-actionable.

The B+ runtime overlay consumes only the B_PLUS level. This keeps the operational maturation path aligned with the prior near-match research instead of creating a separate scoring model. For the current PAPER-only throughput pass, the active 15-lane overlay sets `operational_maturation_b_plus_threshold=0.775`; this is NON_PRODUCTION and NOT_PROMOTION_ELIGIBLE.

## Runtime Boundaries

The active overlay config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml declares operational_maturation_entry_acceptance_level: B_PLUS and `operational_maturation_b_plus_threshold=0.775` for the 15 PAPER lanes. It also disables the older timed-entry maturation shortcut so B+ is the operational escalation path for tonight.

Hard gates remain mandatory:

- PAPER only.
- live_money_eligible=false.
- Configured session restriction must match.
- Strategy side/direction must match.
- Own-instrument Phase-1 runtime artifact source/provenance must match.
- Basic range/activity minimum must pass.
- Broker reconciliation and governance remain authoritative.
- One B+ entry per lane per session.

B+ diagnostics are sidecar metadata on runtime intent summaries. The SignalPacket source id and OrderIntent.reason_code stay on the canonical source id so approved-source governance remains intact.

## Diagnostic Artifacts

Every live scorer invocation writes diagnostic-only score payloads, including rejected B+ evaluations:

- Lane latest: `outputs/probationary_pattern_engine/paper_session/lanes/<lane_id>/b_plus_setup_score_latest.json`
- Lane history: `outputs/probationary_pattern_engine/paper_session/lanes/<lane_id>/b_plus_setup_score_events.jsonl`
- Session aggregate: `outputs/probationary_pattern_engine/paper_session/b_plus_setup_score_events.jsonl`

These artifacts are explicitly non-authoritative. They do not create order intents, do not attempt routes, do not mutate broker or lifecycle state, and do not change live-money eligibility.

## Non-Goals

- No production promotion.
- No live-money path.
- No global session widening.
- No canonical session-label change.
- No broker safety relaxation.
- No strategy threshold tuning outside the PAPER-only B+ consumption level.

## Rollback

Set operational_maturation_b_plus_enabled=false or revert this overlay/config commit. Exact-match lanes remain intact, and the older timed-entry shortcut is still code-supported but disabled by the active overlay.
