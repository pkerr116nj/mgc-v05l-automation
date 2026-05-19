# Track B PAPER Operational Maturation Overlay

Status: PAPER_ONLY, OPERATIONAL_MATURATION, NON_PRODUCTION, NOT_PROMOTION_ELIGIBLE.

This overlay is a bounded runtime participation mode for the active 15-lane Track B PAPER set. It exists to exercise the supervised PAPER execution stack when production-style predicates are too sparse for operational learning. It is not a promotion package, not a live-money candidate, and not evidence of production edge.

## Scope

Active profile: `PAPER_ONLY_OPERATIONAL_MATURATION_V1`

Config:

- `config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml`

Runtime behavior is enabled only when a lane has:

- `runtime_overlay_params.operational_maturation_mode=true`
- `runtime_overlay_params.operational_maturation_profile=PAPER_ONLY_OPERATIONAL_MATURATION_V1`
- `paper_only=true`
- `live_money_eligible=false`

## Predicate Changes

The overlay relaxes operational-maturation predicates only:

- Timed fallback entry moves to bar 5 of the lane's configured session.
- The exact selected-entry-bar gate allows up to two completed-bar catch-up opportunities.
- Preferred directional trigger before fallback is no longer required for the operational timed entry.
- A minimum two-tick setup range remains required to avoid zero-movement bars.

The overlay preserves safety/session boundaries:

- No global session widening.
- No canonical session-label changes.
- Existing lane `allowed_sessions` and runtime segment checks remain authoritative.
- `max_position_quantity=1`, `max_concurrent_entries=1`, and `SINGLE_ENTRY_ONLY` remain unchanged.
- Phase-1 provenance, freshness, governance, reconciliation, and broker safety gates remain unchanged.
- `live_money_eligible=false` remains enforced.

## Participation Suppressors Identified

Ranked by observed live/runtime impact:

1. Exact selected-entry-bar only gate: a lane can miss its only entry opportunity if readiness/startup was late or a single decision bar was not actionable.
2. Late fallback timing: bar 7/bar 8 fallback delayed and narrowed the only actionable opportunity for many lanes.
3. Preferred directional over-confirmation: breakout, reclaim-fail, dip-reclaim, and contextual fallback checks often defer or eliminate action before fallback.
4. Anti-churn portfolio controls: single-entry, no stacking, no direct flips suppress lifecycle volume but are retained as safety boundaries.
5. Session-open mismatch in Asia/London participation lanes: some lanes are session-eligible at `SESSION_OPEN`, but the runtime's entry segment remains `ASIA_EARLY`; this is documented but not changed because the exit model is built around the Asia/London hold window.

## Rollback

Revert the config lane `runtime_overlay_params` profile or revert the operational overlay commit. With the profile absent, the runtime adapters fall back to their original production-style predicate behavior.
