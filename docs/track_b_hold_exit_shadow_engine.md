# Track B Hold-State / Exit-Selection Shadow Engine v1

## Purpose

The Hold-State / Exit-Selection shadow engine uses read-only `PositionIntent` and `TradeThesis` metadata to explain what a participation-aware exit policy would recommend for open or recently closed Track B PAPER positions.

It is diagnostic only. It does not submit, cancel, close, modify, flatten, mutate lifecycle state, or grant lifecycle/order authority.

## Hold-State Classifications

- `HOLD_THESIS_INTACT`
- `HOLD_EXTEND_PARTICIPATION_STRONG`
- `HARVEST_PROFIT_AVAILABLE`
- `EXIT_DECAY`
- `EXIT_THESIS_FAILURE`
- `TIMEBOX_EXIT_DUE`
- `INSUFFICIENT_EVIDENCE`

## Exit-Selection Recommendations

- `HOLD`
- `EXTEND_HOLD`
- `HARVEST`
- `EXIT_DECAY`
- `EXIT_THESIS_FAILURE`
- `TIMEBOX_EXIT`
- `NO_RECOMMENDATION`

## Inputs

- `PositionIntent` / `TradeThesis` metadata.
- Active lifecycle or broker-reconciled open-position state when available.
- MFE, MAE, current net movement, and completed 5-minute bars since entry.
- Session and regime labels when available.
- ATP, participation, or MicroTrend shadow state when present in the input artifact.
- Actual current managed exit policy for context.
- Strategy Hold/Exit Policy Registry v1 context, including `hold_policy_id`, `exit_policy_family`, `profit_harvest_policy`, `thesis_failure_conditions`, and `participation_decay_inputs`.

## Decision Order

The engine fails closed to `INSUFFICIENT_EVIDENCE` when required intent or position evidence is missing.

When evidence is present, it evaluates:

1. Thesis invalidation.
2. Max-hold / time-box completion.
3. Participation decay with material giveback.
4. Participation decay without material giveback.
5. Strong aligned participation with favorable net movement.
6. Thesis-intact favorable movement without a stronger exit signal.

## Closed Trade Learning

Closed-trade reconstruction is limited to clean `ALPHA_EXIT` trades that are explicitly eligible for alpha-exit analysis. `BUG_FIX_EXIT`, remediation, leak-test cleanup, lifecycle repair, aggregate close repair, and unknown-intent exits are excluded from exit-policy learning.

## Artifacts

- `outputs/track_b_execution_core/research_shadow/latest_hold_exit_shadow_recommendations.json`
- `outputs/track_b_execution_core/research_shadow/hold_exit_shadow_recommendations.jsonl`

Every artifact carries:

- `shadow_only=true`
- `read_only=true`
- `submit_allowed=false`
- `broker_mutation_allowed=false`
- `lifecycle_authority=false`
- `live_money_eligible=false`
- `paper_proof_invoked=false`

## Promotion Boundary

This engine is not a promotion mechanism. Future live exit policy changes require separate evidence showing enough clean alpha exits, reduced giveback, better MFE capture, no worse MAE, no lifecycle/order-management regressions, and explainable state transitions.
