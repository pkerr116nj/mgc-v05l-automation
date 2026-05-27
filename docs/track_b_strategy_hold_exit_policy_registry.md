# Track B Strategy Hold/Exit Policy Registry v1

## Purpose

The strategy hold/exit policy registry maps each approved Track B guarded PAPER strategy to explicit, read-only hold and exit policy metadata. It centralizes the relationship between entry thesis, hold intent, exit-family selection, order policy, pyramiding policy, conflict group, and attribution eligibility.

This registry is declarative only. It does not change live exit behavior, submit orders, cancel orders, modify orders, close positions, flatten positions, mutate lifecycle state, grant lifecycle authority, enable live-money routing, or invoke paper proof.

## Policy Fields

Each strategy mapping includes:

- `strategy_id`
- `lane_id`
- `lane_family`
- `thesis_type`
- `expected_hold_type`
- `hold_policy_id`
- `exit_policy_family`
- `order_policy_id`
- `max_hold_policy`
- `profit_harvest_policy`
- `thesis_failure_conditions`
- `participation_decay_inputs`
- `pyramiding_policy`
- `conflict_group`
- `eligible_for_alpha_exit_analysis`

## Initial Conservative Mapping

- Snap-turn strategies use quick-scalp / time-box behavior with profit-harvest shadow context.
- Asian Drift uses drift / participation-hold context with anchor failure and participation-decay shadow context.
- Breakout/retest uses breakout/retest hold context with failed-retest and profit-harvest shadow context.
- Derivative bear-turn uses directional continuation context with decay or reversal invalidation.
- Pause/resume strategies use continuation hold context with pause-failure exit context.

## Integration

The registry is consumed as read-only context by:

- PositionIntent / TradeThesis contract audit.
- Hold-State / Exit-Selection shadow engine.
- Exit Attribution / Policy v2 documentation and future clean-exit analysis.

## Artifact

Run:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry \
  --repo-root /Users/patrick/Dev/MGC-v05l-automation \
  --json
```

Output:

- `outputs/track_b_execution_core/diagnostics/latest_strategy_hold_exit_policy_registry_audit.json`

## Safety

Every registry and audit artifact carries:

- `read_only=true`
- `shadow_only=true`
- `submit_allowed=false`
- `broker_mutation_allowed=false`
- `lifecycle_authority=false`
- `live_money_eligible=false`
- `paper_proof_invoked=false`

The registry is not enforced in live runtime in v1.
