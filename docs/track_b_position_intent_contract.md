# Track B PositionIntent / TradeThesis Contract v1

Status: read-only diagnostic infrastructure. This contract does not grant broker, submit, lifecycle, cancel, close, modify, flatten, live-money, or paper_proof authority.

## Purpose

Every Track B entry should carry enough structured context to explain why the position exists, how long it is expected to be held, what would invalidate it, how it should exit, how pyramiding/conflicts are constrained, and how the result should be attributed later.

The v1 contract is intentionally an audit layer first. It validates that approved guarded PAPER strategies can produce complete metadata, emits a coverage artifact, and gives downstream diagnostics a stable shape to consume before any runtime enforcement is added.

## Schemas

`PositionIntent` is the top-level object. Required fields:

- `strategy_id`
- `lane_id`
- `instrument_family`
- `contract_key`
- `local_symbol`
- `con_id`
- `expiry`
- `side`
- `quantity`
- `pyramiding_policy`
- `conflict_group`

`TradeThesis` describes the reason for the entry:

- `thesis_type`: `SCALP`, `DRIFT`, `TREND_PARTICIPATION`, `SNAP_TURN`, `BREAKOUT_RETEST`, or `OTHER`
- `thesis_summary`
- `invalidation_conditions`
- `regime_tags`

`HoldPolicy` describes intended duration:

- `expected_hold_type`: `QUICK_SCALP`, `TIMEBOXED`, `PARTICIPATION_HOLD`, or `SESSION_HOLD`
- `max_hold_policy`
- `expected_hold_bars_5m`

`ExitPolicy` describes managed-exit intent:

- `intended_exit_family`
- `managed_exit_policy_id`
- `exit_profile_id`
- `exit_profile_source`

`OrderPolicy` describes non-authoritative order intent:

- `order_type`
- `time_in_force`
- `pricing_policy`
- `broker_mutation_allowed=false`
- `submit_authority=false`

`AttributionTags` describes later analytics eligibility:

- `eligible_for_alpha_exit_analysis`
- `strategy_family`
- `session_tags`
- `regime_tags`
- `contamination_flags`

## Safety

The contract and audit artifacts are read-only:

- `submit_authority=false`
- `broker_mutation_allowed=false`
- `lifecycle_authority=false`
- `live_money_eligible=false`
- `paper_proof_invoked=false`

Missing metadata is reported as a contract gap. It is not enforced in live runtime in v1.

## Integration Points

The audit artifact exposes read-only context for:

- Exit Attribution / Policy v2: `eligible_for_alpha_exit_analysis`, `thesis_type`, `intended_exit_family`
- Managed lifecycle reports: `position_intent`, `trade_thesis`, `hold_policy`, `exit_policy`
- Shadow Exit Policy v2 recommendations: `expected_hold_type`, `invalidation_conditions`, `attribution_tags`
- Hold-State / Exit-Selection Shadow Engine v1: `thesis_type`, `expected_hold_bars_5m`, `invalidation_conditions`, `intended_exit_family`

These integrations are diagnostic-only until a separate enforcement gate is designed and approved.

## Artifact

Run:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_position_intent_contract \
  --repo-root /Users/patrick/Dev/MGC-v05l-automation \
  --json
```

Output:

- `outputs/track_b_execution_core/diagnostics/latest_position_intent_contract_audit.json`

The audit validates the active guarded PAPER roster in `config/track_b_guarded_paper_roster.json` against the v1 contract templates.
