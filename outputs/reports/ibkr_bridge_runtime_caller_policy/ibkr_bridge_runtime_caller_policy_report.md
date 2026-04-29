# IBKR runtime caller policy fix

- classification: `IBKR_RUNTIME_CALLER_POLICY_FIXED`
- generated_at: `2026-04-29T17:01:28.967397+00:00`
- root cause: manual-only caller enforcement plus preview-harness forbidden-frame reuse blocked approved supervised paper runtime callers.
- no order was submitted in this pass.

## Approved paper callers

- `manual_strategy_bridge_cli`
- `probationary_paper_runtime_lane`
- `supervised_paper_runtime_bridge`
- `ibkr_paper_strategy_executor`

## Blocked caller classes

- unknown callers
- live-money callers
- scheduler/unattended live-like callers
- runtime callers without explicit PAPER / 127.0.0.1 / 7497 / DUM882026 metadata lock

## Safety gates preserved

- PAPER-only lock
- account lock
- TWS host/port lock
- monitor health gate
- backend/source readiness gate
- governance gate
- exposure gate
- quote gate
- broker/ledger reconciliation
- one active order per strategy
- per-strategy cap
- aggregate cap

## Midday family impact

- `index_ny_early_core_us_midday`: historical `8` actionable entry bars, `0` routed intents, `0` fills. Caller-path suppressor is fixed; next fresh midday signal should reach real downstream bridge preflight.
- `gc_all_lanes_us_midday`: historical `1` actionable entry bar, `0` routed intents, `0` fills. Caller-path suppressor is fixed; next fresh midday signal should reach real downstream bridge preflight.

## Test proof

- bridge policy slice: `22 passed`
- probationary runtime slice: `7 passed`
- approved supervised paper runtime callers now pass caller-path validation and explicit PAPER-only route metadata checks.
- unknown/live/scheduler callers remain fail-closed.
