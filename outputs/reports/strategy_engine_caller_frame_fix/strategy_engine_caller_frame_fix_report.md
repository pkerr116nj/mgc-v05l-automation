# Strategy Engine Caller-Frame Fix Report

- classification: `STRATEGY_ENGINE_CALLER_FRAME_FIXED`
- guard file/function: `src/mgc_v05l/execution/ibkr_paper_strategy_bridge.py :: evaluate_strategy_bridge_caller`
- original purpose: fail closed when preview/manual-only or suspicious automation caller frames reach the paper bridge submit path
- why it still triggered: preview-harness forbidden caller prefixes still treated `mgc_v05l.strategy.strategy_engine` as suspicious even after the supervised-paper caller metadata policy was added
- duplication with caller-type policy: `true`
- correct boundary: preview/manual misuse should stay stack-sensitive, but approved supervised PAPER runtime routing should be authorized by explicit route metadata

## Replay Result

- `gc_1x_all_lanes__asia_early_long` at `2026-04-29T19:06:00-04:00`: prior blocker `BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge detected forbidden caller frames: mgc_v05l.strategy.strategy_engine` -> caller gate passed `True` -> blocking failures `0` -> classification `REACHES_BRIDGE_PREFLIGHT`
- `gc_1x_all_lanes__asia_early_short` at `2026-04-29T19:05:00-04:00`: prior blocker `BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge detected forbidden caller frames: mgc_v05l.strategy.strategy_engine` -> caller gate passed `True` -> blocking failures `0` -> classification `REACHES_BRIDGE_PREFLIGHT`

## Safety Preserved

- unknown callers remain blocked
- missing metadata remains blocked
- live-mode metadata remains blocked
- scheduler-style callers remain blocked
- downstream monitor/governance/exposure gates still apply after caller-frame allowance
- historical replay was diagnostic only and submitted no order
