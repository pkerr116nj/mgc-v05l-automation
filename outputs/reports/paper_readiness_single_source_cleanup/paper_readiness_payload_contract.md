## Paper Readiness Contract
Authoritative producer:
- `src/mgc_v05l/app/operator_dashboard.py:_paper_readiness_payload`

Top-level paper usability producer:
- `src/mgc_v05l/app/operator_dashboard.py:_supervised_paper_operability_payload`

Transport/summarizer only:
- `src/mgc_v05l/app/operator_surface.py:_build_runtime_readiness`

Desktop presentation only:
- `desktop/src/main/shared/operatorTriage.ts`
- `desktop/src/main/shared/operationalReadiness.ts`

Final broker-path authority:
- `src/mgc_v05l/execution/ibkr_paper_strategy_bridge.py`

## Required Semantics
The paper readiness contract is authoritative for whether supervised paper trading is allowed to evaluate and wait for signals.

Downstream layers may:
- display the contract
- summarize the contract
- add live-mode-specific authority state
- add attachment/source-mode context

Downstream layers may not:
- re-block PAPER mode because live authority is unavailable
- treat WATCH/advisory rows as paper blocking faults
- override authoritative healthy paper readiness with stale fallback fault labels

## Authoritative Fields
- `paper_runtime_ready`
  - meaning: paper runtime is healthy enough for supervised paper operation
- `paper_trade_allowed`
  - meaning: supervised paper evaluation is allowed at the paper-readiness layer
- `paper_trade_block_reason`
  - meaning: exact paper-specific block reason when `paper_trade_allowed=false`
- `paper_readiness_source`
  - meaning: source function path for the readiness contract
- `paper_readiness_timestamp`
  - meaning: timestamp of the readiness contract
- `session_eligible_count`
  - meaning: lanes currently in allowed session and otherwise eligible to evaluate
- `waiting_for_bar_count`
  - meaning: session-eligible lanes waiting for next completed decision bar
- `no_setup_count`
  - meaning: lanes evaluated but without setup present
- `actionable_now_count`
  - meaning: lanes with current actionable intent on this completed bar
- `true_blocked_count`
  - meaning: lanes blocked by a real paper-side condition, not merely waiting/no-setup
- `advisory_fault_count`
  - meaning: WATCH/advisory runtime exceptions
- `blocking_fault_count`
  - meaning: true blocking runtime faults

## Consumer Rules
### `_build_runtime_readiness`
- transports authoritative paper fields into operator-surface payloads
- may summarize counts and display text
- must not independently mark paper blocked from generic `fault_state`

### `operatorTriage.ts`
- in PAPER mode, uses authoritative `paper_trade_allowed` when present
- keeps `live_trade_allowed` separate
- may use compatibility fallback only when authoritative paper fields are absent

### `operationalReadiness.ts`
- may present app-level state like `READY`, `BLOCKED`, `RECONCILING`
- must respect explicit `paper_trade_allowed` / `paper_trade_block_reason`
- must not require live authority for PAPER mode

## Compatibility Fallback
If older partial paper payloads are encountered and the explicit paper authority fields are absent, downstream compatibility fallback may infer paper runtime readiness from:
- paper runtime running
- entries enabled
- no blocking faults

That fallback exists only for older payload compatibility. When authoritative paper fields are present, they win.
