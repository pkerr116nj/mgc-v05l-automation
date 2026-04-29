# Schwab Runtime Dependency Purge Phase 1

- Classification: `SCHWAB_RUNTIME_DEPENDENCY_PURGE_READY`
- Generated at: `2026-04-29T09:27:26.606385+00:00`

## Phase 1 result

- IBKR paper monitor startup no longer requires Schwab auth.
- IBKR paper bridge/gating no longer inherits a hidden Schwab dependency.
- Operator dashboard bootstrap now treats Schwab as provider-specific fallback readiness instead of global runtime failure.
- Schwab remains available as fallback/legacy support and is not removed.

## Provider readiness

- Broker truth (`IBKR`): `ready`
- IBKR paper monitor: `ready`
- Databento primary market data: `configured`
- Schwab fallback: `optional`
- Local cache: `ready`
- Dashboard bootstrap: `ready`

## Fixed in Phase 1

- `scripts/common_env.sh`
- `scripts/run_supervised_paper_host.sh`
- `scripts/run_headless_supervised_paper_service.sh`
- `scripts/run_probationary_operator_control.sh`
- `scripts/run_probationary_shadow.sh`
- `scripts/run_probationary_paper_soak.sh`
- `scripts/run_operator_dashboard.sh`
- `src/mgc_v05l/app/operator_dashboard.py`
- `src/mgc_v05l/execution/live_strategy_broker.py`

## Remaining Schwab-specific or legacy paths

- `scripts/run_schwab_auth_gate.sh`: Explicit Schwab auth helper remains by design and is no longer part of IBKR paper startup.
- `scripts/run_schwab_token_web.sh`: Explicit Schwab auth/token workflow remains available as a legacy fallback path.
- `scripts/show_probationary_status.sh`: Still assumes Schwab-backed probationary flow and should be migrated or retired later.
- `scripts/show_probationary_paper_status.sh`: Still tied to the older Schwab-centered paper summary flow.
- `scripts/run_probationary_paper_summary.sh`: Still requires Schwab auth even though core IBKR paper runtime no longer does.
- `scripts/run_probationary_daily_summary.sh`: Legacy summary path remains Schwab-centric and is outside the critical IBKR paper startup path.
- `scripts/run_probationary_live_strategy_pilot.sh`: Still Schwab-specific because the legacy live pilot is not part of the IBKR paper runtime.
- `src/mgc_v05l/execution/live_strategy_broker.py`: Fallback remains supported but is now explicit and disabled by default unless the config and environment both allow it.
- `src/mgc_v05l/market_data/schwab_provider.py`: Provider stays in-tree for fallback and parity work until later migration phases are complete.
- `scripts/backfill_schwab_1m_history.sh`: Research backfill scripts remain Schwab-specific until Databento parity and cache migration are complete.
