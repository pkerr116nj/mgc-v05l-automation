# Locked Implementation Decisions for Codex

These values are implementation-locked inside the original benchmark lane, but they are no longer repo-wide hard locks.

- `SESSION_TIMEZONE = America/New_York`
- `REPLAY_FILL_POLICY = NEXT_BAR_OPEN`
- `VWAP_POLICY = SESSION_RESET`
- `DATABASE = SQLITE`
- `REPLAY_DATA_COLUMNS = timestamp,open,high,low,close,volume`

In code, the old prototype-wide lock model is replaced by:
- `AUTOMATION_PLATFORM_DEFAULTS` for shared defaults that remain broadly applicable
- `LEGACY_BASELINE_SETTINGS` for the original single-symbol benchmark snapshot only

That means the following are preserved only as legacy baseline truth, not as global platform defaults:
- `TIMEFRAME = 5m`
- `INITIAL_RUN_MODE = REPLAY_FIRST`
- `SYMBOL_SCOPE = SINGLE_SYMBOL_MGC`
- `symbol = "MGC"`

## Automation Settings Model

`src/mgc_automation/settings.py` intentionally separates:
- platform defaults that still make sense across lanes
- legacy benchmark-only defaults kept for reproducibility

It must not reintroduce repo-wide hard-coded assumptions that every runtime uses:
- no universal `timeframe = 5m`
- no universal `initial_run_mode = REPLAY_FIRST`
- no universal `symbol_scope = SINGLE_SYMBOL_MGC`
- no universal `symbol = "MGC"`

The recently completed same-day futures delegated Flatten Session Risk live-broker hardening lane remains a separate narrow supported path. Removing these old global prototype locks does not broaden any live trading semantics.
