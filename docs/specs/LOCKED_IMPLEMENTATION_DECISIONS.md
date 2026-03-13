# Locked Implementation Decisions for Codex

These values are implementation-locked and must not be changed by inference.

- `TIMEFRAME = 5m`
- `SESSION_TIMEZONE = America/New_York`
- `REPLAY_FILL_POLICY = NEXT_BAR_OPEN`
- `VWAP_POLICY = SESSION_RESET`
- `INITIAL_RUN_MODE = REPLAY_FIRST`
- `DATABASE = SQLITE`
- `SYMBOL_SCOPE = SINGLE_SYMBOL_MGC`
- `REPLAY_DATA_COLUMNS = timestamp,open,high,low,close,volume`
