# IBKR Paper Monitor Service

This service keeps the IBKR paper strategy monitor running in the background for `PAPER / 127.0.0.1 / 7497 / DUM882026`.

## Scope

- paper only
- monitor only
- no order submission
- no flattening
- no autonomous strategy execution

## Runtime files

The live bridge and operator dashboard should consume these runtime files under `var/`:

- `var/paper_strategy_monitor_runtime_status.json`
- `var/paper_strategy_position_ledger.json`
- `var/paper_strategy_pnl_snapshot.json`
- `var/paper_strategy_monitor_heartbeat.json`
- `var/paper_strategy_monitor_audit.jsonl`

## Control scripts

- `bash scripts/start-paper-monitor`
- `bash scripts/status-paper-monitor`
- `bash scripts/stop-paper-monitor`

## Default behavior

- poll interval: `5s`
- freshness window: `45s`
- reconnects on IBKR disconnect without crashing
- bridge submit stays blocked unless runtime status is present, fresh, running, and healthy

## Notes

- the service is paper-only and hard-locked to the proven MGC paper lane
- if TWS closes or the IBKR API disconnects, monitor health becomes `DISCONNECTED` and bridge submit must fail closed
- if the runtime status ages out, bridge submit must fail closed
