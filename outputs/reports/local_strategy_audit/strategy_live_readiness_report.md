# Local Paper Strategy Audit

- local strategy audit classification: `LOCAL_STRATEGY_AUDIT_READY`
- migration classification: `IBKR_MIGRATION_READY_FOR_NEXT_SCOPE`
- total lanes audited: `45`
- broker-path capable lanes now: `10`
- legacy local-paper lanes still trading: `35`
- overlooked recent local-only traders: `29`
- monitor health: `HEALTHY`
- monitor stale: `False`
- broker net MGC: `0.0`
- strategy-attributed MGC: `0.0`
- broker-minus-ledger difference: `0.0`

## Key Findings

- supported `GC/MGC` scope is effectively fully ported: `10` lanes are broker-path ready.
- unsupported scope remains the main source of ongoing local-only paper trading: `35` lanes.
- `KILL_CANDIDATE` remains a warning label, not an automatic execution stop; current count: `1`.

## Near-Term Readiness

- `gc_1x_all_lanes__asia_early_long` / `GC`: readiness=`PAPER_PROBATION` governance=`PROMISING` trades_last_10=`6` total_net=`920.00`
- `gc_1x_all_lanes__asia_early_short` / `GC`: readiness=`WATCHLIST` governance=`DEGRADED` trades_last_10=`8` total_net=`-240.00`
