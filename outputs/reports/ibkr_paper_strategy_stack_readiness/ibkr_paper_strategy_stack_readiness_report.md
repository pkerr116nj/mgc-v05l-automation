# IBKR Paper Strategy Stack Readiness

- classification: `IBKR_PAPER_STACK_READY_FOR_SUPERVISED_EVALUATION`
- generated at: `2026-04-29T12:14:52.699216+00:00`
- TWS paper connection: `CONNECTED`
- account: `DUM882026`
- monitor: `running=True` `health=HEALTHY` `stale=False`
- Schwab fallback: `optional`
- market data primary: `databento`
- broker truth provider: `ibkr`

## Broker Runtime

- current broker positions: `MGC=0.0` `MNQ=0.0` `MES=0.0`
- current open orders: `MGC=0` `MNQ=0` `MES=0`
- broker-minus-ledger: `MGC=0.0` `MNQ=0.0` `MES=0.0`
- orphan exposure: `MGC=0.0` `MNQ=0.0` `MES=0.0`

## Supported Scope

- `GC/MGC` proxy=`MGC` submit-capable=`9` quote=`DELAYED` exposure=`max_total_mgc_contracts=20; max_total_gc_equivalent=2; max_per_strategy_mgc_contracts=1` actionable=`0`
- `MNQ/NQ` proxy=`MNQ` submit-capable=`20` quote=`DELAYED_ONLY` exposure=`max_total_mnq_contracts=20; max_total_nq_equivalent=2; max_per_strategy_mnq_contracts=1` actionable=`0`
- `MES/ES` proxy=`MES` submit-capable=`15` quote=`DELAYED_ONLY` exposure=`max_total_mes_contracts=20; max_total_es_equivalent=2; max_per_strategy_mes_contracts=1` actionable=`0`
- `Rates` roadmap only; no routing implemented

## Performance Governance

- top paper-probation candidates: `gc_1x_all_lanes__asia_early_long, gc_1x_all_lanes__us_early_short, gc_1x_all_lanes__us_midday_short`
- KILL_CANDIDATE lanes: `gc_1x_all_lanes__london_early_long, nq_1x_ny_early_core__us_late_short_reclaim_fail, nq_1x_ny_early_core__us_midday_short_breakdown`
- strategies with recent local-only trades: `35`
- strategies with broker-path trades: `1`
- readiness buckets: `{'ready but waiting for signal': 45}`
- live-money tracker counts: `{'WATCHLIST': 24, 'NOT_READY': 15, 'PAPER_PROBATION': 3, 'DIAGNOSTIC_ONLY': 3}`

## Notes

- delayed quote modes are treated as paper-only pricing inputs, not live market data
- internal-only performance remains separated from broker-path paper performance
- no order was placed in this report pass
