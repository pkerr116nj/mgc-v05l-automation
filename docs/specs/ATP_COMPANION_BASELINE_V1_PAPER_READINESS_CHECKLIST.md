# ATP Companion Baseline v1 Paper Readiness Checklist

Strategy:
- `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`
- internal label: `ATP_COMPANION_V1_ASIA_US`
- runtime kind: `atp_companion_benchmark_paper`

Purpose:
- define the minimum operator gates for calling the ATP benchmark paper-ready
- keep paper-ready explicitly separate from live-ready

## Current Readiness Status

Current outcome after the March 28, 2026 connected-soak pass:
- connected paper runtime path verified
- operator dashboard/tracked strategy path verified
- operator controls verified
- restart/reattach observability verified
- further soak time still required before any live-trading conversation

Paper-ready does not mean live-ready.

## Required Gates

`PASS` Auth/bootstrap verified
- `bash scripts/run_schwab_auth_gate.sh` succeeds
- token refresh succeeds
- market-data probe succeeds for `MGC`

`PASS` Runtime launch verified
- ATP benchmark runtime starts from the shared paper launcher `scripts/run_probationary_paper_soak.sh`
- runtime attaches with `runtime_kind = atp_companion_benchmark_paper`

`PASS` Dashboard attachment verified
- tracked strategy row is present for `atp_companion_v1_asia_us`
- dashboard health endpoint reports `ready: true`
- tracked row shows current heartbeat and persisted paper metrics

`PASS` Staleness detection verified
- closed-market session truthfully surfaces `data_stale = true`
- tracked status remains conservative instead of claiming healthy `READY`

`PASS` Restart / reattach behavior verified
- tracked strategy remains visible from persisted truth
- lane-local fallback keeps ATP visible even if the generic paper supervisor does not currently enumerate the ATP lane

`PASS` Operator halt / flatten controls verified
- `halt_entries` applies through the ATP-specific control file
- `resume_entries` applies through the ATP-specific control file
- `flatten_and_halt` applies cleanly while flat
- `stop_after_cycle` exits the runtime cleanly at a safe cycle boundary

`PASS` No unresolved reconciliation or fault defects observed in the connected pass
- recent fault count remained `0`
- recent reconciliation issue count remained `0`

`NOT YET` Minimum soak duration completed
- recommendation: at least `2` open-market sessions of `2+` hours each
- recommendation: at least `1` closed-market idle session verifying heartbeat aging and stale-data truth

`NOT YET` Minimum clean-session count completed
- recommendation: at least `3` clean connected sessions
- recommendation: at least `1` clean controlled shutdown and restart on a later day

## Explicit Non-Gate

These are still not authorized by paper-readiness:
- live broker order execution
- London ATP execution
- promotion/add logic
- exit redesign
- broader strategy optimization

## Promotion Rule

Only after the remaining soak-duration and clean-session gates are satisfied should the ATP benchmark be discussed as:
- ready for broader paper soak
- or eligible for a separate live-readiness review

It is not yet a live strategy.
