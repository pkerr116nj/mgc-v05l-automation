# ATP Companion Baseline v1 Benchmark

## Canonical Benchmark Name

`ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`

Short internal label:
- `ATP_COMPANION_V1_ASIA_US`

Benchmark status:
- current promoted replay/paper ATP companion benchmark
- freeze branch reference for future ATP research
- current tracked paper strategy benchmark for app-facing paper monitoring
- current continuously running live-data paper benchmark for soak validation

Governance note:
- this document freezes an ATP benchmark lane
- it does not define global platform defaults for all research or production lanes

## Promotion Note

This benchmark freezes the current ATP companion candidate with no new semantic work beyond the already-earned London gate-off decision.

What changed before freeze:
- ATP executable coverage is now `ASIA + US` only.
- `LONDON` remains available for ATP context and diagnostics, but not for ATP executable entries.

What did not change:
- `5m` context plus `1m` timing ATP architecture
- session-reset VWAP
- stricter `VWAP_NEUTRAL` posture
- blocked `VWAP_CHASE_RISK` posture
- one-position-at-a-time
- replay/paper-only ATP scope
- legacy v0.5l benchmark-lane behavior
- legacy Bull Snap / Asia VWAP reclaim / Bear Snap semantics
- replay fill policy, session timezone, and other preserved legacy baseline assumptions

Why London is disabled:
- the London-favorable-only experiment improved results, but London remained negative
- the full London gate-off benchmark materially improved aggregate ATP economics
- until a better London-specific continuation model exists, London should remain diagnostic-only in the ATP companion baseline

This benchmark is now the reference baseline for future ATP research branches.
It is a benchmark lane, not a statement that London or ATP timing assumptions are universal truth across the platform.

## Productized Paper Tracking Note

This benchmark is now productized as a tracked paper strategy in the app-facing paper surface.

Tracked paper strategy identity:
- strategy id: `atp_companion_v1_asia_us`
- display name: `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`
- internal label: `ATP_COMPANION_V1_ASIA_US`
- environment: `paper`
- benchmark designation: `CURRENT_ATP_COMPANION_BENCHMARK`

What changed in productization:
- the benchmark is now enumerated as a tracked paper strategy
- app-facing paper telemetry now shows benchmark status, paper metrics, and recent audit rows without raw SQLite inspection
- restart-safe monitoring now restores benchmark visibility from persisted paper/runtime truth
- a dedicated continuous paper runtime can now attach this benchmark to current market data without changing ATP semantics

What did not change in productization:
- no ATP signal semantics
- no London reopen
- no promotion/add logic
- no live order routing
- no legacy v0.5l baseline behavior

## Connected Paper Readiness Note

The benchmark has now passed a real environment-connected paper runtime/readiness pass:
- real Schwab auth/bootstrap was verified
- the ATP benchmark attached as a monitored paper runtime
- the operator dashboard showed benchmark heartbeat, paper metrics, and recent audit rows
- ATP-specific operator controls were isolated through a dedicated benchmark control file

This confirms paper-soak readiness only.

It does not authorize:
- live broker routing
- semantic changes to the benchmark
- live-readiness promotion without additional soak duration

## Benchmark Summary

Validation basis:
- replay source: `mgc_v05l.replay.sqlite3`
- same cleaned broader sample used in prior ATP validation
- `35` one-day windows

Aggregate before/after:
- before = prior ATP candidate with London `VWAP_FAVORABLE` gating
- after = current benchmark with London executable gate-off
- before: `152` trades, net P/L `699.1058`, PF `1.2645`, win rate `55.9211%`, avg trade `4.5994`, max drawdown `274.8687`, entries/100 bars `1.911`
- after: `111` trades, net P/L `804.3658`, PF `1.4369`, win rate `57.6577%`, avg trade `7.2465`, max drawdown `253.1842`, entries/100 bars `1.3955`

London before/after:
- before: `41` trades, net P/L `-105.26`, PF `0.8688`, win rate `51.2195%`, avg trade `-2.5673`, max drawdown `262.7187`, entries/100 bars `0.5155`
- after: `0` trades by design

Asia unchanged:
- `47` trades, net P/L `459.9588`, PF `1.6611`, avg trade `9.7864`

US unchanged:
- `64` trades, net P/L `344.4071`, PF `1.3007`, avg trade `5.3814`

Test status:
- focused ATP freeze-validation suite passed during promotion

## Benchmark Artifacts

Primary benchmark artifacts:
- `outputs/reports/atp_companion_baseline_v1_asia_us_benchmark/broader_sample_performance_validation.json`
- `outputs/reports/atp_companion_baseline_v1_asia_us_benchmark/cross_run_summary.json`

Companion documents:
- [Active Trend Participation Engine v1 Baseline Overlay](./ACTIVE_TREND_PARTICIPATION_ENGINE_V1_BASELINE_OVERLAY.md)
- [ATP Companion Baseline v1 Paper Tracking](./ATP_COMPANION_BASELINE_V1_PAPER_TRACKING.md)
- [ATP Companion Baseline v1 Paper Runtime Runbook](./ATP_COMPANION_BASELINE_V1_PAPER_RUNTIME_RUNBOOK.md)
- [ATP Companion Baseline v1 Paper Readiness Checklist](./ATP_COMPANION_BASELINE_V1_PAPER_READINESS_CHECKLIST.md)
- `config/atp_companion_baseline_v1_asia_us.yaml`

## Future Branching Rule

Future ATP research should branch from this benchmark rather than continuing to modify the candidate branch in place.

Recommended next research branch:
- promotion/add logic on top of `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`
