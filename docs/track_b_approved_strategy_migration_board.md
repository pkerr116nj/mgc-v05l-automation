# Track B Approved Strategy Migration Board v1

Generated: 2026-05-24

Scope: read-only inventory of the approved/registered Track B strategy set against the hardened shared-services runtime model. This board does not enable, launch, submit, cancel, close, replace, modify, flatten, run paper_proof, or touch broker/lifecycle state.

## Authority Sources

- Registry: `src/mgc_v05l/execution_core/track_b_strategy_registry.py`
- Runtime cycle: `src/mgc_v05l/execution_core/track_b_multi_strategy_runtime_cycle.py`
- Submit boundary: `src/mgc_v05l/execution_core/track_b_strategy_paper_runner.py`
- Managed lifecycle submitter: `src/mgc_v05l/execution_core/track_b_strategy_managed_paper_lifecycle.py`
- Control plane gates: Control Plane Snapshot, Safe-State Envelope, Runtime Supervisor/Resume v2, Agent Health v2, Recovery Budget Ledger, Lifecycle Matrix v2, Managed Order Registry, Order Adjustment Planner

## Summary

- Total registered candidates inventoried: 15
- P0 count: 5
- P1 count: 7
- P2 count: 1
- P3 count: 2
- Migration-required count: 8
- Runtime-not-eligible count: 2
- Fully first-batch ready count: 5

The first practical PAPER migration batch should be:

1. `asian_drift_v1`
2. `ASIA_EARLY_PAUSE_RESUME_SHORT_V1`
3. `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1`
4. `MNQ_FIRST_BEAR_SNAP_TURN_V1`
5. `MNQ_FIRST_BULL_SNAP_TURN_V1`

Ordering reason: these give the best mix of already registered strategy metadata, explicit state/feature envelopes, managed time-boxed exits, low live-money risk, and cross-symbol coverage. The MGC Asia pair exercises the original Track B migration surface; the MNQ snap-turn pair exercises the concurrent-position and index-futures path without requiring a universal flat precondition.

## Board

| Priority | Strategy | Symbol | Lane/session | Family | Classification | Missing requirements | Proof risk | Next action |
|---|---|---:|---|---|---|---|---|---|
| P0 | `asian_drift_v1` | MGC | `mgc_example_long_lmt_day` / Asia drift | Asia early / drift | `APPROVED_TRACK_B_READY` | Fresh envelope and clean Control Plane Snapshot at runtime | Low | First-batch PAPER observation candidate; use only strategy-managed lifecycle. |
| P0 | `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` | MGC | `mgc_asia_early_pause_resume_short` / Asia early | pause/resume short | `APPROVED_TRACK_B_READY` | Fresh Phase-1 envelope and clean Safe-State | Low | First-batch MGC short candidate. |
| P0 | `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1` | MGC | `mgc_asia_early_normal_breakout_retest_hold_long` / Asia early or London extension | breakout/retest long | `APPROVED_TRACK_B_READY` | Fresh Phase-1 envelope and clean Safe-State | Low | First-batch MGC long candidate. |
| P0 | `MNQ_FIRST_BEAR_SNAP_TURN_V1` | MNQ | `mnq_first_bear_snap_turn` / session allowed | snap turn short | `APPROVED_TRACK_B_READY` | Fresh MNQ snap-turn envelope and clean Control Plane Snapshot | Medium | First-batch MNQ short candidate; watch index-futures feed provenance. |
| P0 | `MNQ_FIRST_BULL_SNAP_TURN_V1` | MNQ | `mnq_first_bull_snap_turn` / session allowed | snap turn long | `APPROVED_TRACK_B_READY` | Fresh MNQ snap-turn envelope and clean Control Plane Snapshot | Medium | First-batch MNQ long candidate; pairs with MGC for concurrent-position proof. |
| P1 | `FIRST_BULL_SNAP_TURN_V1` | MGC | `mgc_first_bull_snap_turn` / session allowed | snap turn long | `APPROVED_REQUIRES_PORT` | Current runtime envelope freshness and priority/arbitration tuning | Medium | Port into the first post-batch MGC snap-turn lane. |
| P1 | `FIRST_BEAR_SNAP_TURN_V1` | MGC | `mgc_first_bear_snap_turn` / session allowed | snap turn short | `APPROVED_REQUIRES_PORT` | Current runtime envelope freshness and priority/arbitration tuning | Medium | Port with the MGC bull snap-turn pair. |
| P1 | `LONDON_LATE_PAUSE_RESUME_SHORT_V1` | MGC | `mgc_london_late_pause_resume_short` / London late | London pause/resume short | `APPROVED_REQUIRES_PORT` | London-late live envelope and session-specific observation window | Medium | Add after Asia first batch if London feed/session evidence is clean. |
| P1 | `ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1` | MGC | `mgc_asia_late_flat_pullback_pause_resume_long` / Asia late | pause/resume long | `APPROVED_REQUIRES_PORT` | Asia-late envelope freshness and arbitration priority | Medium | Add after P0 if Asia-late windows produce enough fresh observations. |
| P1 | `US_DERIVATIVE_BEAR_TURN_V1` | MGC | `mgc_us_derivative_bear_turn` / US derivative-bear window | derivative bear short | `APPROVED_REQUIRES_PORT` | US-session observation and historical/live parity check | Medium | Port after US-session feed/provenance is stable. |
| P1 | `MNQ_US_DERIVATIVE_BEAR_TURN_V1` | MNQ | `mnq_us_derivative_bear_turn` / US derivative-bear window | derivative bear short | `APPROVED_REQUIRES_PORT` | MNQ US-session feed parity and Safe-State limit tuning | Medium | Pair with MGC US derivative-bear after P0 MNQ proves stable. |
| P1 | `US_LATE_PAUSE_RESUME_LONG_V1` | MGC | `mgc_us_late_pause_resume_long` / US late | pause/resume long | `APPROVED_REQUIRES_PORT` | US-late envelope and late-session observation coverage | Medium | Add after US derivative-bear surfaces are stable. |
| P2 | `mgc_ema_momentum_reclaim_long_v1` | MGC | `mgc_example_long_lmt_day` / 1m | trend continuation long | `APPROVED_REQUIRES_EXIT_HARDENING` | Managed exit policy, lifecycle matrix evidence, runtime-cycle binding | High | Do not use for sustained PAPER submit until exit policy and cycle binding are added. |
| P3 | `track_b_demo_wiring_proof` | MGC | demo quote snapshot | demo wiring | `APPROVED_NOT_RUNTIME_ELIGIBLE` | Not a modern runtime strategy; legacy paper_proof branch is disabled | High | Keep as diagnostic/test fixture only. |
| P3 | `human_review_only` | MGC | manual review quote snapshot | human review | `APPROVED_NOT_RUNTIME_ELIGIBLE` | `paper_eligible=false`; no automated runtime path | Low | Keep as no-submit review sentinel. |

## Shared-Services Compliance Posture

All P0/P1 strategy-managed submit paths must pass through:

- one coherent Control Plane Snapshot;
- Safe-State Envelope with submit allowed and no hard hold;
- Runtime Supervisor / Runtime Resume v2 generation posture;
- strategy submit authorization packet;
- Managed Order Registry / Order Adjustment Planner for exits;
- Lifecycle Matrix v2 for lifecycle/ledger interpretation;
- Recovery Budget Ledger where recovery is involved.

Dashboard/operator projections are display-only and must not be consumed as strategy authority.

## Data Dependencies

- MGC and MNQ P0/P1 entries depend on Phase-1 realtime-derived completed 5m state/feature envelopes.
- `mgc_ema_momentum_reclaim_long_v1` depends on MGC 1m feature input and is not a first-batch sustained PAPER candidate because exit maturity is incomplete.
- Research/offline/replay artifacts may inform prioritization but are not runtime authority.
- ATP/probationary configs that declare `non_approved=true` are excluded from this approved Track B board until explicitly registered in `track_b_strategy_registry`.

## Exit Maturity

- P0/P1 strategy entries use `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1`; this is managed, time-boxed, and compatible with the hardened managed-order path, including modify-in-place and cancel/replace fallback boundaries when exact identity evidence exists.
- `mgc_ema_momentum_reclaim_long_v1` and demo/human-review entries lack a managed runtime exit policy and must not be treated as sustained PAPER submit candidates.

## Known Prior Proof/Leak History

- Legacy paper_proof is disabled for strategy runtime and is no longer a valid submit route.
- May 20 PAPER leak-test work proved important MNQ/MGC lifecycle and exit-suppression failure modes and led to managed-exit remediation; no current board entry should bypass the shared-services submit authorization packet.
- Existing parity/replay diagnostics support predicate migration confidence, but replay and research artifacts remain non-runtime authority.

## Concurrent-Position Compatibility

- No universal "must be flat before submit" rule is part of this board.
- Concurrent strategy-scoped PAPER positions are allowed when shared truth is clean, Safe-State permits submit, and identities are unambiguous.
- MGC and MNQ can be open concurrently through distinct strategy/lane identities.
- Blocking applies to stale/incoherent snapshots, unsafe Safe-State, duplicate writers, live-money eligibility, paper_proof invocation, unresolved broker/order/position identity, suspicious orders before mutation, or lifecycle/reconciliation ambiguity.

## Recommended First Batch

The recommended first batch is `asian_drift_v1`, `ASIA_EARLY_PAUSE_RESUME_SHORT_V1`, `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1`, `MNQ_FIRST_BEAR_SNAP_TURN_V1`, and `MNQ_FIRST_BULL_SNAP_TURN_V1`.

This ordering starts with the already hardened MGC Asia migration surface, then adds MNQ snap-turn coverage to validate cross-symbol concurrency. It avoids the older MGC 1m EMA candidate until managed exit hardening is complete, and avoids demo/human-review sentinels entirely.
