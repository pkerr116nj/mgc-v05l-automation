# Track B P0 Strategy Migration Batch Plan

Generated: 2026-05-24

Scope: planning-only runbook for the five approved P0 Track B PAPER strategy candidates. This plan does not enable runtime submit, start or restart any process, mutate broker/order/lifecycle state, invoke `paper_proof`, or create a live-money route.

## Control Doctrine

- The Control Plane Snapshot is the coherent pre-action evidence packet.
- Safe-State Envelope must permit submit before any guarded PAPER submit can be considered.
- Runtime Supervisor and Runtime Resume v2 must agree on runtime generation posture.
- Strategy submit authorization must bind strategy, lane, symbol, contract, action, quantity, order type, and limit price to the same snapshot/generation.
- Dashboard/operator projections are display-only and must never be consumed as authority.
- P0 migration begins in observe-only mode, then submit-disabled dry-run. Guarded PAPER submit is a later explicit enablement step.

## Batch Strategies

| Strategy | Symbol / instrument | Session / lane | Entry signal source | Required runtime market data | Exit policy | Managed lifecycle path | Expected order |
|---|---|---|---|---|---|---|---|
| `asian_drift_v1` | `MGC-202606` / `MGCM6` / conId `712565978` | Asia drift / `mgc_example_long_lmt_day` | Explicit Asian Drift 5m state snapshot; direction from `asia_drift_regime` | Bounded runtime MGC 1m candles rolled to completed 5m state, realtime quote evidence, `MGC.v.0` / `GLBX.MDP3` provenance | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | `track_b_multi_strategy_runtime_cycle` -> `track_b_strategy_paper_runner` -> `track_b_strategy_managed_paper_lifecycle` | LIMIT, qty `1`, `BUY` for LONG or `SELL` for SHORT |
| `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` | `MGC-202606` / `MGCM6` / conId `712565978` | Asia early / `mgc_asia_early_pause_resume_short` | Completed 5m Asia early pause/resume short feature/state envelope | Phase-1 realtime completed 5m MGC envelope plus realtime quote evidence | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Same strategy-managed lifecycle path | LIMIT, qty `1`, `SELL` |
| `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1` | `MGC-202606` / `MGCM6` / conId `712565978` | Asia early or GC/MGC London-open extension / `mgc_asia_early_normal_breakout_retest_hold_long` | Completed 5m breakout/retest/hold feature/state envelope | Phase-1 realtime completed 5m MGC envelope plus realtime quote evidence | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Same strategy-managed lifecycle path | LIMIT, qty `1`, `BUY` |
| `MNQ_FIRST_BEAR_SNAP_TURN_V1` | `MNQ-202606` / `MNQM6` / conId `770561201` | session allowed / `mnq_first_bear_snap_turn` | Completed 5m first bear snap-turn feature/state envelope | Phase-1 realtime completed 5m MNQ envelope, realtime quote evidence, `MNQ.v.0` / `GLBX.MDP3` provenance | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Same strategy-managed lifecycle path | LIMIT, qty `1`, `SELL` |
| `MNQ_FIRST_BULL_SNAP_TURN_V1` | `MNQ-202606` / `MNQM6` / conId `770561201` | session allowed / `mnq_first_bull_snap_turn` | Completed 5m first bull snap-turn feature/state envelope | Phase-1 realtime completed 5m MNQ envelope, realtime quote evidence, `MNQ.v.0` / `GLBX.MDP3` provenance | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Same strategy-managed lifecycle path | LIMIT, qty `1`, `BUY` |

## Per-Strategy Readiness Details

### `asian_drift_v1`

- Safe-State limits: max one managed open position per strategy/lane, no duplicate intent, no tripped broker mutation limits, no lifecycle/reconciliation disagreement limit, no duplicate writer.
- Concurrent-position compatibility: compatible with other strategy-scoped MGC/MNQ positions when shared truth is clean and identities are unambiguous; no universal flat rule.
- Known proof/leak-test history: Asian Drift has a hardened watch-chain/state-snapshot path; legacy `paper_proof` is no longer an acceptable submit route.
- Required artifacts: Control Plane Snapshot, Safe-State Envelope, Runtime Supervisor, Runtime Resume v2, Agent Health v2, Open Order Truth, Managed Order Registry, Position Truth, Managed Position Registry, Reconciliation/Broker Lease, Asian Drift state snapshot, strategy rule report, strategy submit authorization packet.
- Missing implementation gaps: first-batch runtime wiring should prove fresh bounded runtime candle context and quote evidence without relying on maintained historical data.
- Tests required before enablement: observe-only no-signal, observe-only signal-ready-no-submit, stale runtime candle context blocks, side matches explicit direction, submit authorization blocks without snapshot, lifecycle path receives exact strategy/lane identity.
- First PAPER proving mode: observe-only, then submit-disabled dry-run. Guarded PAPER submit only after the enablement gates below are true.

### `ASIA_EARLY_PAUSE_RESUME_SHORT_V1`

- Safe-State limits: same P0 envelope, plus duplicate short intent must remain zero before submit.
- Concurrent-position compatibility: compatible with other strategy-scoped positions if open-order/position truth remains exact and non-ambiguous.
- Known proof/leak-test history: registered as a first post-guardrail Asia short candidate; no direct adapter submit path.
- Required artifacts: Control Plane Snapshot, Safe-State Envelope, Runtime Supervisor, Runtime Resume v2, Agent Health v2, Open Order Truth, Managed Order Registry, Position Truth, Managed Position Registry, Reconciliation/Broker Lease, session strategy envelope, strategy rule report, strategy submit authorization packet.
- Missing implementation gaps: verify current live envelope production and no-submit reporting for the exact Asia early short state fields.
- Tests required before enablement: missing feature fields produce NOT_READY, no-signal state produces NO_SIGNAL_NO_MUTATION, signal state without submit flags stays SIGNAL_READY_NO_SUBMIT, side is forced/matched to `SELL`, authorization blocks with unsafe Safe-State.
- First PAPER proving mode: observe-only.

### `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1`

- Safe-State limits: same P0 envelope, plus no competing unambiguous managed long intent for the same strategy/lane/contract.
- Concurrent-position compatibility: compatible with MGC/MNQ strategy-scoped positions when lifecycle and managed position registry stay clean.
- Known proof/leak-test history: registered as the paired first-batch MGC long candidate; submit must use shared-services authorization.
- Required artifacts: Control Plane Snapshot, Safe-State Envelope, Runtime Supervisor, Runtime Resume v2, Agent Health v2, Open Order Truth, Managed Order Registry, Position Truth, Managed Position Registry, Reconciliation/Broker Lease, breakout/retest/hold session envelope, strategy rule report, strategy submit authorization packet.
- Missing implementation gaps: verify London-open extension data freshness and arbitration interaction with the Asia short candidate.
- Tests required before enablement: missing feature fields produce NOT_READY, no-signal produces NO_SIGNAL_NO_MUTATION, signal without submit flags stays SIGNAL_READY_NO_SUBMIT, side is `BUY`, arbitration blocks multiple PAPER candidates.
- First PAPER proving mode: observe-only.

### `MNQ_FIRST_BEAR_SNAP_TURN_V1`

- Safe-State limits: same P0 envelope, with MNQ-specific contract identity and cross-symbol mutation counters visible in Safe-State.
- Concurrent-position compatibility: compatible with concurrent MGC positions when MNQ order/position identity is scoped and Safe-State permits submit.
- Known proof/leak-test history: prior MNQ leak-test exposed managed-exit and lifecycle edges; current path must preserve exact identity and managed lifecycle evidence.
- Required artifacts: Control Plane Snapshot, Safe-State Envelope, Runtime Supervisor, Runtime Resume v2, Agent Health v2, Open Order Truth, Managed Order Registry, Position Truth, Managed Position Registry, Reconciliation/Broker Lease, MNQ snap-turn envelope, strategy rule report, strategy submit authorization packet.
- Missing implementation gaps: prove fresh MNQ Phase-1 realtime envelope and quote provenance in the sustained runtime stack.
- Tests required before enablement: MNQ contract allowlist identity, no-signal/no-mutation, signal-ready-no-submit, `SELL` side match, concurrent MGC+MNQ scoped-position fixture, submit authorization blocks if MNQ truth is stale or ambiguous.
- First PAPER proving mode: submit-disabled dry-run after MGC Asia observe-only is clean.

### `MNQ_FIRST_BULL_SNAP_TURN_V1`

- Safe-State limits: same P0 envelope, with MNQ-specific contract identity and cross-symbol mutation counters visible in Safe-State.
- Concurrent-position compatibility: compatible with concurrent MGC positions when MNQ order/position identity is scoped and Safe-State permits submit.
- Known proof/leak-test history: pairs with the MNQ bear candidate to prove direction symmetry under the hardened lifecycle path.
- Required artifacts: Control Plane Snapshot, Safe-State Envelope, Runtime Supervisor, Runtime Resume v2, Agent Health v2, Open Order Truth, Managed Order Registry, Position Truth, Managed Position Registry, Reconciliation/Broker Lease, MNQ snap-turn envelope, strategy rule report, strategy submit authorization packet.
- Missing implementation gaps: prove fresh MNQ Phase-1 realtime envelope and quote provenance in the sustained runtime stack.
- Tests required before enablement: MNQ contract allowlist identity, no-signal/no-mutation, signal-ready-no-submit, `BUY` side match, concurrent MGC+MNQ scoped-position fixture, submit authorization blocks if MNQ truth is stale or ambiguous.
- First PAPER proving mode: submit-disabled dry-run after MGC Asia observe-only is clean.

## Recommended Migration Order

1. `asian_drift_v1`
2. `ASIA_EARLY_PAUSE_RESUME_SHORT_V1`
3. `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1`
4. `MNQ_FIRST_BEAR_SNAP_TURN_V1`
5. `MNQ_FIRST_BULL_SNAP_TURN_V1`

This starts with the MGC Asia surface, then adds the paired MGC long/short candidates, then exercises MNQ cross-symbol behavior. MNQ is deliberately after MGC because it validates more moving pieces: second instrument, second exchange routing metadata, and concurrent-position evidence.

## Exact Enablement Gates

Before any P0 strategy may move beyond observe-only:

- Control Plane Snapshot exists, is fresh, and has `shared_truth_coherence_status=COHERENT`.
- Safe-State Envelope is `SAFE_STATE_NORMAL`, `submit_allowed=true`, `broker_mutation_allowed=true`, `observe_only=false`, and `tripped_limits=[]`.
- Runtime Supervisor is `SUPERVISOR_RUNTIME_ALREADY_HEALTHY` or an equivalent submit-capable runtime posture.
- Runtime Resume v2 generation matches the active runtime generation and does not require generation reuse from stale state.
- Agent Health v2 has no duplicate writer and no blocker for runtime submit.
- Open Order Truth, Managed Order Registry, Position Truth, Managed Position Registry, Reconciliation, and Broker Lease are fresh and clean for the exact strategy/lane/contract target.
- Strategy submit authorization packet is `STRATEGY_SUBMIT_AUTHORIZED` for the exact target identity.
- Quantity is exactly `1`; order type is LIMIT; side matches the strategy signal; manual limit prices are explicit.
- `live_money_eligible=false`, `paper_proof_invoked=false`, dashboard projection authority is false.

## Before First Guarded PAPER Submit

The first guarded PAPER submit is not part of this plan. Before it is separately enabled, the batch must have:

- at least one clean observe-only cycle per MGC P0 strategy;
- at least one clean submit-disabled dry-run with a real signal-ready-no-submit report for the candidate being enabled;
- passing tests for authorization blocking, Safe-State blocking, stale snapshot blocking, target mismatch blocking, and concurrent MGC/MNQ strategy-scoped positions;
- a fresh Control Plane Snapshot and Safe-State artifact captured immediately before the guarded submit attempt;
- explicit operator command invoking the guarded strategy-managed lifecycle path, not `paper_proof` or any direct bridge/manual submit harness.

## Rollback / Observe-Only Behavior

- If any gate is missing, stale, incoherent, or ambiguous, keep the strategy in observe-only mode.
- If multiple P0 candidates emit real signals in the same cycle, arbitration blocks submit and preserves signal evidence.
- If Safe-State trips duplicate intent, broker mutation, lifecycle disagreement, or position limits, shift to observe-only or recovery-only according to Safe-State.
- If MNQ feed provenance is stale, MNQ candidates remain disabled while MGC observe-only can continue if its own truth remains clean.
- Rollback never performs broad cancel, broad flatten, runtime restart, `paper_proof`, or live-money routing.
