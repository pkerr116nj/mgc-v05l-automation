# Track B Continuation-Aware Exit Framework v1

Generated: 2026-05-24

Scope: design/specification only. This document does not implement runtime exit behavior, start or restart processes, mutate broker/order/lifecycle state, submit/cancel/close/replace/modify/flatten, invoke `paper_proof`, or create a live-money route.

## Goal

Replace primitive time-only managed exits with a strategy-family-aware framework:

1. hold for a minimum evidence window;
2. extend while continuation quality remains favorable;
3. exit on decay, reversal, stagnation, unsafe lifecycle/reconciliation posture, or Safe-State override;
4. always enforce a hard maximum hold duration.

The new policy family is `TIME_PLUS_CONTINUATION_EXIT_V1`.

## Design Doctrine

- PAPER remains bounded autonomous failure discovery, not hidden recovery.
- Exit decisions stay inside managed lifecycle and must flow through Managed Order Registry, Order Adjustment Planner, modify-in-place where eligible, and targeted cancel/replace only when the planner requires it.
- No exit policy may directly submit, cancel, replace, modify, close, or flatten from local strategy logic.
- Dashboard projections are never authority.
- Safe-State, lifecycle state, broker truth, and reconciliation can only tighten or block an exit; they cannot make suspicious state clean.
- Future LIVE mode can apply stricter ambiguity handling over the same policy inputs.

## Policy Components

`TIME_PLUS_CONTINUATION_EXIT_V1` produces a close-intent recommendation, not a broker action. It requires a lifecycle-managed open position and exact target identity before any close order can be planned.

| Component | Purpose |
|---|---|
| `minimum_hold_duration` | Earliest point at which discretionary continuation/decay exits may be evaluated. |
| `continuation_extension_window` | Number of completed 5m bars to extend while continuation quality remains confirmed. |
| `hard_max_hold_duration` | Absolute maximum hold duration; produces `EXIT_HARD_MAX_DURATION`. |
| `continuation_quality_state` | Family-specific assessment of directional participation, microtrend, pullback quality, and MFE/MAE posture. |
| `decay_exit_trigger` | Detects continuation fading after the minimum hold. |
| `reversal_exit_trigger` | Detects adverse directional confirmation. |
| `stagnation_exit_trigger` | Detects time passing without enough favorable excursion or participation. |
| `safe_state_override` | Forces observe-only/recovery-only/hard hold handling when Safe-State is unsafe. |
| `lifecycle_reconciliation_override` | Blocks discretionary close planning or forces review when lifecycle/reconciliation evidence is unsafe or ambiguous. |

## Required Inputs

- completed 5m candles for the active strategy instrument;
- directional participation and microtrend state from the strategy family envelope;
- position age by completed 5m bars and wall-clock time;
- unrealized MFE/MAE from broker-backed entry price and current quote/mark;
- current Managed Order Registry state;
- current Order Adjustment Planner recommendation for existing close orders;
- Safe-State Envelope;
- lifecycle state and Lifecycle State Matrix metadata;
- reconciliation and Broker Lease evidence;
- strategy family and strategy id;
- exact managed position identity: account, strategy, lane, contract, conId, side, quantity, lifecycle id, and runtime generation id when available.

## Exit States

| Exit state | Meaning | Broker mutation authority |
|---|---|---|
| `HOLD_MINIMUM_WINDOW` | Position is too young for discretionary exit unless Safe-State/lifecycle overrides require review. | None |
| `HOLD_CONTINUATION_CONFIRMED` | Continuation quality is favorable; extend within configured leash. | None |
| `EXIT_DECAY_DETECTED` | Continuation quality faded after minimum hold. | Close-intent planning only |
| `EXIT_REVERSAL_DETECTED` | Adverse reversal evidence appeared. | Close-intent planning only |
| `EXIT_STAGNATION` | Position failed to progress within the extension window. | Close-intent planning only |
| `EXIT_HARD_MAX_DURATION` | Hard maximum hold was reached. | Close-intent planning only |
| `EXIT_SAFE_STATE_OVERRIDE` | Safe-State blocks normal discretionary handling or requires recovery/observe-only. | None unless a separate scoped recovery path is authorized |
| `EXIT_LIFECYCLE_UNSAFE` | Lifecycle, managed registry, open-order truth, or reconciliation is ambiguous/unsafe. | None |

## Family Defaults

| Strategy family | Minimum hold | Continuation extension | Hard max | Decay sensitivity | Reversal sensitivity | Stagnation rule |
|---|---:|---:|---:|---|---|---|
| Breakout/retest | 2 completed 5m bars | up to 4 additional 5m bars | 8 completed 5m bars | Medium | Medium | Exit if retest hold fails to expand after extension window |
| Snap-turn reversal | 1 completed 5m bar | up to 2 additional 5m bars | 5 completed 5m bars | Fast | Fast | Exit if reversal impulse fails quickly |
| Asia drift | 2 completed 5m bars | up to 3 additional 5m bars | 7 completed 5m bars | Medium | Medium | Session/volatility aware exit if drift does not continue |
| Pause/resume | 2 completed 5m bars | up to 3 additional 5m bars | 6 completed 5m bars | Medium-fast | Medium-fast | Exit when continuation after resume fails |
| Trend continuation | 3 completed 5m bars | up to 5 additional 5m bars | 10 completed 5m bars | Slow | Medium | Hold extension allowed while trend participation remains strong |

Defaults are PAPER v1 starting points. They are deliberately bounded and should be tuned from artifacts, not widened silently.

## P0 Strategy Mapping

| Strategy | Current exit | Proposed profile | First adoption safety |
|---|---|---|---|
| `asian_drift_v1` | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Asia drift profile: 2-bar minimum, 3-bar extension, 7-bar hard max | Good first adoption after observe-only exit-plan dry-run because its state snapshot already carries explicit regime/direction. |
| `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Pause/resume profile: 2-bar minimum, 3-bar extension, 6-bar hard max | Good first MGC directional adoption; exit on failed continuation after resume. |
| `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1` | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Breakout/retest profile: 2-bar minimum, 4-bar extension, 8-bar hard max | Adopt after first two because longer leash needs clean continuation-quality evidence. |
| `MNQ_FIRST_BEAR_SNAP_TURN_V1` | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Snap-turn reversal profile: 1-bar minimum, 2-bar extension, 5-bar hard max | Adopt after MGC profile tests prove cross-symbol identity and quote/MFE plumbing. |
| `MNQ_FIRST_BULL_SNAP_TURN_V1` | `PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1` | Snap-turn reversal profile: 1-bar minimum, 2-bar extension, 5-bar hard max | Adopt with MNQ bear pair after direction symmetry tests. |

All five P0 strategies currently use the primitive time-boxed policy and are candidates for `TIME_PLUS_CONTINUATION_EXIT_V1` after dry-run exit planning exists. The safest first implementation slice is not broker mutation; it is a read-only exit-decision builder that emits the exit state and intended close-intent reason.

## Managed Lifecycle Integration

The future implementation should add a pure builder before close submit:

- input: open lifecycle state, completed 5m context, continuation evidence, Safe-State, managed order/open order/position truth, reconciliation, and strategy family profile;
- output: `TIME_PLUS_CONTINUATION_EXIT_V1` decision artifact with exit state, reason, target identity, and `close_intent_allowed=false` by default;
- integration point: `track_b_strategy_managed_paper_lifecycle` should ask the builder for an exit decision instead of using only elapsed completed 5m bars;
- downstream action: close submit still requires snapshot-gated managed lifecycle, Managed Order Registry, Order Adjustment Planner, and exact identity checks.

Existing modify-in-place and cancel/replace boundaries remain downstream order-management tools, not exit signal generators.

## Safety Overrides

Safe-State and lifecycle/reconciliation evidence override family logic:

- `SAFE_STATE_HARD_HOLD`, duplicate writer, or live-money eligibility blocks exit mutation and marks `EXIT_SAFE_STATE_OVERRIDE`.
- suspicious open order, duplicate close risk, or unknown managed order state marks `EXIT_LIFECYCLE_UNSAFE`.
- stale shared truth or incoherent Control Plane Snapshot blocks close-intent planning.
- broker exposure identity ambiguity blocks close-intent planning.
- lifecycle states outside the Lifecycle State Matrix block clean exit stats and require review classification.

## Tests Required Before Runtime Behavior

- minimum hold produces `HOLD_MINIMUM_WINDOW`;
- favorable continuation produces `HOLD_CONTINUATION_CONFIRMED`;
- decay produces `EXIT_DECAY_DETECTED`;
- reversal produces `EXIT_REVERSAL_DETECTED`;
- low MFE/time progression produces `EXIT_STAGNATION`;
- hard max produces `EXIT_HARD_MAX_DURATION`;
- unsafe Safe-State produces `EXIT_SAFE_STATE_OVERRIDE`;
- ambiguous lifecycle/reconciliation produces `EXIT_LIFECYCLE_UNSAFE`;
- no dashboard projection is consumed;
- no broker/order/lifecycle mutation happens in the builder.

## Recommended First Implementation Slice

Implement a pure `TIME_PLUS_CONTINUATION_EXIT_V1` decision builder and artifact writer for `asian_drift_v1` and `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` first. Both are MGC P0 candidates, both already use explicit completed-5m state envelopes, and together they exercise variable side handling without adding MNQ cross-symbol complexity.

The builder should run in dry-run/report mode only, feed the managed lifecycle report, and leave the existing time-boxed close submit behavior untouched until tests prove the decision artifact is stable.
