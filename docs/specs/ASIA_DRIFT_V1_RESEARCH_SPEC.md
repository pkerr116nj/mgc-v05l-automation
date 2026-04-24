# Asia Drift v1 Research Spec

Status: design/specification only.

Scope guardrails:
- Keep the current validation-layer architecture intact.
- Do not start by building a live strategy.
- Do not widen broker, runtime, or operator-control scope in this pass.
- Do not use opaque ML-first discovery as the primary engine.
- Treat this as a bounded replay-first research engine that may later earn paper/demo consideration only through the existing validation machine.

## Repo Alignment

Asia Drift v1 should fit the current repo in the same spirit as the ATP research stack:
- explicit feature rows
- explicit state classification
- completed-bar state determination
- optional lower-timeframe timing only after state is stable
- replay-first backtests with auditable state transitions
- Layer 1 provenance before Layer 2 validation

It should not overwrite existing global session logic. In particular:
- preserve `src/mgc_v05l/market_data/session_clock.py` as the coarse runtime session classifier
- preserve `src/validation_layer/...` orchestration and scoring semantics
- add a research-only derived session scope for Asia Drift rather than changing benchmark/global session labels

## 1. Strategy Definition

### Plain-English definition

Asia Drift v1 studies whether some overnight sessions develop a clean one-direction drift from the New York evening reset, then offer a measured pullback entry before the London handoff.

The engine only participates when three things are all true:
- the session is mathematically classified as directional rather than choppy or unstable
- the direction has enough persistence to justify a continuation thesis
- the pullback is controlled enough to look like a reset inside drift rather than a structural reversal

### Operational definition of "Asia drift"

For direction `d in {+1, -1}` where `+1 = long` and `-1 = short`, define:

- `O_s`: session-open price at 18:00 ET
- `C_t`: close of the latest completed decision bar
- `VWAP_t`: session VWAP from 18:00 ET through bar `t`
- `ATR_t`: rolling decision-timeframe ATR

Signed session displacement:

`disp_t(d) = d * (C_t - O_s) / max(ATR_t, eps)`

Signed VWAP displacement:

`vwap_disp_t(d) = d * (C_t - VWAP_t) / max(ATR_t, eps)`

Drift exists only when:
- `disp_t(d)` is materially positive
- the majority of directional structure features agree with `d`
- chop and instability vetoes are not active

### When the strategy says drift exists

The session is considered a drift candidate in direction `d` when all of the following hold after warmup:

- `disp_t(d) >= 1.0`
- `vwap_disp_t(d) >= 0.15`
- directional efficiency ratio `ER_12 >= 0.38`
- multi-window slope consensus is aligned with `d`
- reversal frequency is low enough to avoid chop
- overlap ratio is low enough to avoid two-sided rotation
- the directional drift score is at least `MEDIUM`

### When the session is disqualified as chop/no-trade

The session is `NO_TRADE` if any of the following hold:

- absolute session displacement is too small: `|disp_t| < 0.6`
- efficiency is weak: `ER_12 < 0.30`
- bar overlap ratio is high: `overlap_8 > 0.65`
- reversal frequency is high: `reversal_freq_12 > 0.45`
- both long and short drift scores are weak or too close together
- a post-spike instability veto is active

### What kind of pullback is entry-worthy

An entry-worthy pullback must:
- retrace enough to reset price quality
- preserve the session drift structure
- occur with controlled speed and controlled range expansion
- avoid decisive failure through VWAP plus slow-trend references

Normal and stretched-but-valid pullbacks are eligible. Disqualifying pullbacks are not.

### What ends the thesis

The continuation thesis ends when one of the following occurs:
- protected pullback structure breaks
- the drift score falls below tradable strength and stays degraded
- price reclaims VWAP against the position with confirming weakness
- a session timeout or London-handoff timeout is reached
- a post-spike instability condition emerges after entry

## 2. Session Scope

### Core rule

Asia Drift v1 should use a research-only session scope without modifying global runtime session labels.

### Session boundaries

- Session open anchor: `18:00 ET`
- Research session end / mandatory handoff exit: `02:55 ET`
- No new entries after: `01:30 ET`
- No entries during the first `40` minutes after the anchor

### Why this scope

The repo's coarse runtime labels currently treat Asia as `18:00-23:00 ET` and London as `03:00-08:30 ET`. Asia Drift v1 should not change that shared infrastructure. Instead it should derive:

- `asia_drift_session_id`: New York session date anchored at `18:00 ET`
- `asia_drift_subphase`

Proposed subphases:
- `ASIA_DRIFT_BUILD`: `18:00-20:29 ET`
- `ASIA_DRIFT_MATURE`: `20:30-00:29 ET`
- `ASIA_DRIFT_PRE_HANDOFF`: `00:30-02:55 ET`

### Asia-early vs Asia-late handling

They should be treated separately.

- `ASIA_DRIFT_BUILD` is primarily for drift discovery and regime classification.
- `ASIA_DRIFT_MATURE` is the preferred pullback-entry zone.
- `ASIA_DRIFT_PRE_HANDOFF` allows continuation only if drift remains intact; thresholds should be slightly stricter because time-to-handoff is shorter and contamination risk rises.

### Mandatory exit / time-stop

All open trades are flattened by `02:55 ET`, regardless of score, to avoid London-open contamination in v1.

## 3. State Model

Asia Drift v1 should be implemented as an explicit state machine with measurable transitions.

### States

- `NO_TRADE`
- `DRIFT_LONG_CANDIDATE`
- `DRIFT_SHORT_CANDIDATE`
- `PULLBACK_PENDING`
- `ENTRY_ARMED`
- `IN_TRADE`
- `THESIS_INVALIDATED`
- `SESSION_TIMEOUT`

### State variables

At each completed 5m decision bar, maintain at least:

- `session_id`
- `subphase`
- `directional_regime`
- `drift_score_long`
- `drift_score_short`
- `drift_strength_bucket`
- `pullback_state`
- `protected_swing_price`
- `current_drift_leg_start`
- `current_drift_leg_extreme`
- `entry_zone_low`
- `entry_zone_high`
- `armed_since_ts`
- `invalidated_reason`
- `trade_count_this_session`

### Transition rules

`NO_TRADE -> DRIFT_LONG_CANDIDATE`
- regime filter returns `ASIA_DRIFT_LONG`
- `trade_count_this_session == 0`
- current time before `01:30 ET`

`NO_TRADE -> DRIFT_SHORT_CANDIDATE`
- same logic, mirrored for short

`DRIFT_*_CANDIDATE -> PULLBACK_PENDING`
- drift score remains tradable
- no qualifying pullback yet

`DRIFT_*_CANDIDATE -> ENTRY_ARMED`
- pullback classifier returns `NORMAL_PULLBACK` or `STRETCHED_BUT_VALID`
- entry zone is non-empty
- no invalidation trigger is active

`PULLBACK_PENDING -> ENTRY_ARMED`
- same as above

`ENTRY_ARMED -> IN_TRADE`
- preferred limit order fills inside the defined zone
- or secondary confirmation entry triggers within its allowed chase cap

`ENTRY_ARMED -> THESIS_INVALIDATED`
- pullback becomes disqualifying
- drift score falls below tradable threshold
- structure breaks before fill

`ENTRY_ARMED -> NO_TRADE`
- setup expires without fill and drift regime disappears

`IN_TRADE -> THESIS_INVALIDATED`
- stop or invalidation exit trigger fires

`IN_TRADE -> SESSION_TIMEOUT`
- current time reaches `02:55 ET`

`THESIS_INVALIDATED -> NO_TRADE`
- only after a new session begins

`NO_TRADE/DRIFT_*/PULLBACK_PENDING/ENTRY_ARMED/IN_TRADE -> SESSION_TIMEOUT`
- current time reaches `02:55 ET`

v1 recommendation:
- allow at most one trade per session
- once `THESIS_INVALIDATED`, do not recycle the session

## 4. Mathematical Feature Set

Decision timeframe recommendation:
- state on `5m`
- optional timing on `1m`

All directional features should be computed for both long and short symmetry where applicable.

### Net displacement from session open

Definition:
- `net_disp_t = (C_t - O_s) / ATR_t`

Measures:
- how far the session has actually traveled from its anchor

Use:
- session filter
- drift score
- invalidation degradation

### Signed VWAP displacement

Definition:
- `signed_vwap_disp_t(d) = d * (C_t - VWAP_t) / ATR_t`

Measures:
- whether price is holding on the favorable side of session value

Use:
- session filter
- drift score
- entry timing
- invalidation logic

### Multi-window slope

Definition:
- regression or simple close-to-close slope over windows `3`, `6`, and `12` decision bars
- `slope_w(d) = d * (C_t - C_{t-w}) / (w * ATR_t)`
- aggregate slope:
  `slope_combo(d) = 0.5 * slope_3 + 0.3 * slope_6 + 0.2 * slope_12`

Measures:
- short, medium, and mature directional persistence

Use:
- drift score
- degradation logic

### Directional efficiency ratio

Definition:
- `ER_n = abs(C_t - C_{t-n}) / sum_{i=t-n+1..t} abs(C_i - C_{i-1})`

Measures:
- whether movement is travel or noise

Use:
- session filter
- drift score

### Close-location persistence

Definition:
- bar close location:
  `CL_i = (C_i - L_i) / max(H_i - L_i, eps)`
- long persistence over `n` bars:
  `CLP_n(long) = mean(1[CL_i >= 0.60])`
- short persistence:
  `CLP_n(short) = mean(1[CL_i <= 0.40])`

Measures:
- whether bars keep finishing in the drift direction

Use:
- drift score
- degradation logic

### Bar overlap / chop ratio

Definition:
- for consecutive bars:
  `overlap_i = max(0, min(H_i, H_{i-1}) - max(L_i, L_{i-1})) / max(max(H_i-L_i, H_{i-1}-L_{i-1}), eps)`
- `overlap_n = mean(overlap_i)`

Measures:
- two-sided auction behavior rather than drift

Use:
- session filter
- drift score

### Drift persistence vs reversal frequency

Definition:
- let `sgn_i = sign(C_i - C_{i-1})`
- reversal count over `n` bars:
  `rev_n = count(sgn_i != 0 and sgn_i = -sgn_{i-1})`
- `reversal_freq_n = rev_n / max(n-1, 1)`

Measures:
- how often the session flips direction

Use:
- session filter
- drift score
- invalidation logic

### Local realized volatility / ATR

Definition:
- rolling ATR on the decision timeframe
- realized variance or realized absolute return over the last `6` to `12` bars
- `rv_ratio = local_realized_vol / median_realized_vol_lookback`

Measures:
- whether current movement is ordinary, compressed, or unstable

Use:
- session filter
- pullback severity
- post-spike instability veto

### Pullback depth as fraction of current drift leg

Definition for long:
- `leg_size = current_leg_extreme - current_leg_start`
- `pb_depth = current_leg_extreme - C_t`
- `pb_depth_frac = pb_depth / max(leg_size, eps)`

Short is symmetric.

Measures:
- how much of the drift leg is being given back

Use:
- pullback score
- entry model
- invalidation logic

### Pullback speed

Definition:
- `pb_speed = (pullback_depth / ATR_t) / max(pullback_duration_bars, 1)`

Measures:
- whether the countertrend move is controlled or violent

Use:
- pullback score
- invalidation logic

### Pullback relation to VWAP / fast EMA / slow EMA / envelope

Definition:
- `dist_fast = d * (C_t - EMA_fast_t) / ATR_t`
- `dist_slow = d * (C_t - EMA_slow_t) / ATR_t`
- `dist_vwap = d * (C_t - VWAP_t) / ATR_t`
- retracement band anchors from the current leg:
  `r38`, `r50`, `r62`

Measures:
- whether the pullback is a routine mean reversion to support/resistance or a structural break

Use:
- pullback score
- entry timing
- invalidation logic

### Compression vs violent countertrend expansion

Definition:
- pullback expansion ratio:
  `pb_expansion = mean(range_i during pullback) / median(range_j over prior 12 bars)`
- violent adverse bar flag if `max(range_i during pullback) / ATR_t > 1.5`

Measures:
- whether the pullback is orderly compression or aggressive reversal energy

Use:
- pullback score
- disqualification logic

## 5. Regime Filter

The session-level regime filter should output exactly one of:
- `NO_TRADE`
- `ASIA_DRIFT_LONG`
- `ASIA_DRIFT_SHORT`

### Filter structure

Step 1: warmup
- require at least `8` completed 5m bars from `18:00 ET`

Step 2: hard vetoes
- veto if post-spike instability is active
- veto if chop metrics are above their ceilings

Step 3: directional scoring
- compute `drift_score_long` and `drift_score_short`

Step 4: classification
- `ASIA_DRIFT_LONG` if:
  - `drift_score_long >= 3.1`
  - `drift_score_long - drift_score_short >= 0.75`
  - no hard veto
- `ASIA_DRIFT_SHORT` mirrors the above
- else `NO_TRADE`

### Separation logic

Real drift:
- meaningful signed displacement
- strong efficiency
- directional close persistence
- low overlap
- low reversal frequency

Random walk:
- low displacement
- low efficiency
- no clear score separation

Two-sided chop:
- high overlap
- high reversal frequency
- small VWAP displacement despite movement

Post-spike instability:
- extreme one-bar or two-bar session expansion
- elevated realized-volatility ratio
- weak follow-through efficiency afterward

## 6. Drift Strength Model

### Score form

The drift score should be additive, monotone, interpretable, and bounded.

For direction `d`:

- `x1 = clip(disp_t(d) / 2.0, 0, 1.5)`
- `x2 = clip(vwap_disp_t(d) / 1.0, 0, 1.0)`
- `x3 = clip(slope_combo(d) / 0.35, 0, 1.25)`
- `x4 = clip((ER_12 - 0.30) / 0.40, 0, 1.0)`
- `x5 = clip((CLP_8(d) - 0.50) / 0.40, 0, 1.0)`
- `x6 = clip((0.60 - overlap_8) / 0.35, 0, 1.0)`
- `x7 = clip((0.55 - reversal_freq_12) / 0.35, 0, 1.0)`

Penalty terms:
- `p1 = 0.6` if extension from fast EMA exceeds `2.2 ATR`
- `p2 = 0.8` if post-spike instability flag is active
- `p3 = 0.4` if realized-volatility ratio surges without stronger displacement
- `p4 = 0.5` if recent countertrend closes are too frequent or too large

Directional drift score:

`drift_score_d = 1.25*x1 + 1.00*x2 + 1.00*x3 + 0.75*x4 + 0.50*x5 + 0.50*x6 + 0.50*x7 - (p1 + p2 + p3 + p4)`

### Buckets

- `WEAK`: `2.4 <= score < 3.1`
- `MEDIUM`: `3.1 <= score < 3.9`
- `STRONG`: `score >= 3.9`

Tradable recommendation:
- only `MEDIUM` and `STRONG`

### Fixed vs tunable

Likely fixed in v1:
- feature families
- score algebra
- monotone direction of each component
- penalty structure
- bucket semantics

Tunable within narrow research ranges:
- bucket cutoffs
- warmup length
- penalty thresholds
- clip ceilings

### What degrades score even if price still trends

- excessive extension from fast EMA or VWAP
- late-session decay in efficiency
- rising overlap despite continued displacement
- rising reversal frequency
- increasing countertrend bar quality
- volatility surge that looks more like instability than drift

## 7. Pullback-Quality Classifier

Outputs:
- `NO_PULLBACK`
- `NORMAL_PULLBACK`
- `STRETCHED_BUT_VALID`
- `DISQUALIFYING_PULLBACK`

### Depth

For the active drift leg:

- `pb_depth_frac = pullback_depth / leg_size`
- `pb_depth_atr = pullback_depth / ATR_t`

Depth bands:
- `NO_PULLBACK` if `pb_depth_frac < 0.18` or `pb_depth_atr < 0.30`
- `NORMAL_PULLBACK` if `0.18 <= pb_depth_frac <= 0.45` and `pb_depth_atr <= 0.90`
- `STRETCHED_BUT_VALID` if `0.45 < pb_depth_frac <= 0.62` or `0.90 < pb_depth_atr <= 1.25`
- `DISQUALIFYING_PULLBACK` if `pb_depth_frac > 0.62` or `pb_depth_atr > 1.25`

### Duration

- normal: `1-5` decision bars
- stretched but valid: `2-7` bars
- disqualifying by decay: `>7` bars unless drift score remains `STRONG`

### Speed

- `pb_speed = pb_depth_atr / pullback_duration_bars`
- normal if `pb_speed <= 0.35`
- stretched but valid if `0.35 < pb_speed <= 0.50`
- disqualifying if `pb_speed > 0.55`

### Volatility-normalized severity

- `pb_expansion = mean(pullback_bar_range) / median(prior_12_bar_range)`
- normal if `pb_expansion <= 1.10`
- stretched but valid if `1.10 < pb_expansion <= 1.35`
- disqualifying if `pb_expansion > 1.35`

### VWAP interaction

Long case:
- normal: pullback remains above VWAP or only tags it intrabar
- stretched but valid: at most one completed close below VWAP, provided slow EMA and protected swing still hold
- disqualifying: two consecutive closes below VWAP, or one close below VWAP plus one close below slow EMA

Short case is symmetric.

### Structure preservation vs structural break

Structure is preserved when all are true:
- protected swing is intact
- slow EMA slope still favors the original direction
- opposite-side drift score does not overtake by more than `0.50`

Structural break occurs when any of the following happen:
- protected swing break
- close through slow EMA with confirming VWAP failure
- opposite directional score overtakes and reaches tradable strength
- pullback becomes both deep and fast

### Too-extended case

`NO_PULLBACK` should also carry a reason code:
- `too_shallow`
- `too_extended_without_reset`

The second reason blocks chasing after drift has already become extended but never reset.

## 8. Entry Model

Asia Drift v1 should define two bounded entry models.

### Entry Model A: Preferred pullback limit entry

Prerequisites:
- regime is `ASIA_DRIFT_LONG` or `ASIA_DRIFT_SHORT`
- drift bucket is `MEDIUM` or `STRONG`
- pullback classifier is `NORMAL_PULLBACK` or `STRETCHED_BUT_VALID`
- current time is before `01:30 ET`
- `trade_count_this_session == 0`

Price-zone logic for long:
- define retracement anchors from the active drift leg:
  - `r38 = leg_extreme - 0.382 * leg_size`
  - `r50 = leg_extreme - 0.500 * leg_size`
  - `r62 = leg_extreme - 0.618 * leg_size`
- define reference band:
  - `[min(VWAP_t, EMA_fast_t) - 0.10*ATR_t, max(VWAP_t, EMA_fast_t) + 0.10*ATR_t]`
- core favorable zone:
  - intersection of `[r38, r62]` and the reference band
- limit price:
  - midpoint of the intersection, rounded to tick

Short is symmetric.

Timing logic:
- arm on the first completed 5m bar that qualifies
- order remains live for at most `3` decision bars or `15` one-minute bars

Cancellation logic:
- pullback becomes disqualifying
- drift score falls below `MEDIUM`
- zone is broken through by more than `0.10 ATR`
- latest entry time passes
- price re-breaks the session extreme by more than `0.15 ATR` before fill

When chasing is forbidden:
- no entry above the session extreme after an unfilled long arm
- no entry below the session extreme after an unfilled short arm
- no fill conversion to market if price has moved more than `0.20 ATR` away from the limit anchor

### Entry Model B: Secondary confirmation entry

Use only if Model A did not fill and the setup remains intact.

Prerequisites:
- setup was armed earlier
- pullback remains valid
- drift score remains `MEDIUM` or `STRONG`

Price logic for long:
- enter on break above the pullback pivot high
- require the confirming bar to close in the top `35%` of its range
- require signed VWAP displacement to improve versus the prior bar
- cap entry price at `pivot_high + 0.08*ATR_t`

Short is symmetric.

Cancellation logic:
- same invalidation rules as Model A
- cancel if confirmation has not occurred within `2` decision bars after the limit-entry expiry

## 9. Exit Model

Exit logic should be ranked and explicit.

### Priority 1: Thesis invalidation exit

Immediate exit if any hold:
- protected swing breaks
- adverse move reaches hard stop
- opposite drift score reaches `MEDIUM` and exceeds the active-direction score by `0.75`
- disqualifying pullback occurs after entry

### Priority 2: VWAP-based failure logic

VWAP reclaim against the position should be conditional, not unconditional.

Recommendation:
- single close through VWAP against the position = `warning_state`
- immediate exit if either:
  - two consecutive closes through VWAP, or
  - one close through VWAP plus one close through slow EMA, or
  - one close through VWAP after `MFE >= 1.0R` and open-profit giveback exceeds `40%`

### Priority 3: Profit-protection / giveback logic

After the trade reaches `MFE >= 1.0R`:
- activate giveback protection
- exit if open-profit giveback exceeds `45%` of peak MFE
- once `MFE >= 0.8R`, do not allow the trade to turn into a full-risk loser; minimum stop floor should tighten to approximately breakeven plus fees/slippage allowance

### Priority 4: Profit-taking logic

For bounded v1 research, use a hard cap:
- full exit at `2.0R`

This keeps tail dependence from masking poor invalidation logic.

### Priority 5: Session / time exits

- flatten by `02:55 ET`
- if a trade remains open more than `12` decision bars after entry, exit at market on the next bar close even before `02:55 ET`

## 10. Risk Model

### Stop placement

For long:
- stop below the lowest of:
  - pullback low
  - protected swing low
  - lower edge of the entry zone
- add a buffer of `0.08 ATR`

Short is symmetric.

Recommended clipping:
- minimum initial risk: `0.45 ATR`
- maximum initial risk: `1.25 ATR`

### Max holding time

- `12` decision bars after entry
- absolute session timeout at `02:55 ET`

### Trade frequency

v1 recommendation:
- one trade per session
- no same-session re-entry

Reason:
- re-entry materially raises optimization surface size before the base thesis is proven

### Position sizing philosophy for research

Use fixed-risk normalization, not PnL maximization.

Research sizing options:
- `1R` normalized synthetic sizing in replay
- fixed one-contract-equivalent sizing for instrument-specific economics

Do not optimize size in v1.

### Hard kill-switches

Disable new trades for the session if:
- post-spike instability veto triggers
- data-quality/provenance checks fail
- session reaches `01:30 ET` without a filled entry
- thesis invalidates once after arming

## 11. Research Plan

### Target instruments

Primary:
- `MGC`

Confirmation / transfer:
- `GC`

Later transfer only if the base idea survives:
- `MES`
- `MNQ`

### Data requirements

- 1m OHLCV with complete overnight coverage
- 5m bars resampled or loaded directly for decision-state computation
- session VWAP inputs
- explicit provenance fields for source, data version, and artifact lineage

### Session segmentation

Segment by `asia_drift_session_id` anchored at `18:00 ET`.

Retain both:
- global coarse session labels already used elsewhere in the repo
- Asia Drift specific derived scope and subphase labels

### In-sample / out-of-sample discipline

Recommended first pass:
- contiguous early in-sample block: `60%`
- contiguous validation / model-selection block: `20%`
- untouched holdout block: `20%`

Then require walk-forward over contiguous windows rather than a single split.

### Optimization boundaries

Keep locked in v1:
- feature families
- state names
- score structure
- pullback state taxonomy
- one-trade-per-session rule
- session anchor and hard handoff exit

Allow tuning only within bounded ranges:
- drift-score cutoffs
- pullback depth and speed thresholds
- warmup length
- entry expiry length
- giveback percentage

Do not optimize:
- unlimited feature combinations
- unrestricted session windows
- re-entry trees
- large indicator menus

### What counts as survivability

Evidence threshold for "survivability" should include all of:
- positive expectancy after realistic costs
- acceptable walk-forward and cross-validation scores in the existing validation machine
- no single month or small cluster dominating the edge
- reasonable trade count across multiple non-adjacent periods
- positive or at least non-catastrophic behavior in both MGC and GC confirmation lanes
- robustness that survives entry-delay, cost, and boundary perturbations

Survivability is not profitability proof. It is admission to a later, still-constrained implementation phase.

## 12. Validation Plan

### Layer 1: provenance / producer contract

Before Layer 2, the replay producer for Asia Drift v1 must emit:
- strategy id and variant id
- formula/version identifiers for drift score and pullback classifier
- decision timeframe and execution timeframe
- session anchor rules and subphase labels
- raw decision timestamps
- state-transition log
- per-trade setup family, setup variant, decision time, fill policy, fees, slippage
- artifact references for feature exports and replay reports

This maps cleanly onto the current Layer 1 prerequisite contract in `src/validation_layer/layer1/prerequisites.py`.

### Layer 2: core validation

Asia Drift replay backtests should then flow through the current validation pipeline:
- `canonical_performance`
- `drawdown`
- `trade_normalization`
- `walkforward`
- `cross_validation`
- `split_comparison`

The strategy should be rejected or held as insufficient evidence if Layer 1 metadata is incomplete.

### Optimization skepticism

Any threshold sweep must be evaluated with:
- `parameter_surface`
- `train_bias`
- `selection_bias`
- `cscv_pbo`

Interpretation rule:
- if the candidate only works at isolated parameter points, it is not a real gray-box edge yet

### Phase 4 robustness

After a bounded candidate is selected, require robustness work including:
- bootstrap
- monte carlo
- market permutation
- top-trade removal checks
- leave-one-subphase-out checks
- leave-one-month-out checks
- entry-delay perturbation: `+/- 1` decision bar
- session-boundary perturbation: `+/- 15` minutes
- cost inflation beyond baseline assumptions

The current validation-layer robustness modules already cover part of this. Asia Drift should add the session-specific perturbations as strategy-level diagnostics, not as a rewrite of validation-layer architecture.

### Author-intent fidelity modules

Because this is a gray-box strategy, fidelity checks matter.

Add author-intent fidelity artifacts that verify:
- state names and transition semantics match this spec
- pullback classification is stable on curated fixture sessions
- drift score components are logged and reconstructible from exported features
- implementation does not silently replace explicit rules with opaque learned behavior

These can live as strategy-level fixture tests and spec snapshots even if they are not first-class validation-layer modules yet.

## 13. Failure Modes

### Asia chop misclassified as drift

Most likely when session displacement looks decent but overlap and reversal frequency stay high. The remedy is to keep chop vetoes hard and not let displacement alone carry the regime filter.

### Pullback entry too early

If the engine arms on shallow resets, it will buy into unfinished countertrend pressure. This is why `NO_PULLBACK` needs both `too_shallow` and `too_extended_without_reset` reasons.

### Pullback entry too late

If thresholds demand too much pullback depth, fills occur only on structurally damaged sessions. This is why `STRETCHED_BUT_VALID` must be separate from `DISQUALIFYING_PULLBACK`.

### London contamination

Sessions can change character sharply approaching `03:00 ET`. The v1 solution is a hard flatten at `02:55 ET` and no new entries after `01:30 ET`.

### Execution friction

A favorable-looking pullback can disappear if the limit zone is too ambitious or if the secondary confirmation logic effectively chases breakouts. Both entry models need explicit price caps and expiry.

### Low sample robustness

Overnight directional-drift sessions may be sparse, especially on MGC alone. This is why GC confirmation and later transfer checks matter, and why one-trade-per-session is preferable at first.

### Overfit thresholding

If too many thresholds are opened at once, the edge will likely collapse under parameter-surface and selection-bias tests. Keep the feature algebra fixed and search only narrow threshold bands.

### Drift disappears after structural market change

Overnight participation can shift if liquidity, macro scheduling, or exchange microstructure changes. This is why ongoing walk-forward and leave-recent-period-out evidence matter more than a single historical backtest.

### Post-spike instability masquerades as drift

Fast overnight news spikes can create displacement without orderly continuation. The dedicated post-spike veto is meant to stop those sessions from contaminating the sample.

## 14. Implementation Recommendation

### What to create

Create a new research package, for example:
- `src/mgc_v05l/research/asia_drift/__init__.py`
- `src/mgc_v05l/research/asia_drift/models.py`
- `src/mgc_v05l/research/asia_drift/session_scope.py`
- `src/mgc_v05l/research/asia_drift/features.py`
- `src/mgc_v05l/research/asia_drift/state_machine.py`
- `src/mgc_v05l/research/asia_drift/entries.py`
- `src/mgc_v05l/research/asia_drift/exits.py`
- `src/mgc_v05l/research/asia_drift/backtest.py`
- `src/mgc_v05l/research/asia_drift/report.py`

### What to reuse

Reuse patterns from the ATP research stack, especially:
- replay and storage flow from `src/mgc_v05l/research/trend_participation/engine.py`
- feature-row style and explicit dataclasses from `models.py`
- separation of bias/pullback/timing concepts from `state_layers.py`
- Layer 1 adapter and provenance habits already used by the ATP validation pilot

Do not directly transplant ATP thresholds. Reuse the architecture style, not the ATP market assumptions.

### What to log

Every decision bar should log at least:
- session id and subphase
- all raw feature values used by the regime filter
- drift score components and penalties
- drift bucket and regime output
- pullback metrics and classifier output
- protected swing and leg anchors
- entry zone bounds
- arm / cancel / fill / exit reasons
- MFE / MAE / giveback metrics
- provenance ids for source bars and feature version

### What to test first

Start with unit tests for:
- session-scope boundaries around `18:00`, `01:30`, and `02:55 ET`
- drift-score feature calculations
- regime-filter classification on hand-built fixtures
- pullback classifier boundary conditions
- state-machine transitions
- entry-zone construction and expiry
- Layer 1 producer completeness

### What not to build yet

Do not build yet:
- live broker integration
- paper runtime lane
- promotion/add-on logic
- re-entry logic
- conflict handling with other strategies
- opaque ML overlays
- portfolio-level orchestration

The first engineering milestone should be a replay-only producer that emits audit-grade feature/state artifacts and a backtest subject suitable for the existing validation pipeline.
