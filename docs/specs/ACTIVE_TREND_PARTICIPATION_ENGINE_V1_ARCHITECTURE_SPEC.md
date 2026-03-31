# Active Trend Participation Engine v1 Architecture Spec

Status: architecture/specification pass only.

Scope guardrails:
- Do not widen live strategy submit scope in this pass.
- Do not change broker plumbing, control-plane behavior, runtime safety semantics, or conflict-priority semantics in this pass.
- Do not touch manual-live stock code in this pass.
- Do not center the design on Bear Snap or other sparse reversal families.
- Do not treat entries per 100 bars as the optimization target.

## 1. Engine Objective

ATP Engine v1 exists to produce economically relevant participation in directional intraday moves without turning the engine into an activity maximizer.

The practical objective hierarchy is:
- First: participate often enough in real trends to matter after costs.
- Second: participate in the prevailing direction, not against it.
- Third: enter on disciplined pullbacks or countertrend pauses that are likely to resolve back into trend.
- Fourth: refuse sharp or structurally damaging countertrend moves that are more likely to continue against the position.
- Fifth: scale only after the existing position has earned additional risk.
- Sixth: avoid arbitrary overtrading; activity is acceptable only when it improves profitable participation.

Optimization target:
- Prefer profitable trend capture, controlled drawdown, and healthy add-on contribution over raw trade frequency.
- Entries per 100 bars remains a diagnostic for under-participation or over-filtering, not a target function.

## 2. State Hierarchy

ATP v1 should use orthogonal state axes rather than one overloaded enum. The engine snapshot for any cycle is the combination of these axes.

### Bias axis
- `LONG_BIAS`
- `SHORT_BIAS`
- `NEUTRAL`

### Pullback axis
- `NO_PULLBACK`
- `NORMAL_PULLBACK`
- `STRETCHED_PULLBACK`
- `VIOLENT_PULLBACK_DISQUALIFY`

### Entry readiness axis
- `ENTRY_ELIGIBLE`
- `ENTRY_BLOCKED`

Required blocker taxonomy for `ENTRY_BLOCKED`:
- `NEUTRAL_BIAS`
- `NO_PULLBACK`
- `PULLBACK_TOO_DEEP`
- `PULLBACK_VIOLENT`
- `STRUCTURE_DAMAGED`
- `CONTINUATION_NOT_CONFIRMED`
- `VWAP_CHASE_RISK`
- `POSITION_ALREADY_OPEN`
- `PROMOTION_NOT_EARNED`
- `ORDER_STATE_UNCLEAR`
- `RUNTIME_SAFETY_BLOCK`

### Position axis
- `FLAT`
- `PROBE_POSITION`
- `IN_POSITION`
- `PROMOTION_1_ELIGIBLE`
- `PROMOTION_2_ELIGIBLE`

Interpretation:
- `PROBE_POSITION` is the initial small participation unit.
- `IN_POSITION` means exposure is open but no additional promotion is currently earned.
- `PROMOTION_1_ELIGIBLE` and `PROMOTION_2_ELIGIBLE` are earned sub-states of an existing position, not standalone entry states.

### Exit axis
- `EXIT_BLOCKED`
- `EXIT_ELIGIBLE`

Exit blocker taxonomy:
- `ORDER_STATE_UNCLEAR`
- `NO_EXIT_TRIGGER`
- `PROTECTIVE_STOP_NOT_HIT`
- `TREND_STILL_VALID`

### State precedence

Precedence rules for the combined model:
- `NEUTRAL` bias forces `ENTRY_BLOCKED`.
- `VIOLENT_PULLBACK_DISQUALIFY` forces `ENTRY_BLOCKED` and suppresses promotion eligibility.
- `ORDER_STATE_UNCLEAR` forces `ENTRY_BLOCKED`, promotion blocked, and `EXIT_BLOCKED` except for pre-existing protective orders.
- Promotions can exist only while `IN_POSITION`.
- `EXIT_ELIGIBLE` can coexist with `IN_POSITION`, `PROMOTION_1_ELIGIBLE`, or `PROMOTION_2_ELIGIBLE`.

### Suggested implementation records

- `BiasAssessment`
- `PullbackAssessment`
- `EntryEligibility`
- `PositionProgression`
- `ExitAssessment`
- `EngineSnapshot`

Each record should be serializable for replay, paper review, and live shadow observability.

## 3. Bias Layer

ATP v1 bias should be minimal, explicit, and based only on completed-bar context. v1 should not depend on a large indicator stack.

Required completed-bar inputs:
- fast EMA versus slow EMA
- slow EMA slope
- price relative to VWAP
- recent directional persistence
- optional ATR-normalized distance from a trend reference to detect extreme extension

Conceptual model:
- `LONG_BIAS` requires most of the completed-bar evidence to support upside continuation:
  fast EMA above slow EMA, slow EMA rising, close at or above VWAP, and recent closes showing upside persistence.
- `SHORT_BIAS` is the mirror image:
  fast EMA below slow EMA, slow EMA falling, close at or below VWAP, and recent closes showing downside persistence.
- `NEUTRAL` is used when evidence is mixed, trend slope is weak, or the bar is so extended from the trend reference that fresh participation should pause until a pullback resets price quality.

Design rules:
- Bias is evaluated from the latest completed decision bar, not from intrabar fluctuations.
- Bias should require directional agreement across multiple simple inputs rather than any single trigger.
- One contradictory input does not automatically force neutral, but direct conflict between trend structure and price location should degrade confidence.
- Bias output should include both a class and a small interpretable score or reason set.

## 4. Pullback Quality Layer

The pullback classifier answers: "Is this a routine pullback inside trend, a stretched but still tradable reset, or a sharp countertrend move that should disqualify entry?"

Required conceptual inputs:
- retracement depth
- ATR-normalized displacement
- countertrend velocity
- countertrend range expansion
- structure damage
- VWAP/EMA displacement

Long-bias interpretation:
- Measure pullback from the most recent in-bias impulse high toward current price.
- Evaluate how far price retraces relative to ATR, recent impulse size, fast/slow EMA, and VWAP.
- Evaluate how fast the countertrend move occurred and whether one or two bars expanded abnormally.
- Evaluate whether the move merely tags usual references or actually damages trend structure by breaking protected swing structure or decisively losing the slow trend reference.

Short-bias interpretation is symmetric.

Classification rules:
- `NO_PULLBACK`: price has not reset enough to create a discounted continuation opportunity.
- `NORMAL_PULLBACK`: retracement is within the usual envelope, countertrend speed is controlled, bar ranges are not explosively expanding, and structure remains intact.
- `STRETCHED_PULLBACK`: retracement is deeper than usual but still inside acceptable structure; speed and range may be elevated, but not enough to imply trend failure.
- `VIOLENT_PULLBACK_DISQUALIFY`: the countertrend move is too fast, too large, too structurally damaging, or too far through key references to treat as a routine pause.

The pullback classifier must disqualify on either of two paths:
- depth path: the move retraces materially beyond the stretched envelope, or
- violence path: the move is unusually fast or range-expanded even if raw depth has not yet exceeded the full disqualify depth.

Structure damage indicators that should push toward disqualification:
- break of the most recent protected higher low in long bias or protected lower high in short bias
- decisive loss of slow EMA support/resistance rather than a brief tag
- countertrend close through VWAP and through the trend reference with follow-through
- failed recovery attempts that leave lower highs in long bias or higher lows in short bias

## 5. Standard Pullback Envelope

ATP v1 needs a concrete model for "usual" pullback depth so that pullback classification is not narrative-only.

For the active bias, derive a standard pullback envelope from four components:
- `ATR component`: expected routine giveback in current volatility terms
- `impulse component`: fraction of the most recent same-direction impulse size
- `reference displacement component`: distance from price to fast EMA, slow EMA, and VWAP
- `structural component`: retracement depth that still preserves the last protected swing

Recommended envelope construction:
- Identify the latest completed directional impulse in the active bias.
- Compute a `usual_depth` from a blend of:
  ATR-normalized routine retracement,
  a partial retracement of that impulse,
  and a return toward fast-reference mean levels such as fast EMA or VWAP.
- Compute a `stretched_depth` as a wider but still acceptable envelope that may reach the slow EMA or a deeper structural retracement while still preserving the trend thesis.
- Compute a `disqualify_depth` at the point where retracement meaningfully threatens the protected swing or implies that the countertrend move is no longer routine.

Depth classification relative to the envelope:
- `NO_PULLBACK` if depth is below the minimum reset needed to create price advantage.
- `NORMAL_PULLBACK` if current depth is inside `usual_depth`.
- `STRETCHED_PULLBACK` if depth exceeds `usual_depth` but remains inside `stretched_depth` and structure still holds.
- `VIOLENT_PULLBACK_DISQUALIFY` if depth exceeds `disqualify_depth` or if violence rules override before that depth is reached.

Important design constraint:
- The envelope should be derived from the same volatility, impulse, and structure context that defines the setup. It should not be a single global fixed threshold.

## 6. Entry Timing

ATP v1 should explicitly separate state determination from execution timing.

State determination:
- Bias, pullback class, structure integrity, and initial entry eligibility are computed from completed-bar context on the decision timeframe.
- This keeps the market-state model stable and interpretable.

Execution timing:
- Once a setup is armed from completed-bar context, actual entry may occur during a qualifying lower-timeframe bar.
- Intrabar continuation confirmation is allowed for timing, not for redefining bias or pullback state.
- The lower-timeframe timing window should remain bounded; if continuation does not appear within the allowed window, the entry expires and the engine waits for the next completed-bar reassessment.

Required separation:
- Completed-bar layer decides whether the market state is participation-friendly.
- Intrabar layer decides whether the trend is reasserting right now at an acceptable price.

This replaces the older "completed-bar signal, next-bar-only execution" idea with:
- completed-bar state stability
- intrabar continuation timing
- bounded execution windows

## 7. VWAP Execution Preference

VWAP is a price-quality preference, not a mandatory hard wall by default.

Price-quality classes:
- `FAVORABLE`
- `NEUTRAL`
- `CHASE_RISK`

Long-side interpretation:
- `FAVORABLE`: entry price is at or below current-bar VWAP, or otherwise improves on the bar's average participation price.
- `NEUTRAL`: entry is slightly above VWAP but still within an acceptable continuation band.
- `CHASE_RISK`: entry is meaningfully above VWAP and likely paying up into short-term extension.

Short-side interpretation:
- `FAVORABLE`: entry price is at or above current-bar VWAP.
- `NEUTRAL`: entry is slightly below VWAP but not materially extended.
- `CHASE_RISK`: entry is meaningfully below VWAP and likely selling late into extension.

Effect on eligibility:
- `FAVORABLE`: fully eligible when other setup conditions hold.
- `NEUTRAL`: still eligible for v1, but should be surfaced distinctly for later performance review.
- `CHASE_RISK`: blocks fresh probes by default and blocks promotions unless a later explicit exception is added.

VWAP therefore acts as a price-discipline filter:
- not a mandatory barrier for all entries,
- but a direct control against low-quality chasing.

## 8. Continuation Trigger

ATP v1 should use one continuation-trigger family, symmetric by direction.

Trigger concept:
- a pullback happened,
- the pullback quality remained acceptable,
- the trend is now reasserting itself.

Long trigger:
- active `LONG_BIAS`
- pullback class is `NORMAL_PULLBACK` or `STRETCHED_PULLBACK`
- no disqualifying violence or structure damage
- lower-timeframe price reclaims a micro continuation reference created by the pause, such as the pullback pivot, the prior timing-bar high, or the local countertrend trendline break level
- current price quality is not `CHASE_RISK`

Short trigger is the mirror image:
- active `SHORT_BIAS`
- acceptable pullback
- no disqualifying violence or structure damage
- lower-timeframe price breaks back below the pause reference
- current price quality is not `CHASE_RISK`

Design constraints:
- This is not a rare reversal-bar trigger.
- This is not five separate trigger families in v1.
- The continuation trigger should be simple enough that fills are frequent during healthy trend days, but selective enough that violent countertrend moves do not qualify just because price bounced once.

## 9. Position Progression Model

ATP v1 replaces one-position-at-a-time participation with staged participation.

Stages:
- `PROBE_POSITION`: initial small-risk participation unit
- `PROMOTION_1`: first earned add
- `PROMOTION_2`: second earned add, optional in v1

Hard rule:
- Adds are allowed only when a position has earned promotion.

Position progression rules:
- Initial entries start as a probe, not full-size conviction.
- Promotions require ongoing trend validity plus evidence that the existing position is working.
- No averaging down into invalidation.
- No adds during unresolved ambiguity.
- No adds while broker or order state is unclear.
- No adds if the trend-participation thesis is damaged.

Interpretation:
- ATP v1 should grow into winners, not press losers.
- Staging is meant to improve economic relevance without turning every pullback into a fresh independent trade sequence.

## 10. Promotion Criteria

A trade earns promotion only when the original thesis has improved.

Required conceptual inputs:
- unrealized progress
- movement beyond trigger structure
- improved stop quality
- continued trend validity
- acceptable current price quality
- absence of violent reversal evidence

Promotion 1 should require all of the following:
- the probe has positive unrealized progress beyond noise
- price has moved beyond the original continuation trigger structure, showing actual follow-through
- the stop for the combined position can be improved versus the initial probe risk
- bias and pullback context still support trend continuation
- current add location is not `CHASE_RISK`
- no violent countertrend evidence has appeared since entry
- order and position state are fully reconciled

Promotion 2 should be stricter than Promotion 1:
- the position already contains a successful promotion path
- another valid pause/resume or controlled add location appears
- combined-position stop quality remains acceptable after the add
- the engine is still participating with trend, not simply adding because it is in profit

Promotion should be denied when:
- unrealized P/L is flat to negative relative to setup noise
- price has not moved beyond the trigger structure
- new pullback quality is ambiguous or violent
- price quality has degraded into chase
- trend bias has weakened to neutral or opposite

## 11. Exit Model

Time or stagnation exits are removed from v1 by default.

The v1 exit stack is:
- protective stop / invalidation
- trend failure exit
- optional later profit protection, not required in this spec pass

Protective stop / invalidation:
- Immediate risk control for a position whose setup is wrong.
- Based on the invalidation structure for the active probe or promoted stack.

Trend failure exit:
- Used when the trend-participation thesis is materially damaged even if the hard stop is not yet touched.
- Should be driven by completed-bar evidence such as bias degradation, structure failure, repeated violent pullback evidence, or inability to regain continuation references after the pullback.

Behavioral intent:
- ATP v1 should remain involved in trends that pause and continue.
- It should not exit merely because time passed.
- Exits should distinguish between healthy pause, deeper-but-valid reset, and actual trend failure.

## 12. Metrics Framework

ATP v1 scorekeeping should separate target metrics from diagnostics.

### Primary metrics
- expectancy
- net P/L
- drawdown
- average win / average loss
- trend-aligned participation rate
- adverse excursion / favorable excursion
- promotion success rate
- contribution from adds versus initial probes

Interpretation:
- Expectancy and drawdown remain core outcome metrics.
- Trend-aligned participation rate measures whether the engine is actually joining directional moves it claims to target.
- MAE/MFE and add contribution show whether entries and promotions are improving trade quality or merely increasing noise.

### Secondary diagnostics
- entries per 100 bars
- promotions per 100 entries
- time in market
- average holding time

Important correction:
- entries per 100 bars is a diagnostic only.
- Higher is not automatically better.
- Low activity can signal over-filtering, and high activity can signal noise-chasing. The target function remains profitable participation.

## 13. Observability Requirements

On every evaluation cycle, ATP v1 should surface a structured snapshot with at least:
- current bias
- bias score or contributing reasons
- pullback classification
- pullback depth score
- pullback violence score
- continuation trigger state
- VWAP price-quality state
- entry eligible / blocked
- exact blocker
- position phase (`FLAT`, `PROBE_POSITION`, promoted state, exit state)
- promotion eligibility
- promotion blocker if not eligible
- exit eligibility
- exit reason if applicable

Recommended snapshot fields:
- `decision_context_ts`
- `timing_context_ts`
- `bias_state`
- `bias_score`
- `bias_reasons`
- `pullback_state`
- `pullback_depth_score`
- `pullback_violence_score`
- `structure_damage_state`
- `continuation_trigger_state`
- `vwap_price_quality`
- `entry_state`
- `entry_blocker`
- `position_phase`
- `promotion_state`
- `promotion_blocker`
- `exit_state`
- `exit_reason`

Observability intent:
- Operators and replay tooling should be able to answer not only "why did ATP enter" but also "why did ATP refuse a setup" and "why was a promotion withheld."

## 14. Phased Implementation Plan

### Phase 1: bias engine
- Implement the minimal completed-bar bias classifier.
- Emit interpretable bias states and reasons.
- Do not alter live submit scope or broker semantics.

### Phase 2: pullback classifier
- Implement the standard pullback envelope and pullback quality states.
- Add violence and structure-damage classification.
- Surface exact blocker reasons.

### Phase 3: continuation entry family
- Implement the single pullback-resumption continuation trigger family.
- Separate completed-bar state determination from lower-timeframe timing.
- Add VWAP price-quality classification.

### Phase 4: staged position progression
- Add probe, Promotion 1, and optional Promotion 2 progression logic.
- Enforce the hard rule that adds must be earned.
- Preserve existing broker/runtime safety constraints.

### Phase 5: replay/paper validation
- Validate expectancy, drawdown, participation quality, and promotion contribution in replay and paper-only contexts.
- Review blocker distributions, chase-risk frequency, and violent-pullback disqualifications.

### Phase 6: live shadow
- Surface full ATP v1 observability in live shadow mode.
- Keep lower-priority conflict semantics unchanged.
- Confirm that entry/promotion decisions are stable under live timing conditions.

### Phase 7: live pilot
- Consider a tightly controlled live pilot only after replay, paper, and live-shadow evidence support the architecture.
- No broker/control-plane redesign should be coupled to this strategy step.

## 15. Stay Tightly Scoped

This pass explicitly does not:
- implement broad code changes
- widen live strategy submit scope
- change broker plumbing
- change manual-live stock flows
- start tuning lots of thresholds
- keep centering the design on Bear Snap

This pass does:
- define the ATP Engine v1 objective hierarchy
- define a concrete state model
- define a minimal bias layer
- define a pullback-quality and continuation-entry architecture
- define staged participation and promotion gating
- define the revised metrics hierarchy
- define required observability and phased implementation order

## 16. Existing Concepts: Retained, Demoted, Removed

### Retained
- Completed-bar context as the source of market-state truth.
- Multi-timeframe design where a higher timeframe defines structure and a lower timeframe handles timing.
- VWAP and EMA references as interpretable context.
- Lower-priority conflict handling and shadow logging versus higher-priority strategies.
- Conservative focus on economically meaningful participation rather than cosmetic signal counts.

### Demoted
- Entries per 100 bars: remains a diagnostic, no longer a proxy objective.
- Broad pattern-family taxonomy: may remain useful for research labeling, but is secondary to the v1 state machine.
- Rare reversal-bar logic, including Bear Snap-centered thinking: legacy reference material only, not the core v1 engine design.
- One-shot entry evaluation: replaced by completed-bar state plus bounded intrabar timing.

### Removed From v1 Core
- Reversal-only participation as the organizing principle.
- One-position-at-a-time as the default participation model.
- Time/stagnation exit as a default v1 exit.
- Any assumption that more trades are automatically better.
- Any requirement that ATP wait for sparse, dramatic reversal bars before participating in a live trend.
