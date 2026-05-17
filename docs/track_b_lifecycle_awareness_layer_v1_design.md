# Track B Lifecycle Awareness Layer v1 Design

## Status

- Track: B
- Document type: architecture/design proposal for review
- Implementation status: not started
- Scope: reusable advisory trade-lifecycle classification for open strategy-managed positions
- Proposed artifact path: `outputs/track_b_execution_core/lifecycle_awareness/latest_lifecycle_awareness_state.json`

This document defines a design-only Lifecycle Awareness Layer for Track B. It does not implement runtime behavior, does not create trade authority, does not mutate lifecycle state, and does not write the proposed artifact in v1.

## Purpose

Design a reusable advisory layer that classifies where an open strategy-managed position is in its trade lifecycle, so exit profiles, participation logic, future sizing or position-management logic, and operator explainability can consume consistent state context.

The layer is deliberately context-producing only. It should help downstream consumers understand whether a position is newly opened, working, expanding favorably, pulling back constructively, stalling, decaying, under adverse dominance, defensively managed, exit-pending, or unknowable because inputs are stale or unreconciled.

## Problem Statement

Current strategies know that a position is open or closed, but not enough about the state of the trade after entry. An open position can mean several materially different things:

- newly opened
- working
- stalled
- favorable expansion
- pullback but healthy
- decaying
- defensive
- exit pending

Treating all open positions as equivalent limits exit selection, reentry decisions, future sizing or position adjustment, and operator explainability. A fixed exit can become too blunt, an adaptive exit can lack lifecycle context, and a future participation-driven exit can overreact to normal pullback behavior if it does not know the trade's current lifecycle phase.

Track B needs a reusable layer that turns explicit, validated position and completed-candle context into advisory lifecycle state without becoming an execution authority.

## Layer Role

Lifecycle Awareness is an advisory context layer.

It may:

- classify lifecycle state for an open strategy-managed position
- explain hold quality, patience, exit urgency, and advisory sizing context
- expose MFE, MAE, progress, bars since entry, and stale or unreconciled input conditions
- provide state reasons and failure reasons for operator UI and future offline analysis
- provide read-only context to exit profile selection, participation/pressure analysis, future sizing logic, and post-trade attribution

It does not:

- create orders
- close positions
- mutate lifecycle rows
- authorize submit
- override governance, exposure, reconciliation, or operator controls
- create order intent
- restart runtime processes
- run lanes
- use `paper_proof`
- change strategy behavior
- infer missing lifecycle authority from broker state

Authority remains with the existing Track B lifecycle owner, governance and exposure gates, reconciliation policy, order-intent path, and explicit paper/live authorization rules.

## Safety Boundary

Hard invariants for this layer:

- No broker commands.
- No broker imports.
- No runtime restart.
- No lane execution.
- No `paper_proof`.
- No submit, cancel, close, flatten, replace, or `placeOrder` calls.
- No strategy behavior changes in v1.
- No order intent creation.
- No lifecycle mutation.
- No broker state mutation.
- No generated/runtime artifacts staged as part of this design work.
- No use of research artifacts as runtime truth.
- Advisory context only.

Required v1 safety flags must always be false:

```json
{
  "strategy_authority": false,
  "broker_state_mutated": false,
  "submit_attempted": false,
  "order_intent_created": false,
  "lifecycle_mutated": false,
  "runtime_trade_eligible": false
}
```

If any future implementation cannot prove those flags remain false for an evaluation, it must fail closed into `LOW_CONFIDENCE_STALE_OR_UNRECONCILED`.

## Inputs

Lifecycle Awareness consumes one deterministic payload assembled by a caller, fixture, offline runner, or future read-only runtime context builder. Every source must be explicit, validated, timestamped, and provenance-labeled.

Required or expected input groups:

- `lifecycle_position_record`: lifecycle-owned position id, strategy id, lane id, symbol, contract key, side, quantity context, entry timestamp, fill timestamp, average fill price, lifecycle status, and lifecycle provenance.
- `entry_metadata`: strategy family, entry candidate family, entry acceptance class or score when available, entry side, entry timeframe, initial thesis, invalidation context, and acceptance reasons.
- `entry_acceptance_context`: optional Entry Acceptance Layer output if available, including candidate quality, confidence, failure reasons, and timeframe metadata.
- `exit_context`: optional Exit Architecture context if available, including selected exit profile, hold quality, exit urgency, protective context, and failure reasons.
- `completed_candle_context`: completed OHLCV candles or completed-bar features only, including latest completed timestamp, candle completion markers, lookback windows, and source provenance.
- `mfe_mae_context`: MFE and MAE in points, ticks, dollars if available, source timestamp, calculation window, and whether the values are lifecycle-owned or derived by the context builder.
- `position_age_context`: bars since fill, wall-clock time since fill, session age, and optional thesis-resolution windows.
- `unrealized_progress_context`: current unrealized progress relative to entry, initial risk, observed range, MFE, MAE, and expected movement bands when supplied.
- `participation_pressure_context`: future optional Participation / Pressure Layer output, including impulse quality, pullback pressure, adverse pressure, continuation quality, and confidence.
- `regime_session_context`: future optional Regime State Layer and session context, including session bucket, volatility regime, liquidity context, time to session boundary, and event-risk metadata.
- `broker_reconciliation_context`: read-only broker/reconciliation status as safety context only, including reconciled, stale, missing, ambiguous, or mismatch status.

Broker/reconciliation state can lower confidence, block runtime eligibility, and create failure reasons. It cannot supply missing lifecycle ownership, create trade authority, or become a mutable dependency.

## Input Readiness And Fail-Closed Policy

The layer must fail closed when required context is missing, stale, malformed, ambiguous, unreconciled, or timeframe-incoherent. Fail-closed means producing low-confidence advisory context with no runtime eligibility and no action authority.

Suggested failure reasons:

- `MISSING_LIFECYCLE_POSITION`
- `POSITION_NOT_LIFECYCLE_OWNED`
- `POSITION_CLOSED_OR_FLAT`
- `STALE_INPUT`
- `MALFORMED_POSITION_RECORD`
- `MALFORMED_CANDLES`
- `INCOMPLETE_CANDLES`
- `THIN_DATA`
- `MISSING_PROVENANCE`
- `MISSING_ENTRY_CONTEXT`
- `MISSING_MFE_MAE_CONTEXT`
- `MISSING_PROGRESS_CONTEXT`
- `MIXED_TIMEFRAME`
- `AMBIGUOUS_TIMEFRAME`
- `UNRECONCILED_POSITION_CONTEXT`
- `BROKER_LIFECYCLE_MISMATCH`
- `RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE`
- `EXIT_PENDING_WITHOUT_CONFIRMED_LIFECYCLE_STATE`

Research artifacts may be used for offline design, replay, tests, and diagnostics. They must not be treated as runtime truth. If a future payload presents research output as runtime decision context, the layer must return low confidence with `RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE`.

## Output State Taxonomy

V1 should use a deterministic enum that is easy for exits, participation logic, sizing logic, and UI to consume.

### `NOT_IN_POSITION`

No open lifecycle-owned position is present for the evaluated identity. This can be used by batch evaluators that are asked to evaluate a lane or strategy identity rather than a specific open position.

Expected context:

- no hold context
- no add or reduce context
- no exit urgency beyond not-in-position
- confidence depends on freshness of lifecycle state

### `NEWLY_OPENED`

The position has recently filled and has not accumulated enough completed management bars or progress evidence to classify trade quality.

Expected context:

- patience context is usually neutral or early
- exit urgency is low unless protective or reconciliation context is already active
- add/reduce context should normally be unavailable

### `WORKING_IN_FAVOR`

The position is moving favorably but has not yet reached a stronger favorable-expansion threshold. Progress is constructive and not dominated by adverse pressure.

Expected context:

- hold quality constructive
- patience supported
- exit urgency low to moderate depending on progress and pullback behavior

### `FAVORABLE_EXPANSION`

The trade has expanded favorably relative to entry, initial risk, MFE/progress thresholds, expected movement bands, or participation strength.

Expected context:

- hold quality high when continuation evidence remains fresh
- reduce-size context may be advisory if extension is large or session risk rises
- add-size context remains advisory only and should require future continuation confirmation and governance

### `HEALTHY_PULLBACK`

The trade has pulled back from favorable progress, but the pullback remains consistent with the entry thesis and completed-candle context does not show adverse dominance.

Expected context:

- patience context elevated
- hold quality constructive or guarded
- exit urgency dampened compared with raw adverse movement
- reduce-size context normally low unless pullback depth, session risk, or participation context deteriorates

### `STALLED`

The trade has not made sufficient favorable progress within expected bars or time since fill, but adverse dominance is not yet strong. The position may be using time without resolving the thesis.

Expected context:

- hold quality neutral to weakening
- patience context declining
- exit urgency moderate or watchful
- failure reasons remain empty if data is valid; state reasons explain time/progress deficiency

### `DECAYING`

Favorable progress, impulse quality, close location, or pullback recovery is weakening after the trade had time or opportunity to work. Decay is weaker than adverse dominance but stronger than simple stall.

Expected context:

- hold quality weak or guarded
- patience context low
- exit urgency elevated
- reduce-size context may become advisory for future authorized consumers

### `ADVERSE_DOMINANCE`

Completed-candle context shows adverse movement or pressure dominating the position relative to entry, MFE, MAE, invalidation context, or expected pullback behavior.

Expected context:

- hold quality poor
- exit urgency high
- patience low
- reduce-size context may be high as advisory context only
- add-size context blocked

### `DEFENSIVE_MANAGEMENT`

The position is still lifecycle-open, but context suggests it should be managed defensively because of degraded progress, adverse pressure, session/regime risk, protective context, stale confidence, or exit profile selection.

Expected context:

- hold quality guarded to poor
- exit urgency elevated but not a command
- reduce-size context may be elevated as advisory context only
- add-size context blocked

### `EXIT_RECOMMENDED_CONTEXT`

Advisory context indicates that an authorized future layer may consider exit. This state is not an exit signal, not an order intent, and not permission to close.

Expected context:

- exit urgency high
- hold quality poor or invalidated
- state reasons identify advisory evidence
- safety flags remain false

### `EXIT_PENDING_OBSERVED`

Read-only lifecycle or reconciliation context indicates an exit is already pending or observed. The layer should avoid competing recommendations and should report pending status only as context.

Expected context:

- no new order intent
- no submit eligibility
- exit urgency reflects observed pending state, not new authority
- confidence depends on lifecycle/reconciliation freshness

### `CLOSED_OR_FLAT`

The lifecycle-owned record is no longer open or reconciliation confirms flat according to lifecycle policy.

Expected context:

- no active hold, add, or reduce context
- state can support post-trade attribution but not active management

### `LOW_CONFIDENCE_STALE_OR_UNRECONCILED`

The layer cannot safely classify lifecycle state because data is stale, unreconciled, malformed, incomplete, provenance-missing, or timeframe-ambiguous.

Expected context:

- fail closed
- confidence low or zero
- exit/add/reduce contexts unavailable or blocked
- failure reasons required
- all safety flags false

## Outputs

The latest artifact, when a future writer exists, should be one bounded JSON object. V1 design does not implement the writer.

Proposed artifact:

```text
outputs/track_b_execution_core/lifecycle_awareness/latest_lifecycle_awareness_state.json
```

Required top-level fields:

- `schema_version`: proposed `track_b_lifecycle_awareness_state_v1`
- `producer`: proposed `TrackBLifecycleAwarenessLayer`
- `authority_mode`: `ADVISORY_CONTEXT_ONLY`
- `artifact_path`
- `generated_at`
- `source_id`
- `input_source_path`
- `input_source_category`
- `input_mode`
- `source_provenance_status`
- `freshness_status`
- `latest_input_timestamp`
- `lifecycle_position_id`
- `strategy_id`
- `lane_id`
- `instrument`
- `contract_key`
- `side`
- `quantity_context`
- `entry_timestamp`
- `fill_timestamp`
- `average_fill_price`
- `lifecycle_status`
- `broker_reconciliation_status`
- `lifecycle_awareness_state`
- `hold_quality_context`
- `exit_urgency_context`
- `reduce_size_context`
- `add_size_context`
- `patience_context`
- `state_reasons`
- `failure_reasons`
- `confidence`
- `bars_since_entry`
- `wall_clock_seconds_since_entry`
- `session_seconds_since_entry`
- `mfe_points`
- `mae_points`
- `mfe_ticks`
- `mae_ticks`
- `mfe_dollars`
- `mae_dollars`
- `current_unrealized_points`
- `current_unrealized_ticks`
- `current_unrealized_dollars`
- `progress_ratio`
- `drawdown_from_mfe_ratio`
- `adverse_progress_ratio`
- `lifecycle_evaluation_timeframe`
- `fast_reaction_timeframe`
- `trend_context_timeframe`
- `timeframe_source`
- `base_timeframe_if_derived`
- `aggregation_method`
- `anchor_rule`
- `timeframe_alignment_status`
- `strategy_authority`
- `broker_state_mutated`
- `submit_attempted`
- `order_intent_created`
- `lifecycle_mutated`
- `runtime_trade_eligible`

Suggested context enums:

- `hold_quality_context`: `NO_POSITION`, `EARLY_UNKNOWN`, `STRONG_HOLD`, `CONSTRUCTIVE_HOLD`, `GUARDED_HOLD`, `WEAK_HOLD`, `NO_HOLD_LOW_CONFIDENCE`
- `exit_urgency_context`: `NONE`, `LOW`, `WATCH`, `ELEVATED`, `HIGH_ADVISORY`, `PENDING_OBSERVED`, `LOW_CONFIDENCE_BLOCKED`
- `reduce_size_context`: `NO_POSITION`, `NOT_INDICATED`, `CONSIDER_ONLY_AFTER_AUTHORIZED_REVIEW`, `ELEVATED_ADVISORY`, `BLOCKED_LOW_CONFIDENCE`
- `add_size_context`: `NOT_INDICATED`, `ONLY_AFTER_CONTINUATION_CONFIRMATION`, `BLOCKED_DEGRADED_CONTEXT`, `BLOCKED_LOW_CONFIDENCE`
- `patience_context`: `NO_POSITION`, `EARLY_PATIENCE`, `PATIENCE_SUPPORTED`, `PATIENCE_NEUTRAL`, `PATIENCE_DECLINING`, `PATIENCE_NOT_SUPPORTED`, `LOW_CONFIDENCE`

All size contexts are advisory only. They do not imply add, reduce, scale, close, submit, or order-intent authority.

## Timeframe Policy

Lifecycle classification must carry explicit timeframe metadata. No consumer should infer the evaluation timeframe from filenames, strategy family, candle count, or current implementation habits.

Required fields:

- `lifecycle_evaluation_timeframe`: lead timeframe for lifecycle classification.
- `fast_reaction_timeframe`: optional lower-latency context used for future fast reaction classification.
- `trend_context_timeframe`: optional higher-level trend or regime context.
- `timeframe_source`: `NATIVE` or `DERIVED`.
- `base_timeframe_if_derived`: native source timeframe when derived candles are used.
- `aggregation_method`: deterministic OHLCV aggregation method when derived.
- `anchor_rule`: candle boundary, timezone, session/calendar anchor, and boundary alignment rule.
- `timeframe_alignment_status`: explicit status such as `ALIGNED`, `DERIVED_ALIGNED`, `MIXED_TIMEFRAME_BLOCKED`, `MISSING_TIMEFRAME_SCHEMA`, or `UNKNOWN`.

Policy:

- Use completed candles only.
- Do not use incomplete live bars for lifecycle classification.
- Do not silently mix lifecycle, exit, fast reaction, or trend timeframes.
- Derived timeframe metadata is required whenever candles are aggregated.
- Entry timeframe, lifecycle evaluation timeframe, exit management timeframe, fast reaction timeframe, and trend context timeframe may differ only when the payload declares that relationship explicitly.
- Silent mixed timeframe, missing timeframe schema, conflicting anchors, or incomplete candles must fail closed into `LOW_CONFIDENCE_STALE_OR_UNRECONCILED`.

## Relationship To Current Exit Candidates

### Fixed 36b Exit Candidate

The fixed 36-bar exit candidate can use lifecycle awareness to explain why time-based exit pressure is or is not appropriate. A position that is `HEALTHY_PULLBACK` or `FAVORABLE_EXPANSION` near the fixed window may deserve different advisory language than one that is `STALLED` or `DECAYING`.

Lifecycle Awareness does not change the fixed exit rule in v1. It provides context so future evaluation can distinguish "36 bars elapsed during constructive trade development" from "36 bars elapsed with no thesis resolution."

### Adaptive 24-To-36 Candidate

The adaptive 24-to-36 candidate is a natural consumer of lifecycle state. It can treat `STALLED`, `DECAYING`, `ADVERSE_DOMINANCE`, or `DEFENSIVE_MANAGEMENT` as evidence for earlier advisory urgency, while treating `WORKING_IN_FAVOR`, `FAVORABLE_EXPANSION`, or `HEALTHY_PULLBACK` as evidence for patience within the allowed window.

V1 does not wire this into strategy behavior. The design only defines the context that a future adaptive exit selector could consume.

### Future Participation-Driven Exits

Participation-driven exits need to know whether adverse pressure is true deterioration or a normal pullback inside a still-healthy trade. Lifecycle Awareness gives Participation / Pressure context a position-aware frame:

- `FAVORABLE_EXPANSION` can make mild pullback pressure less urgent.
- `HEALTHY_PULLBACK` can prevent overreaction to ordinary retracement.
- `DECAYING` or `ADVERSE_DOMINANCE` can make participation collapse more meaningful.
- `LOW_CONFIDENCE_STALE_OR_UNRECONCILED` can block participation context from becoming accidental authority.

### Future Sizing And Position Adjustment

Future sizing or position-management logic can consume `reduce_size_context`, `add_size_context`, hold quality, and confidence as advisory input only. Governance, exposure, lifecycle ownership, and order-intent policy remain authoritative.

Examples:

- A future add-size evaluator may require `FAVORABLE_EXPANSION`, strong participation, fresh reconciliation, and explicit governance permission before even considering an add.
- A future reduce-size evaluator may inspect `DECAYING`, `DEFENSIVE_MANAGEMENT`, or `ADVERSE_DOMINANCE`, but still cannot create a reduction without an authorized path.
- Operator UI can show why sizing is blocked, unavailable, or advisory-only.

## Relationship To Future Layers

Lifecycle Awareness should be reusable and composable with later Track B context layers.

### Participation / Pressure Layer

Participation / Pressure can provide impulse quality, adverse pressure, pullback health, continuation pressure, and participation collapse evidence. Lifecycle Awareness can consume that context, and participation-driven exits can consume lifecycle state in return. The integration must remain explicit to avoid circular hidden authority.

### Regime State Layer

Regime State can provide volatility, trend, session, liquidity, and event-risk context. Lifecycle Awareness can use regime state to explain why a position is defensive, why patience is reduced into session risk, or why low-volatility stall should be treated differently from high-volatility adverse dominance.

### Sizing / Position Management Layer

Sizing and position management can consume lifecycle state as advisory context. Lifecycle Awareness should not know exposure limits, account authority, or order mechanics. It should simply classify trade state and produce add/reduce context labels with confidence and reasons.

### Operator UI / Explainability

Operator UI can display lifecycle state, hold quality, exit urgency, patience, MFE/MAE, progress, bars since entry, confidence, failure reasons, and safety flags. The UI should label all outputs as advisory and read-only.

### Post-Trade Attribution

Post-trade attribution can compare realized exit decisions against contemporaneous lifecycle states. This can help answer whether exits happened during healthy pullbacks, actual decay, adverse dominance, stale data, or pending lifecycle transitions.

## Proposed Evaluation Shape

A future implementation should be rule-first, deterministic, and inspectable. Suggested evaluation order:

1. Validate provenance, lifecycle ownership, lifecycle status, freshness, reconciliation, and timeframe schema.
2. Return `NOT_IN_POSITION`, `CLOSED_OR_FLAT`, `EXIT_PENDING_OBSERVED`, or `LOW_CONFIDENCE_STALE_OR_UNRECONCILED` when structural state requires it.
3. Compute position age, completed management bars, MFE, MAE, current progress, drawdown from MFE, and adverse progress.
4. Classify early position state as `NEWLY_OPENED` until enough completed bars or progress evidence exists.
5. Classify constructive states: `WORKING_IN_FAVOR`, `FAVORABLE_EXPANSION`, and `HEALTHY_PULLBACK`.
6. Classify weakening states: `STALLED`, `DECAYING`, `ADVERSE_DOMINANCE`, and `DEFENSIVE_MANAGEMENT`.
7. Elevate to `EXIT_RECOMMENDED_CONTEXT` only as advisory context when evidence exceeds configured thresholds and confidence is sufficient.
8. Emit reasons, failure reasons, confidence, timeframe metadata, progress fields, and false safety flags.

V1 thresholds should be explicit constants or fixture-backed rules. They should not be optimized, fitted, or inferred from research artifacts presented as runtime truth.

## Test And Fixture Plan

When implementation begins, fixtures should live under a dedicated lifecycle-awareness fixture path, for example:

```text
tests/fixtures/track_b_lifecycle_awareness/
```

Required fixture scenarios:

- favorable expansion: position has strong MFE/progress, constructive completed candles, high hold quality, low exit urgency, and advisory-only add/reduce context.
- healthy pullback: position has favorable prior progress and a bounded pullback that preserves thesis context, produces patience support, and avoids premature exit urgency.
- stalled: position has enough bars/time since fill with insufficient favorable progress and no strong adverse dominance.
- decaying: position shows weakening progress, deteriorating close location, increasing drawdown from MFE, or recovery failure after having time to work.
- adverse dominance: completed candles show adverse expansion or pressure against the position, poor hold quality, high advisory exit urgency, and blocked add-size context.
- stale or unreconciled fail-closed: stale inputs, missing lifecycle ownership, broker/lifecycle mismatch, incomplete candles, or ambiguous timeframe produces `LOW_CONFIDENCE_STALE_OR_UNRECONCILED`.
- exit pending observed: lifecycle or reconciliation context indicates an exit is already pending, with no new order intent or lifecycle mutation.
- closed or flat: lifecycle state is closed/flat and active management contexts are unavailable.

Test categories:

- schema and enum validation
- provenance and freshness validation
- lifecycle ownership validation
- completed-candle validation
- timeframe alignment validation
- MFE/MAE and progress calculation fixtures
- state classification fixtures
- state reason and failure reason assertions
- confidence scoring bounds
- safety flags always false
- no broker import or mutation symbol scan
- no lifecycle mutation symbol scan
- no submit/cancel/close/placeOrder symbol scan
- artifact writer writes only to tmp paths in tests, once a writer exists

The no-mutation symbol scan should include at least these forbidden terms for this layer's implementation namespace:

- `submit`
- `cancel`
- `close`
- `flatten`
- `placeOrder`
- `order_intent`
- `broker_state_mutated`
- `lifecycle_mutated`

Tests should assert that any appearance is either in tests, docs, schema safety flags, or explicit forbidden-symbol guard code, not executable broker or lifecycle mutation behavior.

## V1 Non-Goals

- no runtime implementation
- no runtime artifact writer
- no strategy integration
- no strategy behavior changes
- no order intent creation
- no submit, cancel, close, flatten, replace, or `placeOrder`
- no lifecycle mutation
- no broker mutation
- no broker commands
- no runtime restart
- no lane execution
- no `paper_proof`
- no generated runtime artifact committed
