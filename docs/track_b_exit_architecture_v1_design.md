# Track B Exit Architecture v1 Design

## Purpose

Design a modular, non-broker-authoritative exit architecture that can eventually pair exit behavior with entry quality, participation quality, position lifecycle state, and market deterioration or continuation context.

This is not a strategy, not a signal generator, not a broker authority layer, and not a managed-close implementation. V1 produces advisory context only.

## Problem Statement

Current exits are too event-specific. A single adverse event can trigger sell or cover behavior during a healthy continuation pullback, even when the broader entry thesis and participation context remain intact.

Track B needs a modular exit layer that can distinguish:

- healthy continuation pullbacks that should preserve patience
- impulse decay that warrants elevated monitoring
- participation collapse that may justify urgent exit context
- protective hard-stop conditions that must remain distinct from discretionary exit logic
- low-confidence or stale data cases that must fail closed instead of inventing authority

Exit behavior should be selected by entry type, entry quality, participation state, current lifecycle context, and market deterioration or continuation evidence.

## Core Architecture

### 1. Exit Context Builder

Builds a read-only, normalized input object for exit evaluation.

Responsibilities:

- bind lifecycle position identity without mutating lifecycle state
- attach entry acceptance and entry quality context
- attach participation quality context from the offline participation layer
- attach completed candle context with explicit timeframe metadata
- compute read-only position context such as MFE, MAE, bars since fill, and session age
- attach session, regime, freshness, provenance, and reconciliation context
- fail closed when data is stale, thin, malformed, unreconciled, or timeframe-ambiguous

### 2. Exit Profile Selector

Chooses an advisory exit profile context from normalized inputs.

The selector should be deterministic, inspectable, and conservative. It does not submit orders, create order intents, close positions, update lifecycle state, or override strategy/governance.

### 3. Exit Module Evaluator

Runs independent exit modules against the selected profile and context. Each module returns advisory evidence with reasons, confidence, failure reasons, and safety flags.

Modules must be composable. Protective stop logic should not be tangled with continuation patience logic, and participation-collapse logic should not silently override low-confidence data.

### 4. Exit Recommendation Context

Aggregates module results into artifact-safe advisory output:

- exit urgency context
- hold quality context
- reduce-size context
- scale-up context
- protective-exit context
- reasons for exit and hold
- confidence and failure reasons

The recommendation context is not a command and has no trade authority.

### 5. Managed Exit Executor, Future Only

A future managed exit executor may consume exit recommendation context, but authority remains elsewhere. Any future executor must pass through governance, lifecycle ownership, broker reconciliation, order intent policy, and explicit operator or strategy authorization as applicable.

V1 explicitly does not implement this layer.

## Exit Profile Taxonomy

- `PATIENT_CONTINUATION`: strong entry quality, healthy participation, constructive pullback or continuation structure. Bias is patience unless hard protective context fires.
- `NORMAL_CONTINUATION`: acceptable continuation state with no urgent deterioration. Standard management rules may apply.
- `DEFENSIVE_TIGHT`: entry or participation quality has degraded, MFE may be under pressure, or range behavior suggests reduced patience.
- `TIME_BOXED`: trade thesis depends on resolution within a bounded bar/session window.
- `PROTECTIVE_HARD_STOP`: protective risk boundary context is active. This remains advisory in V1 but must be separated from discretionary exit modules.
- `PARTICIPATION_COLLAPSE_EXIT`: participation quality has collapsed with sufficient confidence and freshness. Produces high exit urgency context but no direct command.
- `NO_EXIT_LOW_CONFIDENCE`: stale, thin, malformed, unreconciled, or ambiguous inputs. Fail closed with no strategy or runtime trade eligibility.

## Exit Module Types

### Hard Protective Stop

Evaluates pre-defined protective boundaries such as stop distance, invalidation price, or hard risk threshold. It must not place or close orders in V1.

### Time-Boxed Exit

Evaluates bars since fill, session clock, and thesis resolution windows. It can raise urgency when the trade has not behaved as expected within its allowed window.

### Participation Decay Exit

Consumes participation quality context to distinguish impulse decay from true participation collapse. It should require freshness and sufficient candle history.

### Continuation Hold / Patience Module

Recognizes healthy continuation behavior, orderly pullbacks, and constructive recovery. It can lower exit urgency and strengthen hold quality.

### Pullback Health Module

Evaluates pullback depth, recovery quality, close location, adverse overlap, and whether pullback behavior remains consistent with the entry thesis.

### Adverse Range Expansion Module

Detects range expansion against the position, failed recoveries, and volatility expansion that changes trade risk.

### Target / Scale-Out Module

Produces advisory context for taking partial profit, holding full size, or avoiding scale-out when continuation quality remains high. Governance and exposure controls remain authoritative.

### End-of-Session / Session-Risk Module

Evaluates session boundaries, liquidity changes, news/event windows if available, and whether the trade should be treated as time-sensitive into a risk boundary.

## Inputs

All inputs are read-only and must include freshness and provenance fields.

Required input categories:

- lifecycle position identity: strategy id, lane id, symbol, side, entry timestamp, lifecycle position id, quantity context
- entry acceptance context: entry type, entry quality, entry timeframe, acceptance reason, initial invalidation context
- participation quality context: participation state, hold quality, pullback health, continuation confidence, confidence failure reasons
- completed candle context: OHLCV candles only, explicit timeframe, candle completion markers, lookback windows
- MFE and MAE: points, dollars if available, source, timestamp, calculation window
- bars since fill: primary management bars elapsed and optional fast reaction bars elapsed
- session/regime context: session label, liquidity regime, time to session end, event-risk metadata if available
- freshness/provenance: generated_at, source path, source type, runtime eligibility, stale threshold, source hash if available
- reconciliation context: current broker/lifecycle reconciliation status as read-only context only

The layer must not consume research artifacts as runtime truth. Research-derived metadata may be included only as non-runtime provenance or offline annotation and must set failure reasons when presented as runtime truth.

## Timeframe Policy

Exits may use different management or confirmation timeframes than entries. A 5m entry can be managed with 5m primary context plus faster reaction context, provided the relationship is explicit.

Required fields:

- `entry_timeframe`
- `exit_management_timeframe`
- `fast_reaction_timeframe`
- `trend_context_timeframe`
- `timeframe_source`
- `base_timeframe_if_derived`
- `aggregation_method`
- `anchor_rule`
- `timeframe_alignment_status`

Policy:

- completed candles only
- lead timeframe for V1 exit management should be explicit, initially expected to align with completed 5m context unless a profile explicitly declares otherwise
- derived candles must disclose base timeframe, aggregation method, and anchor rule
- silent mixed timeframe inputs fail closed
- ambiguous, incomplete, or conflicting timeframe metadata yields `NO_EXIT_LOW_CONFIDENCE`

## Outputs

V1 output is advisory-only and artifact-safe.

Top-level output fields:

- `exit_profile_context`
- `exit_urgency_context`
- `hold_quality_context`
- `reduce_size_context`
- `scale_up_context`
- `protective_exit_context`
- `exit_reasons`
- `hold_reasons`
- `confidence`
- `failure_reasons`

Required safety flags:

- `strategy_authority: false`
- `broker_state_mutated: false`
- `submit_attempted: false`
- `order_intent_created: false`
- `lifecycle_mutated: false`
- `runtime_trade_eligible: false`

Suggested failure reasons:

- `THIN_DATA`
- `STALE_INPUT`
- `INCOMPLETE_CANDLES`
- `MIXED_TIMEFRAME`
- `AMBIGUOUS_TIMEFRAME`
- `MALFORMED_CANDLES`
- `LOW_RANGE_QUALITY`
- `MISSING_PROVENANCE`
- `UNRECONCILED_POSITION_CONTEXT`
- `RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE`
- `MISSING_LIFECYCLE_POSITION`
- `MISSING_ENTRY_CONTEXT`
- `MISSING_PARTICIPATION_CONTEXT`

## Size Adjustment Context

V1 may produce size-adjustment context but never size-adjustment authority.

Initial advisory states:

- `HOLD_FULL_SIZE`: continuation or pullback context supports patience.
- `REDUCE_SIZE_CONTEXT`: risk or participation context suggests partial reduction may be considered by an authorized layer.
- `SCALE_ONLY_AFTER_CONTINUATION_CONFIRMATION`: adding is not supported until continuation confirmation improves.
- `NO_ADD_DEGRADED_ENTRY_OR_WEAK_PARTICIPATION`: entry quality, participation, or market context does not justify adding.
- `NO_SIZE_ADJUSTMENT_LOW_CONFIDENCE`: data is not eligible for advisory sizing context.

Governance, exposure limits, lifecycle ownership, operator controls, and strategy authorization remain the only future sources of action authority.

## Safety Boundaries

V1 must enforce:

- no broker imports
- no broker mutation
- no submit, cancel, close, replace, or `placeOrder`
- no direct trade command
- no order intent creation
- no lifecycle mutation
- no strategy integration
- no runtime restart or lane execution
- artifact-only output
- fail closed on stale, thin, malformed, incomplete, unreconciled, research-as-runtime-truth, or timeframe-ambiguous data

The architecture can read reconciliation status as context, but it cannot use broker state as a mutable dependency and cannot infer missing lifecycle authority from broker positions.

## Artifact Path

Proposed latest-state artifact:

```text
outputs/track_b_execution_core/exit_context/latest_exit_context_state.json
```

The artifact should be bounded latest-state output. Any future JSONL history should have explicit retention, archive, and hot-path size policies.

## Test And Fixture Plan

Fixtures should live under a dedicated exit-context fixture path when implementation begins, for example:

```text
tests/fixtures/track_b_exit_context/
```

Required fixture scenarios:

- healthy pullback, no exit: high hold quality, low exit urgency, no protective trigger
- impulse decay, elevated exit urgency: weakening directional persistence without full collapse
- participation collapse: high exit urgency context with explicit participation-collapse reason
- hard protective stop context: protective context active, still no order intent or broker mutation
- time-box expiry: thesis unresolved after allowed bars/session window
- scale-down context: reduced hold quality or adverse range behavior produces reduce-size context
- low-confidence stale data: fail closed with `STALE_INPUT`
- mixed timeframe fail-closed: conflicting or silent mixed timeframe metadata produces `MIXED_TIMEFRAME` or `AMBIGUOUS_TIMEFRAME`

Test categories:

- schema and enum validation
- freshness and provenance validation
- completed-candle and timeframe validation
- module-level rule fixtures
- aggregation behavior
- safety flags always false
- broker mutation symbol and import scan
- artifact writer writes only to tmp paths in tests

## Future Integration

### Strategy Rule Runners

Strategies may eventually consume advisory exit context as one input to their own management logic. Exit context must not become a hidden signal generator or strategy authority.

### Managed Close Path

A future managed close path may consume exit context only after explicit authorization through governance, lifecycle ownership, reconciliation, and order-intent policy. The exit layer itself remains context-producing.

### Dynamic Position Adjustment

Size adjustment context can help future governance decide whether reduce, hold, or scale-up actions are even eligible for consideration. It cannot bypass exposure limits or add authority.

### Operator UI

Operator UI can display exit profile, urgency, hold quality, protective context, confidence, failure reasons, and safety flags. UI should clearly label outputs as advisory and read-only.

### Post-Trade Attribution

Post-trade attribution can compare eventual exits with contemporaneous exit context to analyze whether exits occurred during healthy pullbacks, participation collapse, protective-stop context, or low-confidence states.

## V1 Non-Goals

- no strategy integration
- no order intent creation
- no close command
- no lifecycle mutation
- no broker mutation
- no live broker state dependency
- no generated runtime artifact committed
- no runtime executor
