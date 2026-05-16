# Track B Entry Acceptance Layer v1 Design

## Status

- Track: B
- Document type: architecture/design proposal for review
- Implementation status: not started
- Scope: offline entry-candidate quality/context layer plus future layer stubs
- Primary artifact path: `outputs/track_b_execution_core/entry_acceptance/latest_entry_acceptance_state.json`

This document defines the first Entry Acceptance Layer for Track B entry reengineering. It is intentionally non-authoritative. It does not create strategy authority, broker authority, order intent, lifecycle transitions, paper proof, lane execution, or submit permission.

## Purpose

Track B entry handling should move from rigid exact-pattern acceptance toward structured candidate quality scoring. Exact structural matches remain valuable, but binary acceptance misses close-enough setups that preserve the important pattern geometry and rejects nuance that future strategies, exits, and operator surfaces should be able to inspect.

Entry Acceptance Layer v1 scores exact and near-match Track B pattern candidates using completed market context, candidate metadata, optional participation quality, session/instrument context, timeframe metadata, and provenance/freshness fields. Its output explains whether a setup is strong, acceptable, marginal, invalid, or unknowable from the available data.

The layer is not a strategy. It is not a broker submitter. It must not create order intent or mutate lifecycle state. It produces artifact-only quality/context for review and future consumption.

## Track B Stack Placement

The layer sits after candidate generation and before any future strategy-specific decision that may choose to consume the context. It is advisory only.

```text
runtime or fixture market context
candidate pattern metadata
optional Participation Quality Layer output
session / instrument context
explicit timeframe schema
provenance / freshness fields
        |
        v
Data Readiness / Provenance Gate, future stub
        |
        v
Pattern Engine candidate generation
        |
        v
Entry Acceptance Layer v1
        |
        +--> artifact: latest_entry_acceptance_state.json
        +--> operator explainability, display only
        +--> future strategy runner, advisory input only
        +--> future initial sizing context, advisory input only
        +--> future exit profile selector, advisory input only
```

Authority remains elsewhere:

- Governance and exposure gates decide whether risk is allowed.
- Strategy runners decide whether a setup belongs to a strategy.
- Order-intent creation, if ever allowed by a future strategy, must occur only through the normal Track B authority path.
- Broker state and lifecycle state are never owned by this layer.

## Safety Boundary

Hard invariants for this layer:

- No broker commands.
- No broker imports.
- No runtime restart.
- No lane execution.
- No `paper_proof`.
- No submit, cancel, close, flatten, or `placeOrder` calls.
- No strategy integration in v1.
- No order intent creation.
- No lifecycle mutation.
- No generated/runtime artifacts staged as part of this design work.
- No use of research artifacts as runtime truth.
- Artifact-only output.
- Quality/context only.

V1 safety flags must always be false:

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

## Inputs

Entry Acceptance Layer v1 consumes a single deterministic payload assembled by a caller, fixture, or future offline runner.

Required input groups:

- `candidate_pattern_metadata`: candidate id, family, side, rule id, expected direction, required predicates, optional predicates, matched predicates, missing predicates, tolerance diagnostics, and structural notes.
- `market_context`: completed OHLCV bars or summarized completed-bar features, including latest completed timestamp, timeframe, directional features, volatility/range features, pullback/retest features, and candle completeness.
- `timeframe_context`: explicit timeframe schema fields for the candidate, context, entry lead surface, exit management, fast reaction, derivation source, and alignment status.
- `session_instrument_context`: instrument, contract family, timeframe, session bucket, and expected candidate session fit.
- `provenance_freshness`: `source_id`, `input_source_path`, `input_source_category`, `input_mode`, `generated_at`, latest input timestamp, freshness status, completed-candle status, and runtime eligibility status.

Optional input groups:

- `participation_quality`: output from the Participation Quality Layer, including participation state, long/short hold quality, continuation confidence, pullback health, confidence, and failure reasons.
- `prior_quality_context`: prior advisory context from future layers, if explicitly marked non-authoritative.

Research artifacts may be used for offline research, fixtures, replay, or diagnostics. They must not be treated as runtime truth. If `input_mode=RUNTIME_DECISION` and the source category is `RESEARCH`, the layer must return low confidence and include a research-source runtime rejection reason.

## Timeframe Policy

The current Track B v1 discipline uses completed 5m candles as the default lead surface for entry acceptance. That default is a v1 operating choice, not a permanent architectural assumption. All layers must treat timeframe as explicit schema carried in the input and output contract, not as an implicit convention inferred from field names, file paths, or current implementation habits.

Required timeframe fields:

- `primary_timeframe`: the lead decision surface for the layer. For v1 this defaults to `5m`.
- `candidate_timeframe`: the timeframe on which the candidate pattern was generated.
- `context_timeframe`: the timeframe used for surrounding market context.
- `exit_management_timeframe`: the timeframe expected to guide exit management or confirmation when available.
- `fast_reaction_timeframe`: the lower-latency or finer-grained timeframe used for future fast reaction context when available.
- `timeframe_source`: `NATIVE` or `DERIVED`.
- `base_timeframe_if_derived`: the native lower-level source timeframe when `timeframe_source=DERIVED`.
- `aggregation_method`: the approved aggregation method used to derive candles, such as OHLCV rollup with volume sum and deterministic timestamp anchors.
- `anchor_rule`: the explicit candle boundary rule, including session/calendar anchor, timezone, and boundary alignment.
- `timeframe_alignment_status`: explicit status such as `ALIGNED`, `DERIVED_ALIGNED`, `MIXED_TIMEFRAME_BLOCKED`, `MISSING_TIMEFRAME_SCHEMA`, or `UNKNOWN`.

Derived timeframes are allowed only when generated by an approved candle builder with explicit anchor rules. Arbitrary derived intervals such as 10m, 12m, and 15m are valid only if the artifact declares `timeframe_source=DERIVED`, the `base_timeframe_if_derived`, the `aggregation_method`, and the `anchor_rule`, and the builder is approved for that interval. Silent mixed-timeframe inputs must fail closed as low-confidence or invalid context; the layer must not quietly blend native and derived candles or infer a missing alignment policy.

Entry and exit timeframes may differ. Entry acceptance can use the v1 `primary_timeframe=5m` lead surface while future exit modules use a different `exit_management_timeframe` or confirmation timeframe. Fast reaction context may use a finer `fast_reaction_timeframe` after explicit schema support exists, but it remains advisory unless a future lifecycle owner and governance path explicitly consume it.

## Candidate Taxonomy

### `EXACT_STRUCTURAL_MATCH`

The candidate satisfies the required structural predicates and matches the expected Track B pattern geometry without material degradation. Minor context warnings can exist, but they do not undermine the structure.

Expected score range: `0.85` to `1.00`.

### `NEAR_STRUCTURAL_MATCH`

The candidate is close to the expected pattern with small deviations. Examples include a mild timing offset, one non-critical predicate just outside tolerance, or a modest range mismatch while direction, core structure, and session fit remain intact.

Expected score range: `0.70` to `0.84`.

### `DEGRADED_BUT_VALID_MATCH`

The candidate retains recognizable pattern structure but carries meaningful weakness. Examples include marginal volatility fit, weak retest quality, mixed participation support, elevated trap risk, or context that makes the setup marginal rather than strong.

Expected score range: `0.50` to `0.69`.

### `STRUCTURALLY_INVALID`

The candidate fails required pattern structure or contradicts the expected side. Examples include missing mandatory predicates, wrong directional impulse, invalid sequence ordering, incompatible session/instrument context, or impossible candle geometry.

Expected score range: `0.00` to `0.49`.

### `LOW_CONFIDENCE_INSUFFICIENT_DATA`

The layer cannot make a reliable structural judgment. Examples include stale data, thin data, incomplete candles, mixed timeframes, malformed payloads, missing provenance, or research-sourced data presented as runtime truth.

Expected score range: `0.00` to `0.30`. Confidence must be `0.0` when a hard provenance or freshness failure blocks evaluation.

## Scoring Dimensions

The `acceptance_score` is a deterministic advisory score from `0.0` to `1.0`. V1 should use explicit rules and transparent dimension scores, not fitting or optimization.

Recommended initial dimension weights:

| Dimension | Weight | Purpose |
| --- | ---: | --- |
| Structural similarity | 0.30 | Measures required predicates, optional predicates, pattern geometry, sequence ordering, and tolerance distance from the expected candidate shape. |
| Directional alignment | 0.15 | Measures candidate side against completed primary-timeframe impulse, close location, trend/reclaim behavior, and directional context. |
| Timing/session fit | 0.10 | Measures session bucket, time-of-day fit, completed-bar alignment, and expected pattern window. |
| Volatility/range fit | 0.10 | Measures range quality, ATR/range band fit if supplied, and whether movement is too thin, too chaotic, or instrument-inappropriate. |
| Pullback/retest quality | 0.10 | Measures depth, hold/reclaim behavior, failed-break/retest semantics, and whether the retest supports the candidate side. |
| Participation support | 0.10 | Measures optional Participation Quality Layer alignment, continuation confidence, hold quality, and hostile participation flags. |
| Failure-risk flags | 0.10 | Measures adverse wick, late chase, overextension, failed retest, trap risk, conflicting features, and mixed-session warnings. |
| Data/provenance confidence | 0.05 | Measures source category, input mode, freshness, completed-candle status, timestamp coherence, and schema completeness. |

Hard overrides:

- Stale, thin, incomplete, malformed, mixed-timeframe, or missing-candle data should produce `LOW_CONFIDENCE_INSUFFICIENT_DATA`.
- Runtime use of a research source should produce `LOW_CONFIDENCE_INSUFFICIENT_DATA`.
- Wrong-side directional contradiction on a required candidate should produce `STRUCTURALLY_INVALID`.
- Missing required structural metadata should produce `LOW_CONFIDENCE_INSUFFICIENT_DATA` when the structure is unknowable, or `STRUCTURALLY_INVALID` when the structure is clearly failed.

## Outputs

The latest artifact is a single JSON object:

```text
outputs/track_b_execution_core/entry_acceptance/latest_entry_acceptance_state.json
```

Required top-level fields:

- `schema_version`: proposed `track_b_entry_acceptance_state_v1`
- `producer`: proposed `TrackBEntryAcceptanceLayer`
- `authority_mode`: `QUALITY_CONTEXT_ONLY`
- `artifact_path`
- `generated_at`
- `source_id`
- `input_source_path`
- `input_source_category`
- `input_mode`
- `source_provenance_status`
- `freshness_status`
- `latest_input_timestamp`
- `instrument`
- `candle_timeframe`
- `primary_timeframe`
- `candidate_timeframe`
- `context_timeframe`
- `exit_management_timeframe`
- `fast_reaction_timeframe`
- `timeframe_source`
- `base_timeframe_if_derived`
- `aggregation_method`
- `anchor_rule`
- `timeframe_alignment_status`
- `candidate_id`
- `candidate_family`
- `candidate_side`
- `acceptance_class`
- `acceptance_score`
- `entry_quality_context`
- `suggested_exit_profile_context`
- `initial_size_context`
- `confidence`
- `confidence_state`
- `supporting_reasons`
- `failure_reasons`
- `warnings`
- `dimension_scores`
- `feature_summary`
- `safety_flags`

`entry_quality_context` should include:

- `quality_label`: `STRONG`, `ACCEPTABLE`, `MARGINAL`, `INVALID`, or `LOW_CONFIDENCE`
- `structural_summary`
- `directional_summary`
- `session_summary`
- `volatility_summary`
- `pullback_retest_summary`
- `participation_summary`
- `risk_summary`

`suggested_exit_profile_context` is advisory only. It may include:

- `profile_hint`: examples include `STANDARD`, `TIGHTER_INVALIDATION`, `WIDER_STRUCTURE_REQUIRED`, `TIMEBOX_ONLY`, or `NO_EXIT_PROFILE_RECOMMENDED`
- `reason`
- `advisory_only`: `true`
- `authority_boundary`: `exit modules and lifecycle owners remain authoritative`

`initial_size_context` is advisory only. It may include:

- `size_quality_label`: examples include `NORMAL_CONTEXT`, `REDUCED_CONTEXT`, `MINIMUM_CONTEXT`, or `NO_SIZE_CONTEXT`
- `size_multiplier_hint`: bounded descriptive value such as `1.0`, `0.5`, or `0.0`, never an executable quantity
- `reason`
- `advisory_only`: `true`
- `authority_boundary`: `governance and exposure gates remain authoritative`

V1 must not emit executable quantity, target account, contract order details, order type, route, time-in-force, or submit-ready flags.

## Initial V1 Constraints

- Offline only.
- No strategy execution integration.
- No live broker or runtime dependency.
- No fitting, optimization, or learned thresholds.
- No lifecycle writes.
- No order intent reads or writes.
- No broker truth reads.
- No position, open-order, fill, submit, cancel, close, or flatten reads/writes.
- Operator UI consumption is display-only.
- Test fixtures may write temporary artifacts, but production/generated artifacts are not part of this design step.

## Fixture And Test Plan

Future implementation should include deterministic tests for:

- Exact match: all required predicates satisfied, expected `EXACT_STRUCTURAL_MATCH`.
- Near match: small tolerance miss with intact structure, expected `NEAR_STRUCTURAL_MATCH`.
- Degraded but valid: recognizable structure with weak retest, mixed participation, or volatility warning, expected `DEGRADED_BUT_VALID_MATCH`.
- Invalid: missing mandatory predicate, wrong-side direction, or impossible candle sequence, expected `STRUCTURALLY_INVALID`.
- Participation supportive: optional participation output supports candidate side and adds supporting reasons.
- Participation hostile: optional participation output conflicts with candidate side and adds failure reasons.
- Stale/thin data: insufficient completed primary-timeframe candles or stale latest timestamp, expected `LOW_CONFIDENCE_INSUFFICIENT_DATA`.
- Research-source runtime rejection: `input_source_category=RESEARCH` with `input_mode=RUNTIME_DECISION`, expected low confidence and no runtime eligibility.
- Safety boundary: import/symbol test proving no broker, strategy, app, `ibapi`, submit, cancel, close, flatten, `placeOrder`, lifecycle mutation, or order-intent creation path.
- Artifact write: writes only the configured JSON artifact and preserves all false safety flags.

Suggested test file:

```text
tests/unit/execution_core/test_track_b_entry_acceptance.py
```

## Future Layer Placeholder Specs

These are rough stubs only. They should not be implemented until reviewed and explicitly requested.

### 1. Data Readiness / Provenance Gate

- Purpose: Normalize and classify input readiness before quality layers consume market state. Prevent research artifacts, stale data, incomplete candles, mixed timeframes, or unknown sources from being treated as runtime truth.
- Inputs: source path/category, input mode, generated timestamp, latest candle timestamp, completed-candle status, expected timeframe, instrument, source id, and caller-declared purpose.
- Outputs: readiness class, freshness status, source provenance status, runtime eligibility fields, block reasons, warnings, and normalized provenance context.
- Authority boundary: Blocks or annotates data quality only. It does not authorize trades, create strategy decisions, or mutate lifecycle state.
- Future artifact path: `outputs/track_b_execution_core/data_readiness/latest_data_readiness_state.json`
- Integration point: Upstream of Entry Acceptance Layer, Participation Quality Layer, regime/session context, and any future strategy context builder.

### 2. Regime / Session Context

- Purpose: Describe the current session, market regime, and instrument context for candidate interpretation.
- Inputs: completed primary/context timeframe market context, session calendar labels, instrument metadata, volatility/range summaries, optional historical regime labels, timeframe schema, and provenance fields.
- Outputs: session bucket, regime label, range regime, trend/chop context, active window quality, warnings, confidence, and supporting/failure reasons.
- Authority boundary: Context-only. It does not enable a lane, change submit permission, or override strategy/governance rules.
- Future artifact path: `outputs/track_b_execution_core/regime_session/latest_regime_session_context.json`
- Integration point: Feeds Entry Acceptance scoring, future strategy runner context, operator display, and future exit profile selection.

### 3. Initial Sizing Context

- Purpose: Provide advisory pre-entry size context based on candidate quality, data confidence, session/regime fit, and risk warnings.
- Inputs: Entry Acceptance output, Data Readiness output, Regime/Session Context output, optional participation quality, and future governance/exposure read models marked as advisory inputs.
- Outputs: size quality label, size multiplier hint, size caution reasons, no-size reasons, confidence, and explicit advisory boundary.
- Authority boundary: Sizing is context-only here. Governance and exposure gates remain the sole authority for allowed size, caps, account constraints, and whether any trade is allowed.
- Future artifact path: `outputs/track_b_execution_core/initial_sizing/latest_initial_sizing_context.json`
- Integration point: Future strategy planning and operator explainability before entry. It must not write executable quantity, order intent, or broker route fields.

### 4. Dynamic Position Adjustment Context

- Purpose: Provide advisory scale/hold/reduce context after a position lifecycle owner exists and after position state is managed elsewhere.
- Inputs: lifecycle-owned position state, Entry Acceptance strength at entry, current participation quality, regime/session context, adverse/favorable excursion summaries, exit module context, and governance/exposure read models.
- Outputs: adjustment context label, scale-up/hold/reduce advisory reasons, confidence, risk warnings, and no-adjustment reasons.
- Authority boundary: Context-only. Dynamic size or scale action can only occur after position lifecycle ownership exists, and governance/exposure remains authoritative. This layer must not submit, cancel, close, flatten, or mutate lifecycle state.
- Future artifact path: `outputs/track_b_execution_core/dynamic_position_context/latest_dynamic_position_context.json`
- Integration point: Future lifecycle-owned position management and operator display after entry. It must remain downstream of broker-confirmed lifecycle ownership, not a substitute for it.

### 5. Exit Profile Selector / Modular Exit Layer

- Purpose: Select or describe advisory exit profile context based on acceptance strength, regime/session, participation, and position lifecycle state.
- Inputs: Entry Acceptance output, Regime/Session Context, Participation Quality output, lifecycle-owned position state when available, and approved exit module registry metadata.
- Outputs: suggested exit profile id or profile hint, rationale, invalidation context, timebox context, confidence, and warnings.
- Authority boundary: Advisory until explicitly promoted. Exit modules and lifecycle owners remain authoritative. This layer must not close, flatten, cancel, or submit.
- Future artifact path: `outputs/track_b_execution_core/exit_profile/latest_exit_profile_context.json`
- Integration point: Future modular exit layer, strategy runner display, operator explainability, and post-trade attribution.

### 6. Post-Trade Attribution / Learning Loop

- Purpose: Attribute outcomes back to entry acceptance, participation quality, regime/session context, sizing context, and exit profile context so future reviews can understand what worked and what failed.
- Inputs: completed trade/lifecycle artifacts, broker-confirmed fills, reconciliation outputs, Entry Acceptance artifact captured at entry, exit context, MFE/MAE/path summaries, and operator annotations.
- Outputs: attribution labels, feature snapshots, outcome summaries, false-positive/false-negative notes, review recommendations, and research-only learning records.
- Authority boundary: Research and audit only. It must not change runtime thresholds automatically, promote strategies, alter governance/exposure, or create live/paper authority.
- Future artifact path: `outputs/track_b_execution_core/post_trade_attribution/latest_post_trade_attribution_report.json`
- Integration point: Offline review, future research workbench, candidate taxonomy refinement, operator audit, and explicit promotion review.

### 7. Operator Explainability / Audit Artifacts

- Purpose: Make entry acceptance and adjacent context understandable to an operator without implying trade approval.
- Inputs: Entry Acceptance output, Data Readiness output, Regime/Session Context, Participation Quality output, future sizing context, future exit context, and post-trade attribution when available.
- Outputs: operator-facing summary, reason codes, supporting/failure reason lists, provenance/freshness panel, advisory-only badges, safety flag panel, and audit snapshot.
- Authority boundary: Display and audit only. The UI must not become source of truth, submit authority, lifecycle authority, or governance override.
- Future artifact path: `outputs/track_b_execution_core/operator_explainability/latest_operator_entry_explainability.json`
- Integration point: Operator UI, review reports, audit packs, and future design review workflows.

## Open Design Questions

- Should V1 accept exactly one candidate per artifact, or support a batch shape with `candidates[]` and a top-level summary?
- Should structural predicates be passed from Pattern Engine diagnostics, recomputed from raw metadata, or both?
- Should dimension weights be fixed constants in code, supplied as explicit config, or versioned in a small JSON schema?
- Should `suggested_exit_profile_context` begin as a free-form advisory object or a fixed enum tied to an approved exit module registry?
- Should `initial_size_context` expose only labels, or is a non-executable multiplier hint acceptable if governance remains authoritative?
