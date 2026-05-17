# Track B Regime State Layer v1 Design

Status: design / contract slice only. No runtime writer, runtime producer, strategy integration, broker action, lane execution, or generated output is introduced by this document.

## 1. Purpose

The Track B Regime State Layer provides reusable advisory context about the market environment surrounding a candidate or open lifecycle-managed position.

The v1 contract name is `track_b_regime_session_context_v1`.

The layer describes:

- session context, including fine-grained and coarse session buckets.
- volatility and range context, including low, normal, high, compressed, and expanded range behavior.
- trend/chop context, including directional trend, balanced chop, compression, and expansion.
- liquidity or thinness context when volume, range, freshness, or candle quality suggests low-confidence interpretation.

The layer is context-only. It is designed to be consumed by:

- Entry Acceptance, for timing/session fit and volatility/range fit.
- Exit Context, for patience, urgency, and session-risk interpretation.
- Lifecycle Awareness, for explaining whether progress, pullback, stall, or adverse movement is regime-consistent.
- future sizing and position-management layers, as a risk multiplier context only.
- Operator UI and research explainability.

The layer must not create trades, authorize entries, authorize exits, mutate lifecycle state, override governance, or change strategy behavior.

## 2. Inputs

V1 inputs must be explicit, validated, and advisory-safe:

- completed primary-timeframe OHLCV candles.
- completed context-timeframe candles or completed-bar summaries when supplied.
- `session_phase_labels` utility output, or timestamps that can be labeled with that utility.
- ATR, range, body, close-location, and volume fields where available.
- timeframe metadata, including primary evaluation timeframe, optional context timeframe, timeframe source, aggregation method, anchor rule, and alignment status.
- freshness metadata, including generated timestamp and latest completed candle timestamp.
- provenance metadata, including input source path, input source category, input mode, source id, instrument, and dataset when available.
- optional daily or VIX research regime fields later, read as advisory enrichment only.

Research-derived regime fields may be included later only when clearly marked as research/offline context. Research sources must not be treated as runtime truth.

Broker and lifecycle records are not required v1 inputs. If supplied later, they may be used only as read-only safety or attribution context and must not create authority.

## 3. Output Contract

The top-level output contract is `track_b_regime_session_context_v1`.

Required v1 fields:

- `schema_version`: `track_b_regime_session_context_v1`.
- `producer`: stable producer name for the evaluator.
- `authority_mode`: `ADVISORY_REGIME_CONTEXT_ONLY`.
- `instrument`.
- `source_id`.
- `generated_at`.
- `input_source_path`.
- `input_source_category`.
- `input_mode`.
- `source_provenance_status`.
- `freshness_status`.
- `primary_timeframe`.
- `context_timeframe`.
- `timeframe_source`.
- `base_timeframe_if_derived`.
- `aggregation_method`.
- `anchor_rule`.
- `timeframe_alignment_status`.
- `latest_candle_timestamp`.
- `candles_received`.
- `completed_candles_used`.
- `session_bucket`.
- `session_phase`.
- `coarse_session_group`.
- `market_regime_state`.
- `volatility_range_state`.
- `trend_chop_state`.
- `liquidity_state`.
- `directional_context`.
- `confidence`.
- `regime_reasons`.
- `warning_reasons`.
- `failure_reasons`.
- `feature_summary`.

Safety flags must always be false:

- `strategy_authority=false`
- `broker_state_mutated=false`
- `submit_attempted=false`
- `order_intent_created=false`
- `lifecycle_mutated=false`
- `runtime_trade_eligible=false`

The output may include future read-only enrichment fields such as `daily_vix_context`, `event_risk_context`, or `historical_regime_context`, but those fields must not change authority semantics.

## 4. Initial State Taxonomy

Session examples:

- `SESSION_SESSION_OPEN`
- `SESSION_ASIA_EARLY`
- `SESSION_ASIA_LATE`
- `SESSION_LONDON_OPEN`
- `SESSION_LONDON_LATE`
- `SESSION_US_EARLY`
- `SESSION_US_PREOPEN_OPENING`
- `SESSION_US_CASH_OPEN_IMPULSE`
- `SESSION_US_OPEN_LATE`
- `SESSION_US_MIDDAY`
- `SESSION_US_LATE`
- `SESSION_UNCLASSIFIED`

Market regime examples:

- `REGIME_TRENDING`
- `REGIME_CHOP_BALANCED`
- `REGIME_EXPANSION`
- `REGIME_COMPRESSION`
- `REGIME_THIN_OR_STALE`
- `REGIME_LOW_CONFIDENCE`

Volatility/range examples:

- `VOL_LOW`
- `VOL_NORMAL`
- `VOL_HIGH`
- `RANGE_COMPRESSED`
- `RANGE_NORMAL`
- `RANGE_EXPANDED`
- `RANGE_THIN_OR_INVALID`

Trend/chop examples:

- `TREND_UP`
- `TREND_DOWN`
- `TREND_MIXED`
- `CHOP_BALANCED`
- `CHOP_DIRECTIONLESS`
- `COMPRESSION_COILING`
- `EXPANSION_DIRECTIONAL`
- `LOW_CONFIDENCE_TREND_CHOP`

Liquidity examples:

- `LIQUIDITY_NORMAL`
- `LIQUIDITY_THIN_RANGE`
- `LIQUIDITY_THIN_VOLUME`
- `LIQUIDITY_STALE_OR_INCOMPLETE`
- `LIQUIDITY_UNKNOWN`

Directional context examples:

- `DIRECTIONAL_BULLISH`
- `DIRECTIONAL_BEARISH`
- `DIRECTIONAL_BALANCED`
- `DIRECTIONAL_MIXED`
- `DIRECTIONAL_UNKNOWN`

V1 implementation should keep the taxonomy intentionally small and explainable. Additional buckets can be added after tests show they improve consumer clarity without becoming strategy logic.

## 5. Fail-Closed Rules

The layer must fail closed to low-confidence advisory context when input quality is not sufficient.

Fail-closed conditions include:

- stale completed candle data.
- incomplete candles in the evaluation window.
- mixed timeframe candles.
- missing timeframe metadata.
- ambiguous derived timeframe metadata.
- missing provenance.
- insufficient completed rows.
- malformed candle fields or invalid OHLC geometry.
- zero-range or extremely thin-range candles that make regime classification unreliable.
- research source presented as runtime truth.
- future-dated latest candle timestamp.
- duplicated or out-of-order candle timestamps.

Fail-closed output must:

- set `market_regime_state` to `REGIME_LOW_CONFIDENCE` or `REGIME_THIN_OR_STALE`.
- set `liquidity_state` to `LIQUIDITY_STALE_OR_INCOMPLETE`, `LIQUIDITY_THIN_RANGE`, or `LIQUIDITY_UNKNOWN` as appropriate.
- set `confidence` to `0.0` or a low bounded value.
- include concrete `failure_reasons`.
- preserve all safety flags as false.
- avoid producing any runtime eligibility, strategy authority, order intent, lifecycle mutation, or broker action.

Expected failure reason constants include:

- `THIN_DATA`
- `STALE_INPUT`
- `INCOMPLETE_CANDLES`
- `MIXED_TIMEFRAME`
- `MISSING_TIMEFRAME_SCHEMA`
- `AMBIGUOUS_TIMEFRAME`
- `MALFORMED_CANDLES`
- `LOW_RANGE_QUALITY`
- `MISSING_PROVENANCE`
- `RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE`

## 6. Consumer Mapping

### Entry Acceptance

Entry Acceptance can use Regime State for advisory scoring dimensions:

- `timing_session_fit`
- `volatility_range_fit`
- session-window explanation
- range-expansion or compression warnings
- degraded-but-valid context when structure is present but regime fit is weak

Regime State must not decide whether a candidate is accepted. It provides context that Entry Acceptance may score within its own advisory contract.

### Exit Context

Exit Context can use Regime State to explain patience and urgency:

- high-volatility expansion may increase urgency after adverse movement.
- low-volatility compression may explain a stall differently from failed participation.
- session boundary proximity may reduce patience.
- thin or stale regime context must fail closed and lower confidence.

Regime State must not select or trigger an exit profile by itself.

### Lifecycle Awareness

Lifecycle Awareness can use Regime State to interpret trade state:

- favorable expansion can be understood as regime-consistent continuation.
- healthy pullback can be distinguished from chop or adverse expansion.
- stalled positions in compression can be labeled differently from stalled positions in directional expansion.
- defensive management can include session/regime reasons without creating action authority.

Lifecycle Awareness remains the lifecycle classifier; Regime State is explanatory context.

### Participation Quality

Participation Quality and Regime State are complementary.

Participation Quality describes pressure and continuation quality from recent completed bars. Regime State describes broader session, volatility/range, trend/chop, and liquidity context. The two layers may share raw candle-derived features, but neither should duplicate the other's contract or override the other's output.

### Future Sizing

Future sizing and position-management layers may consume Regime State as a risk multiplier context only, such as:

- reduce confidence in thin or stale regimes.
- avoid add-size context in chop or adverse expansion.
- mark high-volatility expansion as requiring extra review.

Regime State must never output executable size, broker quantity, order intent, or permission to trade.

## 7. Artifact Path

Proposed advisory artifact path:

`outputs/track_b_execution_core/regime_session/latest_regime_session_context.json`

This is design only. No runtime writer, CLI writer, scheduler, launchd job, strategy runner integration, or runtime producer is introduced in this slice.

## 8. Later Implementation Plan

### Slice 1: Pure Offline Evaluator

- Add `src/mgc_v05l/execution_core/track_b_regime_state.py`.
- Implement enums/constants/schema helpers.
- Accept explicit payloads only.
- Validate completed candles, timeframe metadata, freshness, and provenance.
- Classify session bucket, volatility/range state, trend/chop state, liquidity state, directional context, and confidence.
- Emit fail-closed advisory output with safety flags false.
- Add unit tests and fixtures for trending, chop, expansion, compression, low volatility, high volatility, stale, mixed timeframe, incomplete candles, thin data, and research-as-runtime rejection.

### Slice 2: Offline CLI And Fixtures

- Add an explicit JSON-input CLI.
- Write `latest_regime_session_context.json` only to a user-specified output directory.
- Print compact JSON summary.
- Keep CLI offline/advisory only.
- Add small fixtures covering common sessions and fail-closed paths.

### Slice 3: Consumer Contract Alignment

- Add optional pass-through or normalized consumption in Entry Acceptance, Exit Context, Lifecycle Awareness, and future sizing.
- Preserve all existing behavior unless a consumer explicitly opts into advisory context.
- Add tests proving consumers do not gain strategy authority or runtime eligibility.

### Slice 4: Runtime Producer Design

- Design runtime/backfill producer separately before any implementation.
- Define source ownership, freshness windows, replay/backfill behavior, operator display, retention, and no-action safety checks.
- Do not promote research regime outputs into runtime truth without a dedicated provenance and backfill design.
