# ADR-002: Prospective Market Context Capture

## Status

Accepted for fixture-only Phase 1.

Real-candle population and CRR integration are not approved by this ADR.

## Decision Date

2026-08-06

## Context

The Research Evidence Explorer, INV-005, INV-006, and the prospective NQ cohort monitor can identify whether frozen NQ cohorts continue to hold up out of sample. They cannot yet explain future differences using decision-time market context because BA-002 shows that key fields are absent, validity-only, or not historically reconstructable.

Missing historical evidence must remain missing. The appropriate next step is a prospective context contract that can be validated using fixtures before any real durable candle population occurs.

## Decision

Adopt the DP-002 prospective market context architecture for fixture-only Phase 1.

The accepted v1 research anchor is CRR `entry_anchor.entry_time`, interpreted as first-opening-fill timestamp. This is a research proxy for context-at-first-fill, not true pre-decision context.

The accepted P0 fields are:

- `strategy_setup_family`
- `session`
- `vwap_relationship`
- `opening_range_position`
- `trend_state`
- `volatility_state`

## Timestamp And Durability

Context records use one authoritative decision timestamp:

- Source artifact: `outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_records.jsonl`
- Source field: `entry_anchor.entry_time`
- Timezone: UTC
- Precision: source precision preserved
- Meaning: first opening fill

A candle may be used only when:

- candle `close_time <= decision_timestamp`; and
- candle `source_generated_at`, durability watermark, or equivalent authoritative persistence timestamp is also `<= decision_timestamp`.

If no authoritative durability timestamp exists, exact-boundary inclusion is prohibited. The producer must not infer that the candle existed at decision time.

## Candle Cutoffs

1m and 5m cutoffs are separate. Inclusion of a 1m candle never implies inclusion of a 5m candle.

At exact candle-close boundaries, the candle is usable only when durable completion is proven at or before the decision timestamp.

## Contract Identity

V1 uses literal contract-specific candles only.

Accepted statuses:

- `CONTRACT_MATCH_VALID`
- `ROLL_ADJACENT`
- `INSUFFICIENT_SAME_CONTRACT_HISTORY`
- `CONTRACT_IDENTITY_MISMATCH`

Continuous series and cross-roll calculations are not approved for v1.

## VWAP Contract

VWAP is session-specific and literal-contract-specific.

Accepted v1 identifiers:

- `NQ_GLOBEX_SESSION_VWAP_V1`
- `NQ_US_RTH_SESSION_VWAP_V1`
- `NQ_LONDON_SESSION_VWAP_V1`

VWAP uses 1m completed candles, typical price `(high + low + close) / 3`, durable volume, and the applicable session reset. `NEAR` is absolute distance less than or equal to `2.0` NQ points.

## Opening-Range Contract

Supported v1 opening ranges:

- `NQ_GLOBEX_OPENING_RANGE_30M_V1`
- `NQ_US_RTH_OPENING_RANGE_30M_V1`

London opening range is not supported in v1 and returns `NOT_APPLICABLE` until a governed London calendar proposal exists.

Opening-range states are mutually exclusive and exhaustive for supported sessions once the range is valid.

## Trend Classifier

Accepted classifier: `NQ_TREND_STATE_5M_LINEAR_SLOPE_V1`.

The classifier uses 12 completed 5m candles sorted oldest to newest. Index `0` is oldest and index `11` is most recent. Ordinary least-squares slope is measured in NQ points per 5m bar. Rising prices produce positive slope.

Thresholds:

- `UP`: slope >= `+3.0`
- `DOWN`: slope <= `-3.0`
- `SIDEWAYS`: otherwise

## Volatility Classifier

Accepted classifier: `NQ_VOLATILITY_STATE_5M_ATR_RATIO_V1`.

True range is:

```text
max(
  high - low,
  abs(high - previous_close),
  abs(low - previous_close)
)
```

The previous close must be from the immediately preceding completed same-contract candle.

The classifier requires 85 completed 5m same-contract candles:

- one predecessor candle;
- 72 prior baseline candles;
- 12 current candles.

Baseline and current windows do not overlap.

Thresholds:

- `LOW`: ratio <= `0.75`
- `NORMAL`: `0.75 < ratio < 1.25`
- `HIGH`: ratio >= `1.25`

## Status Model

Record-level statuses:

- `VALID`
- `INVALID_DECISION_TIMESTAMP`
- `INVALID_CONTRACT_IDENTITY`
- `INVALID_SOURCE_PROVENANCE`

Field-level statuses:

- `VALID`
- `MISSING`
- `INVALID`
- `STALE`
- `NOT_YET_DEFINED`
- `NOT_APPLICABLE`
- `INSUFFICIENT_HISTORY`

Any invalid record-level status prevents field computation.

## CRR Boundary

Future CRR integration is reference-only for v1:

- `market_context_ref`
- context fingerprint
- per-field status summaries

No duplicated market-context values in CRR are approved by this ADR.

## Authority Boundaries

This ADR introduces no broker, runtime, Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, production recommendation, or trading-gate authority.

The fixture-only producer must not read real durable candles, live runtime state, broker state, or CRR artifacts.

## Approved Phase

Fixture-only Phase 1 may proceed:

- schema and enums;
- record and field status model;
- pure fixture classifiers;
- no-look-ahead fixture enforcement;
- contract identity fixtures;
- deterministic fixture/demo generation.

## Later-Phase Gates

Real-candle population may begin only after:

- exact decision-anchor audit passes;
- literal contract candle identity is proven;
- no-look-ahead boundary tests pass;
- fixture-only Phase 1 validation is clean.

CRR integration may begin only after real-candle population validation is clean and remains reference-only.

## Consequences

Positive:

- future NQ cohort monitoring can gain explainable context without historical fabrication;
- classifier definitions become reproducible before implementation touches real data;
- authority boundaries remain intact.

Costs:

- first-opening-fill is a weaker research anchor than true pre-decision timestamp;
- literal-contract history may produce `INSUFFICIENT_HISTORY` near rolls;
- fixture-only Phase 1 does not yet improve real prospective coverage.

## Deferred Questions

- Whether a future pre-fill decision anchor should replace first-opening-fill.
- Whether a governed continuous NQ series should be introduced.
- Whether AVWAP, GRE, CRFD, and richer regime fields should become P1.
- Whether CRR should ever materialize context cache values after v1.

## Related Documents

- [DP-002: Prospective Market Context Capture](../proposals/DP-002-prospective-market-context-capture.md)
- [RR-002: DP-002 Review Resolution](../reviews/RR-002-DP-002-prospective-market-context-capture.md)
- [BA-002: Prospective Market Context Coverage](../baselines/BA-002-prospective-market-context-coverage.md)
- [ADR-001: Canonical Research Record](ADR-001-canonical-research-record.md)
