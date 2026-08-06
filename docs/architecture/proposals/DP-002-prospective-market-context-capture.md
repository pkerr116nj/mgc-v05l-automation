# DP-002: Prospective Market Context Capture

Status: Draft; revised after focused rereview. Approved with minor revisions for ADR-002 and fixture-only Phase 1 only. Real-candle population and CRR integration remain blocked.

## Problem Statement

INV-005 and INV-006 identified NQ cohorts worth prospective monitoring, but BA-002 shows that several fields needed to explain future cohort behavior are absent, validity-only, or not defensibly reconstructable from existing historical artifacts.

The goal is to capture future decision-time context for qualified NQ trades without fabricating unavailable historical evidence and without changing strategy behavior, broker authority, runtime authority, Managed Exit, Guardian, Safe-State, readiness, reconciliation, services, or trading gates.

This proposal defines the smallest prospective context-capture contract needed to explain future NQ cohort performance. It authorizes only fixture-only schema/classifier validation after ADR-002 acceptance.

## BA-002 Baseline Summary

BA-002 records the current baseline:

- Strategy/setup family is available through CRR entry anchors.
- Session is available through CTOE context validity summaries materialized into CRR.
- Regime validity state exists, but rich regime explanation is limited.
- GRE and CRFD are available mainly as validity classifications where CTOE provides them.
- RA8/path evidence is optional and coverage-limited.
- VWAP relationship, AVWAP relationship, opening-range position, trend state, slope, curvature, and volatility state are absent or insufficient in CRR v1.
- Historical reconstruction is not defensible for fields absent from CRR or upstream artifacts.

Related baseline: [BA-002](../baselines/BA-002-prospective-market-context-coverage.md).

## Minimum P0 Field Set

P0 is unchanged by review and remains limited to the fields required to explain frozen NQ prospective cohorts at the first 10-trade checkpoint.

| Field | Research Question | Producer | Decision-Time Availability | Instrument / Timeframe | Historical Backfill | CRR Treatment | Cost | ROI |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `strategy_setup_family` | Which setup family produced the trade? | Existing durable decision anchor metadata | Already available if anchor exists | NQ decision anchor | Not needed | Reference/status only | Low | High |
| `session` | Did performance differ by session? | Existing session classifier / CTOE context summary | Already available if anchor exists | NQ decision timestamp | Not needed | Reference/status only | Low | High |
| `vwap_relationship` | Was entry above, below, or near session VWAP? | Prospective context producer from completed literal-contract candles | Available only after required completed candles exist | NQ, 1m source summarized to decision timeframe | Prohibited unless source existed at decision time | Reference/status only | Medium | High |
| `opening_range_position` | Was entry inside, above, below, or extending from the applicable opening range? | Prospective context producer from completed literal-contract candles | Available only after applicable opening range completes | NQ, session-specific 5m lead surface | Prohibited for missing historical rows | Reference/status only | Medium | High |
| `trend_state` | Was entry aligned with reproducible local trend state? | Prospective context producer from completed 5m candles | Available with required completed 5m history | NQ, 5m lead surface | Prohibited for missing historical rows | Reference/status only | Medium | High |
| `volatility_state` | Did cohort behavior depend on volatility expansion/compression? | Prospective context producer from completed 5m candles | Available with required completed 5m history and baseline window | NQ, 5m lead surface | Prohibited for missing historical rows | Reference/status only | Medium | High |

P0 deliberately excludes AVWAP, GRE, CRFD, slope, and curvature as first-class v1 fields.

## P1 Fields

| Field | Reason To Defer |
| --- | --- |
| `AVWAP_relationship` | High value, but anchor definitions must be governed before capture. |
| `directional_participation_state` | Potentially useful, but strategy/setup family and trend state should be tested first. |
| `GRE_status` | Useful if the existing producer can emit fresh decision-time state without runtime coupling. |
| `CRFD_status` | Useful if source freshness and validity can be preserved prospectively. |
| `regime` | Valuable, but should be linked to a clear regime producer schema rather than inferred from validity labels. |

## P2 Fields

| Field | Reason To Defer |
| --- | --- |
| `slope` | Methodology is included inside `trend_state_v1` and should not be separately exposed until needed. |
| `curvature` | Higher overfitting risk and unclear first-checkpoint value. |
| Additional multi-anchor AVWAP variants | Requires anchor governance and could easily sprawl. |

## Decision Timestamp Authority

V1 uses one authoritative decision timestamp:

- Source artifact: `outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_records.jsonl`.
- Source schema field: `entry_anchor.entry_time`.
- Timezone: ISO-8601 UTC, normalized with explicit offset.
- Precision: source precision is preserved; comparisons use microsecond-capable timestamp parsing.
- Meaning: first opening fill timestamp, not signal creation, final strategy decision, submit intent, or broker submission.
- Rationale: CRR v1 already reconciles entry anchor values against Canonical Trade Records; the producer is research-only and must not choose among competing runtime timestamps.

This is a research decision-time proxy, not proof of signal timestamp. Any interpretation must disclose that the context is anchored to first opening fill unless a later architecture decision introduces an authoritative pre-fill decision anchor.

Clock-skew and contradiction handling:

- If `entry_anchor.entry_time` is missing, malformed, timezone-naive, or contradictory with the source provenance, the record status is `INVALID_DECISION_TIMESTAMP`.
- The producer must not fall back to submit time, bar time, lifecycle time, or fill-adjacent timestamps.
- If a future exact pre-fill decision anchor is approved, that change is breaking and requires a new classifier/producer version.

## Per-Timeframe And Per-Field Candle Cutoffs

The proposal replaces a single global cutoff with `completed_candle_cutoffs` keyed by timeframe.

Boundary rule:

- For 1m evidence, include only candles whose close timestamp is strictly less than or equal to `decision_timestamp`.
- For 5m evidence, include only candles whose close timestamp is strictly less than or equal to `decision_timestamp`.
- A candle may be used only when `close_time <= decision_timestamp` and the candle `source_generated_at`, durability watermark, or equivalent authoritative persistence timestamp is also `<= decision_timestamp`.
- At an exact candle-close boundary, the candle is considered usable only if both the close-time rule and durability proof rule pass.
- If no authoritative durability timestamp exists, exact-boundary inclusion is prohibited.
- The producer must not infer that a candle existed at decision time.
- If source semantics cannot prove availability, status is `MISSING`, `STALE`, or `INVALID` according to the evidence rather than included heuristically.

Time and calendar rules:

- Candle timestamps are interpreted in UTC.
- Session classification uses the exchange/session calendar for NQ and records the calendar identifier and timezone.
- 1m and 5m cutoffs are computed separately.
- Field records may reference either `completed_candle_cutoffs["1m"]`, `completed_candle_cutoffs["5m"]`, or both.

Required tests:

- decision exactly at 1m close includes that 1m candle only when durable completion is proven;
- close time before decision but `source_generated_at` after decision excludes the candle;
- close time equal to decision and `source_generated_at` equal to or before decision includes the candle;
- close time equal to decision and `source_generated_at` after decision excludes the candle;
- decision one microsecond before 1m close excludes that 1m candle;
- decision exactly at 5m close includes that 5m candle only when durable completion is proven;
- decision one microsecond before 5m close excludes that 5m candle;
- 1m inclusion must not imply 5m inclusion.

## Market-Data Identity And Contract-Roll Policy

V1 uses literal contract-specific candles. Continuous series are prohibited unless a future architecture decision governs the continuous series construction.

Each context record must preserve:

- instrument: `NQ`;
- `local_symbol`;
- contract;
- `con_id`;
- expiry;
- source candle series identity;
- candle source artifact path and fingerprint;
- roll status.

Contract selection:

- The expected contract identity is taken from CRR `trade_identity.contract`, `trade_identity.con_id`, and `trade_identity.instrument`.
- Candle rows must match instrument, local symbol or contract, and `con_id` where available.
- If `con_id` is unavailable in candle data, the source must provide a documented exact local-symbol contract identity. Otherwise the field is `INVALID`.

Roll policy:

- Calculations crossing contracts are prohibited in v1.
- If the decision occurs near a roll boundary but same-contract history is sufficient, roll status is `ROLL_ADJACENT`.
- If the required lookback crosses into a prior contract, status is `INSUFFICIENT_SAME_CONTRACT_HISTORY`.
- If candle identity does not match the CRR trade identity, status is `CONTRACT_IDENTITY_MISMATCH` and field status is `INVALID`.

Contract identity statuses:

- `CONTRACT_MATCH_VALID`
- `ROLL_ADJACENT`
- `INSUFFICIENT_SAME_CONTRACT_HISTORY`
- `CONTRACT_IDENTITY_MISMATCH`

## VWAP Relationship Contract

V1 VWAP is session-specific and literal-contract-specific.

VWAP identifiers:

- `NQ_GLOBEX_SESSION_VWAP_V1`
- `NQ_US_RTH_SESSION_VWAP_V1`
- `NQ_LONDON_SESSION_VWAP_V1`

Methodology:

- Calendar/timezone: CME equity-index futures calendar, interpreted from UTC timestamps with session definitions recorded by identifier.
- Anchor/reset: reset at the configured session start for the applicable session identifier.
- Price input: candle typical price `(high + low + close) / 3`.
- Volume source: durable 1m candle volume.
- Decision-time cutoff: only 1m candles with close time less than or equal to the decision timestamp and matching literal contract identity.
- Relationship states:
  - `ABOVE`: decision reference price is more than `2.0` NQ points above VWAP.
  - `BELOW`: decision reference price is more than `2.0` NQ points below VWAP.
  - `NEAR`: absolute distance is less than or equal to `2.0` NQ points.
- Distance units: NQ points, rounded to six decimals.
- Decision reference price: CRR `entry_anchor.entry_price`.

Holiday and shortened-session behavior:

- If the session calendar marks the session closed, status is `NOT_APPLICABLE`.
- If the session is shortened, VWAP uses the shortened session boundaries.
- If no completed session candles exist yet, status is `NOT_YET_DEFINED`.
- If required volume is absent, status is `MISSING`.

No generic “session VWAP” is allowed without a VWAP identifier.

## Opening-Range Position Contract

V1 defines opening range per supported session.

| Range Identifier | Session | Calendar/Timezone | Start | End | Duration | Required Candles |
| --- | --- | --- | --- | --- | --- | --- |
| `NQ_GLOBEX_OPENING_RANGE_30M_V1` | GLOBEX | CME equity-index futures session calendar | Globex session open | open + 30 minutes | 30 minutes | six completed 5m candles |
| `NQ_US_RTH_OPENING_RANGE_30M_V1` | US | CME equity-index futures RTH calendar | 09:30 America/New_York | 10:00 America/New_York | 30 minutes | six completed 5m candles |

London is not a supported opening range in v1 because there is no governed London session-calendar artifact in this proposal. London returns `NOT_APPLICABLE` until a future governed proposal defines calendar identifier, open time, timezone, DST handling, holiday handling, and range completion rules.

Position semantics:

- `ABOVE`: entry price is above range high.
- `BELOW`: entry price is below range low.
- `INSIDE`: entry price is between range low and range high inclusive.
- `EXTENDING_ABOVE`: entry price is above range high and the latest completed 5m candle close is also above range high.
- `EXTENDING_BELOW`: entry price is below range low and the latest completed 5m candle close is also below range low.

Opening-range states are mutually exclusive and exhaustive for supported sessions once the range is valid: extending states take precedence over plain `ABOVE`/`BELOW`, and `INSIDE` applies only when neither extending nor outside-range states apply.

Before the applicable opening range completes, status is `NOT_YET_DEFINED`.

Unsupported sessions return `NOT_APPLICABLE`. Missing bars within the opening range return `MISSING` unless the source explicitly marks valid no-trade synthetic candles. Early closes and holidays follow the session calendar; if the session is closed, status is `NOT_APPLICABLE`.

## Trend-State Methodology

Classifier: `NQ_TREND_STATE_5M_LINEAR_SLOPE_V1`.

Method:

- Timeframe: 5m lead surface.
- Required candles: 12 completed 5m candles ending at `completed_candle_cutoffs["5m"]`.
- Measure: ordinary least-squares slope of close price over candle index `0..11`.
- Candle ordering: the 12 completed 5m candles are sorted oldest to newest; index `0` is oldest and index `11` is most recent.
- Sign convention: rising closes produce positive slope.
- Units: NQ points per 5m bar.
- Thresholds:
  - `UP`: slope >= `+3.0` points per 5m bar.
  - `DOWN`: slope <= `-3.0` points per 5m bar.
  - `SIDEWAYS`: otherwise.
- Tie handling: exact threshold values classify as directional (`UP` or `DOWN`).
- Boundary handling: no candle with close after the 5m cutoff may be used.
- Insufficient same-contract history returns `INSUFFICIENT_HISTORY`.
- Roll-adjacent but sufficient same-contract history may be `VALID` with contract roll status `ROLL_ADJACENT`.

The slope value may be included inside the field payload as method evidence, but `slope` is not a separate P0 field.

Required hand-computed fixture: closes `[100, 103, 106, ... 133]` over indices `0..11` produce slope `+3.0` points per 5m bar and state `UP`.

## Volatility-State Methodology

Classifier: `NQ_VOLATILITY_STATE_5M_ATR_RATIO_V1`.

Method:

- Timeframe: 5m.
- True range: `max(high - low, abs(high - previous_close), abs(low - previous_close))`.
- `previous_close` must be from the immediately preceding completed same-contract candle.
- No cross-contract predecessor close is allowed.
- Current measure: mean true range over the most recent 12 completed 5m candles.
- Baseline measure: median of rolling 12-candle mean true ranges over the prior 72 completed 5m candles for the same literal contract.
- Required history: 85 completed 5m candles:
  - one predecessor candle for the earliest true-range calculation;
  - 72 prior baseline candles;
  - 12 current candles.
- Window boundaries: after the predecessor candle, candles `1..72` form the baseline true-range population and candles `73..84` form the current true-range population. Baseline and current windows do not overlap.
- Ratio: `current_atr_12 / baseline_median_atr_12`.
- Thresholds:
  - `LOW`: ratio <= `0.75`.
  - `NORMAL`: `0.75 < ratio < 1.25`.
  - `HIGH`: ratio >= `1.25`.
- Boundary handling: exact threshold values classify as `LOW` or `HIGH`.
- Insufficient same-contract history returns `INSUFFICIENT_HISTORY`.
- Roll-adjacent but sufficient same-contract history may be `VALID` with contract roll status `ROLL_ADJACENT`.

Thresholds are versioned and must not be implementation-defined.

Required hand-computed fixture: a same-contract 85-candle fixture with baseline true ranges of `4.0` and current true ranges of `6.0` produces ratio `1.5` and state `HIGH`.

## Status Model

Field statuses:

- `VALID`: evidence exists, matches identity, is fresh, and satisfies the field contract.
- `MISSING`: evidence was expected but absent.
- `INVALID`: evidence exists but violates schema, identity, timestamp, or join contract.
- `STALE`: source exists but exceeds allowed freshness.
- `NOT_YET_DEFINED`: the field cannot yet be computed at that point in the session.
- `NOT_APPLICABLE`: the field does not apply to the session/setup/calendar state.
- `INSUFFICIENT_HISTORY`: correct source exists but required lookback is unavailable.

Applicability:

- `strategy_setup_family`: `VALID`, `MISSING`, `INVALID`.
- `session`: `VALID`, `MISSING`, `INVALID`, `NOT_APPLICABLE`.
- `vwap_relationship`: `VALID`, `MISSING`, `INVALID`, `STALE`, `NOT_YET_DEFINED`, `NOT_APPLICABLE`, `INSUFFICIENT_HISTORY`.
- `opening_range_position`: `VALID`, `MISSING`, `INVALID`, `STALE`, `NOT_YET_DEFINED`, `NOT_APPLICABLE`.
- `trend_state`: `VALID`, `MISSING`, `INVALID`, `STALE`, `INSUFFICIENT_HISTORY`.
- `volatility_state`: `VALID`, `MISSING`, `INVALID`, `STALE`, `INSUFFICIENT_HISTORY`.

No missing, stale, invalid, or insufficient state may be treated as favorable.

Record-level statuses:

- `VALID`: the record has valid decision timestamp, contract identity, and source provenance, so fields may be computed.
- `INVALID_DECISION_TIMESTAMP`: the authoritative decision timestamp is missing, malformed, timezone-naive, or contradictory.
- `INVALID_CONTRACT_IDENTITY`: contract identity cannot be matched exactly or violates the literal-contract policy.
- `INVALID_SOURCE_PROVENANCE`: required source provenance is missing or inconsistent.

Any invalid record-level status prevents field computation. Field outputs must be omitted or marked `INVALID` with an explicit reason.

Compatibility:

| Record / Roll Status | Field Computation |
| --- | --- |
| `VALID` + `CONTRACT_MATCH_VALID` | Fields may be `VALID` if field evidence is complete. |
| `VALID` + `ROLL_ADJACENT` | Fields may be `VALID` only when same-contract history is sufficient. |
| `VALID` + `INSUFFICIENT_SAME_CONTRACT_HISTORY` | Lookback-dependent fields return `INSUFFICIENT_HISTORY`. |
| `INVALID_CONTRACT_IDENTITY` or `CONTRACT_IDENTITY_MISMATCH` | All candle-derived fields are `INVALID` or omitted. |

## Producer And Data-Flow Design

V1 reads already-existing durable decision anchors and durable candles. It creates no new runtime-hot-path emission.

Proposed flow:

1. Load CRR rows and research eligibility records.
2. Select qualified NQ rows at or after the prospective start.
3. Read the authoritative decision timestamp from CRR `entry_anchor.entry_time`.
4. Resolve literal contract-specific candle identity from CRR trade identity.
5. Compute fixture-only classifier outputs in Phase 1.
6. After approval, populate real durable candle context records without changing runtime or broker state.
7. After validation, add CRR reference-only integration.

No research producer reads live mutable runtime state. If existing durable anchors prove insufficient, implementation stops and returns to Architecture Track.

## Schema Contract

Proposed record shape:

```json
{
  "schema_version": "prospective_market_context_v1",
  "producer_version": "prospective_market_context_producer_v1",
  "context_record_id": "deterministic_id",
  "record_status": "VALID",
  "generated_at": "2026-08-07T14:36:00+00:00",
  "observed_at": "2026-08-07T14:35:12.123456+00:00",
  "research_record_id": "canonical_research_record_id_if_available",
  "source_trade_id": "canonical_trade_source_id",
  "instrument": "NQ",
  "local_symbol": "NQU6",
  "contract": "NQU6",
  "con_id": 770561204,
  "expiry": "202609",
  "decision_timestamp": {
    "value": "2026-08-07T14:35:12.123456+00:00",
    "source_artifact": "outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_records.jsonl",
    "source_schema_version": "canonical_research_record_v1",
    "source_field": "entry_anchor.entry_time",
    "meaning": "first_opening_fill_timestamp",
    "timezone": "UTC",
    "precision": "microsecond",
    "status": "VALID"
  },
  "candle_series_identity": {
    "policy": "literal_contract_specific",
    "source_artifact": "durable_candle_source_path",
    "source_schema_version": "durable_candle_schema_version",
    "source_fingerprint": "sha256",
    "durability_timestamp_field": "source_generated_at",
    "contract_identity_status": "CONTRACT_MATCH_VALID",
    "roll_status": "CONTRACT_MATCH_VALID"
  },
  "exchange_calendar": {
    "calendar_id": "CME_EQUITY_INDEX_FUTURES_V1",
    "timezone": "America/New_York",
    "session_id": "US"
  },
  "completed_candle_cutoffs": {
    "1m": "2026-08-07T14:35:00+00:00",
    "5m": "2026-08-07T14:35:00+00:00"
  },
  "fields": {
    "strategy_setup_family": {
      "status": "VALID",
      "state": "PAPER_ACTIVE_EVIDENCE_NQ_US_PARTICIPATION_LONG_V1",
      "classifier_version": "strategy_setup_family_from_crr_entry_anchor_v1"
    },
    "session": {
      "status": "VALID",
      "state": "US",
      "classifier_version": "session_from_ctoe_context_summary_v1"
    },
    "vwap_relationship": {
      "status": "VALID",
      "state": "ABOVE",
      "distance_points": 3.25,
      "vwap_identifier": "NQ_US_RTH_SESSION_VWAP_V1",
      "field_cutoff_ref": "completed_candle_cutoffs.1m",
      "classifier_version": "NQ_SESSION_VWAP_RELATIONSHIP_V1"
    },
    "opening_range_position": {
      "status": "VALID",
      "state": "EXTENDING_ABOVE",
      "range_identifier": "NQ_US_RTH_OPENING_RANGE_30M_V1",
      "field_cutoff_ref": "completed_candle_cutoffs.5m",
      "classifier_version": "NQ_OPENING_RANGE_POSITION_30M_V1"
    },
    "trend_state": {
      "status": "VALID",
      "state": "UP",
      "slope_points_per_5m_bar": 3.4,
      "field_cutoff_ref": "completed_candle_cutoffs.5m",
      "classifier_version": "NQ_TREND_STATE_5M_LINEAR_SLOPE_V1"
    },
    "volatility_state": {
      "status": "VALID",
      "state": "HIGH",
      "atr_ratio": 1.31,
      "field_cutoff_ref": "completed_candle_cutoffs.5m",
      "classifier_version": "NQ_VOLATILITY_STATE_5M_ATR_RATIO_V1"
    }
  },
  "source_provenance": [],
  "source_fingerprints": {},
  "deterministic_fingerprint": "sha256",
  "guardrails": {
    "diagnostic_only": true,
    "production_recommendation": false,
    "trading_gate": false
  }
}
```

## Provenance And Freshness

Each context record must include:

- source artifact path;
- source schema version;
- source generated timestamp;
- source fingerprint/hash where available;
- decision timestamp provenance;
- completed candle cutoffs per timeframe;
- observed timestamp;
- producer schema version;
- classifier version per computed field;
- candle series identity.

Freshness rules:

- A field is `VALID` only when all required source candles are complete, identity-matched, and fresh for that field.
- `STALE` means the source exists but exceeds the configured research freshness threshold for the producing artifact.
- `MISSING` means required evidence is absent.
- `INVALID` means evidence exists but violates schema, identity, timestamp, or join contract.

## CRR Integration Boundary

Preferred v1 CRR integration is reference-only:

- `market_context_ref`;
- context fingerprint;
- per-field status summaries;
- no duplicated context values in CRR.

No optional CRR materialized context cache fields are approved for v1. If a future proposal adds caches, it must define source fingerprint linkage, regeneration triggers, mismatch behavior, and consumer rejection of stale caches.

CRR remains non-authoritative for raw candle data and live context state. Source context records remain research evidence. Runtime truth, broker truth, Guardian, Safe-State, readiness, reconciliation, Managed Exit, and trading gates remain unaffected.

## Schema Versioning

Versioned elements:

- producer schema version;
- producer implementation version;
- classifier version per field;
- candle source schema version;
- CRR schema version;
- source artifact fingerprints.

Additive changes may add optional fields or new statuses without changing existing semantics. Breaking changes include timestamp authority changes, VWAP anchor changes, opening-range definitions, trend thresholds, volatility thresholds, contract-series policy changes, or cache policy changes.

Consumers must reject unsupported major versions and report unsupported minor fields explicitly.

Deterministic fingerprint inputs include all identity fields, timestamp provenance, candle cutoffs, classifier versions, field statuses/values, source provenance, source fingerprints, and guardrails.

If source candle artifacts are later corrected, regenerated context records receive new fingerprints. Previously published checkpoints remain tied to the exact producer, classifier, and source versions used at checkpoint time.

## Runtime And Research Isolation

The context producer is research-only. It may read CRR, research eligibility, existing durable decision anchors, and durable candle artifacts. It must not:

- import broker clients;
- place, cancel, close, flatten, or modify orders;
- change strategy decisions;
- influence submit authority;
- publish runtime truth;
- publish readiness or safety classifications;
- mutate lifecycle ownership;
- create production recommendations or gates;
- read live mutable runtime state.

## Failure And Fail-Closed Behavior

Failure behavior:

- Missing context remains missing.
- Invalid context is marked invalid and excluded from context-supported comparisons.
- Stale context is marked stale and remains non-authoritative.
- Insufficient history remains insufficient and does not become missing or valid.
- Producer failure must not affect trading, Managed Exit, broker truth, or runtime health.
- Monitor comparisons must disclose missingness and continue where the population contract remains valid.

Fail-closed means research confidence does not improve when required evidence is missing, stale, invalid, or insufficient. It does not mean trading is blocked.

## Retention And Artifact Paths

Proposed output root:

`outputs/track_b_execution_core/research_analytics/prospective_market_context/`

Suggested artifacts:

- `prospective_market_context_records.jsonl`
- `prospective_market_context_schema.json`
- `prospective_market_context_validation_report.json`
- `prospective_market_context_validation_report.md`
- `prospective_market_context_coverage_summary.json`

Retention must preserve generated context records long enough to reproduce prospective checkpoints. If later compaction is needed, it must preserve source fingerprints and checkpoint reproducibility.

## Tests And Validation

Required tests:

- authoritative decision timestamp is CRR `entry_anchor.entry_time`;
- missing or contradictory decision timestamp is invalid and does not fall back heuristically;
- 1m exact-boundary no-look-ahead;
- 5m exact-boundary no-look-ahead;
- 1m and 5m cutoffs are independent;
- exact literal contract identity required;
- roll-adjacent status is explicit;
- cross-roll calculation is prohibited;
- insufficient same-contract history is distinct from missing evidence;
- VWAP methodology is reproducible with session-specific identifier;
- opening-range states are reproducible per session;
- trend-state formula is reproducible;
- volatility-state formula is reproducible;
- CRR integration is reference-only;
- missing historical rows are not backfilled;
- no runtime, broker, Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, or gate imports;
- no production recommendations.

Validation:

- JSON/JSONL parse;
- deterministic regeneration;
- provenance completeness;
- source fingerprint preservation;
- durability timestamp enforcement;
- field-level freshness classification;
- schema-version compatibility checks;
- git diff checks;
- prohibited import/action scan.

## Incremental Phases

Phase 1 may proceed after ADR-002 only if it remains fixture-only:

1. Define schema.
2. Define classifier contracts.
3. Build fixture-only producer.
4. Add no-look-ahead fixture tests.
5. Do not read real candles.
6. Do not integrate with CRR.

Phase 2 may begin only after:

- ADR-002 acceptance;
- exact decision-anchor audit passes;
- literal contract candle identity is proven;
- no-look-ahead boundary tests pass.

Phase 3:

- populate from real durable candles;
- preserve research-only isolation;
- no CRR integration until validation is clean.

Phase 4:

- add lightweight CRR reference and reconciliation;
- keep CRR reference-only for v1.

## Acceptance Criteria

- Same inputs and producer/classifier/source versions produce identical context records.
- No 1m or 5m look-ahead is possible.
- Candle durability proof is required; close-time evidence alone is insufficient at the decision boundary.
- Exact literal contract identity is required.
- Cross-roll calculations are prohibited.
- VWAP state is reproducible by session-specific identifier.
- Opening-range state is reproducible by session-specific identifier.
- Trend-state formula is reproducible from completed 5m candles.
- Volatility-state formula is reproducible from completed 5m candles.
- Missing evidence remains missing.
- Insufficient history remains distinct from missing evidence.
- No historical backfill is performed.
- No runtime or broker authority is introduced.
- CRR remains reference-only.

## Non-Goals

- No strategy change.
- No entry or exit tuning.
- No broker integration.
- No runtime authority.
- No trading gate.
- No production recommendation.
- No historical fabrication.
- No attempt to reconstruct unavailable historical context.
- No broad CRR redesign.
- No real-candle population before the Phase 2 gates pass.

## Cost And ROI Assessment

Expected implementation cost is moderate because the work requires exact timestamp, freshness, completed-candle, and contract-identity handling but does not require broker or strategy changes.

Expected research ROI is high for P0 because the current prospective monitor can tell whether NQ cohorts hold up out of sample, but cannot yet explain whether future divergence is tied to VWAP, opening range, trend, or volatility context.

P1 has medium ROI until the first checkpoint shows which missing context would change interpretation. P2 has lower immediate ROI and higher overfitting risk.

## Stop Conditions

Stop before implementation beyond fixture-only Phase 1 if:

- durable decision anchors are ambiguous or unavailable;
- durable candle evidence cannot support completed-candle semantics;
- literal contract candle identity cannot be proven;
- no-look-ahead boundary tests fail;
- context fields would require strategy changes;
- runtime-hot-path production would be required without Architecture Track approval;
- CRR integration would require changing authority boundaries;
- the design would infer or fabricate missing historical evidence.

## Related Documents

- [BA-002: Prospective Market Context Coverage](../baselines/BA-002-prospective-market-context-coverage.md)
- [ADR-001: Canonical Research Record](../decisions/ADR-001-canonical-research-record.md)
- [NQ Prospective Cohort Validation Contract](../../research/NQ_PROSPECTIVE_COHORT_VALIDATION_CONTRACT.md)
