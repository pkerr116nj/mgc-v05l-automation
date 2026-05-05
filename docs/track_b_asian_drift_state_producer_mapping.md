# Track B Asian Drift State Producer Mapping

Track B now has an `ASIAN_DRIFT_V1` consumer and guarded PAPER handoff. This
mapping documents the narrow Track B-safe extraction from the existing
research/replay implementation:

runtime 5m candles -> `track_b_asian_drift_feature_rows` ->
`track_b_asian_drift_live_state` -> explicit Asian Drift state snapshot.

## Research Sources

- State names and transition semantics:
  `src/mgc_v05l/research/asia_drift/state_machine.py`
- Feature row contract:
  `src/mgc_v05l/research/asia_drift/models.py::AsiaDriftFeatureRow`
- Regime, pullback, warmup, and calibration labels:
  `src/mgc_v05l/research/asia_drift/features.py`
- Narrative spec:
  `docs/specs/ASIA_DRIFT_V1_RESEARCH_SPEC.md`

## Track B Boundary

`track_b_asian_drift_feature_rows` and `track_b_asian_drift_live_state` do not
import research modules. The feature-row producer accepts bounded completed 5m
MGC candles, mirrors the research-defined Phase 1 feature row computation, and
writes `latest_asian_drift_5m_feature_rows.json`. The live-state producer
accepts those explicit feature rows, applies the stable state-machine
transition rules, and then delegates schema writing to
`track_b_asian_drift_state`.

Raw OHLC candles are accepted only at the feature-row boundary. The live-state
producer still refuses raw OHLC-only input and blocks instead of inferring
strategy state from missing feature fields.

## Feature Row Computation

`track_b_asian_drift_feature_rows` mirrors the research definitions from
`features.py` for:

- Asia session scope and anchor observation
- session VWAP, fast/slow EMA, ATR, slope, displacement, persistence,
  overlap, reversal, realized-volatility, and extension fields
- drift scores, strengths, dominant direction, and regime classification
- pullback state, warning/invalidation fields, recovery score, and
  `hypothetical_entry_ready`

The producer requires completed 5m bars and at least 8 completed rows before
the downstream live state can be computed. Fewer rows produce
`ASIAN_DRIFT_NOT_READY_FOR_TONIGHT`, not a guessed signal.

## Field Mapping

- `asia_drift_state`: computed from the state-machine transition rules in
  `state_machine.py`, using each completed 5m feature row in timestamp order.
- `asia_drift_regime`: direct mapping from feature row `regime`; valid values
  are `NO_TRADE`, `ASIA_DRIFT_LONG`, and `ASIA_DRIFT_SHORT`.
- `hypothetical_entry_ready`: direct mapping from feature row
  `hypothetical_entry_ready`; Track B never forces it true.
- `entry_window_open`: direct mapping from feature row `entry_window_open`.
- `in_scope`: direct mapping from feature row `in_scope`.
- `feature_version`: direct mapping from feature row `feature_version`.
- `calibration_profile`: direct mapping from feature row
  `calibration_profile`.
- `close`: direct mapping from feature row `close`.
- Realtime quote evidence: direct mapping from top-level input or supplied
  quote report fields: `quote_provider_mode=REALTIME`,
  `realtime_quote_received=true`, and `current_quote_available=true`.

## State Computation

The producer mirrors the stable state-machine structure:

- out-of-scope rows produce `NO_TRADE` or `SESSION_TIMEOUT` when a prior live
  state leaves scope
- `session_timeout=true` produces `SESSION_TIMEOUT`
- missing anchor or warmup before 8 completed 5m bars produces `NO_TRADE`
- `regime=NO_TRADE` produces `NO_TRADE`
- directional regimes produce `DRIFT_LONG_CANDIDATE` /
  `DRIFT_SHORT_CANDIDATE`, `PULLBACK_PENDING`, or `ENTRY_ARMED` according to
  research pullback state and entry window rules
- at-risk/recovery handling follows the explicit failure/recovery fields when
  supplied by the feature rows

## Live State Required Inputs

The input JSON must contain one of:

- `asian_drift_feature_rows`
- `feature_rows`
- `candles`, if each candle is already enriched with the Asia Drift feature
  fields

Each row must include:

- `timeframe=5m`
- `decision_ts` or `candle_timestamp`
- `calibration_profile`
- `instrument=MGC` or `instrument_family=MGC`
- `session_bar_index`
- `asia_drift_session_id`
- `in_scope`
- `entry_window_open`
- `session_timeout`
- `anchor_observed`
- `close`
- `regime`
- `pullback_state`
- `hypothetical_entry_ready`
- `feature_version`

## Assumptions

- `recovery_confirmed` is the default calibration profile because it is the
  latest profile used by the replay artifacts inspected for this Track B
  bridge.
- Realtime quote evidence is separate from the 5m feature context.

## Unresolved Gaps

- The first feature-row producer mirrors only the `recovery_confirmed`
  calibration profile.
- Historical/replay state rows are valid lineage but are not valid tonight
  runtime state unless they are produced from current runtime evidence.

This is not guessed raw-candle inference: the raw-candle boundary produces the
explicit research feature row contract first, and downstream state production
fails closed when that contract is absent.
