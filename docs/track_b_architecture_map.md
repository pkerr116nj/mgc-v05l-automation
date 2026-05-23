# Track B Architecture Map And Replacement Roadmap

Track B is the replacement execution spine. Track A remains legacy/reference
only until specific behavior is intentionally extracted, tested, and re-owned by
Track B. The long-term destination is not Track B feeding back into Track A.

## Current Chain

Track B is now a PAPER-stage trading engine, not a passive monitor and not a
broker-safety demo. The current phase is PAPER-only, while the intended future
destination is live trading after explicit promotion gates. Qualified strategy
signals are expected to become controlled, attributable, broker-backed PAPER
trades when business rules, data readiness, contract/account guards, and managed
lifecycle rules allow.

The current Track B signal-to-trade funnel is:

```text
strategy evaluation
-> hard signal
-> eligible signal
-> strategy trade intent
-> managed PAPER entry submit
-> broker-confirmed fill
-> OPEN_MANAGED
-> managed exit trigger
-> managed PAPER exit submit
-> broker-confirmed close
-> CLOSED_FLAT / reconciled P&L
```

The broader artifact chain is:

```text
Databento market-data observer, optionally
-> candle/event JSON
-> track_b_data_maintenance, for maintained local MGC 1m history
-> track_b_runtime_candle_capture, for bounded execution-time MGC 1m context
-> track_b_mgc_candle_history_producer, optionally
-> track_b_market_history, optionally
-> track_b_feature_builder, optionally
-> track_b_strategy_rule_runner, optionally
-> track_b_strategy_paper_runner, explicitly for PAPER handoff
-> candle_signal_producer or strategy_signal_adapter, optionally
-> shadow_signal
-> signal_intent_proposal
-> strategy_intent
-> strategy_lane_registry
-> order_plan
-> shadow_evaluation
-> shadow_run_manifest
-> shadow_run_assembler
-> shadow_replay_runner
-> signal_batch_writer
-> shadow_listener
-> operator_status
-> attrition_report
-> readiness_summary
-> track_b_readiness_check_runner for bounded no-submit pre-proof evidence
-> recovery / preflight / managed lifecycle timing
-> strategy-managed PAPER lifecycle, only through explicit PAPER gates
```

The chain is file/report driven today, but its product purpose is opportunity
capture under PAPER controls. Safety means controlled, attributable,
broker-reconciled trading, not unexplained non-participation. Unexplained
silence or unexplained signal-to-trade drop-off is a product defect until the
drop-off is classified.

## Mode And Lane Lifecycle

Engine-running state is separate from submit authority. A future Track B engine
may have `engine_running=true` while every report still has
`submit_allowed=false`. Shadow evaluation can run continuously because it is
no-submit. Paper submit requires paper-specific recovery, preflight, timing,
pricing, explicit submit flags, and operator confirmation. Live submit is not
implemented and will require future live-specific gates.

## Phase 2 Paper Execution Stance

Track B Phase 1 proved the PAPER open/guarded-close/flat lifecycle. The current
PAPER phase is trading-oriented: real strategy signals should move through the
strategy-managed PAPER lifecycle when the signal-to-trade funnel qualifies them.
Listener, replay, operator status, and UI flows remain read-only surfaces, not
execution authority.

The current stance is:

- PAPER execution is allowed through Track B-controlled paths after explicit
  readiness, business-rule, contract/account, and managed lifecycle gates pass.
- Strategy-rule runners may hand off real qualified strategy signals to durable
  trade intent creation and the strategy-managed PAPER lifecycle. `paper_proof`
  remains explicit proof/debug/canary only, not the normal strategy path.
- No strategy rule, listener, observation runner, operator status report, or UI
  state may create hidden submit authority.
- Live-money execution is not active in the current phase. Live trading is the
  future destination only after promotion gates, live-specific readiness, risk
  controls, and explicit live authority exist.
- UI/dashboard surfaces remain read models and controls over explicit Track B
  APIs only; they are not execution authority.
- Every PAPER execution must write durable artifacts, including submit
  diagnostics, lifecycle state, fills/ambiguity blockers, close/flatten
  outcome when applicable, and final broker-state classification.
- Any missing, stale, or ambiguous broker/quote/readiness evidence must fail
  closed rather than being interpreted as permission to submit.

Lane lifecycle states:

- `DISABLED`: no review and no submit.
- `SHADOW_ONLY`: no-submit observation, replay, attrition, and reporting only.
- `PAPER_REVIEW`: paper-mode no-submit review artifacts are allowed.
- `PAPER_SUBMIT_ELIGIBLE`: the lane may proceed to paper readiness and explicit
  paper submit gates. This state does not itself allow submit.
- `LIVE_REVIEW`: future live-mode review artifacts are allowed, with no live
  submit.
- `LIVE_SUBMIT_ELIGIBLE`: future state only. It will require future
  live-readiness gates, risk controls, account controls, and explicit live
  submit authority.

Mode-aware flow must keep candidates separated by authority:

- The same signal may generate shadow artifacts.
- A paper candidate exists only if the lane is paper eligible and the paper
  review/submit gates pass.
- A live candidate exists only if the lane is live eligible and future live
  gates pass.
- Each mode has separate account, contract, risk, readiness, and operator
  controls.

Current implementation status:

- The current Track B replay runner is shadow/no-submit orchestration.
- The paper proof path exists and has passed one full open/guarded-close/flat
  lifecycle for `DUM882026` / `MGC-202606`; future paper proof remains a
  separate explicit operator action.
- Live trading is not implemented and not implied.
- Current no-submit artifacts keep `live_money_readiness=false`.

Promotion discipline:

- Strategies do not promote themselves.
- Lane lifecycle changes require explicit registry/config changes.
- Promotion should be based on evidence from shadow reports, paper reports,
  attrition, fills, risk metrics, and operator review.

Dashboard implication:

- A future dashboard should display lifecycle state, listener health, and
  mode-specific readiness.
- Dashboard controls must remain observer/control surfaces over Track B
  artifacts, not source of truth or hidden submit authority.

## Authority Boundaries

- `shadow_signal` is evidence only. It can carry BINARY or scored observations,
  but it is not an intent and cannot authorize a lane or submit.
- `databento_candle_observer` is a market-data evidence bridge only. It accepts
  a supplied Databento quote/candle artifact or an explicit bounded
  realtime Databento quote pull, writes a Track B candle/event JSON file,
  and stops. It does not connect to IBKR/TWS, authorize trades, infer submit
  readiness, invoke the listener/runner/operator status, create order plans, or
  submit. The bounded realtime pull uses Databento Live subscription capability
  long enough to receive a quote, then closes the market-data session. It is not
  live trading. Historical `available_end` quote windows remain diagnostic only
  and must not count as readiness. Databento symbols remain market-data selectors only; the
  local execution contract key and IBKR allowlist remain execution authority. It
  updates
  `outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_event.json`
  and
  `outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json`
  as read-model conveniences. Its event JSON is already compatible with
  `strategy_signal_adapter_cli`; no separate bridge module is required in this
  slice. Direction is explicit: pass `--signal-direction` to create directional
  BINARY review input on either the observer command or the strategy adapter
  command, or omit it to let the adapter emit review-only HUMAN_REVIEW input.
  Optional bounded watch mode re-runs the same observer conversion on a supplied
  artifact path or bounded current-quote pull and updates
  `outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_heartbeat.json`;
  it is still market-data evidence only and does not imply trading mode.
- `candle_signal_producer` is an upstream no-submit producer scaffold. It
  translates explicit candle/event JSON into Track B shadow signal observations
  and delegates validated inbox writes to `signal_batch_writer`. It does not
  infer direction from price movement, authorize trades, invoke the listener,
  create order plans, or connect to broker/market-data paths. It updates
  `outputs/track_b_execution_core/candle_signal_producer/latest_candle_signal_producer_report.json`
  as a read-model convenience.
- `strategy_signal_adapter` is the first minimal strategy-like adapter proof.
  The `demo_candle_direction_signal` adapter accepts explicit direction/side
  from an event, emits BINARY or review-only HUMAN_REVIEW signal input through
  `candle_signal_producer`, and stops. It does not port production strategy
  rules, infer price direction, authorize lanes, invoke listener/runner/status
  flows, create order plans, or submit. It updates
  `outputs/track_b_execution_core/strategy_signal_adapter/latest_strategy_signal_adapter_report.json`
  as a read-model convenience.
- `track_b_data_maintenance` owns the Track B local rolling MGC 1m history
  foundation for research/replay historical data. It is the Track A-style
  maintenance lane: default cadence is weekly, the intended cutoff is normally
  Friday close, and the purpose is research, replay, backtesting, and maintained
  historical base data. It supports initial bounded backfill, incremental
  append/update, duplicate handling, monotonic ordering, gap detection,
  cutoff-completeness classification, and a latest-good bounded export for the
  feature builder. It writes
  `outputs/track_b_execution_core/track_b_data_maintenance/latest_good_mgc_1m_history.json`
  and
  `outputs/track_b_execution_core/track_b_data_maintenance/latest_track_b_data_maintenance_report.json`.
  This is the normal source for strategy feature history. The maintenance
  layer is registry-based; `MGC` is the first and only runtime-enabled entry in
  this slice (`MGC-202606`, `MGCM6`, `MGC.v.0`, `GLBX.MDP3`, `ohlcv-1m` /
  `1m`). Broader Track A-style symbols are represented as disabled planning
  entries for future research/runtime migration and are not fetched or
  maintained by default. Databento historical `available_end` is acceptable for
  this maintenance boundary only: if a requested OHLCV window is ahead of
  provider availability, the maintenance fetch may retry ending at
  `provider_available_end`, label the result `history_provider_mode =
  HISTORICAL_AVAILABLE_END`, and report `requested_history_end`,
  `provider_available_end`, `history_end_used`, `available_end_lag_seconds`,
  and `history_freshness_seconds`. Historical maintenance reports
  `historical_maintenance_cutoff_policy`, `intended_cutoff_timestamp`,
  `latest_bar_timestamp`, `complete_through_cutoff`, and `missing_bars`. It
  does not fail merely because the latest historical bar is older than 900 or
  1200 seconds from now. Maintained bars are not realtime quote evidence;
  strategy/paper execution still requires a separate realtime current quote
  report, and same-session candle context belongs to a separate runtime live
  candle/quote capture lane.
- `track_b_runtime_candle_capture` owns the first bounded runtime MGC 1m candle
  context artifact for execution-time feature building. It is separate from
  weekly historical maintenance and is not a research archive. The first slice
  supports bounded recent Databento `ohlcv-1m` fetches and supplied runtime
  candle JSON, bounds the window with `max_bars` (default 250), overwrites
  stable latest artifacts, prunes old run folders, and writes
  `outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json`
  plus
  `outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_candle_capture_report.json`.
  It reports `data_written`, `fresh_for_execution`,
  `execution_freshness_blocker`, `runtime_candle_context_ready`, candle
  counts/timestamps, duplicate and gap counts, requested window, Databento
  `provider_available_end`, `history_end_used`, latest 1m/completed 5m ages,
  provider lag versus wall clock, completed-5m lag versus provider/wall clock,
  realtime quote-evidence fields, and no-submit safety fields. If Databento
  available_end fallback is used, valid bounded candles are still written for
  backfill/gap-fill/data context, but strategy evaluation may proceed only when
  `fresh_for_execution=true` unless an explicit research/shadow replay override
  is supplied. The CLI can load `DATABENTO_API_KEY` from the repo `.env.local`
  when it is absent from the process environment; reports expose only
  credential status/source, never the key value.
  It does not connect to broker/TWS/IBKR, run paper proof, infer execution
  authority, submit/cancel/place orders, or write an unbounded raw stream.
  Future live Databento candle capture should feed this bounded artifact shape
  rather than replacing historical maintenance.
- `track_b_strategy_paper_runner` distinguishes historical context from runtime
  intraday context when consuming `latest_good_mgc_1m_history.json`. It reports
  `historical_context_ready`, `runtime_candle_context_required`,
  `runtime_candle_context_supplied`, and `runtime_intraday_freshness_policy`.
  The 900/1200-second age threshold is only an intraday runtime policy when
  explicitly requested with `runtime_intraday_freshness_policy =
  REQUIRE_MAX_AGE`; weekly historical maintenance can be many hours or days old
  while still being complete through its cutoff. Realtime current quote
  evidence remains separate. When `mgc_ema_momentum_reclaim_long_v1` requires
  execution-time candle context, the runner should consume
  `--runtime-candle-context-json` pointing at
  `outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json`
  rather than treating weekly historical maintenance as current intraday
  context.
- `track_b_mgc_candle_history_producer` is the bounded upstream producer for
  the MGC 1m history JSON consumed by `track_b_market_history`. It can
  normalize a supplied Databento-like OHLCV history artifact, or make an
  explicit bounded Databento `ohlcv-1m` historical request, but it also
  requires a separate realtime current quote report before writing strategy
  history input. This path is now maintenance/diagnostic only, not the normal
  trade-decision path. The output labels history provenance separately from
  current quote evidence: bounded historical candles are
  `DATABENTO_HISTORICAL_BOUNDED_WITH_REALTIME_CURRENT`, while
  `quote_provider_mode=REALTIME`, `realtime_quote_received=true`, and
  `current_quote_available=true` come from the quote report. This prevents a
  single snapshot candle or stale historical `available_end` data from
  masquerading as realtime strategy history. It updates
  `outputs/track_b_execution_core/track_b_mgc_candle_history_producer/latest_track_b_mgc_candle_history_input.json`
  and
  `outputs/track_b_execution_core/track_b_mgc_candle_history_producer/latest_track_b_mgc_candle_history_producer_report.json`.
- `track_b_market_history` is the bounded MGC market-history collector
  upstream of Phase 2 feature building. It normalizes explicit realtime
  quote/candle history into a Track B event with a `candles` /
  `candle_history` array, source/provider metadata, current quote evidence, and
  no-submit safety flags. It requires `MGC-202606` and explicit realtime
  evidence (`quote_provider_mode=REALTIME`, `realtime_quote_received=true`,
  `current_quote_available=true`). If the input contains only a single
  snapshot candle or non-realtime/stale evidence, it writes a blocked report
  instead of fake EMA/VWAP history. It updates
  `outputs/track_b_execution_core/track_b_market_history/latest_track_b_market_history_event.json`
  and
  `outputs/track_b_execution_core/track_b_market_history/latest_track_b_market_history_report.json`.
- `track_b_feature_builder` is the no-submit feature/event producer for the
  first real Track B MGC rule. It consumes explicit MGC quote/candle history,
  requires current realtime Databento evidence, and writes an enriched feature
  event containing the EMA/VWAP momentum fields used by
  `mgc_ema_momentum_reclaim_long_v1`. If history is insufficient or evidence
  is non-realtime/stale, it writes a blocked report instead of fake features or
  signals. It updates
  `outputs/track_b_execution_core/track_b_feature_builder/latest_track_b_feature_event.json`
  and
  `outputs/track_b_execution_core/track_b_feature_builder/latest_track_b_feature_builder_report.json`.
- `track_b_strategy_rule_runner` is the first Track B Phase 2 strategy-rule
  runner. The current default rule is
  `mgc_ema_momentum_reclaim_long_v1`, a narrow MGC-only LONG rule that consumes
  realtime Databento quote/candle evidence plus explicit precomputed EMA
  momentum/VWAP fields from the event metadata. It emits a no-submit signal
  only when `--rule-mode MGC_EMA_MOMENTUM_RECLAIM_LONG --emit-signal` is
  explicitly supplied, the input is proven realtime/current, and the rule
  conditions pass. Without the emit flag it writes review/NO_SIGNAL artifacts.
  It does not infer execution authority, call paper proof, invoke the listener,
  create order plans, authorize lanes, or submit in its default mode. The older
  `DEMO_LONG_ONLY` mode remains available only as an explicit wiring proof. It
  delegates actual signal batch production to `strategy_signal_adapter ->
  candle_signal_producer -> signal_batch_writer` and updates
  `outputs/track_b_execution_core/track_b_strategy_rule_runner/latest_track_b_strategy_rule_runner_report.json`.
  The runner also has a narrow `ASIAN_DRIFT_V1` watch mode for tonight's Asia
  Drift observation. That mode does not import Track A/research code or compute
  Asia Drift from raw candles. It requires an explicit Asia Drift state/feature
  snapshot with completed 5m decision-bar fields such as `asia_drift_state`,
  `asia_drift_regime`, `hypothetical_entry_ready`, `entry_window_open`,
  `in_scope`, `feature_version`, and `calibration_profile`. Missing fields
  produce `ASIAN_DRIFT_NOT_READY_FOR_TONIGHT`; no-submit non-setups produce
  `ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION`; explicit entry-ready snapshots produce
  `ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT`.
- `track_b_strategy_registry` is the migration guardrail for adding more Asia
  strategies. Track B uses one runner with many registered adapters; adapters
  may emit signal/state decisions only and must never submit. Each registry
  entry must declare `strategy_id`, `rule_mode`, `instrument_family`,
  `timeframe`, `required_feature_schema`, optional `required_state_schema`,
  `feature_version`, `calibration_profile`, `paper_eligible`, and
  `live_money_eligible=false`. The rule runner rejects unregistered strategies
  and blocks missing required feature/state fields as NOT_READY instead of
  guessing or falling back to raw candles. Multi-strategy watch/arbitration may
  report chosen and suppressed candidates, but it permits at most one PAPER
  candidate per cycle; conflicting signals require explicit arbitration or no
  trade. Broker mutation remains exclusively through the guarded Track B PAPER
  proof lifecycle.
- `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` is the first post-guardrail Asia strategy
  migration. It is registered as MGC/5m, `paper_eligible=true`, and
  `live_money_eligible=false`. The runner consumes only an explicit
  `metadata.asia_early_pause_resume_short_state` plus
  `metadata.asia_early_pause_resume_short_features` envelope mirroring the
  research-defined `asiaEarlyPauseResumeShortTurn` predicates from
  `src/mgc_v05l/signals/bear_snap.py` and
  `config/replay.asia_early_pause_resume_short_pattern_v1.yaml`. Missing fields
  block as NOT_READY; valid non-setups produce
  `ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION`; valid setups produce
  `ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT`. The adapter itself
  remains signal-only and cannot submit. A valid real short signal can enter
  the guarded PAPER lifecycle only through `track_b_strategy_paper_runner`
  when explicit PAPER submit flags are supplied, registry
  `paper_eligible=true`, registry `live_money_eligible=false`, and the
  requested PAPER side matches the explicit short signal (`SHORT -> SELL`).
  `track_b_session_strategy_envelope_producer` now writes the Track B-safe
  envelope from bounded completed realtime MGC 5m context at
  `outputs/track_b_execution_core/session_strategy_state/latest_asia_early_pause_resume_short_event_envelope.json`.
- `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1` is the guarded PAPER
  migration of the research-defined `asiaEarlyNormalBreakoutRetestHoldTurn`
  branch. It is registered as MGC/5m, `paper_eligible=true`, and
  `live_money_eligible=false`. It consumes only an explicit
  `metadata.asia_early_normal_breakout_retest_hold_long_state` plus
  `metadata.asia_early_normal_breakout_retest_hold_long_features` envelope
  mirroring `src/mgc_v05l/signals/bull_snap.py` and
  `config/replay.asia_early_breakout_retest_hold_pattern_v1_normal.yaml`.
  Required predicates include the Asia-early/London-extension state gate,
  no competing first-bull-snap turn, anti-churn spacing, flat breakout slope,
  normal breakout expansion, breakout above prior high, and retest/hold of
  the breakout level. Missing fields block as NOT_READY; valid non-setups
  produce `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION`;
  valid setups produce
  `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT`. The
  adapter itself remains signal-only; guarded PAPER handoff requires explicit
  PAPER flags and side `BUY` for the real LONG signal.
  `track_b_session_strategy_envelope_producer` writes its Track B-safe envelope
  at
  `outputs/track_b_execution_core/session_strategy_state/latest_asia_early_normal_breakout_retest_hold_long_event_envelope.json`.
- `track_b_asian_drift_state` is the explicit 5m state snapshot boundary for
  the Asian Drift watch path. It validates and writes
  `outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json`
  plus `latest_asian_drift_state_builder_report.json`. It is intentionally a
  state snapshot writer, not a state machine: Track B does not infer Asian
  Drift from raw candles in this slice.
- `track_b_asian_drift_feature_rows` is the bounded runtime 5m candle to Asian
  Drift feature-row producer. It mirrors the research-defined Asia Drift Phase
  1 feature semantics without importing broad research modules, requires
  completed 5m MGC bars, blocks fewer than 8 completed rows as not ready, and
  writes
  `outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_feature_rows.json`
  plus `latest_asian_drift_feature_rows_report.json`.
- `track_b_asian_drift_live_state` is the narrow Track B-safe producer for that
  snapshot. It does not import the research package and does not infer Asian
  Drift state from raw OHLC candles. It accepts bounded completed 5m rows that
  already carry the research-defined Asia Drift feature fields, mirrors the
  stable replay state-machine transition rules documented in
  `docs/track_b_asian_drift_state_producer_mapping.md`, and then delegates the
  final snapshot write to `track_b_asian_drift_state`. Missing feature rows,
  missing realtime quote evidence, or raw candles without the explicit feature
  contract produce `ASIAN_DRIFT_NOT_READY_FOR_TONIGHT` and no broker mutation.
- `track_b_asian_drift_watch_chain` is the no-submit bounded operator wrapper
  for the Asian Drift watch path. It can derive completed 5m bars from a
  bounded runtime 1m MGC artifact, run
  `track_b_asian_drift_feature_rows -> track_b_asian_drift_live_state ->
  track_b_strategy_rule_runner --rule-mode ASIAN_DRIFT_V1`, and write
  `latest_asian_drift_5m_candles.json` plus
  `latest_asian_drift_watch_chain_report.json`. It reports the source path,
  latest 1m candle timestamp, latest completed 5m candle timestamp, candle age,
  provider/source fields, and `runtime_candle_context_stale`. The CLI defaults
  to rejecting completed 5m candles older than 900 seconds, returning
  `TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_STALE_RUNTIME_CONTEXT_NOT_READY` instead of
  evaluating stale data-maintenance history as live context. The wrapper never
  invokes readiness, paper proof, or broker mutation; any PAPER execution
  remains the separate guarded strategy paper runner path after a real
  `ASIAN_DRIFT_V1` signal.
- `track_b_strategy_paper_runner` is the controlled Phase 2 PAPER handoff. It
  can now compose `track_b_data_maintenance` latest-good history with a
  separate realtime current quote report, then run `track_b_market_history ->
  track_b_feature_builder -> track_b_strategy_rule_runner ->
  track_b_readiness_check_runner -> paper_proof_cli / paper proof lifecycle` in
  one artifacted command. It defaults to dry-run/no-submit. It can invoke paper
  proof only when maintained history is fresh/sufficient, feature building
  succeeds, the strategy rule emits a signal, readiness is
  `READY_FOR_PAPER_PROOF`, and `--mode PAPER`, `--submit-paper`,
  `--confirm-paper-submit`, explicit `--quantity`, and explicit manual open and
  close limit prices are supplied.
  If history/features are insufficient, the rule emits `NO_SIGNAL`, or
  readiness blocks, it stops without invoking proof. It never supports
  live-money execution, UI authority, hidden submit, inferred prices, inferred
  quantity, market orders, or broker mutation outside the Track B paper proof
  lifecycle. It writes
  `outputs/track_b_execution_core/track_b_strategy_paper_runner/latest_track_b_strategy_paper_runner_report.json`
  with maintained-history, market-history collector, feature builder, rule,
  readiness, paper proof, and final broker-state classification.
  For `ASIAN_DRIFT_V1`, it only accepts real Asian Drift state signals
  (`signal_source=ASIAN_DRIFT_V1`, `real_strategy_signal=true`); demo/proof
  signals are rejected, and the requested PAPER side must match the explicit
  signal direction (`LONG -> BUY`, `SHORT -> SELL`).
  `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` follows the same guarded handoff contract:
  it only accepts `signal_source=ASIA_EARLY_PAUSE_RESUME_SHORT_V1`,
  `real_strategy_signal=true`, registry `paper_eligible=true`, registry
  `live_money_eligible=false`, and a side matching the strategy signal. The
  adapter still has no private broker path.
  `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1` follows the same contract
  with `signal_source=ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1`; the
  requested PAPER side must match its explicit LONG signal (`LONG -> BUY`).
- `track_b_multi_strategy_runtime_cycle` is the bounded one-cycle arbitration
  layer for the registered Asia strategies: `ASIAN_DRIFT_V1`,
  `ASIA_EARLY_PAUSE_RESUME_SHORT_V1`, and
  `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1`. It evaluates supplied
  explicit envelopes only, reports each strategy's registry metadata and
  NOT_READY/NO_SIGNAL/SIGNAL_READY status, then uses the strategy registry
  arbitration helper to choose at most one PAPER candidate. Zero signals means
  no mutation. One signal without PAPER flags means `SIGNAL_READY_NO_SUBMIT`.
  Multiple same-direction paper candidates and conflicting LONG/SHORT signals
  block until explicit arbitration exists. If exactly one real signal is
  chosen and explicit PAPER flags are present, it delegates once to
  `track_b_strategy_paper_runner`; it has no private broker path.
- `track_b_real_rule_wait_runner` is the bounded real-rule polling wrapper for
  eventual MGC PAPER signals. It repeatedly refreshes or reads bounded runtime
  candle context, delegates one cycle to `track_b_strategy_paper_runner` using
  `mgc_ema_momentum_reclaim_long_v1`, and stops on `NO_SIGNAL_NO_MUTATION`,
  `SIGNAL_READY_NO_SUBMIT`, or the first real-signal PAPER proof/review result.
  It does not use `DEMO_WIRING_PROOF`, force the real rule, run unbounded
  streams, or submit unless a real strategy signal appears and the explicit
  PAPER submit gates are present. It writes
  `outputs/track_b_execution_core/track_b_real_rule_wait_runner/latest_track_b_real_rule_wait_runner_report.json`.
- `track_b_observation_runner` is a bounded operator convenience wrapper around
  the existing no-submit observation chain. It can run one cycle or bounded
  watch cycles from a fixture quote/candle artifact or explicit current
  Databento quote pull, then update operator status for the Track B Status UI.
  It creates no new authority: direction remains explicit, Databento remains
  market-data evidence only, and submit stays impossible in this path. It
  updates
  `outputs/track_b_execution_core/track_b_observation_runner/latest_track_b_observation_runner_report.json`.
- `track_b_readiness_check_runner` is a bounded no-submit pre-proof evidence
  wrapper. It runs read-only recovery status, read-only preflight, Databento
  realtime wait-for-current quote, readiness summary, and operator status, then writes
  `outputs/track_b_execution_core/track_b_readiness_check_runner/latest_track_b_readiness_check_runner_report.json`.
  It answers whether paper proof may be considered as a separate operator
  decision. It does not call `paper_proof_cli`, submit, cancel, place orders,
  create order plans, mutate broker state, infer direction, or turn fallback
  historical quotes into readiness. Its current quote report must make
  `quote_provider_mode`, `realtime_subscription_attempted`,
  `realtime_quote_received`, `quote_age_seconds`, `quote_freshness_verdict`, and
  `current_quote_available` visible. Dependency/runtime diagnostics are also
  explicit: `databento_dependency_status`, `databento_package_version`,
  `databento_live_api_available`, `provider_error_category`,
  `symbol_subscription_attempted`, and `symbol_subscription_succeeded` identify
  missing package, missing Live API, missing API key, subscription/entitlement
  failures, and bounded-wait no-quote outcomes without falling back to
  historical readiness.
- `signal_intent_proposal` may create a proposed no-submit strategy intent from
  a validated signal under an explicit policy. It does not authorize a lane,
  create an order plan, summarize readiness, or submit.
- `strategy_intent` is a proposed action. It is not permission.
- `strategy_lane_registry` authorizes a strategy/lane/request shape for review.
  It does not submit and does not replace recovery, preflight, timing, or proof
  gates.
- `order_plan` describes what could be submitted later if every gate passes. It
  does not submit.
- `shadow_evaluation` reports the no-submit review chain for an intent and
  optional readiness summary. It does not submit.
- `shadow_run_manifest` governs run/session context only. It does not authorize
  lanes.
- `shadow_run_assembler` orchestrates no-submit review artifacts for one or
  more intents. It is not strategy execution and is not a broker route.
- `shadow_replay_runner` orchestrates the existing no-submit chain from signal
  batch to proposal artifacts, shadow run assembly, attrition report, and a
  top-level summary. It creates no new authority and does not submit.
- `signal_batch_writer` is an upstream no-submit inbox producer. It validates
  supplied signal observations or signal batch payloads, atomically writes a
  JSON file into the listener inbox, and stops. It does not invoke the
  listener, evaluate proposal policy, authorize lanes, create order plans, or
  submit. It updates
  `outputs/track_b_execution_core/signal_batch_writer/latest_signal_batch_writer_report.json`
  as a read-model convenience.
- `shadow_listener` is a poll-once file ingestion skeleton for signal batch
  JSON. It can run while the future engine is active, but it only invokes the
  no-submit replay runner and creates no submit authority. Its health report is
  observer/status data for operators and future dashboards, not trading
  authority. Optional bounded watch mode repeats poll-once cycles and updates a
  listener heartbeat artifact, while preserving no-submit semantics.
- `operator_status` aggregates supplied Track B observer reports into one
  dashboard-ready read model. It is not source of truth, does not connect to
  broker or market data, and does not authorize submit. It updates
  `outputs/track_b_execution_core/operator_status/latest_operator_status_summary.json`
  as the stable latest read-model artifact. It can summarize the Databento
  candle observer report/heartbeat so the UI can display market-data observer
  state without reading Databento artifacts directly. A clean
  `track_b_readiness_check_runner` report may produce
  `OPERATOR_STATUS_READY_FOR_PAPER_PROOF_REVIEW`, which means recovery,
  preflight, realtime quote, and readiness-summary evidence are clean enough for
  operator review only. It still does not call `paper_proof_cli` or authorize
  submit.
- `attrition_report` explains where candidates dropped out across supplied
  no-submit summaries. It is explanatory only and treats missing stages as
  explicit `NOT_PROVIDED` inputs instead of silently reporting zero.
- `readiness_summary` summarizes broker/session/quote state. It does not submit
  and does not override proof gates.
- Strategy-managed PAPER lifecycle is the normal route for real strategy
  signals: durable intent, guarded entry submit, broker-confirmed fill,
  `OPEN_MANAGED`, managed exit trigger, guarded close submit, broker-confirmed
  close, and `CLOSED_FLAT` / reconciled P&L. `paper_proof_cli` is retained for
  explicit proof/debug/canary work only and must not be used as a fallback for
  real strategy signals.
- Databento is market data authority only. Databento symbols and continuous
  selectors are not executable broker contracts.
- IBKR allowlist and the local execution contract key remain execution
  authority.
- Dashboard and app screens must be observer/control surfaces over Track B
  artifacts. They must not become truth sources or hidden execution authority.
- The first Track B app surface is a read-only `Track B Status` tab. It reads
  `outputs/track_b_execution_core/operator_status/latest_operator_status_summary.json`
  as the sanctioned primary read model, displays missing/malformed artifacts as
  unknown, and exposes no submit/action controls.
- Desktop package build metadata is a visibility aid for operators. The local
  package step writes `.mgc-build-metadata.json` into the app bundle, and the
  Track B status tab shows the commit, build timestamp, packaging mode, and
  bundle/source path so stale `/Applications` builds are obvious.

## Shared-Services Control Plane

Track B is converging toward a shared-services control plane. Shared
`execution_core` authority artifacts are the source of truth for PAPER runtime,
broker, order, position, readiness, and restart decisions. Dashboard and
operator artifacts are projections only; they may display shared truth, but they
must never be consumed for routing, restart, readiness, lifecycle, or broker
authority.

New Track B code should prefer shared authority services over local raw artifact
reconstruction. If a status, launcher, remediation, readiness, or self-healing
path needs open-order, position, runtime, broker, managed-position, or managed
order evidence, it should consume the shared `execution_core` builder/artifact
for that evidence rather than rebuilding a private interpretation from broker
snapshots or historical lifecycle files.

Authority hierarchy:

1. IBKR broker truth and Track B broker reconciliation remain the account,
   position, order, fill, and broker/lifecycle consistency authority.
2. Open Order Truth normalizes broker open-order evidence, suspicious order
   states, duplicate close risk, marketability, and broker-flat/open-close
   contradictions.
3. Managed Order Registry projects Track B-managed order identity and
   read-only adjustment planning context on top of Open Order Truth.
4. Position Truth summarizes broker positions, lifecycle state, ownership,
   reconciliation, runtime status, and managed-order evidence per symbol.
5. Runtime Environment Truth answers whether exactly one PAPER runtime is
   running correctly from the Dev root on the expected source/config identity.
6. Managed Position Registry identifies positions Track B is responsible for
   managing and whether they are matched, exit-due, close-working, adoption
   required, metadata-incomplete, or review-required.
7. Agent Registry declares the expected, optional, and diagnostic Track B
   agents/processes/services and their proof/runtime-submit relevance.
8. Agent Health attaches read-only health contract semantics to registered
   agents, including expected stopped/runtime states, artifact freshness, and
   proof/runtime-submit blocking evidence.
9. Proof Readiness combines shared truth, broker lease/reconciliation, and
   Phase-1 runtime market-data session/freshness into the supervised proof
   preflight verdict.
10. Canonical Readiness consumes shared truth/proof-readiness evidence and
   remains the submit-readiness decision surface.
11. Self-Recover Rules centralize read-only recovery recommendations such as
   wait for market reopen, refresh shared truth, restart-runtime-allowed, and
   cleanup-required-before-restart.
12. Self-Healing Restart Evidence consumes shared truth for restart eligibility
   diagnostics and keeps broker lease degradation distinct from reconciliation
   danger.
13. Operator dashboard/status surfaces display projections of this stack. They
    are never routing, readiness, restart, broker, lifecycle, or order
    authority.

Authority and status artifact map:

| Service | Artifact | Role |
| --- | --- |
| Open Order Truth | `outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json` | `execution_core` authority |
| Open Order Truth events | `outputs/track_b_execution_core/open_order_truth/open_order_truth_events.jsonl` | `execution_core` audit |
| Managed Order Registry | `outputs/track_b_execution_core/managed_orders/latest_managed_orders.json` | `execution_core` authority |
| Order Adjustment Planner | `outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json` | `execution_core` read-only planning authority |
| Managed Order events | `outputs/track_b_execution_core/managed_orders/managed_order_events.jsonl` | `execution_core` audit |
| Position Truth | `outputs/track_b_execution_core/position_truth/latest_position_truth.json` | `execution_core` authority |
| Position Truth events | `outputs/track_b_execution_core/position_truth/track_b_trade_outcome_events.jsonl` | `execution_core` audit |
| Runtime Environment Truth | `outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json` | `execution_core` authority |
| Runtime Environment events | `outputs/track_b_execution_core/runtime_truth/runtime_environment_events.jsonl` | `execution_core` audit |
| Managed Position Registry | `outputs/track_b_execution_core/managed_positions/latest_managed_positions.json` | `execution_core` authority |
| Managed Position events | `outputs/track_b_execution_core/managed_positions/managed_position_events.jsonl` | `execution_core` audit |
| Agent Registry | `outputs/track_b_execution_core/agent_registry/latest_agent_registry.json` | `execution_core` authority |
| Agent Health | `outputs/track_b_execution_core/agent_health/latest_agent_health.json` | `execution_core` authority |
| Self-Recover Rules | `outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json` | `execution_core` advisory authority |
| Shared Truth Refresh CLI | `mgc_v05l.execution_core.track_b_shared_truth_refresh_cli` | `execution_core` refresh orchestrator |
| Proof Readiness | `outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json` | `execution_core` proof preflight authority |
| Broker reconciliation | `outputs/track_b_execution_core/broker_reconciliation/latest_track_b_paper_broker_reconciliation.json` | `execution_core` reconciliation authority |
| Broker truth refresh status | `outputs/reports/ibkr_broker_truth_refresh/latest_broker_truth_refresh_status.json` | broker-truth evidence |
| Broker truth lease | `outputs/operator_dashboard/runtime/latest_broker_truth_lease.json` | current lease status path; migration target for `execution_core` ownership |
| Canonical readiness | `outputs/operator_dashboard/runtime/latest_canonical_readiness.json` | current readiness decision output consuming shared truth |
| Canonical readiness summary | `outputs/operator_dashboard/runtime/latest_canonical_readiness_summary.json` | current readiness summary consuming shared truth |
| Self-healing health | `outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json` | current restart diagnostic output consuming shared truth |

Dashboard projections may exist for operator visibility, for example
`outputs/operator_dashboard/runtime/latest_track_b_position_truth.json`,
`outputs/operator_dashboard/runtime/latest_track_b_runtime_environment_truth.json`,
`outputs/operator_dashboard/runtime/latest_track_b_open_order_truth.json`,
`outputs/operator_dashboard/runtime/latest_track_b_managed_positions.json`,
`outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json`,
`outputs/operator_dashboard/runtime/latest_track_b_agent_registry.json`, and
`outputs/operator_dashboard/runtime/latest_track_b_agent_health.json`, and
`outputs/operator_dashboard/runtime/latest_track_b_self_recover_rules.json`.
Each
projection must carry `projection_only=true`, `not_routing_authority=true`, and
`source_authority_path=<execution_core authority path>`. Runtime, readiness,
self-healing, launch, lifecycle, order, and broker code must not consume those
dashboard projection paths as authority.

Current consumer migration status:

- The PAPER runtime launch gate runs the Shared Truth Refresh stack before
  launch readiness evaluation and blocks before start on non-clean authority
  classifications.
- Canonical readiness consumes shared truth/proof-readiness evidence, including
  broker lease degradation and session-aware Phase-1 freshness such as
  `MARKET_CLOSED_NO_FRESH_BARS`.
- Self-healing restart diagnostics consume shared truth and classify distinct
  blockers such as `RESTART_BLOCKED_BROKER_LEASE_DEGRADED`,
  `RESTART_BLOCKED_OPEN_ORDER_TRUTH`,
  `RESTART_BLOCKED_MANAGED_ORDER_TRUTH`,
  `RESTART_BLOCKED_POSITION_TRUTH`, and
  `RESTART_BLOCKED_RECONCILIATION`.
- Operator/status displays now include a compact Shared Truth section and mark
  the displayed data as projection-only.
- Legacy GC, MGC, MNQ, Phase-1, and instrument-specific status panels that
  duplicate shared truth are diagnostic-only and not routing authority.
- Lifecycle transition authority, broker-backed entry auto-adoption, duplicate
  close prevention, broker-position-before-close guards, and runtime stop
  provenance are part of the shared control-plane boundary rather than
  dashboard-owned behavior.
- Agent Registry v1 declares required, optional, and diagnostic Track B agents
  but does not yet evaluate each agent's live health contract.
- Agent Health v1 evaluates read-only health contract status for registered
  agents without granting restart, broker, lifecycle, or routing authority.
- Self-Recover Rules v1 recommends allowed or blocked recovery actions from
  shared authority evidence, but never executes recovery.

Remaining migration backlog:

- Manual remediation scripts should consume Open Order Truth, Managed Order
  Registry, Position Truth, Managed Position Registry, and reconciliation
  evidence as shared inputs instead of rebuilding local broker/order state.
- The deeper runtime supervisor path should continue migrating from local PID,
  process, broker, and lease checks to Runtime Environment Truth plus shared
  proof-readiness evidence.
- Managed order v2 may add explicit operator-authorized modify-in-place
  execution, but the v1 Order Adjustment Planner is read-only and must not
  mutate broker orders.
- Remaining legacy research/status panels should either consume shared
  authority or be labeled diagnostic-only.
- Research/offline root/path migration should keep historical artifacts out of
  runtime, readiness, broker, lifecycle, and dashboard authority paths.
- Any new shared service should write its authority artifact under
  `outputs/track_b_execution_core/`, with dashboard/operator outputs limited to
  marked projections.
- Agent health contract v2 should add richer per-agent probes and self-recover
  recommendations without granting restart or broker authority.
- Self-Recover Rules v2 should add crash-loop budgets and operator approval
  workflow ids before any executor consumes its recommendations.

## Desktop Package / Deploy

Build the local desktop bundle without touching `/Applications`:

```bash
cd /Users/patrick/Dev/MGC-v05l-automation/desktop
npm run package:local
```

After explicit approval, build, package, and replace the installed macOS app:

```bash
cd /Users/patrick/Dev/MGC-v05l-automation/desktop
npm run deploy:applications -- --yes
```

The deploy script refuses to replace `/Applications/MGC Operator.app` unless
`--yes` is supplied. It copies the freshly packaged `MGC Operator.app` bundle
and preserves visible build metadata for the UI. This workflow is packaging and
visibility only; it does not invoke broker, TWS, IBKR, Databento, paper proof,
cancel, placeOrder, or submit behavior.

## Decision Styles

Track B signal intake supports multiple strategy-output styles without forcing
all future strategies into one model:

- `BINARY`: rule-based signal; scoring is not required.
- `SCORED_STATIC`: signal may include research-derived score, probability, EV,
  and confidence metadata. Policy thresholds may allow a no-submit proposed
  intent, but scoring remains informational.
- `SCORED_DYNAMIC`: schema-recognized for future work, but not implemented as
  decision authority.
- `HUMAN_REVIEW`: informational/review-only. It does not auto-propose an intent.

High score never authorizes a lane, creates an order plan, bypasses readiness,
or enables submit.

## Safety Invariants

- Current Track B phase is PAPER-only trading.
- Qualified signals may submit only through the guarded strategy-managed PAPER
  lifecycle when explicit PAPER submit gates pass.
- No-submit artifacts report `submit_allowed=false`.
- No-submit artifacts report `submit_attempted=false`.
- Current PAPER artifacts report `live_money_readiness=false`; live trading is
  future destination work after promotion gates, not current authority.
- Track B must not import `mgc_v05l.execution.*`, dashboard modules, Track A
  runtime/cache/snapshot authority, Schwab, or desktop UI.
- Dashboard/cache/snapshot data is never authority for Track B execution.
- No broker submit may occur while unresolved broker state exists for the same
  account/contract.
- Unexplained silence, zero strategy evaluation for an active strategy, hard
  signals without durable intents, intents without broker-backed lifecycle
  completion, and managed entries without clean close/reconciliation are
  product defects until classified.
- The current known `PendingCancel` order for `DUM882026` / `MGC-202606` blocks
  additional proof submits on that account/contract until broker state is
  terminal and clean.

## Consolidation Audit Note

The no-submit spine uses stage-specific verdicts and a shared operator-facing
report vocabulary: `submit_allowed`, `submit_attempted`,
`live_money_readiness`, `primary_blocker`, `secondary_blockers`,
`required_next_action`, and `generated_at`. Summary-style reports should keep
missing stages explicit instead of treating them as zero.

Small consistency cleanup from this audit:

- Signal batch, shadow run assembler, and attrition reports now expose
  `secondary_blockers` consistently.
- Attrition reports now include a clear `primary_blocker` and
  `required_next_action` when downstream stages are missing.

Deferred cleanup candidates:

- Some reports use `output_paths`; attrition reports use
  `output_artifacts_considered` because they may consume multiple upstream
  summaries. This is intentional for now and should not be renamed without a
  compatibility pass.
- A future shared report helper could reduce duplicated safety fields, but the
  current explicit repetition keeps each no-submit boundary easy to audit.

## Migration Roadmap

1. Finish and maintain the execution safety core.
2. Keep the no-submit shadow architecture stable and boring.
3. Add replay/runner capabilities around Track B-native artifacts.
4. Port one tiny strategy/lane into Track B shadow mode.
5. Compare Track B shadow output against expected Track A/research behavior.
6. After broker state clears, complete the paper proof lifecycle.
7. Later, connect approved Track B-native output to paper execution through the
   existing gated submit path.
8. Much later, design live-readiness separately.
9. Rebuild dashboard/app screens last or near-last on top of Track B artifacts.
10. Retire Track A runtime/dashboard authority.

## Canonical Example

The committed reference example lives under:

```text
examples/track_b_shadow_run/
```

The operator quickstart is:

```text
docs/track_b_shadow_run_quickstart.md
```

The future UI integration contract is:

```text
docs/track_b_ui_integration_contract.md
```

The example demonstrates:

```text
Databento market-data artifact, candle/event, or strategy-like input, optionally
-> candle/event JSON
-> shadow signal
-> signal-to-intent proposal policy
-> proposed intent
-> lane registry authorization
-> order plan
-> shadow evaluation
-> run-level summary
```

Example command chain:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.databento_candle_observer_cli \
  --quote-report-json examples/track_b_databento_candle_observer/databento_quote_report_fixture.json \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --timeframe quote_snapshot \
  --source-id databento_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/databento_candle_observer
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.databento_candle_observer_cli \
  --quote-report-json examples/track_b_databento_candle_observer/databento_quote_report_fixture.json \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --timeframe quote_snapshot \
  --source-id databento_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/databento_candle_observer \
  --watch \
  --max-cycles 5 \
  --poll-seconds 10
```

Bounded realtime current quote pull path:

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.databento_candle_observer_cli \
  --live-current-quote \
  --quote-provider-mode REALTIME \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --tick-size 0.1 \
  --exchange COMEX \
  --currency USD \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --timeframe quote_snapshot \
  --source-id databento_live_demo \
  --output-root outputs/track_b_execution_core/databento_candle_observer
```

When using `quote_provider_mode=REALTIME`, the current quote report must show
`realtime_subscription_attempted`, `realtime_quote_received`,
`quote_age_seconds`, `quote_freshness_verdict`, and
`current_quote_available`. Historical `available_end` reports remain explicit
diagnostic/backfill evidence only. They can preserve `requested_quote_end`,
`provider_available_end`, and fallback diagnostics, but readiness must stay
blocked for `quote_provider_mode=HISTORICAL_AVAILABLE_END`.

Bounded wait-for-current-quote mode:

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.databento_candle_observer_cli \
  --live-current-quote \
  --quote-provider-mode REALTIME \
  --wait-for-current-quote \
  --max-wait-cycles 10 \
  --wait-poll-seconds 15 \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --tick-size 0.1 \
  --exchange COMEX \
  --currency USD \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --timeframe quote_snapshot \
  --source-id wait_for_current_quote_check \
  --output-root outputs/track_b_execution_core/databento_candle_observer
```

Wait mode is a bounded market-data availability loop only. It writes heartbeat
state with `observer_mode=wait_for_current_quote`, realtime quote counts, and
`wait_succeeded`; it does not run strategy/listener/operator steps, submit, or
authorize paper proof.

Bounded current-quote watch path:

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.databento_candle_observer_cli \
  --live-current-quote \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --tick-size 0.1 \
  --exchange COMEX \
  --currency USD \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --timeframe quote_snapshot \
  --source-id databento_live_demo \
  --output-root outputs/track_b_execution_core/databento_candle_observer \
  --watch \
  --max-cycles 5 \
  --poll-seconds 10
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.strategy_signal_adapter_cli \
  --strategy-event-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_event.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id databento_strategy_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/strategy_signal_adapter
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.strategy_signal_adapter_cli \
  --strategy-event-json examples/track_b_strategy_signal_adapter/demo_candle_direction_long.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id strategy_demo \
  --output-root outputs/track_b_execution_core/strategy_signal_adapter
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.candle_signal_producer_cli \
  --candle-event-json examples/track_b_candle_signal_producer/candle_event_binary_long.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id candle_demo \
  --output-root outputs/track_b_execution_core/candle_signal_producer
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_signal_cli \
  --signal-json examples/track_b_shadow_run/signal_binary_valid.json \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/shadow_signals
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.signal_intent_proposal_cli \
  --signal-json examples/track_b_shadow_run/signal_scored_static_valid.json \
  --policy-json examples/track_b_shadow_run/proposal_policy_scored_static.json \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/signal_intent_proposals
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_run_assembler_cli \
  --manifest-json examples/track_b_shadow_run/manifest.json \
  --registry-json examples/track_b_shadow_run/lane_registry.json \
  --intent-json examples/track_b_shadow_run/intent_valid.json \
  --intent-json examples/track_b_shadow_run/intent_blocked_qty.json \
  --output-root outputs/track_b_execution_core/shadow_runs
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_replay_runner_cli \
  --signal-batch-json examples/track_b_shadow_run/signal_batch.json \
  --proposal-policy-json examples/track_b_shadow_run/proposal_policy_scored_static.json \
  --manifest-json examples/track_b_shadow_run/manifest.json \
  --registry-json examples/track_b_shadow_run/lane_registry.json \
  --output-root outputs/track_b_execution_core/shadow_replay_runs
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_listener_cli \
  --listener-config-json examples/track_b_shadow_listener/listener_config.json \
  --output-root outputs/track_b_execution_core/shadow_listener
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_observation_runner_cli \
  --quote-report-json examples/track_b_databento_candle_observer/databento_quote_report_fixture.json \
  --listener-config-json examples/track_b_shadow_listener/listener_config.json \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --source-id track_b_observation_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/track_b_observation_runner
```

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.track_b_observation_runner_cli \
  --live-current-quote \
  --listener-config-json examples/track_b_shadow_listener/listener_config.json \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --tick-size 0.1 \
  --exchange COMEX \
  --currency USD \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --source-id track_b_observation_live_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/track_b_observation_runner
```

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.track_b_readiness_check_runner_cli \
  --mode PAPER \
  --host 127.0.0.1 \
  --port 7497 \
  --client-id 17077 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --broker-order-id 1 \
  --perm-id 736787312 \
  --market-data-mode DELAYED \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --tick-size 0.1 \
  --exchange COMEX \
  --currency USD \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --proof-timing-status ACTIVE_SESSION \
  --max-wait-cycles 10 \
  --wait-poll-seconds 15 \
  --quote-provider-mode REALTIME \
  --output-root outputs/track_b_execution_core/track_b_readiness_check_runner
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.operator_status_cli \
  --track-b-readiness-check-runner-report-json outputs/track_b_execution_core/track_b_readiness_check_runner/latest_track_b_readiness_check_runner_report.json \
  --track-b-observation-runner-report-json outputs/track_b_execution_core/track_b_observation_runner/latest_track_b_observation_runner_report.json \
  --track-b-strategy-rule-runner-report-json outputs/track_b_execution_core/track_b_strategy_rule_runner/latest_track_b_strategy_rule_runner_report.json \
  --track-b-strategy-paper-runner-report-json outputs/track_b_execution_core/track_b_strategy_paper_runner/latest_track_b_strategy_paper_runner_report.json \
  --databento-candle-observer-report-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json \
  --databento-candle-observer-heartbeat-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_heartbeat.json \
  --listener-heartbeat-json <LISTENER_OUTPUT_ROOT>/<LISTENER_ID>/latest_shadow_listener_heartbeat.json \
  --listener-health-json <LISTENER_OUTPUT_ROOT>/<LISTENER_ID>/latest_shadow_listener_health.json \
  --listener-cycle-json <LISTENER_CYCLE_JSON optional> \
  --shadow-runner-summary-json <RUNNER_SUMMARY_JSON optional> \
  --attrition-report-json <ATTRITION_REPORT_JSON optional> \
  --strategy-signal-adapter-report-json outputs/track_b_execution_core/strategy_signal_adapter/latest_strategy_signal_adapter_report.json \
  --candle-signal-producer-report-json outputs/track_b_execution_core/candle_signal_producer/latest_candle_signal_producer_report.json \
  --signal-batch-writer-report-json outputs/track_b_execution_core/signal_batch_writer/latest_signal_batch_writer_report.json \
  --readiness-summary-json <READINESS_SUMMARY_JSON optional> \
  --recovery-report-json <RECOVERY_REPORT_JSON optional> \
  --preflight-report-json <PREFLIGHT_REPORT_JSON optional> \
  --quote-report-json <QUOTE_REPORT_JSON optional> \
  --output-root outputs/track_b_execution_core/operator_status
```

Example commands are no-submit unless they explicitly include PAPER submit
flags such as `--submit-paper --confirm-paper-submit`. Commands that use
`--live-current-quote` make a bounded Databento market-data request only;
Databento remains evidence, not execution authority.

Attrition reports are intended to avoid Track A-style unexplained trade count
loss across competing layers. They can consume signal batch, shadow run, and
readiness summaries and report blocker counts by stage/type.

## Non-Goals

- No strategy port yet.
- No dynamic scoring implementation yet.
- No dashboard rebuild yet.
- No live trading readiness.
- No Track A integration as the destination architecture.
- No code-side cancel behavior for the current stale broker order.
