# Track B Shadow Run Quickstart

This is a no-submit Track B reference example. It does not connect to TWS, does
not connect to Databento, does not create orders, and does not call
`paper_proof_cli`.

## Fixture Files

- `examples/track_b_shadow_run/manifest.json` defines the reproducible shadow
  run context: PAPER mode, explicit account `DUM882026`, explicit execution
  contract `MGC-202606`, required artifacts, and `submit_enabled=false`.
- `examples/track_b_shadow_run/lane_registry.json` authorizes one example lane:
  `track_b_example_gold_shadow_v1` / `mgc_example_long_lmt_day`, LMT DAY, BUY,
  max quantity 1.
- `examples/track_b_shadow_run/intent_valid.json` is a valid PAPER/LMT/DAY
  quantity-1 intent for operator review.
- `examples/track_b_shadow_run/intent_blocked_qty.json` is intentionally
  quantity 2, so the lane registry blocks it.
- `examples/track_b_shadow_run/signal_binary_valid.json` is a valid BINARY
  shadow signal. It needs no scoring block.
- `examples/track_b_shadow_run/signal_scored_static_valid.json` is a
  SCORED_STATIC signal whose informational score passes the example policy.
- `examples/track_b_shadow_run/signal_scored_static_below_threshold.json` is a
  SCORED_STATIC signal whose informational score fails the example policy.
- `examples/track_b_shadow_run/proposal_policy_binary.json` supplies the LMT
  DAY quantity-1 defaults needed to propose an intent from the binary signal.
- `examples/track_b_shadow_run/proposal_policy_scored_static.json` supplies the
  same order defaults plus static score thresholds.

The local execution contract key remains the execution authority. The manifest
does not authorize lanes by itself; the lane registry does. The strategy intent
only describes a proposed action. The order plan and shadow evaluation remain
no-submit.

Scores are informational and policy-interpreted only. A high score does not
authorize a lane, create an order plan, bypass readiness, or permit submit.
`SCORED_DYNAMIC` is schema-recognized for future work, but it is not
decision-authoritative yet. `HUMAN_REVIEW` signals do not automatically propose
an intent.

## Signal Commands

Validate the binary signal:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_signal_cli \
  --signal-json examples/track_b_shadow_run/signal_binary_valid.json \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/shadow_signals
```

Apply the binary signal-to-intent proposal policy:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.signal_intent_proposal_cli \
  --signal-json examples/track_b_shadow_run/signal_binary_valid.json \
  --policy-json examples/track_b_shadow_run/proposal_policy_binary.json \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/signal_intent_proposals
```

Apply the static scored policy to the passing and failing examples:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.signal_intent_proposal_cli \
  --signal-json examples/track_b_shadow_run/signal_scored_static_valid.json \
  --policy-json examples/track_b_shadow_run/proposal_policy_scored_static.json \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/signal_intent_proposals
```

```bash
./.venv/bin/python -m mgc_v05l.execution_core.signal_intent_proposal_cli \
  --signal-json examples/track_b_shadow_run/signal_scored_static_below_threshold.json \
  --policy-json examples/track_b_shadow_run/proposal_policy_scored_static.json \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/signal_intent_proposals
```

The first two proposal commands should create embedded `proposed_intent`
objects in their reports. The below-threshold signal should produce
`INTENT_PROPOSAL_BLOCKED_STATIC_SCORE_BELOW_THRESHOLD`.

Process the committed signal batch fixture:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.signal_batch_cli \
  --batch-json examples/track_b_shadow_run/signal_batch.json \
  --policy-json examples/track_b_shadow_run/proposal_policy_scored_static.json \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/signal_batches
```

The batch should create proposed-intent artifacts for the binary and
above-threshold static-scored signals, and count the below-threshold static
signal as a blocked proposal.

Create an attrition report after generating summaries:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.attrition_report_cli \
  --signal-batch-summary-json <SIGNAL_BATCH_SUMMARY_JSON> \
  --shadow-run-summary-json <SHADOW_RUN_SUMMARY_JSON optional> \
  --readiness-summary-json <READINESS_SUMMARY_JSON optional> \
  --output-root outputs/track_b_execution_core/attrition_reports
```

Attrition reports explain where candidates dropped out by stage and blocker
type. Missing downstream stages are reported as missing, not silently counted
as zero.

Run the one-command no-submit replay runner:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_replay_runner_cli \
  --signal-batch-json examples/track_b_shadow_run/signal_batch.json \
  --proposal-policy-json examples/track_b_shadow_run/proposal_policy_scored_static.json \
  --manifest-json examples/track_b_shadow_run/manifest.json \
  --registry-json examples/track_b_shadow_run/lane_registry.json \
  --output-root outputs/track_b_execution_core/shadow_replay_runs
```

The replay runner is orchestration only. It runs signal batch processing,
shadow run assembly for created proposed-intent artifacts, attrition reporting,
and a top-level runner summary. It creates no new authority, does not connect
to TWS or Databento, and does not submit.

Run the poll-once no-submit shadow listener example:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_listener_cli \
  --listener-config-json examples/track_b_shadow_listener/listener_config.json \
  --output-root outputs/track_b_execution_core/shadow_listener
```

The listener is ingestion/orchestration only. It claims signal batch JSON files
from the configured inbox, invokes `shadow_replay_runner`, writes per-file
event reports and one cycle summary, then moves inputs to processed or failed
directories. The default behavior is poll-once and exit. Engine-running state
does not imply submit authority; this listener remains no-submit and does not
connect to TWS or Databento.

Write a demo signal observation into the listener inbox:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.signal_batch_writer_cli \
  --signal-json examples/track_b_signal_batch_writer/signal_observation.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --source-id manual_demo \
  --batch-id track_b_writer_demo_batch_001 \
  --expected-account-id DUM882026 \
  --output-root outputs/track_b_execution_core/signal_batch_writer
```

The writer is an upstream producer only. It validates the signal or batch
payload, writes a uniquely named JSON file into the listener inbox using a temp
file plus rename, and stops. It does not invoke the listener, apply proposal
policy, authorize lanes, create order plans, connect to broker/data providers,
or submit. It also updates this stable latest report pointer:

```text
outputs/track_b_execution_core/signal_batch_writer/latest_signal_batch_writer_report.json
```

Convert a supplied Databento quote/candle artifact into a Track B candle/event
artifact:

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

The Databento candle observer is a market-data evidence bridge only. The
fixture path consumes a supplied quote/candle artifact and writes a no-submit
candle/event JSON file; it does not connect to Databento live streaming, IBKR,
TWS, the listener, the runner, operator status, or any submit path. Databento
symbols remain market-data selectors only, and the local execution contract key
remains execution authority. It updates:

```text
outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_event.json
outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json
```

For a bounded observation proof, explicitly enable watch mode:

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

Watch mode is still bounded no-submit infrastructure. It repeatedly re-reads
the supplied market-data artifact, writes normal observer reports/events, and
updates:

```text
outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_heartbeat.json
```

No-data or malformed cycles are counted explicitly in the heartbeat. Watch mode
does not run the strategy adapter, listener, runner, operator status, broker,
or Databento live streaming, and it does not mean trading mode.

For a bounded realtime Databento quote pull, explicitly enable the current
quote path. This uses Databento Live subscription capability, writes a current
quote report under `outputs/track_b_execution_core/current_quotes`, then writes
the normal observer event/report artifacts. It still does not infer direction,
authorize trades, invoke the listener, or submit:

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.databento_candle_observer_cli \
  --live-current-quote \
  --quote-provider-mode REALTIME \
  --use-databento-realtime-quote \
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

Bound the current-quote observation loop when using watch mode:

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
  --output-root outputs/track_b_execution_core/databento_candle_observer \
  --watch \
  --max-cycles 5 \
  --poll-seconds 10
```

`DATABENTO_API_KEY` is read from the operator environment and is not written to
reports. If the key, entitlement, window, or quote data is unavailable, the
observer writes an explicit blocked/no-data report instead of silently treating
the state as OK.

Realtime dependency failures are explicit in the current quote and observer
reports. Check `databento_dependency_status`, `databento_package_version`,
`databento_live_api_available`, `provider_error_category`,
`symbol_subscription_attempted`, and `symbol_subscription_succeeded` to
distinguish a missing Databento package, installed package without the Live API,
missing API key, subscription/entitlement failure, and no quote within the
bounded wait. The Databento client is declared as the project’s optional
`databento` dependency; for a fresh venv install it with
`./.venv/bin/pip install -e '.[databento]'`.

The older `HISTORICAL_AVAILABLE_END` provider mode remains diagnostic/backfill
only. If Databento historical reports that a requested window is after
`available_end`, Track B preserves `requested_quote_end`,
`provider_available_end`, and fallback diagnostics, but readiness stays blocked
even when the available-end quote is recent. Do not use
`--allow-available-end-fallback` or `--max-current-quote-age-seconds` as a
substitute for the realtime feed.

To wait safely for a realtime quote artifact, use explicit bounded wait mode:

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.databento_candle_observer_cli \
  --live-current-quote \
  --quote-provider-mode REALTIME \
  --use-databento-realtime-quote \
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

This writes `latest_databento_candle_observer_heartbeat.json` with
`observer_mode=wait_for_current_quote`, current/max cycle counts,
`realtime_subscription_attempted`, `realtime_quote_received`, and
`wait_succeeded`. It waits for market-data availability only; it does not
submit, authorize paper proof, run the listener, or turn fallback/historical
quotes into readiness.

The output event is already compatible with `strategy_signal_adapter_cli`; no
extra bridge command is required in this slice. Direction is explicit, not
inferred from candle shape. Include `--signal-direction LONG` or `SHORT` on the
observer command or on the strategy adapter command only when an upstream
artifact is intentionally directional. Omit it for review-only HUMAN_REVIEW
input.

Feed that market-data event into the explicit strategy adapter as a separate
operator step:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.strategy_signal_adapter_cli \
  --strategy-event-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_event.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id databento_strategy_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/strategy_signal_adapter
```

## Phase 2 Strategy Rule Runner

The first real Track B strategy-rule runner is a no-submit MGC-only rule. It
evaluates realtime Databento quote/candle evidence plus explicit precomputed
EMA momentum/VWAP fields carried on the event metadata, then emits a Track B
signal batch only when rule emission is explicit:

```text
Databento realtime quote/event
-> track_b_data_maintenance
-> track_b_market_history
-> track_b_feature_builder
-> track_b_strategy_rule_runner
-> strategy_signal_adapter
-> candle_signal_producer
-> signal_batch_writer
-> shadow_listener / operator_status / Track B Status UI
```

Track B now has two separate data lanes. Historical/replay data maintenance is
the Track A-style scheduled process: it pulls official Databento historical 1m
bars, normally weekly, intended through Friday close, for research, replay,
backtesting, and historical base data. Runtime live candle/quote capture is
separate and is now represented by a bounded runtime MGC 1m candle capture
artifact when an execution rule needs same-session candle context. Realtime
quote evidence remains separate in all cases.

The realtime observer's latest candle event is only a single snapshot. The
EMA/VWAP rule can use maintained history as historical context, but that should
not be confused with live runtime candle capture. Historical bars and realtime
quote evidence stay separately labeled; no single snapshot is expanded into
fake history.

Maintain the local rolling MGC 1m history from a bounded Databento fetch:

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.track_b_data_maintenance_cli \
  --instrument MGC \
  --fetch-databento-history \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --databento-symbol MGCM6 \
  --dataset GLBX.MDP3 \
  --schema ohlcv-1m \
  --allowlisted-local-symbol MGCM6 \
  --timeframe 1m \
  --lookback-minutes 240 \
  --max-bars 5000 \
  --export-bars 50 \
  --min-bars 20 \
  --historical-maintenance-cutoff-policy WEEKLY_FRIDAY_CLOSE \
  --intended-cutoff-timestamp <FRIDAY_CLOSE_UTC> \
  --runtime-intraday-freshness-policy NOT_REQUESTED \
  --source-id track_b_data_maintenance_mgc_1m \
  --output-root outputs/track_b_execution_core/track_b_data_maintenance
```

Stable data-maintenance artifacts:

```text
outputs/track_b_execution_core/track_b_data_maintenance/latest_good_mgc_1m_history.json
outputs/track_b_execution_core/track_b_data_maintenance/latest_track_b_data_maintenance_report.json
```

The data-maintenance fetch may use Databento historical `available_end` safely.
If the requested history end is later than the provider's available end, the
CLI retries the OHLCV request ending at `provider_available_end` and labels the
artifact `history_provider_mode=HISTORICAL_AVAILABLE_END`. The report exposes
`requested_history_end`, `provider_available_end`, `history_end_used`,
`available_end_lag_seconds`, `history_freshness_seconds`,
`intended_cutoff_timestamp`, `latest_bar_timestamp`,
`complete_through_cutoff`, and `missing_bars`. Historical maintenance is ready
when enough bars are present, gaps are acceptable, and the history is complete
through the intended cutoff. It does not fail merely because Friday-close
history is older than the current minute. Use
`--runtime-intraday-freshness-policy REQUIRE_MAX_AGE` only for an explicit
runtime intraday freshness check; these maintained bars are still not realtime
quote evidence.

On-demand historical fetch through `track_b_mgc_candle_history_producer_cli`
remains available for diagnostics and maintenance inputs, but it is not the
normal trade-decision path.

For execution-time EMA/VWAP/reclaim features, use the bounded runtime candle
capture lane instead of weekly historical maintenance. The first implementation
accepts supplied runtime candle JSON, overwrites stable latest artifacts, keeps
only a small number of run folders, and does not create an unbounded raw
stream:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_runtime_candle_capture_cli \
  --runtime-candle-json examples/track_b_runtime_candle_capture/runtime_mgc_1m_candles.json \
  --expected-account-id DUM882026 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --local-symbol MGCM6 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --timeframe 1m \
  --max-bars 250 \
  --min-bars 3 \
  --source-id track_b_runtime_mgc_capture \
  --output-root outputs/track_b_execution_core/track_b_runtime_candle_capture
```

Stable runtime candle artifacts:

```text
outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json
outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_candle_capture_report.json
```

This runtime capture artifact is not the research archive. It is a bounded
same-session context window for feature building. If live Databento streaming
is needed later, it should feed this same bounded artifact shape rather than
writing an unbounded stream by default.

The data-maintenance registry currently enables only `MGC` for runtime
maintenance. Disabled planning entries preserve the broader Track A-style
universe shape for later migration: `GC`, `MES`, `ES`, `MNQ`, `NQ`, `ZT`, `ZF`,
`ZN`, `ZB`, `ZQ`, `6E`, `6J`, `6B`, `6A`, `HG`, `QC`, `PL`, `CL`, `NG`, `MBT`,
`YM`, plus research placeholders `SPY`, `QQQ`, `TQQQ`, and `SQQQ`. These are
not fetched in this slice.

Fixture/supplied-history path:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_mgc_candle_history_producer_cli \
  --history-json <MGC_OHLCV_HISTORY_JSON> \
  --current-quote-report-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --timeframe 1m \
  --max-candles 50 \
  --min-candles 3 \
  --output-root outputs/track_b_execution_core/track_b_mgc_candle_history_producer
```

Bounded Databento history request path:

```bash
set -a
source .env.local
set +a
./.venv/bin/python -m mgc_v05l.execution_core.track_b_mgc_candle_history_producer_cli \
  --fetch-databento-history \
  --current-quote-report-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json \
  --expected-account-id DUM882026 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --schema ohlcv-1m \
  --allowlisted-local-symbol MGCM6 \
  --timeframe 1m \
  --lookback-minutes 60 \
  --max-candles 50 \
  --min-candles 3 \
  --output-root outputs/track_b_execution_core/track_b_mgc_candle_history_producer
```

Stable producer artifacts:

```text
outputs/track_b_execution_core/track_b_mgc_candle_history_producer/latest_track_b_mgc_candle_history_input.json
outputs/track_b_execution_core/track_b_mgc_candle_history_producer/latest_track_b_mgc_candle_history_producer_report.json
```

Use
`outputs/track_b_execution_core/track_b_mgc_candle_history_producer/latest_track_b_mgc_candle_history_input.json`
as the `--candle-history-json` value for
`track_b_strategy_paper_runner_cli`. The required current quote report for that
same runner is
`outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json`.

Then normalize that input into the market-history event consumed by the feature
builder:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_market_history_cli \
  --market-history-json outputs/track_b_execution_core/track_b_mgc_candle_history_producer/latest_track_b_mgc_candle_history_input.json \
  --expected-account-id DUM882026 \
  --contract-key MGC-202606 \
  --databento-continuous-symbol MGC.v.0 \
  --dataset GLBX.MDP3 \
  --allowlisted-local-symbol MGCM6 \
  --timeframe 1m \
  --max-candles 50 \
  --min-candles 3 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --output-root outputs/track_b_execution_core/track_b_market_history
```

Stable market-history artifacts:

```text
outputs/track_b_execution_core/track_b_market_history/latest_track_b_market_history_event.json
outputs/track_b_execution_core/track_b_market_history/latest_track_b_market_history_report.json
```

The collector is market-data evidence only. It does not connect to broker
paths, infer execution authority, invoke the listener, run paper proof, or
submit. If only one candle is present, or if realtime current quote evidence is
missing/stale, it blocks with
`TRACK_B_MARKET_HISTORY_BLOCKED_INSUFFICIENT_HISTORY` or
`TRACK_B_MARKET_HISTORY_BLOCKED_NON_REALTIME_INPUT`.

The feature builder is the upstream no-submit artifact producer for
`mgc_ema_momentum_reclaim_long_v1`. It takes explicit MGC candle/quote history,
requires current realtime Databento evidence, and writes the EMA/VWAP momentum
fields consumed by the rule. If history is insufficient or evidence is stale,
it blocks explicitly and does not fake a signal-ready event:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_feature_builder_cli \
  --source-event-json outputs/track_b_execution_core/track_b_market_history/latest_track_b_market_history_event.json \
  --expected-account-id DUM882026 \
  --rule-id mgc_ema_momentum_reclaim_long_v1 \
  --output-root outputs/track_b_execution_core/track_b_feature_builder
```

Stable feature builder artifacts:

```text
outputs/track_b_execution_core/track_b_feature_builder/latest_track_b_feature_event.json
outputs/track_b_execution_core/track_b_feature_builder/latest_track_b_feature_builder_report.json
```

Default behavior is review/no-signal. This command evaluates the latest
feature event with the current default rule but does not emit
listener inbox work:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_strategy_rule_runner_cli \
  --input-event-json outputs/track_b_execution_core/track_b_feature_builder/latest_track_b_feature_event.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id track_b_phase2_rule_review \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --rule-id mgc_ema_momentum_reclaim_long_v1 \
  --rule-mode MGC_EMA_MOMENTUM_RECLAIM_LONG \
  --output-root outputs/track_b_execution_core/track_b_strategy_rule_runner
```

To intentionally create no-submit listener inbox work when the rule conditions
pass, add `--emit-signal`. The rule still only creates signal batch work; it
does not call `paper_proof_cli`, create order plans, run lane authorization
directly, or submit:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_strategy_rule_runner_cli \
  --input-event-json outputs/track_b_execution_core/track_b_feature_builder/latest_track_b_feature_event.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id track_b_phase2_rule_demo \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --rule-id mgc_ema_momentum_reclaim_long_v1 \
  --rule-mode MGC_EMA_MOMENTUM_RECLAIM_LONG \
  --emit-signal \
  --output-root outputs/track_b_execution_core/track_b_strategy_rule_runner
```

The `mgc_ema_momentum_reclaim_long_v1` rule requires explicit realtime evidence
fields from the Databento quote report referenced by the event:

- `quote_provider_mode=REALTIME`
- `realtime_quote_received=true`
- `current_quote_available=true`

It also requires these event/metadata fields, typically under
`metadata.ema_momentum_features`:

- `close`
- `vwap` or `reference_vwap`
- `prior_close` or `previous_close`
- `momentum_norm`
- `momentum_acceleration`
- `momentum_turning_positive`

The initial LONG condition is intentionally narrow: close must reclaim VWAP
after a prior close below VWAP, momentum must be turning positive, and momentum
norm/acceleration must be at or above the configured thresholds, defaulting to
zero. Missing fields or failed conditions produce NO_SIGNAL artifacts, not
execution.

Historical `available_end`, fallback, or fixture evidence cannot produce a
strategy signal unless `--allow-fixture-input` is intentionally supplied for a
test/demo. The older `DEMO_LONG_ONLY` mode remains available as an explicit
wiring proof, but it is no longer the default strategy rule. The latest runner
read model is:

```text
outputs/track_b_execution_core/track_b_strategy_rule_runner/latest_track_b_strategy_rule_runner_report.json
```

Phase 2 paper execution stance: because Track B paper proof has passed the
full PAPER open/guarded-close/flat lifecycle, PAPER execution is now allowed
only through explicit Track B-controlled submit paths. The strategy paper
runner can hand off to readiness and paper proof only when explicit
flags/config request that handoff. The strategy rule runner itself remains
no-submit and does not create hidden submit authority. Every PAPER execution
must write artifacts and a final broker-state classification.

## Phase 2 Strategy Paper Runner

The controlled strategy PAPER runner wires:

```text
Databento realtime/current evidence
-> track_b_runtime_candle_capture when runtime candles are required
-> track_b_market_history
-> track_b_feature_builder
-> track_b_strategy_rule_runner
-> track_b_readiness_check_runner
-> paper_proof_cli / paper proof lifecycle
```

Default mode is dry-run/no-submit. This evaluates the rule and, if a signal is
emitted, checks readiness, but stops before paper proof because no submit flags
are present:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_strategy_paper_runner_cli \
  --mode PAPER \
  --runtime-candle-context-json outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json \
  --runtime-candle-context-required \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --rule-id mgc_ema_momentum_reclaim_long_v1 \
  --rule-mode MGC_EMA_MOMENTUM_RECLAIM_LONG \
  --emit-signal \
  --output-root outputs/track_b_execution_core/track_b_strategy_paper_runner
```

This single command reads bounded runtime candle context, then runs the
market-history collector, feature builder, strategy rule, and readiness check
in sequence. Weekly maintained local history remains available as historical
context with `--maintained-history-json`, but rules requiring same-session
candles should use `--runtime-candle-context-json`. If a market-history or
feature event has already been built for review, use
`--build-features-from outputs/track_b_execution_core/track_b_market_history/latest_track_b_market_history_event.json`
or
`--feature-event-json outputs/track_b_execution_core/track_b_feature_builder/latest_track_b_feature_event.json`
instead of `--maintained-history-json`.

PAPER submit requires all explicit gates. Prices and quantity are not inferred:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_strategy_paper_runner_cli \
  --mode PAPER \
  --runtime-candle-context-json outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json \
  --runtime-candle-context-required \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --rule-id mgc_ema_momentum_reclaim_long_v1 \
  --rule-mode MGC_EMA_MOMENTUM_RECLAIM_LONG \
  --emit-signal \
  --quantity 1 \
  --manual-open-limit-price <OPEN_LIMIT_PRICE> \
  --manual-close-limit-price <CLOSE_LIMIT_PRICE> \
  --submit-paper \
  --confirm-paper-submit \
  --output-root outputs/track_b_execution_core/track_b_strategy_paper_runner
```

This path remains PAPER-only. It does not support live-money execution, market
orders, UI authority, hidden submit, inferred direction, inferred prices, or
broker mutation outside the Track B paper proof lifecycle. If the real rule
emits `NO_SIGNAL`, if candle history/features are insufficient or non-realtime,
or if readiness blocks, the runner stops cleanly and `paper_proof_invoked=false`.
Maintained 1m bars are historical context, not realtime quote evidence, so the
runner still requires the separate realtime current quote report. If an
execution rule requires same-session candles, add
`--runtime-candle-context-required`; maintained historical context alone will
then block until a runtime live candle context artifact is supplied. If an
operator explicitly wants an intraday max-age check, use
`--runtime-intraday-freshness-policy REQUIRE_MAX_AGE` together with
`--max-maintained-history-age-seconds <SECONDS>`. The PAPER-only
`--allow-stale-maintained-history-paper` override applies only to that explicit
intraday age check; it does not override missing bars, insufficient bars, gaps,
or missing realtime quote evidence, and it never enables live-money execution.
The latest report is:

```text
outputs/track_b_execution_core/track_b_strategy_paper_runner/latest_track_b_strategy_paper_runner_report.json
```

### Bounded real-rule wait mode

Use `track_b_real_rule_wait_runner_cli` when you want Track B to keep checking
the real MGC rule for a bounded number of cycles without forcing a signal:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_real_rule_wait_runner_cli \
  --mode PAPER \
  --runtime-candle-context-json outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json \
  --max-cycles 10 \
  --poll-seconds 15 \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --allowlisted-local-symbol MGCM6 \
  --con-id 712565978 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --output-root outputs/track_b_execution_core/track_b_real_rule_wait_runner
```

Repeated `NO_SIGNAL` cycles end as
`TRACK_B_REAL_RULE_WAIT_NO_SIGNAL_NO_MUTATION`; readiness, paper proof, submit,
and broker mutation remain false. If a real-rule signal appears without PAPER
submit flags, the runner stops as
`TRACK_B_REAL_RULE_WAIT_SIGNAL_READY_NO_SUBMIT`.

To allow PAPER execution only if a real-rule signal naturally appears, add the
explicit PAPER gates:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_real_rule_wait_runner_cli \
  --mode PAPER \
  --runtime-candle-context-json outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json \
  --max-cycles 10 \
  --poll-seconds 15 \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --allowlisted-local-symbol MGCM6 \
  --con-id 712565978 \
  --strategy-id track_b_example_gold_shadow_v1 \
  --lane-id mgc_example_long_lmt_day \
  --quantity 1 \
  --manual-open-limit-price <OPEN_LIMIT_PRICE> \
  --manual-close-limit-price <CLOSE_LIMIT_PRICE> \
  --submit-paper \
  --confirm-paper-submit \
  --output-root outputs/track_b_execution_core/track_b_real_rule_wait_runner
```

This is real-rule only. `DEMO_WIRING_PROOF` belongs to the strategy paper
runner proof branch and is blocked by the real-rule wait runner so it cannot be
mistaken for `mgc_ema_momentum_reclaim_long_v1`. The wait report stays bounded:
it writes the final run report and latest pointer under
`outputs/track_b_execution_core/track_b_real_rule_wait_runner/`.

### Asian Drift tonight watch path

Track B can watch Asian Drift tonight only from an explicit Asia Drift
state/feature snapshot. It does not compute the Asia Drift state machine from
raw runtime candles in this slice, and it does not guess missing rule
semantics. The minimum snapshot fields are:

- `strategy_id=asian_drift_v1`
- `contract_key=MGC-202606`, `instrument_family=MGC`
- `timeframe=5m`
- `asia_drift_state`
- `asia_drift_regime`
- `hypothetical_entry_ready`
- `entry_window_open`
- `in_scope`
- `feature_version`
- `calibration_profile`
- realtime quote evidence fields or a current quote report path

No-submit watch/evaluation:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_asian_drift_state_cli \
  --state-json <EXPLICIT_ASIAN_DRIFT_5M_STATE_JSON> \
  --expected-account-id DUM882026 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --instrument-family MGC \
  --source-id asian_drift_track_b_watch \
  --strategy-id asian_drift_v1 \
  --lane-id mgc_example_long_lmt_day \
  --output-root outputs/track_b_execution_core/asian_drift_state

./.venv/bin/python -m mgc_v05l.execution_core.track_b_strategy_rule_runner_cli \
  --input-event-json outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id asian_drift_track_b_watch \
  --rule-id asian_drift_v1 \
  --rule-mode ASIAN_DRIFT_V1 \
  --emit-signal \
  --output-root outputs/track_b_execution_core/track_b_strategy_rule_runner
```

Valid no-submit outcomes:

- `ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION`: explicit snapshot is valid, but no entry
  setup is armed.
- `ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT`: explicit snapshot is entry-ready and a
  no-submit signal batch was written for listener observation.
- `ASIAN_DRIFT_NOT_READY_FOR_TONIGHT`: required Asia Drift fields or realtime
  evidence are missing.

This watch mode reports `readiness_invoked=false`,
`paper_proof_invoked=false`, `submit_attempted=false`,
`broker_state_mutated=false`, and `live_money_readiness=false`. PAPER proof
remains a separate explicit Track B paper-runner decision and must not be
triggered by UI controls or hidden submit logic.

If the no-submit rule report shows an actual `ASIAN_DRIFT_V1` signal and the
operator intentionally wants one bounded PAPER lifecycle, use the strategy
paper runner. This is still not UI authority and not live money:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.track_b_strategy_paper_runner_cli \
  --mode PAPER \
  --input-event-json outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --account-id DUM882026 \
  --contract-key MGC-202606 \
  --allowlisted-local-symbol MGCM6 \
  --con-id 712565978 \
  --strategy-id asian_drift_v1 \
  --lane-id mgc_example_long_lmt_day \
  --rule-id asian_drift_v1 \
  --rule-mode ASIAN_DRIFT_V1 \
  --emit-signal \
  --side BUY \
  --submit-paper \
  --confirm-paper-submit \
  --quantity 1 \
  --manual-open-limit-price <SAFE_PAPER_OPEN_LIMIT> \
  --manual-close-limit-price <SAFE_PAPER_CLOSE_LIMIT> \
  --output-root outputs/track_b_execution_core/track_b_strategy_paper_runner
```

Use `--side SELL` only for an explicit `SHORT` Asian Drift state signal. A
side/signal mismatch blocks before readiness or paper proof. `DEMO_LONG_ONLY`
and `DEMO_WIRING_PROOF` cannot drive the Asian Drift PAPER path.

Translate an explicit candle/event input into listener inbox work:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.candle_signal_producer_cli \
  --candle-event-json examples/track_b_candle_signal_producer/candle_event_binary_long.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id candle_demo \
  --output-root outputs/track_b_execution_core/candle_signal_producer
```

The candle signal producer is a scaffold for upstream candle-style inputs. It
translates explicit candle/event JSON into Track B shadow signal observations
and reuses the signal batch writer for validated atomic inbox writes. It does
not infer a trade direction from candle prices. Missing direction becomes
review-only `HUMAN_REVIEW` input, and the producer never invokes the listener,
runner, operator status, broker/data providers, or submit paths. It also
updates:

```text
outputs/track_b_execution_core/candle_signal_producer/latest_candle_signal_producer_report.json
```

Adapt one explicit strategy-like candle direction event into listener inbox
work:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.strategy_signal_adapter_cli \
  --strategy-event-json examples/track_b_strategy_signal_adapter/demo_candle_direction_long.json \
  --inbox-dir examples/track_b_shadow_listener/inbox \
  --expected-account-id DUM882026 \
  --source-id strategy_demo \
  --output-root outputs/track_b_execution_core/strategy_signal_adapter
```

The demo strategy signal adapter is not a production strategy port. It accepts
explicit direction/side from the input event, emits BINARY or review-only
HUMAN_REVIEW signal input through the candle signal producer, and stops. It
does not infer direction from candle movement, authorize lanes, invoke the
listener/runner/operator status, create order plans, connect to broker/data
providers, or submit. It also updates:

```text
outputs/track_b_execution_core/strategy_signal_adapter/latest_strategy_signal_adapter_report.json
```

Run bounded watch mode explicitly:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_listener_cli \
  --listener-config-json examples/track_b_shadow_listener/listener_config.json \
  --output-root outputs/track_b_execution_core/shadow_listener \
  --watch \
  --max-cycles 5 \
  --poll-seconds 10
```

Watch mode repeats poll-once listener cycles, writes normal cycle and health
artifacts for each cycle, and updates `latest_shadow_listener_heartbeat.json`
under the listener output root. Keep `--max-cycles` bounded for operator proof
runs. `--poll-seconds` controls the interval between cycles. Watch mode is
still no-submit and does not create daemon/service authority.

For a simple operator proof later, start bounded watch mode in one terminal,
run `signal_batch_writer_cli` in another terminal, then inspect the listener
heartbeat, latest health report, processed input file, runner summary, and
attrition report paths. The listener/writer chain remains no-submit throughout.

Each listener cycle also writes a health/status artifact plus a
`latest_shadow_listener_health.json` pointer under the listener output root.
The health report summarizes the latest cycle, runner summary paths, file
counts, last blocker/action, and the same no-submit flags. It is observer data
for operators and future dashboards; a healthy listener does not authorize
paper or live submit.

For the committed example listener id, the stable listener latest paths are:

```text
outputs/track_b_execution_core/shadow_listener/track_b_example_shadow_listener_v1/latest_shadow_listener_health.json
outputs/track_b_execution_core/shadow_listener/track_b_example_shadow_listener_v1/latest_shadow_listener_heartbeat.json
```

Create an operator status summary from observer reports:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.operator_status_cli \
  --track-b-readiness-check-runner-report-json outputs/track_b_execution_core/track_b_readiness_check_runner/latest_track_b_readiness_check_runner_report.json \
  --track-b-observation-runner-report-json outputs/track_b_execution_core/track_b_observation_runner/latest_track_b_observation_runner_report.json \
  --track-b-strategy-rule-runner-report-json outputs/track_b_execution_core/track_b_strategy_rule_runner/latest_track_b_strategy_rule_runner_report.json \
  --track-b-strategy-paper-runner-report-json outputs/track_b_execution_core/track_b_strategy_paper_runner/latest_track_b_strategy_paper_runner_report.json \
  --databento-candle-observer-report-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json \
  --databento-candle-observer-heartbeat-json outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_heartbeat.json \
  --listener-heartbeat-json outputs/track_b_execution_core/shadow_listener/track_b_example_shadow_listener_v1/latest_shadow_listener_heartbeat.json \
  --listener-health-json outputs/track_b_execution_core/shadow_listener/track_b_example_shadow_listener_v1/latest_shadow_listener_health.json \
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

The operator status summary is a dashboard-ready read model. It lists supplied
and missing reports explicitly, summarizes the one-command observation runner,
Databento observer watch state, listener heartbeat/watch state, upstream
strategy adapter and candle producer origins, recent writer output when
provided, surfaces listener degradation, readiness blocks, and broker-state
blocks distinctly, and keeps `submit_allowed=false`. Future dashboard screens
should read this artifact instead of inventing state, but the artifact itself is
still not truth authority or submit authority. Each run also updates:

```text
outputs/track_b_execution_core/operator_status/latest_operator_status_summary.json
```

## Readiness Check Runner

The Track B readiness check runner is a bounded no-submit pre-proof evidence
wrapper:

```text
track_b_readiness_check_runner
-> recovery_status read-only
-> preflight read-only
-> Databento wait-for-current quote
-> readiness_summary
-> operator_status / Track B Status UI
```

It answers whether the current artifacts are clean enough to consider a
separate paper proof decision. It does not run `paper_proof_cli`, submit,
cancel, place orders, create order plans, or mutate broker state. A clean
runner verdict is not automatic submit authority. When only the readiness-check
runner report is supplied, `operator_status` may report
`OPERATOR_STATUS_READY_FOR_PAPER_PROOF_REVIEW`; missing listener/shadow replay
reports remain explicit but do not downgrade that readiness-review view to
unknown.

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
  --timeframe quote_snapshot \
  --source-id track_b_readiness_check \
  --proof-timing-status ACTIVE_SESSION \
  --max-wait-cycles 10 \
  --wait-poll-seconds 15 \
  --quote-provider-mode REALTIME \
  --output-root outputs/track_b_execution_core/track_b_readiness_check_runner
```

Each run updates:

```text
outputs/track_b_execution_core/track_b_readiness_check_runner/latest_track_b_readiness_check_runner_report.json
outputs/track_b_execution_core/operator_status/latest_operator_status_summary.json
```

Failure is explicit and fail-closed:

- recovery not clean stops before preflight/quote readiness.
- preflight not clean stops before quote readiness.
- realtime current quote unavailable after bounded wait stops with quote blocker.
- fallback/historical Databento quotes do not count as current readiness.
- readiness-summary blockers are surfaced without calling paper proof.
- current quote provider mode is explicit in artifacts and never implies
  live-money readiness or submit authority.

## Observation Runner

The Track B observation runner is a bounded operator convenience wrapper around
the already-proven no-submit chain:

```text
Databento quote/candle evidence
-> databento_candle_observer
-> strategy_signal_adapter
-> candle_signal_producer
-> signal_batch_writer
-> shadow_listener
-> operator_status
-> Track B Status UI
```

It is not an engine, scheduler, or trading authority. It does not infer
direction from candle shape, does not call broker/TWS/IBKR, does not run
`paper_proof_cli`, and does not submit/cancel/placeOrder.

Fixture/report mode:

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
  --timeframe quote_snapshot \
  --source-id track_b_observation_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/track_b_observation_runner
```

Bounded current Databento quote mode:

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
  --timeframe quote_snapshot \
  --source-id track_b_observation_live_demo \
  --signal-direction LONG \
  --output-root outputs/track_b_execution_core/track_b_observation_runner
```

Bounded watch mode repeats the same no-submit sequence:

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
  --source-id track_b_observation_watch_demo \
  --signal-direction LONG \
  --watch \
  --max-cycles 5 \
  --poll-seconds 10
```

Each run updates:

```text
outputs/track_b_execution_core/track_b_observation_runner/latest_track_b_observation_runner_report.json
outputs/track_b_execution_core/operator_status/latest_operator_status_summary.json
```

## Desktop App Packaging

To build the local desktop app bundle without replacing the installed
`/Applications` app:

```bash
cd /Users/patrick/Dev/MGC-v05l-automation/desktop
npm run package:local
```

To build, package, and replace `/Applications/MGC Operator.app` after explicit
operator approval:

```bash
cd /Users/patrick/Dev/MGC-v05l-automation/desktop
npm run deploy:applications -- --yes
```

The deploy script refuses to overwrite `/Applications/MGC Operator.app` without
`--yes`. The packaged app writes `.mgc-build-metadata.json` into the bundle, and
the Track B Status tab displays the build commit, build timestamp, packaging
mode, and app/source path. This is a stale-bundle check only; it does not call
broker, TWS, IBKR, Databento, `paper_proof_cli`, cancel, placeOrder, or submit
paths.

## Shadow Run Command

Run the committed golden example with:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_run_assembler_cli \
  --manifest-json examples/track_b_shadow_run/manifest.json \
  --registry-json examples/track_b_shadow_run/lane_registry.json \
  --intent-json examples/track_b_shadow_run/intent_valid.json \
  --intent-json examples/track_b_shadow_run/intent_blocked_qty.json \
  --output-root outputs/track_b_execution_core/shadow_runs
```

To run the full signal-to-shadow-run chain, first materialize the two
`proposed_intent` objects from successful proposal reports as JSON intent files,
then pass those generated intent files to `shadow_run_assembler_cli` with the
same manifest and lane registry. The committed unit test
`tests/unit/execution_core/test_track_b_shadow_run_examples.py` exercises this
flow in-process without shell extraction.

## Expected Result

The run should complete with blockers because one fixture intent is
intentionally invalid for the lane:

- valid intent: authorized by the lane registry, order plan created, shadow
  evaluation created
- blocked quantity intent: rejected by the lane registry because quantity 2
  exceeds max quantity 1
- run summary: `SHADOW_RUN_COMPLETED_WITH_BLOCKERS`
- `submit_allowed=false` throughout
- `submit_attempted=false` throughout
- `live_money_readiness=false` throughout

For the signal-to-shadow-run chain:

- BINARY signal + binary policy: proposed no-submit intent created
- SCORED_STATIC passing signal + static policy: proposed no-submit intent
  created
- SCORED_STATIC below-threshold signal + static policy: proposal blocked
- proposed intents that are created can pass through lane registry, order plan,
  and shadow evaluation review
- blocked cases remain explicit and do not become hidden submit attempts
- one-command shadow replay runner: links signal batch, shadow run, attrition,
  and runner summary paths while preserving `submit_allowed=false`,
  `submit_attempted=false`, and `live_money_readiness=false`

This example proves the report chain only. It is not a strategy engine, not a
scheduler, not broker recovery, and not a paper proof submit path.

## Stop Conditions

Do not proceed from this example to `paper_proof_cli` automatically. Paper
proof still requires clean broker recovery, read-only preflight, current quote
or explicitly acknowledged manual paper-only pricing, active proof timing,
explicit submit flags, and operator approval. The readiness check runner only
answers whether paper proof may be considered as a separate manual decision.

When paper proof is explicitly run, `paper_proof_cli` owns the Track B open and
close lifecycle. A filled open BUY proof may be closed by Track B only if broker
truth shows exactly `+1` on the configured PAPER account/contract and no working
same-contract orders. Otherwise the proof refuses the close with an explicit
`BLOCKED_POSITION_NOT_EXPECTED`, `BLOCKED_WORKING_ORDER_EXISTS`, or truly
ambiguous manual-review outcome. A clean proof lifecycle reports
`PROOF_COMPLETE_FLAT`; manual TWS cleanup remains a fallback, not the normal
Track B path.
