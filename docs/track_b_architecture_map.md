# Track B Architecture Map And Replacement Roadmap

Track B is the replacement execution spine. Track A remains legacy/reference
only until specific behavior is intentionally extracted, tested, and re-owned by
Track B. The long-term destination is not Track B feeding back into Track A.

## Current Chain

The current Track B no-submit chain is:

```text
Databento market-data observer, optionally
-> candle/event JSON
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
-> recovery / preflight / proof timing
-> paper_proof_cli later, only when broker state is clean
```

The chain is file/report driven today. It is not a strategy engine, scheduler,
dashboard runtime, or broker route.

## Mode And Lane Lifecycle

Engine-running state is separate from submit authority. A future Track B engine
may have `engine_running=true` while every report still has
`submit_allowed=false`. Shadow evaluation can run continuously because it is
no-submit. Paper submit requires paper-specific recovery, preflight, timing,
pricing, explicit submit flags, and operator confirmation. Live submit is not
implemented and will require future live-specific gates.

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
- The paper proof path exists, but the known unresolved broker order blocks new
  proof submits for `DUM882026` / `MGC-202606`.
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
  a supplied Databento quote/candle artifact, writes a Track B candle/event JSON
  file, and stops. It does not connect to IBKR/TWS, invoke Databento live
  network calls in this scaffold, authorize trades, infer submit readiness,
  invoke the listener/runner/operator status, create order plans, or submit.
  Databento symbols remain market-data selectors only; the local execution
  contract key and IBKR allowlist remain execution authority. It updates
  `outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_event.json`
  and
  `outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json`
  as read-model conveniences. Its event JSON is already compatible with
  `strategy_signal_adapter_cli`; no separate bridge module is required in this
  slice. Direction is explicit: pass `--signal-direction` to create directional
  BINARY review input on either the observer command or the strategy adapter
  command, or omit it to let the adapter emit review-only HUMAN_REVIEW input.
  Optional bounded watch mode re-runs the same observer
  conversion on a supplied artifact path and updates
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
  state without reading Databento artifacts directly.
- `attrition_report` explains where candidates dropped out across supplied
  no-submit summaries. It is explanatory only and treats missing stages as
  explicit `NOT_PROVIDED` inputs instead of silently reporting zero.
- `readiness_summary` summarizes broker/session/quote state. It does not submit
  and does not override proof gates.
- `paper_proof_cli` remains the only current Track B submit path.
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

- Current Track B flow is PAPER only.
- No-submit artifacts report `submit_allowed=false`.
- No-submit artifacts report `submit_attempted=false`.
- No-submit artifacts report `live_money_readiness=false` unless a future
  live-readiness phase explicitly changes this.
- Track B must not import `mgc_v05l.execution.*`, dashboard modules, Track A
  runtime/cache/snapshot authority, Schwab, or desktop UI.
- Dashboard/cache/snapshot data is never authority for Track B execution.
- No broker submit may occur while unresolved broker state exists for the same
  account/contract.
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
./.venv/bin/python -m mgc_v05l.execution_core.operator_status_cli \
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

All example commands are no-submit and do not connect to TWS or Databento.

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
