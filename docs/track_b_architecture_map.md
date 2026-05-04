# Track B Architecture Map And Replacement Roadmap

Track B is the replacement execution spine. Track A remains legacy/reference
only until specific behavior is intentionally extracted, tested, and re-owned by
Track B. The long-term destination is not Track B feeding back into Track A.

## Current Chain

The current Track B no-submit chain is:

```text
Databento market-data observer, optionally
-> candle/event JSON
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
-> recovery / preflight / proof timing
-> paper_proof_cli / paper proof lifecycle, only through explicit PAPER gates
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

## Phase 2 Paper Execution Stance

Track B Phase 1 proved the PAPER open/guarded-close/flat lifecycle. Phase 2 may
therefore execute PAPER trades when, and only when, a Track B-controlled submit
path is explicitly configured. Default strategy, listener, replay, operator
status, and UI flows remain no-submit.

The current stance is:

- PAPER execution is allowed through Track B-controlled paths after explicit
  readiness and submit gates pass.
- Strategy-rule runners may hand off to readiness and paper proof only when
  explicit operator/config flags request that handoff.
- No strategy rule, listener, observation runner, operator status report, or UI
  state may create hidden submit authority.
- No live-money execution is implemented or implied.
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
- `track_b_strategy_rule_runner` is the first Track B Phase 2 strategy-rule
  wiring proof. It evaluates one MGC-only demo rule against realtime Databento
  quote/candle evidence and emits a no-submit LONG signal only when
  `--rule-mode DEMO_LONG_ONLY --emit-signal` is explicitly supplied and the
  input is proven realtime/current. Without the emit flag it writes HUMAN_REVIEW
  or NO_SIGNAL artifacts. It does not infer direction from candle shape, call
  paper proof, invoke the listener, create order plans, authorize lanes, or
  submit in its default mode. A future explicit Phase 2 handoff may pass the
  strategy signal into readiness and `paper_proof_cli` only through reviewed
  Track B flags and artifacts. It delegates actual signal batch production to
  `strategy_signal_adapter -> candle_signal_producer -> signal_batch_writer` and
  updates
  `outputs/track_b_execution_core/track_b_strategy_rule_runner/latest_track_b_strategy_rule_runner_report.json`.
- `track_b_strategy_paper_runner` is the controlled Phase 2 PAPER handoff. It
  composes `track_b_strategy_rule_runner -> track_b_readiness_check_runner ->
  paper_proof_cli / paper proof lifecycle`. It defaults to dry-run/no-submit.
  It can invoke paper proof only when `--mode PAPER`, `--submit-paper`,
  `--confirm-paper-submit`, explicit `--quantity`, and explicit manual open and
  close limit prices are supplied. It never supports live-money execution, UI
  authority, hidden submit, inferred prices, inferred quantity, market orders,
  or broker mutation outside the Track B paper proof lifecycle. It writes
  `outputs/track_b_execution_core/track_b_strategy_paper_runner/latest_track_b_strategy_paper_runner_report.json`
  with rule, readiness, paper proof, and final broker-state classification.
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
- `paper_proof_cli` remains the only current Track B submit path. It owns the
  explicit PAPER proof lifecycle: open submit, open-fill verification, guarded
  close-only submit, close-fill verification, and final flat reconciliation.
  After an open proof fill, Track B may submit the close-only order only when
  broker truth shows the expected exact one-lot position for the configured
  PAPER account/contract and no working same-contract broker orders. It refuses
  the close with `BLOCKED_POSITION_NOT_EXPECTED` or
  `BLOCKED_WORKING_ORDER_EXISTS` when those guards fail. A clean open/close
  proof ends with `PROOF_COMPLETE_FLAT`; manual cleanup is fallback only.
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
