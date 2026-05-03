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
or submit.

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

Create an operator status summary from observer reports:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.operator_status_cli \
  --listener-health-json <LISTENER_HEALTH_JSON> \
  --listener-cycle-json <LISTENER_CYCLE_JSON optional> \
  --shadow-runner-summary-json <RUNNER_SUMMARY_JSON optional> \
  --attrition-report-json <ATTRITION_REPORT_JSON optional> \
  --readiness-summary-json <READINESS_SUMMARY_JSON optional> \
  --recovery-report-json <RECOVERY_REPORT_JSON optional> \
  --preflight-report-json <PREFLIGHT_REPORT_JSON optional> \
  --quote-report-json <QUOTE_REPORT_JSON optional> \
  --output-root outputs/track_b_execution_core/operator_status
```

The operator status summary is a dashboard-ready read model. It lists supplied
and missing reports explicitly, surfaces listener degradation, readiness blocks,
and broker-state blocks distinctly, and keeps `submit_allowed=false`. Future
dashboard screens should read this artifact instead of inventing state, but the
artifact itself is still not truth authority or submit authority.

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

Do not proceed from this example to `paper_proof_cli`. Paper proof still
requires clean broker recovery, read-only preflight, active proof timing,
explicit submit flags, and operator approval. The known stale `PendingCancel`
paper order for `DUM882026` / `MGC-202606` remains a block for new proof submits
on that account/contract until broker state is terminal and clean.
