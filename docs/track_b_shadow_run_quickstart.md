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

This example proves the report chain only. It is not a strategy engine, not a
scheduler, not broker recovery, and not a paper proof submit path.

## Stop Conditions

Do not proceed from this example to `paper_proof_cli`. Paper proof still
requires clean broker recovery, read-only preflight, active proof timing,
explicit submit flags, and operator approval. The known stale `PendingCancel`
paper order for `DUM882026` / `MGC-202606` remains a block for new proof submits
on that account/contract until broker state is terminal and clean.
