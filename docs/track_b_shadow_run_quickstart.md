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

The local execution contract key remains the execution authority. The manifest
does not authorize lanes by itself; the lane registry does. The strategy intent
only describes a proposed action. The order plan and shadow evaluation remain
no-submit.

## Command

Run the committed golden example with:

```bash
./.venv/bin/python -m mgc_v05l.execution_core.shadow_run_assembler_cli \
  --manifest-json examples/track_b_shadow_run/manifest.json \
  --registry-json examples/track_b_shadow_run/lane_registry.json \
  --intent-json examples/track_b_shadow_run/intent_valid.json \
  --intent-json examples/track_b_shadow_run/intent_blocked_qty.json \
  --output-root outputs/track_b_execution_core/shadow_runs
```

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

This example proves the report chain only. It is not a strategy engine, not a
scheduler, not broker recovery, and not a paper proof submit path.

## Stop Conditions

Do not proceed from this example to `paper_proof_cli`. Paper proof still
requires clean broker recovery, read-only preflight, active proof timing,
explicit submit flags, and operator approval. The known stale `PendingCancel`
paper order for `DUM882026` / `MGC-202606` remains a block for new proof submits
on that account/contract until broker state is terminal and clean.
