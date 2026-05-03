# Track B UI Integration Contract

Track B UI work should appear as a new app tab or screen group. Legacy Track A
tabs can remain during migration, but they are not the destination. Hide or
remove legacy tabs only after Track B artifact views provide the same or better
operator function.

This document defines the Track B-native app/dashboard contract. It does not
authorize broker execution, strategy logic, or submit behavior.

## First Phase

The first Track B UI phase is implemented as a read-only `Track B Status` tab.
It displays existing Track B artifacts and links, not computed trading state.
The sanctioned primary input is:

```text
outputs/track_b_execution_core/operator_status/latest_operator_status_summary.json
```

If that artifact is missing or malformed, the tab must show an unknown/missing
state. It must not infer health, spawn a process, or call a CLI to create the
artifact.

Recommended first sections:

- Top status card: `operator_status` verdict and `required_next_action`
- Market data observer card: Databento observer mode, verdict, cycle counts,
  contract, symbol, dataset, event timestamp, and event path when summarized by
  operator status
- Listener health card
- Latest listener cycle card
- Latest shadow replay runner card
- Attrition card
- Readiness / broker state card
- Quote status card, when supplied
- Artifact links and report paths
- Safety flags: `submit_allowed`, `submit_attempted`, `live_money_readiness`

The first tab displays `NO-SUBMIT / SHADOW REVIEW` only when all supplied safety
flags remain explicitly false. Missing or non-false safety flags are warnings,
not readiness.

## Approved Data Sources

The app may read these Track B artifacts:

- `latest_shadow_listener_health.json`
- `latest_shadow_listener_heartbeat.json`
- `latest_databento_candle_observer_report.json`
- `latest_databento_candle_observer_heartbeat.json`
- `latest_strategy_signal_adapter_report.json`
- `latest_candle_signal_producer_report.json`
- `latest_signal_batch_writer_report.json`
- `latest_operator_status_summary.json`
- Shadow listener health reports
- Shadow listener cycle summaries
- Shadow replay runner summaries
- Attrition reports
- Operator status summaries
- Readiness summaries
- Recovery status reports
- Preflight reports
- Quote reports
- Track B architecture docs and committed examples

The app must not scrape random runtime, cache, dashboard, desktop, or Track A
state as authority.

## Display Fields

The UI should display, when present:

- `status_verdict`
- `health_verdict`
- `listener_verdict`
- `runner_verdict`
- `attrition_report_verdict`
- `strategy_adapter_verdict`
- `strategy_id`
- `signal_family`
- `strategy_source_id`
- `strategy_batch_id`
- `strategy_signal_count`
- `strategy_output_batch_path`
- `candle_producer_verdict`
- `candle_source_id`
- `candle_batch_id`
- `candle_signal_count`
- `candle_output_batch_path`
- `signal_batch_writer_verdict`
- `signal_batch_writer_batch_json_path`
- `final_readiness_verdict`
- `classification`
- `quote_status`
- `primary_blocker`
- `secondary_blockers`
- `required_next_action`
- `reports_missing`
- `latest_output_paths`
- `report_json_path`
- `submit_allowed`
- `submit_attempted`
- `live_money_readiness`
- Account, contract, broker order id, permId, broker status, position quantity
  when supplied by recovery/preflight/readiness reports

Missing reports must remain visible. Do not collapse missing reports into OK.

The visible no-submit origin chain is:

```text
databento_candle_observer
-> latest Databento candle event
strategy_signal_adapter
-> candle_signal_producer
-> signal_batch_writer
-> shadow_listener
-> operator_status
-> Track B Status UI
```

The UI should prefer the upstream and market-data observer fields already
summarized in `latest_operator_status_summary.json`; it should not read
Databento, strategy, or candle reports directly unless a future contract
explicitly changes that.

## Non-Authority Rules

The UI must not:

- Call Track B CLIs from the UI
- Connect to TWS, IBKR, Databento, broker, or market-data paths
- Compute `submit_allowed` itself
- Infer `live_money_readiness`
- Infer broker flatness
- Infer lane authorization
- Infer paper or live eligibility
- Collapse missing reports into OK
- Treat listener health as submit authority
- Treat `operator_status` as submit authority
- Call `paper_proof_cli` implicitly
- Hide primary blockers
- Treat dashboard/cache/snapshot state as execution authority
- Show submit/action controls in the read-only first phase

The UI is an observer/control surface over Track B artifacts. It is not source
of truth and not hidden submit authority.

## Future Controls

If the UI later adds buttons, each control must:

- Call explicit Track B CLIs or APIs
- Display the resulting report
- Require explicit mode, account, and contract context
- Preserve Track B recovery, preflight, proof timing, readiness, and proof
  gates
- Never bypass readiness, recovery, or proof gates
- Never create hidden submit authority

Submit controls, if ever added, must remain separate from observer health. A
healthy listener or OK operator status does not authorize paper or live submit.

## Legacy Retirement Plan

Track B replaces Track A as the long-term spine, but the UI migration should be
deliberate:

1. Map each legacy tab to a Track B artifact-backed replacement.
2. Add Track B read-only views beside existing legacy tabs.
3. Feature-flag old tabs before deletion.
4. Disable a legacy tab only after the Track B view provides equal or better
   operator function.
5. Do not merge Track B back into Track A as the destination architecture.

## Current Status

- Backend Track B artifacts exist for no-submit signal intake, replay,
  listener health, attrition, readiness, recovery, preflight, quote
  diagnostics, and operator status.
- The first read-only Track B status tab is implemented behind its own
  `Track B Status` navigation entry and reads
  `latest_operator_status_summary.json` as the primary read model.
- Paper proof remains blocked for `DUM882026` / `MGC-202606` while the known
  unresolved `PendingCancel` broker order exists.
- Live trading is not implemented.
- Dashboard rebuild remains future work.
