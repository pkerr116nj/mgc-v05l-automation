# Track B UI Integration Contract

Track B UI work should appear as a new app tab or screen group. Legacy Track A
tabs can remain during migration, but they are not the destination. Hide or
remove legacy tabs only after Track B artifact views provide the same or better
operator function.

This document defines the future Track B-native app/dashboard contract. It does
not implement UI, app code, broker execution, strategy logic, or submit
behavior.

## First Phase

The first Track B UI phase is read-only. It should display existing Track B
artifacts and links, not compute trading state.

Recommended first sections:

- Top status card: `operator_status` verdict and `required_next_action`
- Listener health card
- Latest listener cycle card
- Latest shadow replay runner card
- Attrition card
- Readiness / broker state card
- Quote status card, when supplied
- Artifact links and report paths
- Safety flags: `submit_allowed`, `submit_attempted`, `live_money_readiness`

## Approved Data Sources

The app may read these Track B artifacts:

- `latest_shadow_listener_health.json`
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

## Non-Authority Rules

The UI must not:

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
- UI is not implemented.
- Paper proof remains blocked for `DUM882026` / `MGC-202606` while the known
  unresolved `PendingCancel` broker order exists.
- Live trading is not implemented.
- Dashboard rebuild remains future work.
