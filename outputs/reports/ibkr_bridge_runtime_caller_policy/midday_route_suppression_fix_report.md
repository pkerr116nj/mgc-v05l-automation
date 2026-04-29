# Midday route suppression fix

Historical midday actionable signals were not quiet strategy logic. They were being blocked at the runtime-to-bridge handoff.

- previous blocker: `BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge rejected a non-manual caller path.`
- current status: approved supervised paper runtime callers are now allowed through caller validation when they present explicit PAPER-only route metadata.
- remaining behavior: downstream monitor/governance/exposure/quote/reconciliation gates still fail closed if unhealthy.
- no order was submitted in this pass.

Affected families:
- `index_ny_early_core_us_midday`
- `gc_all_lanes_us_midday`
