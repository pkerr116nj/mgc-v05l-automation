# Track B PAPER Restart Retry Policy

This policy is PAPER-only operational infrastructure recovery. It does not grant
submit authority, live-money eligibility, strategy changes, session changes, or
broker/lifecycle mutation.

The self-healing planner may retry the Track B PAPER runtime only when broker
state is clean, reconciliation is clean, there are no open or unknown broker
orders, no unresolved submit ownership records, no lifecycle review-required
state, no duplicate runtime writer, no wrong-root process, and
`live_money_eligible=false`.

Recoverable launch classes:

- `LAUNCHCTL_SUBMIT_FAILED`
- `RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT`
- `WRAPPER_PRE_EXEC_FAILURE`

Progressive behavior:

- `LAUNCHCTL_SUBMIT_FAILED` gets one immediate retry when broker state is clean.
- Further same-class failures use cooldown/backoff.
- Repeated same-class failures exhaust the restart budget and require operator
  review.
- Successful runtime start resets the failure budget.

Audit/state artifacts:

- `outputs/operator_dashboard/runtime/self_healing_recovery_audit.jsonl`
- `outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json`
- `outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_launch_status.json`
