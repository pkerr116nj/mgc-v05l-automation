# Track B Lifecycle State Matrix

Track B PAPER lifecycle authority lives in `execution_core`. Lifecycle,
manifest, ownership, ledger, managed-position registry, and reconciliation code
should use the shared transition authority instead of reconstructing local state
rules. Dashboard/operator artifacts are projections only.

## Matrix

| State | Terminal | Broker-backed | Managed position registry | Clean reconciliation eligible | Clean trade stats | Required evidence | Operator action |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `INTENT_CREATED` | no | no | no | no | no | entry intent, lane/strategy, contract, side, quantity | no |
| `SUBMIT_ATTEMPTED` | no | no | no | no | no | submit attempt id | no |
| `SUBMITTED_PENDING_FILL` | no | no | no | no | no | broker order id or perm id | no |
| `BLOCKED_NO_BROKER_EFFECT` | yes | no | no | yes | no | pre-submit/no-broker-effect evidence | no |
| `BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE` | no | partial | no | no | no | partial broker identity, plus review blockers | yes |
| `OPEN_MANAGED_METADATA_INCOMPLETE` | no | yes | no | no | no | broker fill identity, missing management metadata | yes |
| `OPEN_MANAGED` | no | yes | yes | yes | yes | intent/ownership, lane/strategy, contract/conId, side, qty, broker order id or perm id, fill price/time, managed exit policy, lifecycle id/destination | no |
| `CLOSED_FLAT` | yes | yes | no | yes | yes | close fill or broker-flat proof | no |
| `REVIEW_REQUIRED` | no | unknown | yes | no | no | review reason | yes |
| `MANUAL_OR_MALFORMED_CLEANUP` | yes | no | no | yes | no | explicit operator/manual cleanup or malformed artifact evidence | no |

## Invariants

- `OPEN_MANAGED` is allowed only with complete broker-backed fill/adoption
  evidence and a `managed_exit_policy_id`.
- A manifest must not persist `OPEN_MANAGED` while broker order/perm identity,
  fill price/time, or explicit broker-backed adoption evidence is missing.
- `CLOSED_FLAT` requires close-fill evidence or broker-flat proof. A bare
  requested `CLOSED_FLAT` becomes `REVIEW_REQUIRED`.
- `BLOCKED_NO_BROKER_EFFECT` is a terminal non-position state. It must not
  create a managed position or poison clean reconciliation.
- `REVIEW_REQUIRED` is non-clean until explicitly resolved.
- Manual/malformed cleanup classifications are reconciliation artifacts, not
  clean strategy trades or P&L-bearing lifecycle outcomes.

## Shared Authority

The canonical implementation is:

```text
src/mgc_v05l/execution_core/track_b_lifecycle_state_transition.py
```

Primary functions:

- `classify_managed_position_transition(evidence)`
- `validate_open_managed_evidence(evidence)`
- `closed_flat_evidence_blockers(evidence)`
- `ledger_projection_from_transition(transition)`
- `lifecycle_state_matrix()`

New Track B code should call these functions before writing lifecycle,
manifest, ledger, managed-position, or reconciliation state.
