# Track B Broker Truth Lease v1

## Status

- Track: B
- Document type: design proposal for review
- Implementation status: design first; implementation not started
- Scope: PAPER-only account safety lease for broker truth and Phase-1 reconciliation
- Live-money support: explicitly out of scope
- Broker mutation: out of scope
- Runtime restart: out of scope
- Dashboard authority: out of scope; dashboard remains display-only

This document is design-only. It does not restart runtime, restart sidecars,
mutate broker state, mutate lifecycle state, submit/cancel/close/place orders,
change strategy behavior, loosen current runtime gates, or grant live-money
eligibility.

Canonical readiness remains the current authority until this lease design is
implemented, tested, reviewed, and explicitly wired into submit gates.

## Problem

Track B can briefly obtain clean IBKR PAPER broker truth and clean Phase-1
reconciliation, then lose recurring broker-truth freshness when IBKR/TWS returns
intermittent 502 connection failures. The current readiness model treats those
refresh failures as immediate pretrade dependency failures. That is safe, but it
is too brittle for supervised PAPER runtime operation when the account state was
recently proven clean and no contradictory broker truth has appeared.

Broker truth should establish an account-safe lease. Inside that lease, market
data freshness, lifecycle ownership, order-intent ledger state, fill handling,
strategy/session gates, and risk controls govern real-time operation. Broker
refresh failures degrade and eventually expire the lease, but a failed refresh
attempt must not by itself erase a still-valid clean account proof.

## Design Goal

Create a PAPER-only Broker Truth Lease that records:

- which account was proven
- which positions and open orders were observed
- which reconciliation artifact made the account/lifecycle/order state clean
- which instruments and lanes are in scope
- how long new entries and managed exits may rely on that proof
- what contradictions immediately invalidate it

The lease is not broker truth itself. It is a bounded safety contract derived
from broker truth plus reconciliation.

## Non-Goals

- No live-money route.
- No broker mutation.
- No submit, cancel, close, replace, flatten, or `placeOrder` implementation.
- No broad cancel/global cancel.
- No strategy rule changes.
- No lifecycle mutation.
- No dashboard-owned authority.
- No replacement for Phase-1 reconciliation.
- No use of stale or incomplete broker truth to create a lease.
- No permission to submit when open-order state is unknown.

## Authority Model

Authority order remains:

1. IBKR broker truth is authoritative for account, positions, open orders,
   executions, and order status.
2. Phase-1 Track B reconciliation is authoritative for broker/lifecycle/order
   consistency classification.
3. Track B lifecycle ledger and order-intent ledger are authoritative for what
   Track B owns or attempted.
4. Broker Truth Lease is a derived account-safety artifact.
5. Dashboard is a consumer only.

If a later successful broker-truth read contradicts the active lease, the broker
truth wins and the lease is invalidated immediately.

## Lease Artifact

Authoritative latest artifact:

`outputs/operator_dashboard/runtime/latest_broker_truth_lease.json`

Append-only history:

`outputs/operator_dashboard/runtime/broker_truth_lease_history.jsonl`

The latest artifact is a derived status contract. The history file records every
lease creation, renewal, degradation, expiration, and invalidation decision.

## Lease Payload

Required fields:

- `schema_version`: `track_b_broker_truth_lease_v1`
- `lease_id`: stable id for the lease generation
- `account_id`: expected PAPER account, for example `DUM882026`
- `mode`: must be `PAPER`
- `paper_only`: must be `true`
- `live_money_eligible`: must be `false`
- `generated_at`: lease decision time
- `broker_truth_generated_at`: successful broker-truth snapshot time
- `reconciliation_generated_at`: Phase-1 reconciliation time
- `valid_until`: new-entry lease expiry
- `exit_valid_until`: managed-exit lease expiry, equal or later than `valid_until`
- `max_entry_age_seconds`: configured entry tolerance
- `max_exit_age_seconds`: configured exit tolerance
- `lease_state`: one of the states below
- `positions_snapshot_path`
- `open_orders_snapshot_path`
- `broker_truth_status_path`
- `reconciliation_path`
- `positions`: normalized broker positions in allowed scope
- `open_orders`: normalized broker open orders in allowed scope
- `track_b_broker_position_count`
- `track_b_broker_open_order_count`
- `unknown_broker_open_order_count`
- `lifecycle_open_position_count`
- `lifecycle_open_order_count`
- `review_required_count`
- `broker_reconciled`
- `lifecycle_match_status`
- `order_intent_match_status`
- `allowed_instruments`
- `allowed_contracts`
- `allowed_lane_ids`
- `submit_entry_allowed`
- `submit_exit_allowed`
- `warnings`
- `blockers`
- `contradictions`
- `operator_action_required`
- `source_artifact_paths`
- `source_artifact_timestamps`

Optional but recommended fields:

- `latest_attempt_classification`
- `latest_attempt_generated_at`
- `latest_attempt_error`
- `degraded_since`
- `last_successful_lease_id`
- `renewed_from_lease_id`
- `invalidated_by_artifact`
- `invalidated_reason_code`

## Lease States

### `ACTIVE`

Broker truth and Phase-1 reconciliation are fresh, complete, clean, and within
entry tolerance. New entries may be considered if all other submit gates pass.

Required:

- correct PAPER account
- positions complete
- open orders complete
- no unknown open orders
- Phase-1 reconciliation clean
- `review_required_count=0`
- `live_money_eligible=false`
- no contradiction

### `ACTIVE_DEGRADED_REFRESH_FAILING`

The last successful broker truth and reconciliation remain within lease
tolerance, but one or more later broker refresh attempts failed.

New entries may be considered only while still inside `valid_until` and only if
no contradictory successful broker truth exists.

This state must:

- warn that refresh is failing
- keep `live_money_eligible=false`
- preserve the original successful broker truth paths
- require maintenance supervisor retry/rotate/sidecar repair recommendations
- never extend `valid_until`

### `EXPIRED_BLOCK_NEW_ENTRIES`

The entry lease expired. New entries are blocked even if no contradiction is
known.

Managed exits may still be evaluated separately under `EXPIRED_EXITS_ONLY` if a
lifecycle-owned position exists and exit tolerance has not expired.

### `EXPIRED_EXITS_ONLY`

The entry lease expired, but managed exits for known lifecycle-owned positions
remain allowed within `exit_valid_until`.

Allowed only when:

- the lifecycle position is known, exact, and owned by Track B
- no contradictory successful broker truth exists
- open-order state is not unknown
- the exit intent references that owned lifecycle position
- the order-intent ledger has no unresolved conflicting intent
- the exit policy is present and approved for PAPER

This state never permits new entries.

### `INVALIDATED_CONTRADICTION`

A successful broker-truth read contradicts the lease or the reconciliation used
to create it.

Examples:

- unexpected broker position in allowed scope
- expected lifecycle-owned position missing or side/quantity mismatch
- broker open order not known to lifecycle/order-intent ledger
- account id mismatch
- broker truth generated for a different account or route
- successful broker truth is complete and does not match the active lease

Submit entry and submit exit are both blocked unless a narrower protective exit
policy is explicitly added in a later design and approved.

### `INVALIDATED_UNKNOWN_OPEN_ORDERS`

Open-order truth is incomplete or any unknown open order is observed in Track B
scope.

This is immediate invalidation because unknown open orders can create duplicate
or conflicting exposure. New entries are blocked. Managed exits are blocked
unless the unknown order is classified by a future guarded order-resolution tool.

### `INVALIDATED_MANUAL_BROKER_ACTION`

Manual broker action is detected or suspected.

Examples:

- broker position changes with no matching Track B lifecycle/fill evidence
- order appears with no Track B order-intent ownership
- manual close evidence exists but lifecycle cleanup has not been applied
- TWS activity contradicts Track B ledger state

The lease remains invalid until fresh broker truth plus guarded lifecycle
cleanup/adoption plus Phase-1 reconciliation clears the mismatch.

### `OPERATOR_REQUIRED`

The lease engine cannot classify the state safely.

Examples:

- malformed artifact
- missing required source artifact
- account scope ambiguous
- lifecycle or order-intent summary unavailable
- lease producer detects unsupported state transition

All submit is blocked.

## Lease Creation

A lease may be created only from a successful read-only broker-truth snapshot and
a clean Phase-1 reconciliation.

Creation inputs:

- broker-truth refresher last successful snapshot
- broker-truth latest attempt status
- Phase-1 reconciliation artifact
- lifecycle ledger summary
- open-order/order-intent ledger summary
- account and instrument scope
- canonical readiness artifact
- maintenance supervisor decision

Creation gates:

- account id equals configured PAPER account
- `live_money_eligible=false`
- broker positions snapshot complete
- broker open orders snapshot complete
- no unknown open orders
- reconciliation classification is `TRACK_B_PAPER_BROKER_RECONCILED`
- `broker_reconciled=true`
- `review_required_count=0`
- `track_b_broker_open_order_count=0`, unless every order is known and owned
- lifecycle/order-intent ledger has no unresolved ownership mismatch
- source artifact timestamps are inside configured creation tolerances

If any creation gate fails, write a lease artifact with a blocking state rather
than omitting the artifact.

## Renewal

A successful broker-truth refresh renews the lease only if it matches the
current lifecycle/reconciliation state.

Renewal flow:

1. Read successful broker truth.
2. Run or consume fresh Phase-1 reconciliation.
3. Compare account, positions, open orders, lifecycle ownership, and order
   intents.
4. If clean, create a new `lease_id`, extend `valid_until`, and append a
   `renewed` event to history.
5. If contradictory, invalidate immediately and append an `invalidated` event.

Failed refresh attempts never renew the lease.

## Degradation

Failed broker refresh attempts do not immediately invalidate an active lease.
They move `ACTIVE` to `ACTIVE_DEGRADED_REFRESH_FAILING` while inside
`valid_until`.

Required behavior:

- keep the last successful broker truth as source evidence
- record latest failed attempt status and error
- add warnings
- keep `submit_entry_allowed` true only if still inside entry tolerance and all
  other gates pass
- keep `submit_exit_allowed` true only if exit gates pass
- require maintenance supervisor to recommend retry/rotate/sidecar repair
- do not hide the failure from dashboard or operator reports

When `valid_until` passes, state becomes `EXPIRED_BLOCK_NEW_ENTRIES` or
`EXPIRED_EXITS_ONLY`, depending on lifecycle-owned exposure and exit tolerance.

## Contradiction Policy

Contradiction beats staleness. Any complete successful broker truth that
contradicts the lease invalidates immediately, even if an older last-good lease
is still within tolerance.

Immediate invalidators:

- wrong account
- incomplete open-order callback in a successful run
- unknown open order
- unexpected position in allowed scope
- position side or quantity mismatch
- order-intent ownership mismatch
- lifecycle ownership mismatch
- `live_money_eligible=true`
- manual broker action evidence
- malformed source artifact

An incomplete or failed broker refresh attempt degrades or expires the lease; it
does not by itself prove a contradiction.

## Submit Policy

The lease is necessary but not sufficient. Submit remains blocked unless all
non-lease gates pass.

### New Entries

New entries require:

- lease state `ACTIVE` or `ACTIVE_DEGRADED_REFRESH_FAILING`
- current time <= `valid_until`
- no known contradiction
- no unknown open orders
- lifecycle/order-intent ledger clean
- strategy/session gates clean
- market-data semantics and freshness gates clean
- lane eligibility and exposure gates clean
- route/broker governance gates clean
- `live_money_eligible=false`

No new entry may use `EXPIRED_BLOCK_NEW_ENTRIES`, `EXPIRED_EXITS_ONLY`, or any
invalidated state.

### Managed Exits

Managed exits for known lifecycle-owned positions may use a separate exit
tolerance.

Allowed under `EXPIRED_EXITS_ONLY` only if:

- current time <= `exit_valid_until`
- lifecycle position is known, exact, and owned
- exit intent resolves to that lifecycle position
- no successful contradictory broker truth exists
- no unknown open orders exist
- order-intent ledger has no unresolved conflict
- exit policy is approved for PAPER
- `live_money_eligible=false`

If broker truth contradicts the lifecycle position, the lease is invalid and the
exit gate must fail closed.

## Maintenance Supervisor Integration

The maintenance supervisor should consume lease state and recommend repairs.

Examples:

- `ACTIVE`: no lease repair action
- `ACTIVE_DEGRADED_REFRESH_FAILING`: `REFRESH_BROKER_TRUTH`,
  `ROTATE_CLIENT_ID`, or `RESTART_SIDECAR`; warn but do not block solely from a
  failed refresh while inside tolerance
- `EXPIRED_BLOCK_NEW_ENTRIES`: `REFRESH_BROKER_TRUTH`, `REFRESH_RECONCILIATION`,
  `BLOCK_SUBMIT`
- `EXPIRED_EXITS_ONLY`: `REFRESH_BROKER_TRUTH`, `REFRESH_RECONCILIATION`, block
  entries, allow only qualifying managed exits
- invalidated states: `BLOCK_SUBMIT`, `ALERT_OPERATOR`, and guarded cleanup or
  order-resolution recommendations as appropriate

The supervisor does not execute broker/order mutation and does not grant submit
authority.

## Canonical Readiness Integration

Current canonical readiness requires immediate broker-truth freshness. After
lease implementation, canonical readiness should distinguish:

- lease creation readiness
- lease validity for entries
- lease validity for exits
- broker refresh health
- contradiction status

Future canonical behavior:

- use `latest_broker_truth_lease.json` as a derived safety input
- require `submit_entry_allowed=true` for submit-capable entry readiness
- require `submit_exit_allowed=true` for exit readiness
- keep root, market data, lifecycle, order-intent, lane, strategy/session, and
  live-money gates separate
- fail closed if the lease artifact is missing, malformed, expired for the
  requested action, or invalidated

Canonical readiness remains authority. The lease does not bypass canonical
readiness; it gives canonical readiness a less brittle account-safety input.

## Submit Gate Integration

The submit gate should require a valid lease, not a fresh IBKR round trip per
order.

Entry gate:

- `lease.submit_entry_allowed=true`
- `lease_state in {ACTIVE, ACTIVE_DEGRADED_REFRESH_FAILING}`
- current time <= `valid_until`
- all non-lease gates pass

Exit gate:

- `lease.submit_exit_allowed=true`
- current time <= `exit_valid_until`
- lifecycle-owned position matches the exit intent
- all exit-specific non-lease gates pass

Every order intent should record the lease id and lease digest used by the gate.
If the lease changes before submit, the gate must re-evaluate.

## Dashboard Integration

Dashboard remains display-only.

Display fields:

- lease state
- account id
- generated_at
- valid_until
- exit_valid_until
- entry allowance
- exit allowance
- warnings
- blockers
- contradiction details
- latest broker attempt classification/error
- source artifact paths and timestamps
- last renewal/invalidation event

Dashboard actions must not create, renew, invalidate, or override leases.

## Configuration

Initial PAPER defaults should be conservative and explicit:

- `broker_truth_lease.enabled=true`
- `broker_truth_lease.mode=PAPER`
- `broker_truth_lease.account_id=DUM882026`
- `broker_truth_lease.max_entry_age_seconds=300`
- `broker_truth_lease.max_exit_age_seconds=900`
- `broker_truth_lease.allowed_instruments=[MGC,MNQ,GC]` or narrower per launch
- `broker_truth_lease.require_open_orders_complete=true`
- `broker_truth_lease.require_reconciliation_clean=true`
- `broker_truth_lease.live_money_eligible=false`

Exact values should be reviewed before implementation. The important design
property is separate entry and exit tolerances, with entries always stricter.

## Implementation Plan

1. Add pure lease model and classifier.
2. Add lease writer that reads existing broker truth, reconciliation, lifecycle,
   order-intent, canonical readiness, and maintenance supervisor artifacts.
3. Write latest JSON plus append-only history JSONL.
4. Add tests for creation, degradation, expiration, contradiction, and
   live-money false enforcement.
5. Add maintenance supervisor lease input and recommendations.
6. Add canonical readiness lease input, initially report-only.
7. Add dashboard display fields.
8. Only after review, wire submit gates to require lease state instead of
   immediate broker-truth freshness for every order.

Each step should be separately reviewable. Submit-capable behavior must not be
enabled by the report-only steps.

## Required Tests

- Fresh broker truth plus clean reconciliation creates `ACTIVE`.
- Failed broker refresh preserves `ACTIVE_DEGRADED_REFRESH_FAILING` until
  `valid_until`.
- Lease expiry blocks new entries.
- Managed exits can remain allowed under `EXPIRED_EXITS_ONLY` only when a known
  lifecycle-owned position exists and no contradiction exists.
- Unexpected broker position invalidates immediately.
- Unknown open orders invalidate immediately.
- Manual close/manual broker action invalidates until guarded cleanup and fresh
  reconciliation clear the state.
- Contradictory successful broker truth beats stale last-good truth.
- Incomplete open-order callback blocks lease creation or invalidates an active
  lease.
- Wrong account invalidates immediately.
- Malformed source artifacts produce `OPERATOR_REQUIRED`.
- `live_money_eligible` remains false in all lease outputs.
- Dashboard consumes but cannot author or modify lease state.
- Canonical readiness fails closed when the lease artifact is missing or
  malformed.

## Open Review Questions

- What exact entry lease tolerance is acceptable for PAPER: 180, 300, or 600
  seconds?
- What exact exit lease tolerance is acceptable for managed exits: 600, 900, or
  1800 seconds?
- Should `ACTIVE_DEGRADED_REFRESH_FAILING` allow entries for all approved lanes,
  or only lanes with no current lifecycle exposure?
- Should unknown open orders outside active instrument scope invalidate the
  account lease or only block the affected instrument group?
- Should lease renewal require a newly generated Phase-1 reconciliation every
  time, or may it consume a reconciliation artifact proven newer than the broker
  truth snapshot?

## Safety Summary

The lease makes intermittent IBKR refresh failures operationally tolerable
without treating stale broker evidence as authority forever. It preserves the
fail-closed posture:

- successful contradictory broker truth invalidates immediately
- unknown open orders invalidate immediately
- expired leases block new entries
- exits get a separate, stricter lifecycle-owned path
- live-money remains impossible
- dashboard remains a consumer
- canonical readiness and submit gates remain authority after implementation
