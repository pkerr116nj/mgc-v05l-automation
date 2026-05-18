# Track B Live Readiness-Maintenance Agents v1

## Status

- Track: B
- Document type: architecture/design proposal for review
- Implementation status: not started
- Scope: PAPER-only readiness-maintenance control plane for supervised runtime operations
- Live-money support: explicitly out of scope
- Broker mutation: out of scope, except through the existing guarded PAPER route when all submit gates pass

This document is design-only. It does not implement runtime behavior, restart
services, mutate broker state, mutate lifecycle state, change strategy behavior,
or grant submit authority.

## Purpose

Track B needs a small control plane that keeps readiness evidence fresh during
the trading day without allowing fragile one-off failures to masquerade as
permission to trade or as total system failure.

The control plane should separate observation, recovery, quarantine, and submit
authority. Its job is to maintain current truth, classify readiness, surface
operator action, and fail closed when evidence is stale, ambiguous, or unsafe.
It is not an order router.

## Problem Statement

The production PAPER stack currently has too many fragile readiness dependencies:

- broker truth refresh can intermittently fail with TWS 502
- a single restored lane can fail startup reconciliation and stop the whole runtime
- broker truth and lifecycle truth can diverge after an operator manual close
- dashboard/backend can be healthy while submit is blocked
- wrong-root runtime risk can make artifacts look authoritative when they are not
- stale artifacts can masquerade as current evidence
- live data probes can pass in isolation but fail inside runtime

Track B should treat these as normal operational conditions with explicit
classification and bounded recovery, not as implicit authority or vague dashboard
degradation.

## Non-Goals

- No live-money execution support.
- No new submit, cancel, close, flatten, replace, or `placeOrder` surface.
- No strategy behavior changes.
- No broker mutation from readiness agents.
- No lifecycle mutation except through an explicit guarded cleanup/adoption tool
  with dry-run, evidence, audit, and operator approval.
- No dashboard-derived source of truth.

## Control Plane Shape

The readiness-maintenance control plane is a set of independent agents that
write explicit artifacts and a small aggregator that produces one readiness
state and one submit-gate reason tree.

```text
broker truth agent
market data agent
runtime health agent
reconciliation agent
lifecycle cleanup/adoption agent
lane eligibility/quarantine agent
submit gate agent
operator alerting agent
        |
        v
readiness control-plane summary
        |
        v
dashboard read model / operator action
```

Each agent owns one narrow contract. Agents may report `READY`, `DEGRADED`,
`STALE`, `BLOCKED`, or `ACTION_REQUIRED`, but no agent can manufacture submit
authority by itself.

## Agent Responsibilities

### Broker Truth Agent

Maintains read-only IBKR/PAPER account truth.

Responsibilities:

- collect complete positions and open orders for `DUM882026`
- write last successful complete broker truth separately from latest attempt
- preserve last-good canonical truth when a refresh attempt fails
- write incomplete failed attempts under a distinct latest-attempt path
- classify broker truth freshness by age of last successful complete truth
- expose account, provider mode, client id, generated timestamp, completeness,
  current error, and freshness threshold

Failure behavior:

- intermittent TWS 502 writes `latest_attempt_status=FAILED`
- last successful complete truth remains available until stale
- submit gate blocks when last successful complete truth is stale
- failed incomplete attempts must not erase positions or open-order truth

Authority:

- read-only broker API only
- may write broker truth/status artifacts
- may not submit, cancel, close, replace, flatten, or call `placeOrder`
- may not mutate lifecycle

### Market Data Agent

Maintains runtime-relevant market-data health, not just isolated provider
connectivity.

Responsibilities:

- verify provider connectivity for configured symbols/timeframes
- verify candles/bars are fresh inside the runtime ingestion path
- distinguish provider probe success from runtime-feed success
- write latest-good live-bar/candle evidence and latest attempt status
- classify feed gaps, stale bars, malformed bars, and symbol/timeframe mismatch

Failure behavior:

- isolated probe pass plus runtime-feed fail becomes `DEGRADED_NO_SUBMIT`
- stale live bars block submit but do not erase lane/runtime state
- missing feed for one lane can quarantine that lane if other lanes remain valid

Authority:

- read-only market-data access
- may write market-data status artifacts
- may not restart services in v1 first slice
- may not mutate lifecycle or broker state

### Runtime Health Agent

Maintains process and root identity for the supervised PAPER runtime and
dashboard/backend.

Responsibilities:

- verify active root is `/Users/patrick/Dev/MGC-v05l-automation`
- verify active config paths match the approved runtime manifest
- verify runtime PID, dashboard PID, monitor process, and broker refresher state
- record startup, stop, crash, and restart history
- detect wrong-root processes and stale PID files
- expose engine-running state separately from submit authority

Failure behavior:

- wrong root is `NOT_READY_CONFIG`
- backend healthy but runtime stopped is `DEGRADED_NO_SUBMIT` or
  `NOT_READY_DEPENDENCY`, depending on recovery status
- stale process artifacts must not count as current runtime ownership

Authority:

- read-only in the first implementation slice
- future controlled mode may restart observation/runtime services only
- may not restart into a config that changes lane authority
- may not submit or mutate broker/lifecycle state

### Reconciliation Agent

Maintains broker/lifecycle/order-intent consistency as a first-class readiness
input.

Responsibilities:

- consume last successful complete broker truth
- consume lifecycle/open-position and open-order artifacts
- consume submit-intent ownership artifacts
- write official Phase-1 broker reconciliation artifact
- classify lifecycle open positions, unknown broker positions, unknown open
  orders, stale managed exits, review-required records, and freshness blockers
- expose `broker_reconciled`, `review_required_count`,
  `lifecycle_open_position_count`, `track_b_broker_open_order_count`,
  `live_money_eligible=false`, and `submit_authority=false` unless the submit
  gate explicitly qualifies PAPER submit

Failure behavior:

- stale broker truth blocks submit as `NOT_READY_RECONCILIATION`
- broker/lifecycle mismatch blocks submit and requires cleanup/adoption review
- clean reconciliation alone does not make lanes submit-capable

Authority:

- read-only reconciliation by default
- may write reconciliation/read-model artifacts
- may not mutate lifecycle except by invoking a separate guarded cleanup tool
  after dry-run and operator approval
- may not submit/cancel/close/placeOrder

### Lifecycle Cleanup/Adoption Agent

Classifies whether lifecycle state can be repaired or adopted after external
events such as operator manual close.

Responsibilities:

- find stale lifecycle rows and matching close/adoption evidence
- run guarded dry-runs for manual-flat settlement, bridge close cleanup, or
  broker-position adoption
- prove account, contract, conId/local symbol, side, quantity, timestamp, price
  when available, execution id, and perm id when applicable
- compute or preserve P&L according to existing lifecycle logic
- emit exact apply command only when dry-run proves reconciliation would clear

Failure behavior:

- missing close evidence becomes operator review, not silent ledger edit
- multiple or ambiguous executions block cleanup
- unknown realized P&L is allowed only if the guarded tool explicitly supports
  price-unknown settlement and records that uncertainty

Authority:

- read-only discovery by default
- guarded lifecycle mutation only after dry-run and explicit operator approval
- no broker mutation
- no order API calls

### Lane Eligibility/Quarantine Agent

Keeps lane activation failure local to the lane whenever possible.

Responsibilities:

- evaluate each configured lane independently for registry support, route
  support, governance support, lifecycle ownership support, exit policy support,
  exposure conflict, session/event gating, and data readiness
- maintain a lane quarantine list with reason, first_seen, last_seen, evidence,
  and operator action
- prevent a restored/research/non-authority lane from becoming submit-capable
  unless registry and governance explicitly support it
- distinguish observation-eligible, paper-review, paper-submit-eligible, and
  quarantined states

Failure behavior:

- one restored lane startup reconciliation failure quarantines that lane and
  leaves the runtime in observation mode for other clean lanes
- lane quarantine removes that lane from submit eligibility and may remove it
  from active evaluation if its startup path is unsafe
- repeated lane-level failures escalate operator alerting, not global submit
  authority

Authority:

- may write lane quarantine/readiness artifacts
- may request observation-runtime restart in a future controlled mode
- may not change active config by itself
- may not mutate lifecycle or broker state
- may not submit/cancel/close/placeOrder

### Submit Gate Agent

Owns the final PAPER submit-capability verdict.

Responsibilities:

- consume broker truth, market data, runtime health, reconciliation, lifecycle,
  lane eligibility, exposure, session/event gates, and operator controls
- produce one reason tree for global and lane-level submit capability
- separate `engine_running`, `observation_ready`, `paper_submit_capable`, and
  `live_money_eligible`
- keep `live_money_eligible=false` in this architecture
- expose exact blockers and the first required condition for PAPER order
  eligibility

Failure behavior:

- any stale, ambiguous, wrong-root, unreconciled, or lane-quarantined dependency
  blocks submit for the affected scope
- dashboard/backend health never overrides submit gate blockers
- last-good broker truth can keep observation state coherent but cannot allow
  submit once stale

Authority:

- read-only aggregator
- may write submit-gate artifacts
- may not restart services
- may not mutate lifecycle or broker state
- may not submit/cancel/close/placeOrder

### Operator Alerting Agent

Turns readiness state into concise operator action.

Responsibilities:

- surface state transitions and persistent degraded conditions
- classify operator action required versus automatic retry in progress
- report exact command suggestions for approved manual actions when appropriate
- summarize root, broker truth freshness, reconciliation state, lane
  quarantines, restart history, and submit-gate reason tree

Failure behavior:

- stale or ambiguous evidence must be described as such
- no green dashboard summary may hide submit blockers

Authority:

- read-only
- may write alert/status artifacts
- may not mutate broker, lifecycle, config, or runtime state

## Authority Boundary Matrix

| Agent | Broker read | Broker mutation | Artifact writes | Runtime restart | Lifecycle mutation | Config mutation |
| --- | --- | --- | --- | --- | --- | --- |
| Broker truth | yes | no | yes | no | no | no |
| Market data | no broker | no | yes | future controlled observation only | no | no |
| Runtime health | no broker | no | yes | future controlled observation/runtime only | no | no |
| Reconciliation | no broker calls directly; consumes broker truth | no | yes | no | no by default | no |
| Lifecycle cleanup/adoption | optional read-only evidence queries | no | yes | no | guarded after approval only | no |
| Lane eligibility/quarantine | no broker | no | yes | future controlled observation only | no | no |
| Submit gate | no broker | no | yes | no | no | no |
| Operator alerting | no broker | no | yes | no | no | no |

Only the existing guarded PAPER route may submit PAPER orders, and only after
the submit gate is clean for the specific lane, account, contract, session,
market-data state, broker truth, reconciliation, lifecycle ownership, and
operator controls. No readiness-maintenance agent may submit, cancel, close,
flatten, replace, or call `placeOrder`.

## Recovery Policy

### Retry

Use bounded retry for transient dependencies:

- broker truth TWS 502
- market-data transport reconnect
- dashboard health probe warmup
- runtime attachment/read-model refresh

Retries must write attempt status and must not overwrite last-good truth with
incomplete evidence.

### Preserve Last-Good Truth

Broker truth and market-data agents keep two concepts:

- last successful complete truth
- latest attempt status

Submit may use last successful complete truth only while it is fresh. Failed
attempts never erase the last successful complete state, but they do appear in
the dashboard and reason tree.

### Quarantine Lane

Lane-level startup reconciliation, config, governance, route, data, or lifecycle
failures quarantine only the affected lane when isolation is safe.

Quarantine records should include:

- lane id
- symbol/contract family
- classification
- reason code
- first_seen and last_seen
- source artifact
- whether observation may continue
- whether PAPER submit is blocked
- operator action required

### Restart Observation Runtime

Future controlled restart authority should be narrow:

- restart only observation/PAPER runtime services
- preserve active config manifest unless operator approves a changed manifest
- refuse restart on wrong root, broker ambiguity, stale broker truth, or
  unreconciled lifecycle
- record restart reason and before/after evidence

This should be implemented after read-only classification and lane quarantine.

### Block Submit

Submit blocks on:

- stale or incomplete broker truth
- unreconciled broker/lifecycle/order-intent state
- wrong root or stale runtime ownership
- stale runtime market data
- lane quarantine
- missing governance/registry/route/lifecycle/exit policy support
- exposure conflict
- operator halt
- live-money request in this PAPER-only architecture

### Require Operator Approval

Operator approval is required for:

- lifecycle cleanup/adoption apply
- config activation or overlay changes
- runtime restart with changed config
- any guarded PAPER route submit action
- manual settlement with unknown P&L
- promotion of restored/research lanes to submit-capable PAPER

## Desired Behavior For Current Failure

### Restored MNQ Lane Startup Reconciliation Failure

Current observed failure:

```text
mnq_1x_asia_london_participation__asia_london_long_v5:paper_startup_reconciliation_failed
```

Desired behavior:

- quarantine `mnq_1x_asia_london_participation__asia_london_long_v5`
- preserve evidence that broker/lifecycle Phase-1 reconciliation may still be
  globally clean
- keep other non-quarantined lanes in `READY_OBSERVATION` if their dependencies
  are clean
- keep global PAPER submit blocked until each candidate lane has clean lane
  readiness and the submit gate is clean
- do not kill the whole runtime solely because one restored lane failed startup
  reconciliation, unless the failure indicates a shared dependency such as
  wrong root, stale broker truth, or corrupted lifecycle artifacts

### Intermittent IBKR TWS 502

Desired behavior:

- latest attempt records TWS 502 and incomplete broker truth
- last successful complete broker truth remains intact
- dashboard displays both last-good truth and latest failed attempt
- submit is allowed only while last-good truth is fresh and all other gates pass
- when last-good truth becomes stale, state moves to `NOT_READY_DEPENDENCY` or
  `NOT_READY_RECONCILIATION` and submit is blocked

### Broker Truth Stale

Desired behavior:

- disable submit
- preserve runtime and lane observation state where safe
- do not erase positions/open orders with empty incomplete failed artifacts
- require fresh complete broker truth before PAPER order eligibility

## Runtime State Model

### `READY_OBSERVATION`

The engine/dashboard/read models can observe safely, but no lane currently has
full PAPER submit authority.

Typical conditions:

- active root verified
- runtime or observation path alive
- last-good broker truth fresh enough for observation context
- no global dependency ambiguity
- one or more lanes may be observed
- `paper_submit_capable=false`
- `live_money_eligible=false`

### `READY_SUBMIT_CAPABLE`

At least one lane is currently eligible for guarded PAPER submit.

Required conditions:

- active root verified as Dev
- approved config manifest loaded
- broker truth last successful complete state is fresh
- latest broker attempt is either successful or non-authoritative while
  last-good truth remains fresh, per policy
- Phase-1 reconciliation clean
- lifecycle open positions and open orders match broker truth
- market-data runtime feed fresh for the lane
- lane registry/governance/route/lifecycle/exit policy support is clean
- lane not quarantined
- exposure conflict check clean
- session/event gates clean
- operator controls permit PAPER
- guarded PAPER route is the only submit path
- `live_money_eligible=false`

### `DEGRADED_NO_SUBMIT`

Observation may continue, but submit is blocked.

Examples:

- dashboard/backend alive while paper runtime is stopped
- broker truth last-good still fresh but latest attempt failed
- one or more lanes quarantined
- market-data probe passes but runtime feed is stale
- noncritical monitor is warming

### `NOT_READY_DEPENDENCY`

A shared dependency required for safe operation is unavailable or stale.

Examples:

- broker truth last successful complete state is stale
- market-data runtime feed unavailable
- backend unreachable
- TWS unavailable and no fresh last-good truth remains

### `NOT_READY_RECONCILIATION`

Broker, lifecycle, order, or submit-intent state is ambiguous or mismatched.

Examples:

- lifecycle open position with broker flat and no approved cleanup
- unknown broker position
- unknown open order
- review-required lifecycle row
- unresolved submit-intent ownership

### `NOT_READY_CONFIG`

The active runtime/config cannot be trusted.

Examples:

- wrong root
- stale PID/artifact ownership
- unexpected config paths
- restored bulk overlay accidentally loaded
- lane configured without required registry/governance/route support

## Artifacts And Dashboard Fields

The dashboard should display a read model built from explicit artifacts. It
should not infer readiness from backend health alone.

Required control-plane summary fields:

```json
{
  "schema_version": "track_b_live_readiness_maintenance_v1",
  "generated_at": "2026-05-18T00:00:00+00:00",
  "active_root": "/Users/patrick/Dev/MGC-v05l-automation",
  "active_root_verified": true,
  "runtime_state": "DEGRADED_NO_SUBMIT",
  "paper_only": true,
  "live_money_eligible": false,
  "engine_running": false,
  "observation_ready": true,
  "paper_submit_capable": false,
  "last_good_broker_truth": {
    "classification": "BROKER_TRUTH_REFRESH_READY",
    "generated_at": "2026-05-18T10:57:31+00:00",
    "fresh": true,
    "account": "DUM882026",
    "positions_complete": true,
    "open_orders_complete": true,
    "open_order_count": 0
  },
  "latest_broker_attempt": {
    "classification": "BROKER_TRUTH_REFRESH_FAILED",
    "generated_at": "2026-05-18T10:57:09+00:00",
    "last_error": "TWS paper API error 502",
    "positions_complete": false,
    "open_orders_complete": false
  },
  "reconciliation": {
    "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
    "broker_reconciled": true,
    "lifecycle_open_position_count": 0,
    "review_required_count": 0,
    "track_b_broker_open_order_count": 0
  },
  "lane_quarantine": [
    {
      "lane_id": "mnq_1x_asia_london_participation__asia_london_long_v5",
      "reason_code": "paper_startup_reconciliation_failed",
      "paper_submit_capable": false,
      "operator_action_required": true
    }
  ],
  "submit_gate_reason_tree": {
    "global_submit_capable": false,
    "global_blockers": ["runtime_not_running"],
    "lane_blockers": {
      "mnq_1x_asia_london_participation__asia_london_long_v5": [
        "lane_quarantined"
      ]
    }
  },
  "restart_history": [
    {
      "started_at": "2026-05-18T10:55:00+00:00",
      "ended_at": "2026-05-18T10:56:00+00:00",
      "classification": "RUNTIME_EXITED_STARTUP_RECONCILIATION",
      "reason": "mnq lane startup reconciliation failed"
    }
  ],
  "operator_action_required": true,
  "operator_action": "Review quarantined MNQ lane startup reconciliation before reactivation."
}
```

Dashboard sections should include:

- runtime state and submit-capability badge
- active root and config manifest
- last good broker truth
- latest broker attempt
- broker truth freshness age
- reconciliation state
- lifecycle cleanup/adoption pending items
- market-data runtime-feed freshness
- lane quarantine list
- lane-level submit blockers
- global submit-gate reason tree
- restart history
- operator action required

## Implementation Roadmap

### Slice 1: Read-Only Control-Plane Summary

Smallest first slice:

- create a read-only summary builder that consumes existing artifacts
- expose runtime state enum
- expose last-good broker truth and latest broker attempt distinctly
- expose active root and config path validation
- expose reconciliation state
- expose lane quarantine candidates from recent runtime failure artifacts/logs
- expose submit gate reason tree with global and lane-level blockers
- write one summary artifact
- update dashboard to display the summary without changing authority

Tests:

- failed latest broker attempt preserves last-good truth in summary
- stale last-good broker truth blocks submit
- backend healthy but runtime stopped is not submit-capable
- wrong root becomes `NOT_READY_CONFIG`
- stale artifact timestamp does not count as current
- clean reconciliation does not imply submit capability

### Slice 2: Lane Quarantine Artifact

- add a lane-quarantine writer/reader
- classify startup reconciliation failures as lane-level where safe
- prevent one quarantined lane from killing observation for unrelated clean lanes
- require operator review before unquarantine or activation

Initial runtime slice:

- lane startup reconciliation now classifies outcomes as `READY`, `BLOCKED`,
  `QUARANTINED`, or `FATAL_RUNTIME_BLOCKER`
- lane-scoped startup reconciliation failures are recorded in the operator
  status and `paper_lane_quarantine_status.json`
- quarantined lanes are skipped by the supervisor loop and excluded from
  runtime submit/eligibility counts
- broker-wide, account-wide, persistence-corruption, and unsafe opposite-side
  exposure conditions remain fatal runtime blockers

Tests:

- MNQ lane startup reconciliation failure quarantines that lane
- global runtime remains observation-eligible when shared dependencies are clean
- bulk restored MGC/GC/PL/NQ/ES/MES activation is detected as config violation
- quarantined lane cannot be submit-capable

### Slice 3: Market-Data Runtime Feed Health

- add runtime-feed freshness artifact
- distinguish provider probe from runtime ingestion health
- block submit on stale runtime feed

Tests:

- isolated Databento probe pass plus stale runtime bars is `DEGRADED_NO_SUBMIT`
- fresh runtime bars satisfy data dependency for observation
- missing lane symbol feed quarantines only affected lane when safe

### Slice 4: Controlled Observation Restart Policy

- add controlled restart planner in dry-run mode
- record before/after evidence
- refuse wrong root, stale broker truth, unreconciled lifecycle, or changed
  config without operator approval
- later allow controlled observation/PAPER runtime restart only after approval

Tests:

- restart dry-run emits exact command and blockers
- changed config requires operator approval
- stale broker truth blocks restart
- restart history records reason and result

### Slice 5: Guarded Lifecycle Cleanup/Adoption Integration

- surface guarded cleanup/adoption dry-runs in the control-plane summary
- no automatic apply
- require explicit approval and audited evidence

Tests:

- manual close with valid IBKR read-only evidence produces cleanup-ready action
- missing execution id blocks cleanup
- cleanup apply is never invoked by alerting/dashboard/readiness agents

## PAPER-Only Invariants

Every slice must preserve these invariants:

- `live_money_eligible=false`
- no new broker mutation path
- no submit/cancel/close/placeOrder outside the existing guarded PAPER route
- dashboard remains a read model
- strategy behavior unchanged
- config activation requires explicit operator approval
- runtime restart authority, when added, is separate from submit authority
- stale or ambiguous evidence blocks submit

## First PAPER Order Eligibility Condition

The first PAPER order is eligible only when the submit gate reports
`READY_SUBMIT_CAPABLE` for a specific lane and the existing guarded PAPER route
is invoked through its approved authority path.

Minimum exact condition:

- Dev root is active and verified
- approved config manifest is active
- broker truth last successful complete state is fresh
- latest broker attempt status is explicit
- broker account is `DUM882026`
- open orders are complete and count is `0`, unless the lane has an explicitly
  owned managed-exit order
- Phase-1 reconciliation is clean
- lifecycle open positions match broker truth
- `review_required_count=0`
- runtime market-data feed is fresh for the lane
- lane is registry-backed, governance-backed, route-backed, lifecycle-owned,
  exit-policy-backed, not quarantined, and exposure-clean
- session/event gates are explicit and open for that lane
- operator controls allow PAPER
- `live_money_eligible=false`
- no agent other than the existing guarded PAPER route can submit
