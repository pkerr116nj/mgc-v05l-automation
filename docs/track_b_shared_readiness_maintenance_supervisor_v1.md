# Track B Shared Readiness Maintenance Supervisor v1

## Status

- Track: B
- Document type: shared architecture/design proposal for review
- Implementation status: design first; implementation not started
- Scope: PAPER-only readiness maintenance orchestration
- Live-money support: explicitly out of scope
- Broker mutation: out of scope
- Runtime restart: out of scope for this design slice

This document is design-only. It does not restart runtime, mutate broker state,
mutate lifecycle state, submit/cancel/close/place orders, change strategy
behavior, or grant live-money eligibility.

## Purpose

Track B needs one shared maintenance supervisor that can be used by broker,
market data, runtime, lane, reconciliation, lifecycle, artifact, and alert
agents. The supervisor decides which repair action is permitted under policy.
It does not detect every failure itself and does not execute domain-specific
repairs directly.

The intended split is:

- Watchdogs and domain agents detect and classify failures.
- Canonical readiness determines readiness state.
- The shared maintenance supervisor decides permitted repair actions.
- Domain agents execute only actions explicitly allowed by supervisor policy.
- Dashboard displays decisions, blockers, warnings, and history.
- Runtime only trades.

The supervisor is therefore a repair-orchestration layer, not an IBKR component,
not a dashboard, not a submit gate, and not a trading engine.

## Non-Goals

- No live-money execution support.
- No submit, cancel, close, replace, flatten, or `placeOrder` surface.
- No broad cancel or global cancel.
- No broker truth override.
- No reconciliation override.
- No strategy behavior changes.
- No automatic TWS restart in v1.
- No dashboard-owned repair logic.
- No runtime restart in this design slice.

## Architecture

```text
domain watchdogs / agents
  - broker truth
  - IBKR connectivity
  - market data
  - runtime health
  - lane eligibility / quarantine
  - reconciliation
  - lifecycle cleanup / adoption
  - artifact retention
  - operator alerting
        |
        v
shared readiness maintenance supervisor
        |
        +--> permitted action decision
        +--> per-agent action decisions
        +--> retry / cooldown metadata
        +--> repair history
        +--> operator action required
        +--> submit block reason
        |
        v
domain agents execute allowed actions only
        |
        v
canonical readiness remains authority
        |
        v
dashboard / scripts / operator CLI display state
```

The supervisor consumes agent classifications and canonical readiness. It emits
decisions. Domain agents own execution, and every executable action must be
allowed by both the shared supervisor policy and the agent-specific policy.

## Agent Users

### Broker Truth Agent

Detects whether complete read-only broker truth is fresh, stale, missing,
malformed, or failed. It can use supervisor decisions to retry refreshes,
restart its own sidecar, or preserve last-good truth while submit remains
blocked.

Allowed actions:

- `RETRY`
- `REFRESH_BROKER_TRUTH`
- `RESTART_SIDECAR`
- `BLOCK_SUBMIT`
- `ALERT_OPERATOR`

Forbidden:

- broker mutation
- order API calls
- replacing last-good truth with incomplete truth

### IBKR Connectivity Watchdog

Detects TWS/API socket and callback health. It is one producer among many, not
the supervisor itself.

Allowed actions:

- report `TWS_NOT_LISTENING`, handshake timeout, managed-account timeout,
  positions timeout, open-orders timeout, client-id collision suspicion, and
  read-only connected state
- request supervisor decisions for retry, client-id rotation, operator alert,
  or submit block

Forbidden:

- restarting TWS
- refreshing broker truth directly unless invoked as the Broker Truth Agent
- any broker/order mutation

### Market Data Watchdog

Detects provider connectivity, runtime feed freshness, socket closure,
malformed bars, missing symbols, delayed/stale bars, and data path mismatch.

Allowed actions:

- `RETRY`
- `RECONNECT_MARKET_DATA`
- `RESTART_SIDECAR`
- `BLOCK_SUBMIT`
- `ALERT_OPERATOR`

Forbidden:

- changing strategy behavior
- changing lane activation
- treating isolated provider success as runtime feed success

### Runtime Health Agent

Detects process health, active root, config manifest, PID ownership, runtime
heartbeat, backend/monitor state, and stale process artifacts.

Allowed actions:

- `RETRY`
- `RESTART_OBSERVATION_RUNTIME` only under policy and only after gates pass
- `ALERT_OPERATOR`
- `BLOCK_SUBMIT`

Forbidden:

- restarting wrong-root processes automatically
- restarting into changed strategy behavior or unapproved config
- enabling submit authority

### Lane Eligibility/Quarantine Agent

Detects lane-scoped failures, eligibility gaps, registry/governance support,
session gating, exposure conflicts, startup reconciliation failures, and
quarantine state.

Allowed actions:

- `QUARANTINE_LANE`
- `RETRY`
- `ALERT_OPERATOR`
- `BLOCK_SUBMIT`

Forbidden:

- activating lanes by itself
- widening submit eligibility
- hiding account-wide ambiguity as lane-scoped failure

### Reconciliation Agent

Detects broker/lifecycle/order-intent consistency and freshness of Phase-1
reconciliation.

Allowed actions:

- `REFRESH_RECONCILIATION`
- `RETRY`
- `ALERT_OPERATOR`
- `BLOCK_SUBMIT`

Forbidden:

- overriding broker truth
- overriding lifecycle truth
- mutating lifecycle directly
- declaring submit readiness without canonical readiness

### Lifecycle Cleanup/Adoption Agent

Detects stale lifecycle rows, manual-close evidence, broker-position adoption
opportunities, malformed ledger rows, and cleanup dry-run status.

Allowed actions:

- `RETRY`
- `REQUIRE_OPERATOR_APPROVAL`
- `ALERT_OPERATOR`
- `BLOCK_SUBMIT`

Guarded actions:

- lifecycle mutation only through audited cleanup/adoption tools
- dry-run must prove evidence, exact row match, and reconciliation would clear
- apply requires explicit operator approval

Forbidden:

- manual ledger edits outside guarded tools
- broker mutation
- order API calls

### Artifact Retention Agent

Detects artifact pileup, hot-path bloat, stale generated files, malformed
artifacts, and history-retention risks.

Allowed actions:

- `ARCHIVE_ARTIFACTS`
- `RETRY`
- `ALERT_OPERATOR`

Forbidden:

- deleting authoritative latest artifacts without replacement
- mutating broker/lifecycle/runtime state
- archiving evidence needed for unresolved reconciliation or audit

### Operator Alert Agent

Formats and routes operator-required decisions. It does not decide readiness and
does not repair dependencies.

Allowed actions:

- `ALERT_OPERATOR`
- emit notification artifacts or UI display fields

Forbidden:

- executing repairs
- clearing operator approval requirements
- changing canonical readiness

## Supervisor Decisions

The supervisor emits one primary decision and zero or more secondary decisions:

- `NO_ACTION`
- `RETRY`
- `ROTATE_CLIENT_ID`
- `RESTART_SIDECAR`
- `REFRESH_BROKER_TRUTH`
- `REFRESH_RECONCILIATION`
- `RECONNECT_MARKET_DATA`
- `RESTART_OBSERVATION_RUNTIME`
- `QUARANTINE_LANE`
- `ARCHIVE_ARTIFACTS`
- `ALERT_OPERATOR`
- `BLOCK_SUBMIT`
- `REQUIRE_OPERATOR_APPROVAL`

Decision semantics:

- `NO_ACTION`: current state is acceptable or intentionally observation-only.
- `RETRY`: repeat the same read-only or diagnostic action after cooldown.
- `ROTATE_CLIENT_ID`: use an approved diagnostic/sidecar client-id range.
- `RESTART_SIDECAR`: restart an internal read-only/observation producer.
- `REFRESH_BROKER_TRUTH`: run broker truth refresh through read-only tooling.
- `REFRESH_RECONCILIATION`: regenerate official reconciliation from current
  evidence.
- `RECONNECT_MARKET_DATA`: reconnect a market-data producer or socket.
- `RESTART_OBSERVATION_RUNTIME`: restart runtime only in observation-safe mode
  after all policy gates pass.
- `QUARANTINE_LANE`: isolate a lane-scoped failure from global runtime health.
- `ARCHIVE_ARTIFACTS`: move cold artifacts off hot paths while preserving audit.
- `ALERT_OPERATOR`: record and surface human action required.
- `BLOCK_SUBMIT`: record that submit must remain fail-closed.
- `REQUIRE_OPERATOR_APPROVAL`: require explicit approval before a guarded action.

`BLOCK_SUBMIT` is not a direct mutation of submit authority. It is a supervisor
decision that canonical readiness and the submit gate must continue to fail
closed until authoritative evidence recovers.

## Authority Boundaries

Agents may repair dependencies and artifacts only under policy:

- restart internal read-only/observation sidecars
- retry read-only checks
- rotate diagnostic client ids
- refresh broker truth through read-only broker truth tooling
- refresh reconciliation from existing evidence
- reconnect market-data producers
- quarantine lane-scoped failures
- archive artifacts that are not active authority
- alert the operator

Agents may not:

- submit/cancel/close/place orders
- call `placeOrder`
- broad cancel or global cancel
- enable live money
- enable submit authority directly
- override broker truth
- override reconciliation
- mutate lifecycle outside guarded audited tools and policy
- restart TWS automatically
- change strategy behavior
- activate new lanes or overlays without operator-approved config workflow

TWS restart remains explicitly disallowed unless a future policy revision adds
operator-approved TWS restart authority.

## Canonical Readiness Relationship

Canonical readiness remains the readiness authority. The supervisor must treat
canonical readiness as an input and never as something it can bypass.

Examples:

- If canonical readiness says `NOT_READY_WRONG_ROOT`, the supervisor may only
  alert and block submit.
- If canonical readiness says `NOT_READY_RECONCILIATION`, the supervisor may
  choose `REFRESH_RECONCILIATION` if broker truth is fresh and no mismatch exists.
- If canonical readiness says `READY_OBSERVATION_ONLY`, the supervisor may choose
  `NO_ACTION` or a safe observation-runtime repair, but not submit authority.
- If canonical readiness says `READY_SUBMIT_CAPABLE`, the supervisor may still
  emit warnings, but it must not create independent submit permission.

## State Model

### `OBSERVING`

Artifacts are present enough to classify. No repair is active, or the stack is
intentionally observation-only.

Typical decisions:

- `NO_ACTION`
- `REFRESH_RECONCILIATION` if broker truth is fresh and reconciliation is stale

### `DEGRADED`

One or more dependencies are impaired, but the condition is bounded and may be
repairable without operator intervention.

Typical decisions:

- `RETRY`
- `ROTATE_CLIENT_ID`
- `RECONNECT_MARKET_DATA`
- `REFRESH_BROKER_TRUTH`
- `BLOCK_SUBMIT`

### `REPAIRING`

A permitted repair action is in progress or scheduled. The supervisor must emit
retry and cooldown metadata to prevent loops.

Typical decisions:

- `RESTART_SIDECAR`
- `RECONNECT_MARKET_DATA`
- `REFRESH_RECONCILIATION`
- `QUARANTINE_LANE`
- `ARCHIVE_ARTIFACTS`

### `RECOVERED`

A previously degraded dependency returned to acceptable state and canonical
readiness has advanced to the expected observation or submit-capable PAPER state.

Typical decisions:

- `NO_ACTION`
- append recovery event to repair history

### `BLOCKED`

The system is in an unsafe or ambiguous state where repair should not proceed
without more evidence or operator action.

Typical decisions:

- `BLOCK_SUBMIT`
- `ALERT_OPERATOR`
- `REQUIRE_OPERATOR_APPROVAL`

### `OPERATOR_REQUIRED`

The supervisor cannot safely repair the condition alone.

Typical decisions:

- `ALERT_OPERATOR`
- `BLOCK_SUBMIT`
- `REQUIRE_OPERATOR_APPROVAL`

## Artifact Contract

### `latest_maintenance_supervisor_decision.json`

Required fields:

- `schema_version`
- `generated_at`
- `active_root`
- `expected_root`
- `root_match`
- `state`
- `primary_decision`
- `secondary_decisions`
- `per_agent_decisions`
- `operator_action_required`
- `submit_block_reason`
- `retry_count`
- `cooldown_until`
- `last_success`
- `last_action`
- `next_retry_at`
- `decision_reasons`
- `input_artifacts`
- `input_freshness`
- `canonical_readiness`
- `agent_classifications`
- `authority`

Authority block:

```json
{
  "paper_only": true,
  "submit_authority": false,
  "live_money_eligible": false,
  "broker_mutation_allowed": false,
  "order_api_allowed": false,
  "lifecycle_mutation_allowed": "guarded_tools_only_with_operator_approval",
  "tws_restart_allowed": false,
  "dashboard_owner": false
}
```

### `maintenance_repair_history.jsonl`

Append-only history rows:

- `occurred_at`
- `state_before`
- `state_after`
- `agent`
- `decision`
- `action_executed`
- `result`
- `reason`
- `input_artifact_paths`
- `input_artifact_hashes`
- `retry_count`
- `cooldown_until`
- `operator_approval_required`
- `operator_approval_id`
- `submit_block_reason`

### Per-Agent Action Decisions

Each per-agent decision should include:

- `agent`
- `classification`
- `decision`
- `allowed`
- `requires_operator_approval`
- `cooldown_until`
- `retry_count`
- `reason`
- `evidence_path`

## Retry And Cooldown Policy

The supervisor must avoid hot loops:

- every retryable decision has a retry budget
- every failed repair sets `cooldown_until`
- repeated failure escalates to `OPERATOR_REQUIRED`
- repair decisions are idempotent during cooldown
- sidecar and runtime restarts are more tightly rate-limited than read-only
  checks
- lifecycle mutation always requires guarded tool dry-run and approval

Initial recommended budgets:

- read-only retry: 3 attempts over 5 minutes
- client-id rotation: 3 approved ids before alert
- broker-truth sidecar restart: 1 attempt per 10 minutes
- market-data reconnect: 3 attempts over 10 minutes
- observation-runtime restart: 1 attempt per 15 minutes and only after clean
  broker truth, reconciliation, root, config, and data gates
- lane quarantine: one write per lane/reason until operator clears or policy
  expires it
- artifact archive: one pass per hour for hot-path cleanup

## Current Examples

### TWS 502 / ManagedAccounts Timeout

Inputs:

- IBKR connectivity watchdog reports `MANAGED_ACCOUNTS_TIMEOUT` or API 502
- broker-truth refresher latest attempt failed
- last-good broker truth may still be fresh or stale

Decision:

- if last-good broker truth is fresh: state `DEGRADED`, decision `RETRY`, warning
  only, submit follows canonical readiness
- if last-good broker truth is stale: state `DEGRADED`, decisions
  `ROTATE_CLIENT_ID`, `REFRESH_BROKER_TRUTH`, `BLOCK_SUBMIT`
- if repeated timeout persists: state `OPERATOR_REQUIRED`, decisions
  `ALERT_OPERATOR`, `BLOCK_SUBMIT`

Forbidden:

- no TWS restart
- no broker mutation
- no order API calls

### Databento Socket Close

Inputs:

- Market Data Watchdog reports provider socket closed
- runtime bars become stale or incomplete
- canonical readiness no longer submit-capable

Decision:

- state `DEGRADED`
- decisions `RECONNECT_MARKET_DATA`, `BLOCK_SUBMIT`
- if reconnect budget fails: `ALERT_OPERATOR`
- if market data recovers and other gates are clean: `NO_ACTION` or
  `RESTART_OBSERVATION_RUNTIME` only under policy

Forbidden:

- no strategy behavior change
- no pretending isolated provider probe equals runtime-feed freshness

### Lane Startup Reconciliation Failure

Inputs:

- Lane Eligibility Agent reports lane-scoped startup reconciliation failure
- account-wide broker truth and reconciliation are clean

Decision:

- state `REPAIRING`
- decision `QUARANTINE_LANE`
- secondary decision `BLOCK_SUBMIT` only for the affected lane if other eligible
  lanes remain clean

If account-wide ambiguity exists:

- state `BLOCKED`
- decisions `ALERT_OPERATOR`, `BLOCK_SUBMIT`
- no lane-scoped quarantine should hide the global issue

### Wrong-Root Runtime

Inputs:

- Runtime Health Agent reports active root differs from expected Dev root
- canonical readiness reports `NOT_READY_WRONG_ROOT`

Decision:

- state `BLOCKED`
- decisions `ALERT_OPERATOR`, `BLOCK_SUBMIT`
- do not auto-kill or restart wrong-root process in v1

### Stale Broker Truth

Inputs:

- Broker Truth Agent reports last successful complete truth stale
- latest attempt may be failed or missing

Decision:

- state `DEGRADED`
- decisions `REFRESH_BROKER_TRUTH`, maybe `RESTART_SIDECAR`, `BLOCK_SUBMIT`
- preserve last-good truth until replaced by complete successful truth

### Stale Lifecycle After Manual Close

Inputs:

- Reconciliation Agent reports lifecycle open position but broker is flat
- Lifecycle Cleanup/Adoption Agent finds or cannot find audited close evidence

Decision:

- with clean dry-run evidence: state `OPERATOR_REQUIRED`, decisions
  `REQUIRE_OPERATOR_APPROVAL`, `BLOCK_SUBMIT`
- without evidence: state `BLOCKED`, decisions `ALERT_OPERATOR`,
  `BLOCK_SUBMIT`

Allowed follow-up:

- guarded lifecycle cleanup apply only after explicit approval

Forbidden:

- no manual ledger edit
- no broker mutation

### Artifact Pileup / Hot-Path Bloat

Inputs:

- Artifact Retention Agent reports large stale output set, malformed latest
  artifacts, or hot-path scan slowdown

Decision:

- if artifacts are non-authoritative/cold: state `REPAIRING`, decision
  `ARCHIVE_ARTIFACTS`
- if artifacts are current authority or unresolved evidence: state
  `OPERATOR_REQUIRED`, decision `ALERT_OPERATOR`

Forbidden:

- no deletion of active latest artifacts without replacement
- no archiving unresolved reconciliation/lifecycle evidence

## First Implementation Slice

Smallest shared slice:

1. Add a pure decision module that accepts normalized agent classifications and
   canonical readiness, then returns a supervisor decision dictionary.
2. Add a CLI producer that reads known artifacts and writes
   `latest_maintenance_supervisor_decision.json`.
3. Keep all decisions advisory/read-only at first.
4. Append `maintenance_repair_history.jsonl` only when the decision changes.
5. Add tests for the examples above and authority invariants.

No repair execution, runtime restart, sidecar restart, broker mutation, lifecycle
mutation, submit/cancel/close/placeOrder, or strategy behavior change should be
implemented in the first shared slice.

## Validation Expectations

Tests should prove:

- each agent classification maps to the expected supervisor decision
- canonical readiness cannot be overridden
- wrong-root always blocks
- stale broker truth blocks submit and schedules broker-truth refresh
- Databento/socket-close state schedules market-data reconnect
- lane-scoped failure quarantines lane without hiding account-wide ambiguity
- manual-close lifecycle cleanup requires guarded approval
- artifact archive never touches authoritative latest or unresolved evidence
- authority block never permits live money, broker mutation, order APIs, or TWS
  restart
- source contains no broker/order mutation API calls

## Open Questions

- Should per-agent execution policy live in one central policy file or beside
  each agent?
- Should the first implementation write repair history only on decision changes
  or on every supervisor run?
- Should artifact hashes be mandatory in v1 or optional until hot-path costs are
  measured?
- Should observation-runtime restart remain manual approval only until after one
  week of advisory supervisor history?
- How should the supervisor represent a decision that is safe for observation
  but intentionally submit-blocking?
