# Data Acquisition And Storage Policy

## Summary

This project remains replay-first. Strategy logic stays broker-agnostic. Persistence remains SQLite-based for runtime strategy state, while broker-monitor truth stays isolated in the production-link store and UI cache stays derivative-only.

This policy formalizes four data domains:

1. `research_history`
2. `runtime_strategy_state`
3. `broker_monitor_truth`
4. `derived_ui_snapshot_cache`

The intent is to prevent silent purpose drift, silent truth mixing, and ad hoc storage growth.

This policy also enforces three explicit retention tiers:

1. durable forever-retained audit/research truth
2. medium-retention operational detail
3. short-retention disposable UI/cache artifacts

## Domain Ownership

### A. Research History

Purpose:
- long-horizon historical bars and additive research datasets
- scheduled backfill, analytics, experiment outputs, and reports

Current ownership:
- SQLite research tables in the configured runtime DBs: `bars`, `derived_features`, `signal_evaluations`, `trade_outcomes`, `experiment_runs`
- research artifacts under `/Users/patrick/Documents/MGC-v05l-automation/outputs/research`
- visualization outputs under `/Users/patrick/Documents/MGC-v05l-automation/outputs/visualizations`

Rules:
- append-friendly
- forever-retained by default
- never treated as broker/account truth
- may be materially larger than operational state

### B. Runtime Strategy State

Purpose:
- the minimum state required for safe runtime restart, paper truth, and operational audit

Current ownership:
- SQLite runtime tables: `processed_bars`, `strategy_state_snapshots`, `order_intents`, `fills`, `reconciliation_events`, `fault_events`
- runtime files under `/Users/patrick/Documents/MGC-v05l-automation/desktop/outputs/probationary_pattern_engine`

Rules:
- restart-safe
- authoritative for paper/runtime truth
- minimal and operational
- broker-independent
- forever-retained by default

### C. Broker Monitor Truth

Purpose:
- live or near-live broker visibility and reconciliation input

Current ownership:
- broker-monitor SQLite store at `/Users/patrick/Documents/MGC-v05l-automation/outputs/production_link/schwab_production_link.sqlite3`
- broker snapshot file at `/Users/patrick/Documents/MGC-v05l-automation/outputs/operator_dashboard/production_link_snapshot.json`
- selected account file at `/Users/patrick/Documents/MGC-v05l-automation/outputs/production_link/selected_account.json`
- production-link tables:
  - `broker_accounts`
  - `broker_account_balances`
  - `broker_positions`
  - `broker_quotes`
  - `broker_orders`
  - `broker_order_events`
  - `broker_reconciliation_events`
  - `broker_runtime_state`

Rules:
- freshness-scoped and timestamped
- suitable for reconciliation and operator visibility
- never silently merged into paper truth
- not part of strategy execution truth in this pass
- medium-retention operational detail, not forever-retained hot-path cache growth

### D. Derived UI / Snapshot Cache

Purpose:
- fast app rendering and operator-surface rollups

Current ownership:
- `/Users/patrick/Documents/MGC-v05l-automation/outputs/operator_dashboard`
- `/Users/patrick/Documents/MGC-v05l-automation/outputs/operator_dashboard/runtime`

Rules:
- derivative only
- never the ultimate source of truth
- rebuildable from runtime strategy state and broker monitor truth
- short retention

## Symbol Scope Policy

Tracked symbols are divided into explicit categories:

### `broker_held`

Definition:
- any symbol with a live broker position or open broker order

Policy:
- refresh in broker-monitor cadence
- automatically tracked while broker exposure exists
- stored under broker-monitor truth

### `paper_active`

Definition:
- any symbol with active paper/runtime lane activity or paper exposure

Policy:
- updated from runtime bar/event flow
- authoritative from runtime persistence
- stored under runtime strategy state

### `watched`

Definition:
- explicit operator watchlist symbols without active broker or paper exposure

Policy:
- lighter polling
- broker-monitor visibility only
- not promoted into strategy truth

### `research_universe`

Definition:
- explicit backfill/research target sets

Policy:
- backfilled or scheduled separately
- stored in research history only
- not tied to monitor refresh loops

### `ad_hoc`

Definition:
- temporary lookup symbols

Policy:
- expire unless promoted
- no long-lived truth implied
- default expiry: 24 hours

## Refresh Cadence Policy

### Broker monitor truth

- broker quotes / monitor values: near-live polling
- configured broker refresh cadence: 5 seconds
- broker service cache TTL: 15 seconds
- broker stale threshold: 120 seconds

### Runtime strategy state

- bar-driven and event-driven writes
- UI reads may poll more frequently, but persisted truth changes only on runtime events

### Research history

- manual or scheduled backfill
- not part of constant UI polling

### Derived UI snapshot cache

- rebuild from latest domain truth on display cadence
- current operator-surface cadence target: 2 seconds

## Retention Policy

### Durable forever-retained audit/research truth

Default behavior:
- retain forever by default
- compact only with explicit archive tooling
- do not prune by age during normal hot-path operation

Forever-retained tables:
- `bars`
- `strategy_state_snapshots`
- `order_intents`
- `fills`
- `reconciliation_events`
- `fault_events`
- `processed_bars`
- `derived_features`
- `signal_evaluations`
- `trade_outcomes`
- `experiment_runs`

Forever-retained files/artifacts:
- research outputs worth comparing later under `/Users/patrick/Documents/MGC-v05l-automation/outputs/research`
- durable research visualizations/manifests when intentionally preserved for later comparison

### Medium-retention operational detail

Default behavior:
- keep recent operational detail long enough for reconciliation and operator review
- archive or compact by age
- do not keep transient monitor detail forever in the primary hot-path store

Medium-retention compactable tables/files:
- `broker_quotes`
- transient broker-monitor snapshots under `/Users/patrick/Documents/MGC-v05l-automation/outputs/production_link`

Default compact window:
- broker quote/quote-overlay snapshots: 30 days

Latest broker state truth is still retained in:
- `broker_accounts`
- `broker_account_balances`
- `broker_positions`
- `broker_runtime_state`

### Longer-retention broker audit detail

Default behavior:
- keep longer than quote overlays because these support broker-side audit and reconciliation review
- still compactable, but on a longer horizon than quote snapshots

Longer-retention audit tables:
- `broker_order_events`
- `broker_reconciliation_events`

Default audit windows:
- broker order events: 180 days
- broker reconciliation events: 365 days

`broker_orders` retains the latest broker-order state in the hot-path store and may be compacted or snapshotted by explicit broker-monitor maintenance later.

### Short-retention disposable UI/cache layer

Default behavior:
- rebuildable
- replaceable
- aggressively prunable

Short-TTL disposable caches:
- quote overlays / quote snapshots surfaced only for UI caching
- freshness-state rows and summaries
- app-facing rollups
- temporary monitor summaries
- rebuildable dashboard/runtime artifacts under `/Users/patrick/Documents/MGC-v05l-automation/outputs/operator_dashboard`

### Research history

- forever-retained by default
- effectively indefinite until explicit archive/compaction action

### Runtime strategy state

- forever-retained by default
- restart-safe audit truth, not age-pruned by default

### Broker monitor truth

- medium-retention operational detail
- default broker quote/quote-overlay retention: 30 days
- default broker order-event retention: 180 days
- default broker reconciliation-event retention: 365 days
- latest account, balance, position, and reconciliation truth retained

### Derived UI snapshot cache

- short retention
- default cache window: 7 days
- safe to rebuild from deeper truth

### Token / auth files

- local secrets only
- separate from analytical and runtime storage
- outside the main DB retention policy
- current token path: `/Users/patrick/Documents/MGC-v05l-automation/.local/schwab/tokens.json`

## Storage Layout Policy

The project must keep storage purposes physically and logically separated.

### Runtime DBs

- base/default: `/Users/patrick/Documents/MGC-v05l-automation/mgc_v05l.sqlite3`
- replay: `/Users/patrick/Documents/MGC-v05l-automation/mgc_v05l.replay.sqlite3`
- paper: `/Users/patrick/Documents/MGC-v05l-automation/mgc_v05l.paper.sqlite3`

### Broker monitor store

- `/Users/patrick/Documents/MGC-v05l-automation/outputs/production_link/schwab_production_link.sqlite3`

### Derived UI cache

- `/Users/patrick/Documents/MGC-v05l-automation/outputs/operator_dashboard`

### Research artifacts

- `/Users/patrick/Documents/MGC-v05l-automation/outputs/research`
- `/Users/patrick/Documents/MGC-v05l-automation/outputs/visualizations`

### Secrets

- `/Users/patrick/Documents/MGC-v05l-automation/.local/schwab/tokens.json`

## Truth Hierarchy

### Paper runtime

Priority:
1. `runtime_strategy_state`
2. `derived_ui_snapshot_cache`

### Broker monitor

Priority:
1. `broker_monitor_truth`
2. `derived_ui_snapshot_cache`

### Combined UI

Priority:
1. `runtime_strategy_state`
2. `broker_monitor_truth`
3. `derived_ui_snapshot_cache`

Combined views are composed views only. They do not become a new truth layer.

## Reconciliation Input Policy

Broker snapshots used for reconciliation must capture at least:

- symbol
- quantity
- side
- average entry price if available
- mark or last where available
- open order count
- last fill timestamp if available
- freshness timestamps

Reconciliation compares runtime strategy state to broker monitor truth explicitly. It must not alter strategy behavior automatically in this pass.

## Cleanup / Rolloff Policy

Short-retention domains should have explicit cleanup jobs later, but cleanup policy is already defined now:

- UI snapshot cache: keep 7 days
- broker quote snapshots and order events: keep 30 days
- durable runtime/research truth: retain forever by default
- ad hoc symbols: expire after 24 hours

The first enforcement step is configuration and path/cadence centralization, not automatic deletion.

## Explicit Classification

### Forever-retained

Tables:
- `bars`
- `strategy_state_snapshots`
- `order_intents`
- `fills`
- `reconciliation_events`
- `fault_events`
- `processed_bars`
- `derived_features`
- `signal_evaluations`
- `trade_outcomes`
- `experiment_runs`

Files/artifacts:
- durable research outputs under `/Users/patrick/Documents/MGC-v05l-automation/outputs/research`

### Archived or compacted

Tables:
- `broker_quotes`
- `broker_orders`

Files/artifacts:
- transient broker-monitor snapshots in `/Users/patrick/Documents/MGC-v05l-automation/outputs/production_link`

### Longer-retention audit detail

Tables:
- `broker_order_events`
- `broker_reconciliation_events`

### Short-TTL disposable caches

Files/artifacts:
- `/Users/patrick/Documents/MGC-v05l-automation/outputs/operator_dashboard`
- `/Users/patrick/Documents/MGC-v05l-automation/outputs/operator_dashboard/runtime`

These include:
- freshness-state rows
- app-facing rollups
- temporary monitor summaries
- other rebuildable UI/cache artifacts

## Migration Path

1. Keep existing replay/paper SQLite runtime DBs unchanged.
2. Keep existing production-link SQLite store unchanged.
3. Centralize default storage paths, cadence defaults, and tracked-symbol policy in one config source.
4. Use that policy source for broker-monitor defaults first.
5. Add cleanup tooling later as explicit commands or scheduled maintenance.
6. Add tracked-symbol promotion/expiry enforcement later without changing strategy logic.

## Smallest Safe First Code Step

The smallest safe first enforcement step is:

1. introduce a typed data-storage policy config
2. load it centrally from code
3. make broker-monitor defaults read their DB path, snapshot path, cache TTL, and freshness threshold from that policy

That preserves replay-first behavior, keeps the strategy core broker-agnostic, and starts enforcing explicit storage ownership without adding live execution behavior.
