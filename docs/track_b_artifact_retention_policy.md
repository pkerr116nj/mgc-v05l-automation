# Track B Artifact Retention Policy

Track B runtime cleanup must preserve anything that can affect, explain, or reproduce PAPER strategy behavior. Cleanup is allowed only when an artifact's tier is clear and active runtime does not depend on it.

## HOT Decision/Runtime Data

HOT data is trading-decision-oriented market data and strategy-consumable state. It is first-class runtime data, not disposable generated clutter.

Includes:

- Active-ticker candles and bars used by runtime decisions.
- 1m, 3m, and 5m candles for approved active futures tickers.
- Derived decision features such as fitted-curve slope, curvature, second derivative, micro-trend participation, feature rows, and strategy-consumable state.
- Latest freshness/provenance state required by strategy, route, monitor, governance, and source-readiness gates.

Rules:

- Active runtime may read HOT data only from the active runtime store with freshness, provenance, source category, instrument, timeframe, completed-candle status, and staleness validation.
- HOT data is local rolling runtime data and must not be committed.
- Do not delete unique HOT captures during cleanup unless a replacement rolling store is verified and the deleted copy is outside retention.
- Active approved tickers should support parallel collection throughout the trading day.
- Weekly Databento batch maintenance should eventually backfill through Friday close.

Suggested local retention:

- 1m candles: 5 trading days.
- 3m derived candles/features: 5 trading days.
- 5m candles/features: 10 trading days.
- Latest state JSONs: current/latest only plus short local backups.

## WARM Local Evidence/State

WARM artifacts explain runtime behavior, operator decisions, broker/order state, and readiness. They are not trading truth unless they are explicitly the latest artifact and pass freshness validation.

Includes:

- Preflight reports.
- Broker/order evidence.
- Reconciliation and review-required evidence.
- Governance reports.
- Dashboard/operator snapshots.
- Operator logs and routine runtime logs.

Rules:

- Keep locally on rolling retention.
- Do not use archived or stale WARM artifacts as runtime truth.
- Preserve incident, broker/order, reconciliation, and review-required evidence even when older than routine retention.
- Do not commit generated WARM artifacts.

Suggested local retention:

- Dashboard/operator snapshots: 7 days.
- Governance reports: 14 to 30 days.
- Routine preflight reports: 30 days.
- Broker/order/reconciliation/review-required evidence: 90 days locally, then archive later.
- Logs: size-bounded plus 30 days when practical.

## COLD Archive Staging

COLD artifacts are no longer operational but may matter for audit, research reproducibility, or incident review.

Rules:

- Stage locally until the LAN archive/file server exists.
- The future destination is a LAN file server or RAID-backed archive.
- COLD archive is a research, audit, and reproducibility resource only.
- Active runtime, preflight, dashboard, and submit gates must never read COLD archive as operational truth.

## Research/Offline

Research/offline artifacts include replay outputs, diagnostics, retained research examples, and exploratory modules that are not active runtime truth.

Rules:

- Defer broad cleanup until archive server policy is in place.
- Do not delete research/offline queue artifacts in ad hoc cleanup passes.
- Research artifacts may be used for replay, diagnostics, and offline validation only.

## Disposable/Build

Disposable artifacts are generated build metadata and local caches that do not carry unique runtime, research, or evidence value.

Includes:

- `*.egg-info` generated package metadata.
- Python, Node, and test caches.
- Generated build metadata that can be recreated from source.

Rules:

- Purge or restore today when dirty.
- Do not archive unless needed to reproduce a packaging bug.
- Do not commit disposable generated metadata.

## Cleanup Guardrails

Before deleting or restoring generated files:

- Confirm the file is not HOT decision/runtime data.
- Confirm the file is not broker/order/reconciliation/review-required evidence.
- Confirm the file is not needed by Sunday/Monday preflight or current PAPER watch readiness.
- Confirm active runtime does not read it as operational truth.
- Preserve research/offline artifacts until explicit archive migration.
- Never treat cold archive storage as a runtime dependency.
