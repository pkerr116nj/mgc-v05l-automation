# Track B Architectural Invariants

## Status

Established project direction.

These invariants summarize durable Track B architecture expectations. They are not a new authority layer.

Any future proposal to weaken, remove, or materially change one of these invariants must use the Architecture Track defined in `ENGINEERING_PROCESS.md`.

## Purpose

Record the non-negotiable architectural invariants that future implementation, debugging, migration, research, and runtime work must preserve unless an explicitly approved architecture change supersedes them.

## Authority And Safety

- Live money is not eligible unless explicitly approved through a separate governance process.
- PAPER operation must remain explicit.
- Research outputs, dashboards, and UI surfaces have no broker-submit authority.
- Broker mutation must occur only through explicitly guarded Track B PAPER lifecycle paths.
- No hidden submit authority is allowed.
- No broad or global cancel behavior is allowed.
- No unscoped broker mutation is allowed.
- Guardian, Safe-State, reconciliation, broker truth, and runtime authority must fail closed when state is ambiguous.
- Current broker exposure and open orders come from fresh broker truth.
- Concurrent strategy-scoped positions are permitted where designed; do not impose a universal flat-before-entry rule.
- Pre-entry readiness must distinguish unsafe unresolved state from permitted concurrent positions.

## Research-Versus-Runtime Provenance Boundary

This is a global Track B rule, not a local module preference.

- Artifacts under `outputs/track_b_research/` are for research, replay, backtesting, and offline validation.
- Research artifacts must not be treated as active runtime truth.
- Active runtime components must consume explicitly produced execution-core/runtime hot-path artifacts.
- Runtime inputs require provenance, freshness, source category, input mode, generated timestamp, instrument, timeframe, completed-candle status, and staleness validation.
- If only research data is available, runtime components must fail closed as `NOT_READY`, `THIN_OR_STALE`, `LOW_CONFIDENCE`, or an equivalent classification.
- No runtime component may silently bridge research data into live decision state.

## Generalized Fixes Over Local Patches

- Recurring defects should be treated as evidence of a broader contract, authority, lifecycle, resolver, data-plane, or observability gap.
- Prefer central reusable invariants over lane-specific or trade-specific patches.
- Use first-blocker debugging.
- Avoid broad audits when a shallow canonical check can identify the first blocking defect.
- Prefer concise, ROI-aware implementation prompts.
- Avoid perfection work that does not materially improve reliability, autonomy, future development cost, or trading value.

## Runtime Restart Policy

- After a generalized fix is implemented and validated, runtime may be restarted automatically when hard safety gates pass.
- Restart remains blocked if broker state, order state, lifecycle state, Safe-State, Control Plane, or exposure is ambiguous.
- Restart remains blocked if live-money or `paper_proof` risk exists.
- Restart should not be delayed merely for a watch-only period when guarded PAPER testing is the intended validation environment.

## PAPER Leak-Testing Philosophy

- Temporary PAPER losses or open positions are acceptable when intentionally used to expose lifecycle, runtime, exit, reconciliation, or ownership defects.
- The engineering priority is to repair the reusable code defect, not to protect pretend-money P&L.
- PAPER exposure must still remain bounded, supervised where appropriate, and free from uncontrolled broker-state ambiguity.
- Preserve `live_money_eligible=false` and do not invoke `paper_proof` as a bypass.
- Do not confuse tolerance for PAPER losses with tolerance for unsafe authority or unresolved state.

## Data And Artifact Retention

- Hot-path runtime artifacts must remain bounded.
- Dashboards and runtime services must not depend on unbounded scans of historical logs or databases.
- Historical evidence should move into explicit cold/archive retention.
- Logs are operational troubleshooting evidence, not the canonical trading research record.
- Trade research should use structured durable artifacts such as fills, orders, canonical trades, outcomes, paths, context, and P&L records.
- Archive design should support future Linux storage hosts and avoid giant unmanaged artifact accumulation.

Operational retention and archive policy is detailed in `docs/operations/data-retention-and-archive-policy.md`.

## Strategy And Quality-Layer Boundaries

- Feature or market-quality layers are not strategies and do not possess direct trade authority.
- Optional quality layers may modify entry quality, exit urgency, or hold patience only when explicitly integrated through a separate approved decision.
- Diagnostic or quality outputs must not silently become hard trading predicates.
- Preserve 5-minute data as the lead decision surface where established; parallel lower-timeframe research must not silently become production authority.

## Engineering Economics

- Classify work as Infrastructure ROI, Product Development, or Perfection Work.
- Prefer changes whose reliability, autonomy, profitability contribution, or future token/time savings justify implementation cost.
- Avoid excessive Codex cycles on harmless PAPER-only simulator quirks unless they reveal a reusable platform lesson.
- Stop once the first blocker is identified unless broader investigation is explicitly justified.

## Related Documents

- `ENGINEERING_PROCESS.md`
- `PROJECT_PRINCIPLES.md`
- `SYSTEM_OVERVIEW.md`
- `docs/architecture/evidence-driven-engineering-lessons.md`
- `docs/track_b_application_policy_doctrine.md`
- `docs/track_b_architecture_map.md`
- `docs/architecture/narratives/AN-003-trading-platform-evolution.md`
- `docs/architecture/narratives/AN-004-research-platform-evolution.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
