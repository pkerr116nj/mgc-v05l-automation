# AN-003: Trading Platform Evolution

## Purpose

Describe how the trading platform evolved from manual discretionary activity into deterministic PAPER execution with evidence preservation and explicit operational authority.

## Where We Were

The project began with manual discretionary trading and local automation experiments. Early automation could observe, report, and assist, but the authority boundaries were not yet strong enough for autonomous operation.

As the system matured, the central problem changed. The goal was no longer merely to generate signals. It became necessary to preserve broker truth, order truth, fills, managed lifecycle state, exits, safety decisions, and trade evidence in a way that could be audited later.

## What We Decided

The trading platform adopted deterministic PAPER execution before any live-money ambitions.

The major decisions were:

- Broker truth is authoritative for current exposure and open orders.
- PAPER execution must be broker-backed, attributable, and reproducible.
- Managed Exit owns close supervision for managed positions.
- Canonical Trade Records provide completed-trade identity.
- Operational safety artifacts preserve state, blockers, and provenance.
- Research artifacts must not become production authority.

The system moved toward a broker-centered safety model: current exposure and current orders are resolved from fresh broker truth, while historical and derived artifacts remain diagnostic unless explicitly promoted to an authority role.

## What Changed

Trading became reproducible.

Instead of treating a trade as only an account event, the platform began preserving the decision, order, fill, lifecycle, exit, and outcome evidence around each trade. That made autonomous PAPER operation useful as an empirical research source rather than just an execution experiment.

The platform also learned to separate entry authority from exit authority. Entry can be blocked while close-only managed exit authority remains available for risk reduction.

## Where We Are Going

The direction is evidence-driven live readiness.

Future trading work should preserve:

- Broker authority for present exposure.
- Managed Exit authority for supervised risk reduction.
- Canonical identity for completed trades.
- Explicit blocker classification.
- Deterministic safety and provenance artifacts.

Live readiness should emerge from evidence, not from optimistic interpretation of research results.

## Related Documents

- `docs/track_b_architecture_map.md`
- `docs/architecture/baselines/BA-001-current-research-platform.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
- `docs/architecture/narratives/AN-004-research-platform-evolution.md`

