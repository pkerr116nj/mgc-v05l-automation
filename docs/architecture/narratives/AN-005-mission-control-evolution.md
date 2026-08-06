# AN-005: Mission Control Evolution

## Purpose

Describe the operational dashboard philosophy that emerged as the trading and research platform became more autonomous.

## Where We Were

Operational visibility began as independent dashboards and reports. Each surface could be useful, but the overall experience could still leave the operator uncertain about whether the system was alive, stale, blocked, trading normally, or merely displaying old evidence.

As the system grew, dashboard detail increased faster than operational clarity.

## What We Decided

Mission Control should prioritize operational awareness.

The guiding decisions were:

- Health before appearance.
- Evidence before decoration.
- Actionable information before exhaustive detail.
- Freshness and authority should be visible.
- A dashboard should explain current operating state, not become hidden authority.

Mission Control is therefore a read model and awareness surface. It should help the operator understand runtime, Managed Exit, broker truth, Safe-State, Guardian, research freshness, and blocker status without replacing those systems.

## What Changed

Mission Control became an operational command center concept rather than a collection of screens.

The dashboard philosophy moved away from showing everything and toward showing what matters now: whether the system is running, whether evidence is fresh, whether authority is coherent, whether positions are supervised, and whether blockers are real.

## Where We Are Going

The direction is unified operational awareness.

Future Mission Control work should:

- Reduce visual and cognitive noise.
- Make freshness obvious.
- Separate live authority from diagnostic evidence.
- Surface first blockers clearly.
- Avoid becoming a configuration guide or a control-plane substitute.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_PRINCIPLES.md`
- `docs/architecture/decision-intelligence-vision.md`
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md`
- `docs/architecture/narratives/AN-003-trading-platform-evolution.md`
