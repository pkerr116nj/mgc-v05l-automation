# AN-001: Physical Network Evolution

## Purpose

Describe how the physical network supporting trading and research evolved from a home-oriented setup into a simpler engineering foundation.

## Where We Were

The environment began as a workstation-centric trading and research setup on top of a normal home network. Routing responsibilities were split across consumer and edge-network components, and the trading workstation carried too much of the operational burden.

That arrangement was workable while the project was mostly manual and exploratory. As the platform grew into autonomous PAPER trading, durable research artifacts, monitoring, and supporting services, the network became part of the engineering surface rather than background plumbing.

## What We Decided

The guiding decision was to eliminate unnecessary network complexity and establish a single routing authority.

The network direction moved toward:

- One primary routing authority.
- Eero operating as wireless access infrastructure rather than a competing router.
- Dedicated infrastructure hosts for services that should not depend on the primary workstation.
- Linux infrastructure as the preferred base for long-running support services.
- Incremental improvements rather than a wholesale rebuild.

The important decision was not a specific hardware inventory. It was the choice to make the network stable, legible, and maintainable enough to support a trading and research platform.

## What Changed

The network stopped being treated as a passive home utility and became part of the platform architecture.

Responsibility moved away from ad hoc workstation dependency and toward a small infrastructure layer that can support monitoring, dashboards, storage, service access, and research continuity. The result is less operational ambiguity: network routing, wireless access, service placement, and workstation use are now conceptually separate.

## Where We Are Going

The target state is stable and boring infrastructure that supports research without becoming the research project.

Future evolution should remain incremental:

- Keep routing authority simple.
- Keep service access predictable.
- Avoid adding network layers unless they remove more complexity than they introduce.
- Support durable research and operational monitoring without making the workstation a single point of architectural responsibility.

## Related Documents

- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `SYSTEM_OVERVIEW.md`
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md`

