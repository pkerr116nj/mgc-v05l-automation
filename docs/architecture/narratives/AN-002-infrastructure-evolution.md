# AN-002: Infrastructure Evolution

## Purpose

Explain how infrastructure responsibilities became intentionally separated as the project moved from local automation toward a durable trading and research environment.

## Where We Were

Early services lived wherever it was convenient to run them. The primary workstation carried trading, development, research, dashboards, and operational support at the same time.

That was efficient while the system was small, but it made the platform harder to reason about. A service could be important without having a clear owner, and operational health could depend on whether the right machine happened to be awake, logged in, or running a terminal session.

## What We Decided

The project direction became responsibility-driven infrastructure.

The major architectural decisions were:

- Keep the Mac mini as the primary workstation and trading-control environment where that role makes sense.
- Move production-support services toward dedicated infrastructure hosts.
- Use a separate infrastructure-management host for administration and service management.
- Treat NAS systems as storage infrastructure rather than application hosts.
- Prefer Ubuntu for long-running server responsibilities.
- Use Portainer for consistent service visibility and management.
- Use Caddy for stable service access.
- Use Uptime Kuma for operational monitoring.

These decisions are about ownership boundaries, not hardware specifications.

## What Changed

Infrastructure became less opportunistic and more intentional.

The platform gained clearer distinctions between workstation, production-support services, management surfaces, storage, monitoring, and research outputs. That separation makes failures easier to localize and reduces the chance that a convenience deployment becomes an undocumented dependency.

The Regime Monitor and its coupled market-data services were migrated from Jupiter to Atlas to establish clearer service ownership and improve infrastructure responsiveness.

## Where We Are Going

The infrastructure direction is dedicated ownership with minimal operational complexity.

Future work should:

- Keep services placed according to responsibility.
- Avoid hidden dependencies on interactive desktop sessions.
- Preserve simple service access and monitoring.
- Document architectural intent separately from runbooks and configuration details.
- Add infrastructure only when it reduces day-to-day ambiguity.

## Related Documents

- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `SYSTEM_OVERVIEW.md`
- `docs/architecture/narratives/AN-001-physical-network-evolution.md`
- `docs/architecture/narratives/AN-005-mission-control-evolution.md`
- `docs/operations/data-retention-and-archive-policy.md`
