# Current Infrastructure Baseline

## Status

Current operational ownership baseline.

## Purpose

Describe the current infrastructure from an operational ownership perspective.

This document is not a hardware inventory, configuration guide, network map, or runbook. It records which hosts currently own broad responsibilities and the infrastructure philosophy behind that ownership.

## Current Host Roles

### Mac Mini

The Mac mini is the primary trading workstation.

It is the current host for Trader Workstation (TWS) and remains the primary operator desktop for PAPER trading supervision.

### Windows Systems

Windows systems are primarily client platforms.

Two HP mini towers currently run Thinkorswim (TOS). Windows is no longer the preferred infrastructure server platform for long-running project services.

### Atlas

Atlas is the primary infrastructure compute host.

It owns the Regime Monitor and its coupled market-data services. This placement reflects the ongoing move toward dedicated service ownership and improved responsiveness.

### Jupiter

Jupiter is an infrastructure and management host.

It no longer carries the Regime Monitor workload. Jupiter became noticeably more responsive after that workload was migrated away.

### NAS Systems

NAS systems own primary storage responsibilities.

They are the preferred long-term repository for archived artifacts where appropriate.

## Infrastructure Philosophy

Infrastructure responsibilities are intentionally separated.

Current principles:

- Services should have clear ownership.
- Hosts should carry responsibilities that fit their operational role.
- Linux is the preferred platform for headless infrastructure services.
- Simplicity is preferred over unnecessary complexity.
- Infrastructure evolves incrementally rather than through wholesale redesign.
- Coupled services that share live runtime state should be considered together when assigning ownership.

## Current Operational Responsibilities

The current ownership model separates:

- Trading workstation responsibilities.
- Client-platform responsibilities.
- Infrastructure compute responsibilities.
- Infrastructure management responsibilities.
- Storage and archive responsibilities.
- Operational monitoring and service visibility.

This separation is intended to reduce ambiguity, improve responsiveness, and keep service placement understandable.

The operator-facing workstation and trading-application model is described in `docs/operations/trading-workstation-and-operator-workflow.md`.

## Future Considerations

These are future considerations, not approved decisions:

- Possible future migration of TWS from the Mac mini to a dedicated host.
- Continued refinement of service ownership.
- Continued standardization across Linux infrastructure.
- Future archive and storage strategy using dedicated Linux systems.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `ENGINEERING_PROCESS.md`
- `docs/architecture/narratives/AN-001-physical-network-evolution.md`
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md`
- `docs/operations/data-retention-and-archive-policy.md`
- `docs/operations/closeouts/atlas-regime-monitor-migration-closeout.md`
- `docs/operations/trading-workstation-and-operator-workflow.md`
- `docs/roadmap/PARKING_LOT.md`
