# Atlas Regime Monitor Migration Closeout

## Status

Complete.

## Purpose

Record the operational closeout for migrating the Regime Monitor and its coupled market-data services from Jupiter to Atlas.

This closeout preserves the architectural reasoning and validation outcome. It is not a runbook, configuration guide, or hardware inventory.

## Prior State

The Regime Monitor previously ran on Jupiter.

Jupiter had become sluggish under its combined infrastructure and market-data workload. That made it a poor long-term owner for services that depend on responsive access to live market-data state.

## Decision And Work Completed

Atlas was selected as the new host because it had dedicated capacity, faster storage, and was better suited to own the market-data and Regime Monitor workload.

The migration was performed as a controlled cutover, not as a fresh redesign.

The Databento listener, runtime artifact HTTP server, shared runtime state, and Regime Monitor were moved together because they were operationally coupled through the shared live OHLCV database.

Jupiter's copies of the migrated services were stopped and disabled, while retained temporarily as rollback material.

Atlas's services were enabled at boot and verified after reboot. Mission Control was updated to point to Atlas.

## Validation

Validation confirmed:

- Atlas services were enabled at boot.
- Atlas services came back after reboot.
- Network access was available.
- Firewall rules allowed the required access.
- Mission Control reached the migrated Regime Monitor.
- Jupiter became materially more responsive after the workload was removed.

## Post-Migration Emergency Fix

A post-migration Regime Monitor display-logic defect was identified and corrected under the Emergency Track.

That emergency fix affected presentation only. It did not change regime calculations, trading logic, runtime authority, broker authority, or research authority.

## Architecture Impact

The architecture impact was minor and already aligned with the existing infrastructure direction.

No new authority layer was introduced.

The migration reinforced dedicated service ownership by moving coupled market-data and Regime Monitor responsibilities onto a host better suited to own that workload.

## Authority Impact

None.

No broker, runtime, Managed Exit, Guardian, Safe-State, strategy, readiness, or research authority changed.

## Lessons Preserved

- Dedicated service ownership improved responsiveness.
- Coupled services sharing live runtime state should be migrated together.
- Infrastructure cutovers should include reboot persistence, local reachability, cross-host reachability, firewall validation, and dashboard validation.
- Post-migration presentation defects should be handled as operational fixes unless they alter architecture or authority.
- Emergency fixes should remain minimal and should not broaden into unrelated redesign work.

## Follow-Up

None required for the migration itself.

Existing broader parking-lot items remain separate, including any future review of TWS host placement, Mission Control health improvements, and other infrastructure evolution work.

## Related Documents

- `ENGINEERING_PROCESS.md`
- `PROJECT_PRINCIPLES.md`
- `SYSTEM_OVERVIEW.md`
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md`
- `docs/roadmap/PARKING_LOT.md`
