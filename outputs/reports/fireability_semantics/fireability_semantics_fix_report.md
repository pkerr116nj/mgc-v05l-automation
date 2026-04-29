# Fireability Semantics Fix Report

- classification: `FIREABILITY_SEMANTICS_FIXED`
- current fine-grained phase label: `UNCLASSIFIED`
- current broad trading session: `US_EARLY`
- next expected decision bar: `2026-04-29T10:44:00-04:00`

## Current Counts

- Runtime Lanes Loaded: `44`
- Data Fresh Lanes: `44`
- Governance Allowed Lanes: `44`
- Route Ready Lanes: `44`
- Session Eligible Lanes: `13`
- Waiting For Completed Bar: `13`
- Setup Evaluated: `25`
- No Setup: `11`
- Actionable Now: `0`
- Blocked Lanes: `0`
- Ready This Bar: `0`

## Interpretation

- `Session Eligible` now means a lane is admitted, healthy enough, route-ready, and inside its allowed trading session.
- `Waiting For Completed Bar` is the expected between-bars state for the current 3-minute strategies.
- `No Setup` means the lane was evaluated on a completed decision bar and did not produce a setup.
- `Actionable Now` remains the strict count for lanes that actually have BUY/SELL/EXIT intent right now.
- `Blocked Lanes` is reserved for real system problems, not normal between-bar idling.