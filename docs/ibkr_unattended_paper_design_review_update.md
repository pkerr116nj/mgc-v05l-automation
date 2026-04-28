# IBKR Unattended Paper Design Review Update

- classification: `TWS_NO_DIALOG_UNVERIFIED`
- scope: design review update only
- unattended submit implemented: no
- strategy linkage added: no
- scheduler linkage added: no
- live trading enabled: no

## What Changed

The repo now has a read-only TWS no-dialog preflight command for unattended paper readiness. It verifies:

- `PAPER / 127.0.0.1 / 7497`
- expected account `DUM882026`
- current open-order baseline
- no working `MGC` order
- exact `MGC 20260626` qualification

This narrows the remaining uncertainty to TWS GUI settings and live no-dialog behavior, not to broker connectivity or contract construction.

## Current Review Result

The unattended paper design should still be treated as `IBKR_UNATTENDED_PAPER_DESIGN_NEEDS_REVIEW`.

Reason:

- read-only preflight can prove paper connectivity and contract readiness
- read-only preflight cannot prove that TWS will not stop on an API precaution dialog
- the exact TWS paper profile still needs manual setting inspection
- final proof still requires one separate unattended paper rest/cancel test, not a strategy run

## Required TWS Assumptions

Before unattended paper submit is allowed, the design assumes all of these are true:

- `Enable ActiveX and Socket Clients` is enabled
- socket port is `7497`
- `Read-Only API` is disabled for the unattended submit profile
- localhost is allowed
- `Bypass Order Precautions for API orders` is enabled

If any of those assumptions are false or unclear, unattended paper submit must remain blocked.

## Next Safe Step

The next safe step is:

1. Manually inspect the TWS paper API settings with the operator checklist.
2. If the settings appear correct, run exactly one unattended paper rest/cancel test.
3. Only after that should unattended paper fill testing be considered.
