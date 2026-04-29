# IBKR TWS No-Dialog Operator Checklist

- purpose: operator checklist before any unattended paper submit attempt
- scope: `PAPER / 127.0.0.1 / 7497 / DUM882026` only
- unattended submit status: not yet allowed

## TWS API Settings To Inspect

Inspect these manually in TWS paper:

- `Global Configuration > API > Settings > Enable ActiveX and Socket Clients`
  - must be enabled
- `Global Configuration > API > Settings > Socket Port`
  - must be `7497`
- `Global Configuration > API > Settings > Read-Only API`
  - must be disabled for any future unattended submit path
- `Global Configuration > API > Settings > Trusted IPs / localhost policy`
  - localhost must be allowed
- `Global Configuration > API > Precautions > Bypass Order Precautions for API orders`
  - must be enabled for unattended no-dialog paper orders

## Notes To Capture

Capture these before unattended paper submit is allowed:

- whether `Bypass Order Precautions for API orders` is visibly enabled
- whether any other API precaution or warning-bypass settings are visible in the current TWS build
- whether the paper TWS profile is separate from any live profile
- whether `Read-Only API` is clearly disabled in the paper profile

If any setting label is ambiguous, capture a screenshot or exact wording so the design review can be updated without guessing.

## What Must Be True Before Unattended Paper Submit

All of these must be true:

- read-only preflight connects successfully to `127.0.0.1:7497`
- expected account resolves to `DUM882026`
- no working `MGC` open order exists
- exact `MGC 20260626 / conId=712565978 / localSymbol=MGCM6` qualification succeeds
- TWS paper settings above have been manually verified
- manual paper harness remains separate and unchanged
- unattended submit path is still unavailable to strategy callers, ATP/GC, and any scheduler

## Remaining Proof Step

Even if every setting above looks correct, unattended paper is still not proven until one separate unattended paper rest/cancel test shows:

- no manual TWS dialog appears
- broker `openOrder` / `orderStatus` truth flows without human intervention
- for a deliberately non-marketable resting order, the working order is visibly present in TWS Orders / Activity before cancel
- operator confirmation is recorded as `visible_in_tws=true/false`
- no second order is needed
- cleanup and cancel verification work cleanly
- operator can confirm the order disappeared or showed canceled in TWS after cancel
