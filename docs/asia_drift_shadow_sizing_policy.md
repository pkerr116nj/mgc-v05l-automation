# Asia Drift Shadow Sizing Policy

## 1. Corrected Trade Unit

The shadow-trading framework uses the corrected trade unit:

- One active trade per instrument/session
- No overlapping trades
- First qualifying `TRADE_FAVORABLE + LATE_ASIA` signal only per instrument/session

This policy is intentionally conservative and is meant to prevent candidate-row overcounting from inflating simulated trade frequency.

## 2. Recommended Starting Mode

The recommended starting shadow mode is:

- `combined_conservative_half_nq`

This mode keeps the full index basket in view while reducing the larger dispersion contribution from `NQ/MNQ`.

## 3. Starting Risk

Recommended starting shadow risk:

- `0.50%` per trade

This is more conservative than the current Monte Carlo upper recommendation and is intended as the starting validation level for shadow operation rather than the maximum tolerated level.

## 4. Graduation Rules

Suggested shadow-risk graduation path:

- `0.75%` after `30` clean shadow trades
- `1.00%` after `75-100` clean shadow trades
- No increase after process violations or drawdown breach

“Clean” means:

- no process violations
- no data/regime-classification failures
- no logging gaps
- no unexplained signal/execution-state mismatches

## 5. Risk Caps

Risk governance:

- Pause and review at `10%` drawdown
- Hard pause at `15%` drawdown
- No live sizing above `1%` without a fresh Monte Carlo review

These are research and validation guardrails, not live deployment approvals.

## 6. Instrument Weighting

Recommended shadow weighting:

- `ES/MES`: full risk unit
- `NQ/MNQ`: half risk unit

This matches the conservative combined basket used in the current sizing study.

## 7. Caveats

- Sizing research only
- No live execution yet
- No parameter optimization
- Futures leverage risk remains material

This document does not authorize automated trading, live broker integration, or execution rollout.
