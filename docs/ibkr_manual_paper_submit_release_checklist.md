# IBKR Manual Paper Submit Release Checklist

## Classification

Use this checklist before allowing the first IBKR manual paper submit/cancel implementation.

If any item is incomplete, the repo is not ready to submit one paper order.

## Environment Lock

- [ ] Submit-capable path exists only for `mode=PAPER`
- [ ] Submit-capable path exists only for `host=127.0.0.1`
- [ ] Submit-capable path exists only for `port=7497`
- [ ] Live port `7496` fails closed
- [ ] IB Gateway ports `4001` and `4002` fail closed
- [ ] Unknown ports fail closed
- [ ] Non-local hosts fail closed
- [ ] Duplicate client id fails closed
- [ ] Account id is displayed before preview and before submit
- [ ] Expected-account matching is enforced

## Preview Gate

- [ ] Preview must be generated first
- [ ] Preview includes exact digest
- [ ] Preview includes delayed quote warning when applicable
- [ ] Preview includes delayed quote snapshot, chosen limit price, and distance from quote
- [ ] Preview includes open-order baseline
- [ ] Preview states no order was submitted, staged, or transmitted
- [ ] Preview invalidates if material fields change

## Order Scope

- [ ] Manual CLI only
- [ ] No strategy linkage
- [ ] No ATP/GC linkage
- [ ] One order maximum
- [ ] `MGC 202606` is the preferred first instrument
- [ ] `GC 202606` is blocked until explicitly approved later
- [ ] `qty <= 1`
- [ ] `LMT` only
- [ ] `DAY` only
- [ ] Market orders fail closed
- [ ] Non-whitelisted contracts fail closed
- [ ] Missing limit price fails closed
- [ ] Delayed quote unavailable fails closed
- [ ] Delayed quote stale fails closed
- [ ] Far-away placeholder limit fails closed
- [ ] Bracket/OCO is blocked
- [ ] Automation and scheduler paths are blocked

## Manual Approval

- [ ] Typed confirmation phrase is required
- [ ] Exact preview digest is required
- [ ] Confirmation phrase includes account id
- [ ] Confirmation phrase includes mode / host / port
- [ ] Confirmation phrase includes contract / action / qty / order type / price / tif
- [ ] Confirmation phrase includes delayed quote acknowledgment
- [ ] Approval is single-use
- [ ] Approval expires on TTL
- [ ] Approval invalidates if preview changes
- [ ] Approval invalidates if open-order baseline changes

## Submit Verification

- [ ] Account, positions, and open orders are read before submit
- [ ] Open-order baseline freshness is required
- [ ] Contract qualification freshness is required
- [ ] Submit re-checks digest and all material preview fields
- [ ] Submit re-checks delayed quote freshness or requires a fresh preview
- [ ] Open orders are re-read immediately after submit
- [ ] Order id is captured from broker truth
- [ ] Order details are verified against the preview
- [ ] Broker rejection fails closed

## Cancel Verification

- [ ] Cancel is by exact broker order id only
- [ ] Separate cancel confirmation is required
- [ ] Cancel request is audited
- [ ] Open orders are re-read after cancel
- [ ] Final state must be proven by broker truth
- [ ] Unproven cancel verification fails closed

## Audit Logging

- [ ] Preview event is logged
- [ ] Approval event is logged
- [ ] Submit event is logged
- [ ] Open-order verification event is logged
- [ ] Cancel event is logged
- [ ] Cancel verification event is logged
- [ ] Digest is logged
- [ ] Quote snapshot, chosen limit, and distance from quote are logged
- [ ] Error codes and messages are logged
- [ ] Broker order id is logged when present
- [ ] Audit log is append-only

## Strategy Separation

- [ ] Submit-capable code lives in a separate module tree
- [ ] Strategy packages cannot import submit-capable modules
- [ ] No signal-to-order bridge exists
- [ ] No shadow-ledger execution linkage exists
- [ ] No scheduler can invoke submit
- [ ] Manual CLI remains the only entrypoint
- [ ] Strategy-boundary tests exist

## Required Tests

- [ ] Fail closed on account mismatch
- [ ] Fail closed on live port
- [ ] Fail closed on unknown port
- [ ] Fail closed on `qty > 1`
- [ ] Fail closed on non-whitelisted contract
- [ ] Fail closed on market order
- [ ] Fail closed on missing limit price
- [ ] Fail closed on digest mismatch
- [ ] Fail closed on stale open-order baseline
- [ ] Fail closed when cancel verification cannot prove final state

## Final Go/No-Go Rule

- [ ] Every item above is complete before one paper submit/cancel test is allowed

If this checklist is not fully complete, the correct outcome is no submit implementation.
