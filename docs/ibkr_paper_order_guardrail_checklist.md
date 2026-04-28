# IBKR Paper Order Guardrail Checklist

## Classification

Use this checklist as the release gate before any future IBKR paper-order implementation is allowed.

If any unchecked item remains, the repo is not ready to place a paper order.

## Environment Lock

- [ ] Submit-capable path exists only for `mode=PAPER`
- [ ] Submit-capable path exists only for `host=127.0.0.1`
- [ ] Submit-capable path exists only for `port=7497`
- [ ] Live TWS port `7496` is explicitly rejected
- [ ] IB Gateway ports `4001` and `4002` are explicitly rejected in this phase
- [ ] Unknown ports are explicitly rejected
- [ ] Non-local hosts are explicitly rejected
- [ ] Duplicate client id is detected and rejected
- [ ] Account id is shown before any preview or approval
- [ ] Unexpected or live-looking account ids fail closed
- [ ] Connection timestamp and readiness are captured

## Manual-Only Workflow

- [ ] Default mode is preview-only
- [ ] No submit is possible from preview mode alone
- [ ] Submit requires explicit operator approval
- [ ] Approval phrase or flag includes account id
- [ ] Approval phrase or flag includes mode, host, and port
- [ ] Approval phrase or flag includes contract, action, quantity, order type, and price if applicable
- [ ] Approval is bound to a preview digest
- [ ] Approval is single-use
- [ ] Approval expires on TTL
- [ ] Approval is cleared on disconnect or state change

## Preview Requirements

- [ ] Preview displays account id
- [ ] Preview displays mode, host, port, and client id
- [ ] Preview displays contract details and expiry
- [ ] Preview displays action and quantity
- [ ] Preview displays order type
- [ ] Preview displays limit price if applicable
- [ ] Preview displays time in force
- [ ] Preview displays estimated notional
- [ ] Preview displays estimated tick value
- [ ] Preview displays delayed quote when available and labels it `DELAYED`
- [ ] Preview explicitly warns when live market data is unavailable
- [ ] Preview displays transmit behavior
- [ ] Preview displays current open-order baseline

## Allowed Scope

- [ ] Paper only
- [ ] Manual test order only
- [ ] One contract maximum
- [ ] Contract whitelist limited to `GC 202606` and `MGC 202606`
- [ ] Default test instrument is `MGC 202606`
- [ ] Market orders are rejected
- [ ] Stop orders are rejected
- [ ] Bracket and OCO orders are rejected
- [ ] Strategy-generated orders are rejected
- [ ] Batch orders are rejected
- [ ] Scheduler and automation paths are rejected

## Safety Controls

- [ ] Max quantity guard of `1` exists
- [ ] Unknown contract details fail closed
- [ ] Contract qualification must succeed before preview
- [ ] Account truth refresh is required before submit
- [ ] Open-order truth refresh is required before submit
- [ ] Market-data ambiguity blocks submit unless the operator explicitly acknowledges delayed-data-aware pricing
- [ ] Existing open orders require explicit operator acknowledgment
- [ ] Submit path re-checks account, connection, and preview digest immediately before transmit

## Cancel and Verification

- [ ] Manual cancel utility exists by exact broker order id only
- [ ] Cancel shows preview before execution
- [ ] Cancel requires separate confirmation
- [ ] Open-order verification occurs after submit
- [ ] Open-order verification occurs after cancel
- [ ] Stale open-order view blocks further action
- [ ] Cancel rejection is surfaced and classified

## Audit and Observability

- [ ] Audit log records preview events
- [ ] Audit log records approval events
- [ ] Audit log records submit attempts
- [ ] Audit log records submit results
- [ ] Audit log records cancel attempts
- [ ] Audit log records cancel results
- [ ] Audit log records error codes and messages
- [ ] Audit log records preview digest and session identity

## Separation From Strategy

- [ ] ATP/GC strategy code cannot import submit-capable modules
- [ ] No signal-to-order bridge exists
- [ ] No shadow-ledger execution linkage exists
- [ ] No automated entry/exit path exists
- [ ] No scheduler can invoke the harness
- [ ] Manual harness lives behind a separate command path
- [ ] Strategy-boundary test exists and fails on forbidden imports

## Required Tests

- [ ] `PAPER + 7497` passes
- [ ] `PAPER + 7496` fails
- [ ] `LIVE + 7497` fails
- [ ] `PAPER + 4002` fails in this phase
- [ ] Unknown port fails
- [ ] Duplicate client id fails safely
- [ ] Account mismatch fails safely
- [ ] Preview digest mismatch fails safely
- [ ] Approval TTL expiry fails safely
- [ ] Missing confirmation phrase fails safely
- [ ] Market order attempt fails safely
- [ ] Max quantity violation fails safely
- [ ] Unknown contract attempt fails safely
- [ ] Open-order refresh failure blocks submit
- [ ] Cancel verification failure blocks new actions
- [ ] Strategy import boundary test passes

## Final Go/No-Go Rule

- [ ] All items above are complete before any paper-order implementation is allowed

If this checklist is not fully complete, the repo is not allowed to place even one IBKR paper order.

