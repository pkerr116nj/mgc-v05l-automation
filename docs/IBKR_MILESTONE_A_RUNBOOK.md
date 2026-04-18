# IBKR Milestone A Runbook

This runbook is for the first funded-account IBKR proof after API access becomes available.

## Goal

Prove broker truth without trading:

- account truth
- balances
- positions
- open orders
- completed orders / executions for the selected account and current visibility scope
- health / freshness
- reconciliation ingestion readiness

## Preconditions

- IBKR account is funded and API-enabled
- account is on the intended entitlement tier for API access
- TWS or IB Gateway is installed and reachable locally
- the repo has the current IBKR scaffolding:
  - [ibkr_session.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/brokers/ibkr/ibkr_session.py)
  - [ibkr_client.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/brokers/ibkr/ibkr_client.py)
  - [ibkr_truth_adapter.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/brokers/ibkr/ibkr_truth_adapter.py)
  - [ibkr_execution_provider.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/execution/ibkr_execution_provider.py)

## TWS / Gateway Assumptions

- paper mode uses `127.0.0.1:7497` unless explicitly changed
- live mode uses the configured live port only after Milestone C
- client id policy follows [ibkr_order_identity.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/brokers/ibkr/ibkr_order_identity.py)
- the first proof should use the selected account visibility scope, not broad multi-client assumptions

## What To Verify

1. Connection health is live.
2. Selected account id is present.
3. Managed account visibility is what we expect.
4. Balance rows are populated for the selected account scope.
5. Position rows normalize cleanly even if empty.
6. Open orders normalize cleanly even if empty.
7. Completed orders / executions normalize cleanly for the current visibility scope.
8. Freshness fields are populated.
9. Legacy reconciliation compatibility keys are present:
   - `connected`
   - `truth_complete`
   - `position_quantity`
   - `open_order_ids`
   - `order_status`
   - `last_fill_timestamp`

## Acceptance Evaluator

Use [ibkr_milestone_a_acceptance.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/app/ibkr_milestone_a_acceptance.py) against the live IBKR snapshot once API access opens.

The proof is successful only when the evaluator reports all checks green.

## Failure Triage

If the proof does not pass:

- If `health_connected` fails:
  - verify TWS / Gateway is running
  - verify host/port/client id config
- If `account_truth_present` fails:
  - verify API account visibility in the current session
  - confirm managed accounts are exposed to the selected client id
- If `balances_present_for_scope` fails:
  - verify selected account scope and current visibility rules
- If `reconciliation_ingestion_ready` fails:
  - inspect [ibkr_execution_provider.py](/Users/patrick/Documents/MGC-v05l-automation/src/mgc_v05l/execution/ibkr_execution_provider.py)
  - confirm the compatibility bridge is still emitting the runtime fields above

## Exit Criteria

Milestone A is complete when:

- the selected account is visible
- balances are visible
- positions, open orders, completed orders, and executions normalize cleanly
- freshness is populated
- reconciliation ingestion readiness is green
- no order is placed
