# Midday Route Proof Instrumentation Contract

Output root:

- `/Users/patrick/Documents/MGC-v05l-automation/outputs/reports/strategy_activity_instrumentation`

Files:

- `midday_post_fix_route_trace.jsonl`
- `midday_post_fix_bridge_preflight_trace.csv`
- `midday_post_fix_broker_truth_trace.jsonl`

Tracked lane scope:

- submit-capable lanes whose `lane_id` contains `__us_midday_`

## Route trace JSONL

Each row in `midday_post_fix_route_trace.jsonl` contains:

- `observed_at`
- `timestamp`
- `lane_id`
- `action`
- `source_instrument`
- `executable_proxy`
- `caller_metadata`
- `caller_path`
- `route_destination`
- `ibkr_bridge_invoked`
- `classification`
- `bridge_classification`
- `bridge_detail`
- `bridge_preflight_result`
- `gate_blocker`
- `order_intent_id`
- `intent_type`
- `broker_order_id`
- `bridge_order_status`
- `error_message`

Expected classifications:

- `MIDDAY_ROUTE_PROOF_WAITING_FOR_SIGNAL`
- `MIDDAY_ROUTE_PROOF_BRIDGE_INVOKED`
- `MIDDAY_ROUTE_PROOF_ORDER_FILLED`
- `MIDDAY_ROUTE_PROOF_BLOCKED_BY_REAL_GATE`
- `MIDDAY_ROUTE_PROOF_FAILED`

## Bridge preflight CSV

Each row in `midday_post_fix_bridge_preflight_trace.csv` contains:

- `timestamp`
- `lane_id`
- `order_intent_id`
- `route_destination`
- `classification`
- `gate_index`
- `gate_name`
- `passed`
- `status`
- `detail`

## Broker truth JSONL

Each row in `midday_post_fix_broker_truth_trace.jsonl` contains:

- `observed_at`
- `timestamp`
- `lane_id`
- `classification`
- `source_instrument`
- `executable_proxy`
- `route_destination`
- `broker_order_id`
- `perm_id`
- `order_status`
- `exec_details`
- `completed_order`
- `open_order_snapshots`
- `position_reconciliation`
- `delegated_result`
- `error_message`
