# Track B Exit Attribution and Policy v2

Status: research/shadow only. No broker, order, lifecycle, live-money, or paper_proof authority is granted.

## Current Findings
- Closed broker-effect trades reviewed: 25
- Attribution classification counts: `{"ENTRY_GOOD_ORDER_MANAGEMENT_BAD": 20, "INCONCLUSIVE": 5}`
- Shadow recommendation counts: `{"COLLECT_MORE_FORWARD_EVIDENCE": 25}`

## Top Root Causes
- ENTRY_GOOD_ORDER_MANAGEMENT_BAD: Missing or incomplete close fill/price evidence prevents reliable P&L and exit-quality attribution. (20)
- INCONCLUSIVE: Timestamp-coherent candle or fill evidence is incomplete. (5)

## Shadow Policies
- CURRENT_ACTUAL_MANAGED_EXIT
- FIXED_3X5M_TIMEBOX
- PARTICIPATION_AWARE_HOLD_EXTENSION
- PROFIT_HARVEST_EXIT
- THESIS_FAILURE_PARTICIPATION_DECAY_EXIT

## Promotion Gates
- minimum_sample_size: At least 30 timestamp-coherent broker-effect closed trades per strategy/exit-family.
- lower_giveback: Shadow policy reduces median giveback from MFE versus current exit.
- improved_mfe_capture: Shadow policy improves realized/MFE capture without relying on hindsight-only prices.
- no_worse_mae: MAE distribution is no worse than current exit for the same entry set.
- no_lifecycle_failures: No added duplicate close, unmanaged order, modify, or lifecycle persistence failures.
- explainable_rules_only: Rules use participation, MFE/giveback, thesis failure, and session/regime evidence; no black-box promotion.

## Recommended Next Slice
Collect fill-price-complete closed trades and compare fixed time-box against profit-harvest on timestamp-coherent windows.

## Safety
- `submit_allowed=false`
- `broker_mutation_allowed=false`
- `lifecycle_authority=false`
- Existing managed-exit roster behavior is unchanged.
