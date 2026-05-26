# Track B Exit Attribution and Policy v2

Status: research/shadow only. No broker, order, lifecycle, live-money, or paper_proof authority is granted.

## A. All Closed Trades / Evidence Completeness
- Closed broker-effect trades reviewed: 65
- Attribution classification counts: `{"INCOMPLETE_FILL_EVIDENCE": 15, "INCONCLUSIVE": 50}`
- Shadow recommendation counts: `{"EXCLUDED_BUG_FIX_EXIT": 63, "EXCLUDED_UNKNOWN_EXIT_INTENT": 2}`

## B. Exit Intent Classification
- Intent counts: `{"BUG_FIX_EXIT": 63, "UNKNOWN_EXIT_INTENT": 2}`
- Contamination flags: `{"aggregate_close_repair": 8, "broker_flat_reconciliation_cleanup": 37, "duplicate_exit_resolution": 1, "leak_test_trade": 44, "manual_intervention_required": 65, "missing_fill_price": 15, "remediation_trade": 51}`

## C. Bug-Fix / Remediation Exit Inventory
- BUG_FIX_EXIT count: `63`
- These trades are excluded from alpha-exit quality metrics by default.

## D. True Strategy Exit-Quality Analysis
- ALPHA_EXIT eligible count: `0`
- ALPHA_EXIT classification counts: `{}`

## E. Risk Exits
- RISK_EXIT count: `0`
- RISK_EXIT classification counts: `{}`

## F. Recommendations
- No clean alpha-exit underperformance recommendation yet; collect more eligible strategy exits.

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
Collect more clean ALPHA_EXIT samples with timestamp-coherent candles before changing live exits.

## Safety
- `submit_allowed=false`
- `broker_mutation_allowed=false`
- `lifecycle_authority=false`
- Existing managed-exit roster behavior is unchanged.
