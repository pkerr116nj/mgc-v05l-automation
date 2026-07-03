# CAE3 Metric Definition Contract

Generated: 2026-07-03

## Scope

CAE3 advanced metrics use only `realized_pnl_proxy`.

They do not derive or fabricate:

- R multiple
- MFE
- MAE
- true broker P&L
- risk-adjusted return

## Null Rules

Return `null` when a metric lacks required data.

Examples:

- Profit factor with no losses: `null`
- Profit factor with no winners: `null`
- Payoff ratio without winners or losses: `null`
- Percentile metrics without P&L proxy: `null`

Each null condition that matters for interpretation writes a `metric_data_quality_flags` entry.

## Tail Means

Tail means use the lowest or highest decile of available P&L proxy rows. For small samples, CAE uses the nearest bounded subset: at least one observation.

## Streaks

Streaks are ordered by `exit_time` where available, then `entry_time`, then `trade_outcome_id`.

Missing P&L proxy breaks a streak.

## Sample Confidence

`sample_confidence_label` reuses existing sample classes and explicitly avoids implying statistical significance.
