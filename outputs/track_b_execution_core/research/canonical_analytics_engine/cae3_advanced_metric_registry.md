# CAE3 Advanced Metric Registry

Generated: 2026-07-03

CAE3 adds reusable P&L-proxy-only advanced metrics to the Canonical Analytics Engine.

## Metrics

| Metric | Definition |
|---|---|
| `profit_factor_proxy` | sum positive P&L proxy / absolute sum negative P&L proxy |
| `average_winner_pnl_proxy` | average positive P&L proxy |
| `average_loser_pnl_proxy` | average negative P&L proxy |
| `payoff_ratio_proxy` | average winner / absolute average loser |
| `max_win_pnl_proxy` | max positive P&L proxy |
| `max_loss_pnl_proxy` | min negative P&L proxy |
| `median_winner_pnl_proxy` | median positive P&L proxy |
| `median_loser_pnl_proxy` | median negative P&L proxy |
| `p10_pnl_proxy` | 10th percentile P&L proxy |
| `p25_pnl_proxy` | 25th percentile P&L proxy |
| `p75_pnl_proxy` | 75th percentile P&L proxy |
| `p90_pnl_proxy` | 90th percentile P&L proxy |
| `downside_tail_mean_proxy` | mean of lowest decile, minimum one observation |
| `upside_tail_mean_proxy` | mean of highest decile, minimum one observation |
| `consecutive_win_streak_max` | max ordered streak of positive P&L proxy |
| `consecutive_loss_streak_max` | max ordered streak of negative P&L proxy |
| `loss_rate` | share of rows with negative P&L proxy |
| `breakeven_rate` | share of rows with zero P&L proxy |
| `sample_confidence_label` | sample-class label that does not imply statistical significance |

## Data Quality

Metrics do not fabricate R proxy, MFE, MAE, or missing P&L proxy.

When profit factor or payoff ratio lacks winners or losses, CAE returns `null` and adds `metric_data_quality_flags` instead of returning infinity.

## Ordering

Streak metrics sort rows by `exit_time` when available, otherwise `entry_time`, then `trade_outcome_id`.

## Guardrails

These metrics are diagnostic/research only. They do not recommend production changes, trading gates, strategy changes, broker actions, or runtime changes.
