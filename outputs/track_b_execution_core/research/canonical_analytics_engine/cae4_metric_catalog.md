# CAE4 Metric Catalog

| Identifier | Display | Quality | Required Fields | Formula | Null Behavior | Flags |
| --- | --- | --- | --- | --- | --- | --- |
| average_duration | Average Duration | HIGH | hold_seconds | mean(hold_seconds) | Returns null when hold duration is unavailable. |  |
| average_hold_seconds | Average Hold Seconds | HIGH | hold_seconds | mean(hold_seconds) | Returns null when hold duration is unavailable. |  |
| average_loser_pnl_proxy | Average Loser P&L Proxy | PROXY | realized_pnl_proxy | mean(pnl_proxy < 0) | Returns null with flag when no losses exist. | average_loser_no_losses |
| average_pnl_proxy | Average P&L Proxy | PROXY | realized_pnl_proxy | mean(realized_pnl_proxy) | Returns null when P&L proxy is unavailable. |  |
| average_realized_points | Average Realized Points | PROXY | realized_points | mean(realized_points) | Returns null when realized points are unavailable. |  |
| average_winner_pnl_proxy | Average Winner P&L Proxy | PROXY | realized_pnl_proxy | mean(pnl_proxy > 0) | Returns null with flag when no winners exist. | average_winner_no_winners |
| best_trade | Best Trade | PROXY | realized_pnl_proxy | max_by(realized_pnl_proxy) | Returns null when P&L proxy is unavailable. |  |
| breakeven_rate | Breakeven Rate | PROXY | realized_pnl_proxy | count(pnl_proxy == 0) / count(pnl_proxy) | Returns null when P&L proxy is unavailable. |  |
| consecutive_loss_streak_max | Max Consecutive Loss Streak | PROXY | realized_pnl_proxy, exit_time, entry_time | max_streak(pnl_proxy < 0 order by exit_time, entry_time) | Missing P&L proxy breaks a streak. |  |
| consecutive_win_streak_max | Max Consecutive Win Streak | PROXY | realized_pnl_proxy, exit_time, entry_time | max_streak(pnl_proxy > 0 order by exit_time, entry_time) | Missing P&L proxy breaks a streak. |  |
| data_quality_flags | Data Quality Flags | HIGH | data_quality_flags | count_by_flag(data_quality_flags) | Returns an empty object when no flags exist. |  |
| downside_tail_mean_proxy | Downside Tail Mean Proxy | PROXY | realized_pnl_proxy | mean(lowest_decile(realized_pnl_proxy)) | Returns null with flag when P&L proxy is unavailable. | downside_tail_mean_missing_pnl_proxy |
| enrichment_data_quality_flags | Enrichment Data Quality Flags | HIGH | enrichment_data_quality_flags | count_by_flag(enrichment_data_quality_flags) | Returns an empty object when no flags exist. |  |
| expectancy_proxy | Expectancy Proxy | PROXY | realized_pnl_proxy | mean(realized_pnl_proxy) | Returns null when P&L proxy is unavailable. |  |
| loss_rate | Loss Rate | PROXY | realized_pnl_proxy | count(pnl_proxy < 0) / count(pnl_proxy) | Returns null when P&L proxy is unavailable. |  |
| max_loss_pnl_proxy | Max Loss P&L Proxy | PROXY | realized_pnl_proxy | min(pnl_proxy < 0) | Returns null with flag when no losses exist. | loser_extrema_no_losses |
| max_win_pnl_proxy | Max Win P&L Proxy | PROXY | realized_pnl_proxy | max(pnl_proxy > 0) | Returns null with flag when no winners exist. | winner_extrema_no_winners |
| median_hold_seconds | Median Hold Seconds | HIGH | hold_seconds | median(hold_seconds) | Returns null when hold duration is unavailable. |  |
| median_loser_pnl_proxy | Median Loser P&L Proxy | PROXY | realized_pnl_proxy | median(pnl_proxy < 0) | Returns null with flag when no losses exist. | median_loser_no_losses |
| median_pnl_proxy | Median P&L Proxy | PROXY | realized_pnl_proxy | median(realized_pnl_proxy) | Returns null when P&L proxy is unavailable. |  |
| median_realized_points | Median Realized Points | PROXY | realized_points | median(realized_points) | Returns null when realized points are unavailable. |  |
| median_winner_pnl_proxy | Median Winner P&L Proxy | PROXY | realized_pnl_proxy | median(pnl_proxy > 0) | Returns null with flag when no winners exist. | median_winner_no_winners |
| p10_pnl_proxy | P10 P&L Proxy | PROXY | realized_pnl_proxy | percentile(realized_pnl_proxy, 10) | Returns null when P&L proxy is unavailable. | missing_p10_pnl_proxy_pnl_proxy |
| p25_pnl_proxy | P25 P&L Proxy | PROXY | realized_pnl_proxy | percentile(realized_pnl_proxy, 25) | Returns null when P&L proxy is unavailable. | missing_p25_pnl_proxy_pnl_proxy |
| p75_pnl_proxy | P75 P&L Proxy | PROXY | realized_pnl_proxy | percentile(realized_pnl_proxy, 75) | Returns null when P&L proxy is unavailable. | missing_p75_pnl_proxy_pnl_proxy |
| p90_pnl_proxy | P90 P&L Proxy | PROXY | realized_pnl_proxy | percentile(realized_pnl_proxy, 90) | Returns null when P&L proxy is unavailable. | missing_p90_pnl_proxy_pnl_proxy |
| payoff_ratio_proxy | Payoff Ratio Proxy | PROXY | realized_pnl_proxy | avg_winner / abs(avg_loser) | Returns null with flag when winners or losses are insufficient. | payoff_ratio_insufficient_winners_or_losses |
| pnl_percentiles | P&L Percentiles | PROXY | realized_pnl_proxy | percentiles(realized_pnl_proxy) | Percentile values are null when P&L proxy is unavailable. |  |
| profit_factor_proxy | Profit Factor Proxy | PROXY | realized_pnl_proxy | sum(winners) / abs(sum(losers)) | Returns null with flags when no winners, no losses, or no P&L proxy. | missing_pnl_proxy_for_profit_factor, profit_factor_no_winners, profit_factor_no_losses |
| sample_class | Sample Class | HIGH |  | classify(count) | Always returns a class for grouped rows. |  |
| sample_confidence_label | Sample Confidence Label | HIGH |  | label(sample_class) | Always returns a label for grouped rows. |  |
| trade_count | Trade Count | HIGH |  | count(rows) | Returns 0 for empty groups. |  |
| upside_tail_mean_proxy | Upside Tail Mean Proxy | PROXY | realized_pnl_proxy | mean(highest_decile(realized_pnl_proxy)) | Returns null with flag when P&L proxy is unavailable. | upside_tail_mean_missing_pnl_proxy |
| win_rate | Win Rate | PROXY | realized_pnl_proxy | count(pnl_proxy > 0) / count(pnl_proxy) | Returns null when P&L proxy is unavailable. |  |
| worst_trade | Worst Trade | PROXY | realized_pnl_proxy | min_by(realized_pnl_proxy) | Returns null when P&L proxy is unavailable. |  |

Diagnostic/research only. No production recommendations, runtime changes, broker actions, strategy changes, or gates.
