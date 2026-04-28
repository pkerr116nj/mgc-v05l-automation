# ATP Companion Replay Baseline v2 GC Expression Feasibility

- Classification: `GC_EXPRESSION_PROMISING`
- This is research only. It does not switch execution from MGC to GC.
- Replay Baseline v2 remains the controlling reproducible ATP baseline for this pass.
- Old v1 is materialized-only and not controlling truth here.
- Legacy-intent v2 is diagnostic only and not used as the baseline.
- No staged adds, threshold sensitivity, exit redesign, drawdown governance, live execution, IBKR, broker, or US Open research changes were run.
- London remains diagnostic-only.
- GC was built as a replay-safe expression from source, not as a naive `10x` MGC scaling proxy.
- GC materially improves cost robustness versus MGC under both the framework-current and repo-grounded cost lenses.
- GC has much larger absolute drawdown because one GC contract is roughly `10x` one MGC contract in notional expression.
- One GC is not live-ready and is not recommended for immediate execution or paper deployment from this pass alone.
- Under the harsher framework-current cost lens, `2024` remains negative for GC.
- U.S. GC is positive, but materially thinner than Asia under the framework-current cost lens.

## GC Replay Build
- GC trade count: `4512`
- GC self-reproduction classification: `REPLAY_BASELINE_V2_BUILT`
- Shared date span: `2024-01-01T18:01:00-05:00` -> `2026-04-10T17:00:00-04:00`

## Cost Read
- MGC current net P/L: `6109.815` with cost consumed `84.0817%`
- GC framework-current net P/L: `100193.4167` with cost consumed `69.8722%`
- GC repo-current proxy net P/L: `222017.4167` with cost consumed `33.2402%`
- GC conservative live proxy net P/L: `131777.4167`
- Naive 10x MGC reference net P/L: `61098.15`

## Risk Framing
- One GC contract is roughly 10x the notional expression of one MGC contract.
- MGC current max drawdown: `5856.1858`
- GC repo-current proxy max drawdown: `24186.1667`
- MGC drawdown-to-profit ratio: `0.9585`
- GC repo-current proxy drawdown-to-profit ratio: `0.1089`

## Replay Validation
- `trade_count` expected=`4512` observed=`4512` pass=`True`
- `net_pnl_cash` expected=`100193.4167` observed=`100193.4167` pass=`True`
- `average_trade_pnl_cash` expected=`22.206` observed=`22.206` pass=`True`
- `median_trade_pnl_cash` expected=`38.5` observed=`38.5` pass=`True`
- `win_rate` expected=`53.7456` observed=`53.7456` pass=`True`
- `profit_factor` expected=`1.132` observed=`1.132` pass=`True`
- `max_drawdown` expected=`26404.2583` observed=`26404.2583` pass=`True`
- `largest_win` expected=`3892.5667` observed=`3892.5667` pass=`True`
- `largest_loss` expected=`-5832.4583` observed=`-5832.4583` pass=`True`
- `max_consecutive_losers` expected=`9` observed=`9` pass=`True`
- `asia_net_pnl_cash` expected=`87122.55` observed=`87122.55` pass=`True`
- `us_net_pnl_cash` expected=`13070.8667` observed=`13070.8667` pass=`True`

## Interpretation Discipline
- Do not treat GC as superior unless its net edge improves without unacceptable drawdown expansion.
- Do not treat the naive 10x proxy as a real GC result.
- This pass only asks whether GC expression is more cost-efficient than MGC inside the same replay-safe ATP framework.
- This pass does not promote GC to live or paper execution.
