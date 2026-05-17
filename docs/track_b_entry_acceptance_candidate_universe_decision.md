# Track B Entry Acceptance Candidate Universe Decision

Date: 2026-05-17

Scope: `asiaEarlyNormalBreakoutRetestHoldLong`, GC/MGC, research/offline only.

This note preserves the candidate-universe decision from the Entry Acceptance
research fork. It is documentation/provenance only and does not change runtime
strategy behavior, PAPER/live routing, broker state, lifecycle state, or order
intent behavior.

## Evidence

Full-history Entry Acceptance distribution/backtest:

- Source report: `outputs/reports/entry_acceptance_research/full_history_batch/combined_cross_instrument_summary.md`
- Coverage: local GC/MGC canonical minute coverage from `2020-01-01` through `2026-04-22`
- Rows: `848672`
- Exact-rule flags: `899`
- Exact baseline episodes after 12-bar de-dupe: `856`
- NEAR >=0.80 episodes: `23943`
- NEAR/baseline episode ratio: `27.970794`

0.5 point round-trip cost, next-bar-open headline:

| Bucket | Horizon | Trades | Avg Return | Win Rate | PF Proxy |
| --- | ---: | ---: | ---: | ---: | ---: |
| Exact baseline | 12b | 856 | 0.086215 | 0.432243 | 1.052711 |
| Exact baseline | 24b | 856 | 0.137617 | 0.471963 | 1.057154 |
| NEAR >=0.80 | 12b | 23925 | -0.423720 | 0.416134 | 0.800482 |
| NEAR >=0.80 | 24b | 23910 | -0.349260 | 0.448599 | 0.879630 |

NEAR repair exit study:

- Source report: `outputs/reports/entry_acceptance_research/full_history_batch/near_repair_exit_study/near_gte_0_80_smart_exit_repair_study_v1.md`
- Baseline exact episodes: `856`
- NEAR >=0.80 episodes: `23942`
- NEAR >=0.80 context-filtered episodes: `20982`
- Best tested NEAR >=0.80 exit remained `fixed_24_bar_exit`, with avg `-0.349119`, PF `0.879649`, and max DD `9292.4`.
- Best tested context-filtered NEAR exit remained `fixed_24_bar_exit`, with avg `-0.345610`, PF `0.882959`, and max DD `8301.4`.
- Interpretation: tested smart exits and context proxies did not repair NEAR >=0.80 into a robust positive standalone candidate after cost.

Exact baseline fixed-exit study:

- Source report: `outputs/reports/entry_acceptance_research/full_history_batch/exact_baseline_exit_study/exact_baseline_exit_improvement_study_v1.md`
- Exact baseline episodes: `856`
- Fixed 36-bar exit: avg `0.306308`, median `-0.4`, win rate `0.467290`, PF `1.107389`, max DD `254.5`
- Interpretation: fixed 36b was the best full-sample fixed-horizon average return among tested exact-baseline exits.

Exact baseline adaptive 24-to-36 study:

- Source report: `outputs/reports/entry_acceptance_research/full_history_batch/exact_baseline_exit_study/exact_baseline_24_vs_36_extension_rule_study_v1.md`
- Selected risk-managed candidate: `extend_if_progress_gt_0_0_ratio_1_1_giveback_lte_0_4`
- Avg `0.273832`, median `-0.4`, win rate `0.455607`, PF `1.113051`, max DD `228.2`
- Branches: `exit_24=646`, `extend_36=210`
- Interpretation: adaptive 24/36 did not beat fixed 36b on raw average return, but improved PF and max DD profile versus fixed 36b.

## Decision

`EXACT_BASELINE_RETAINED`

The exact-rule baseline remains the tradeable research anchor. It has a much
smaller candidate universe, positive full-history costed diagnostics, and is the
only entry universe promoted into exit-profile research candidates.

`NEAR_EXPANSION_PARKED`

The broad NEAR >=0.80 expansion was tested and should not be promoted
standalone. It expanded the candidate universe by roughly 28x and remained
negative after cost in both fixed-exit and repair-exit diagnostics.

`EXIT_PROFILE_PROMOTION_CONTINUES`

Exit profile promotion continues on the exact baseline only:

- `track_b_exact_baseline_fixed_36b_exit_v1`: raw-return candidate.
- `track_b_exact_baseline_adaptive_24_to_36_exit_v1`: risk-managed PF/DD candidate.

## Authority Boundary

- `research_offline_only=true`
- `paper_eligible=false`
- `live_eligible=false`
- `runtime_wired=false`
- `strategy_behavior_changes=false`
- `broker_state_mutated=false`
- `order_intent_created=false`
- `lifecycle_mutated=false`
