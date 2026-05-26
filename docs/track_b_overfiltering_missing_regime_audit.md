# Track B Over-Filtering / Missing-Regime Audit

Generated: `2026-05-26T10:48:54.235232+00:00`

This is research/shadow only. It does not loosen live rules, mutate broker state, restart runtime, invoke paper_proof, or create live-money authority.

## Summary

- Over-filtering: `OVERFILTERING_REQUIRES_SHADOW_REMEDIATION`
- ATP/trend participation: `ATP_TREND_CANDIDATES_EXIST_NOT_GUARDED_ROSTER_ACTIVE`
- Missed-move fit: `MISSED_MOVES_MOSTLY_MISSING_REGIME_AND_SHADOW_COVERAGE`
- Discovery layer: `MISSED_OPPORTUNITY_DISCOVERY_READY`
- Timestamp-locked evidence: `{'candidate_count': 19, 'classification': 'TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY', 'classification_counts': {'TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY': 5, 'UNCLEAR_FUTURE_BARS_NOT_YET_AVAILABLE': 14}}`
- Persistent shadow families: `[{'broker_mutation_allowed': False, 'candidate_count': 5, 'classification': 'ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_READY', 'dry_run_only': True, 'forward_outcome_counts': {'AVOIDED_LOSER': 1, 'MISSED_WINNER': 4}, 'live_money_route_allowed': False, 'not_lifecycle_authority': True, 'not_order_authority': True, 'paper_proof_allowed': False, 'promotion_allowed': False, 'promotion_gate_status': 'PROMOTION_PAUSED_COLLECT_FORWARD_EVIDENCE', 'research_only': True, 'shadow_only': True, 'shadow_strategy_id': 'ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1', 'submit_allowed': False, 'valid_forward_outcome_count': 5}]`
- B-grade missed-winner families: `[{'atp_overlap_candidate_count': 6, 'average_mae': -4.15, 'average_mfe': 7.15, 'average_near_miss_score': 0.88, 'behavioral_hypothesis': 'LATE_JOIN_DRIFT_CONTINUATION_BEHAVIOR', 'best_forward_window_counts': {'30m': 2, '60m': 2}, 'broker_mutation_allowed': False, 'candidate_count': 4, 'candidate_examples': [{'best_forward_mfe': 6.5, 'best_forward_net_movement': -0.2, 'best_forward_window': '60m', 'candidate_id': 'asian_drift_v1:2026-05-25T01:47:23.758641+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 4.081431836160131, 'instrument': 'MGC', 'mae': -5.0, 'mfe': 6.5, 'near_miss_score': 0.88, 'net_movement': -0.2, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T01:47:23.758641+00:00'}, {'best_forward_mfe': 6.5, 'best_forward_net_movement': -0.2, 'best_forward_window': '60m', 'candidate_id': 'asian_drift_v1:2026-05-25T01:48:24.319105+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 3.2379525893010483, 'instrument': 'MGC', 'mae': -5.0, 'mfe': 6.5, 'near_miss_score': 0.88, 'net_movement': -0.2, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T01:48:24.319105+00:00'}, {'best_forward_mfe': 7.8, 'best_forward_net_movement': -0.7, 'best_forward_window': '30m', 'candidate_id': 'asian_drift_v1:2026-05-25T02:21:27.207589+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 3.488982597521212, 'instrument': 'MGC', 'mae': -3.3, 'mfe': 7.8, 'near_miss_score': 0.88, 'net_movement': -1.4, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T02:21:27.207589+00:00'}, {'best_forward_mfe': 7.8, 'best_forward_net_movement': -0.7, 'best_forward_window': '30m', 'candidate_id': 'asian_drift_v1:2026-05-25T02:21:28.797010+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 3.488982597521212, 'instrument': 'MGC', 'mae': -3.3, 'mfe': 7.8, 'near_miss_score': 0.88, 'net_movement': -1.4, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T02:21:28.797010+00:00'}], 'classification': 'B_GRADE_PROMISING_SHADOW', 'common_failed_predicates': ['missing_18_00_et_session_anchor_context'], 'common_passed_traits': ['directional_hypothesis_present', 'material_forward_mfe_observed', 'near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed'], 'direction': 'LONG', 'dry_run_only': True, 'harvest_exit_style_counts': {'PROFIT_IMPULSE_HARVEST': 4}, 'instrument': 'MGC', 'live_money_route_allowed': False, 'live_rule_change_allowed': False, 'not_lifecycle_authority': True, 'not_order_authority': True, 'paper_proof_allowed': False, 'promotion_allowed': False, 'rank': 1, 'recommended_shadow_instrumentation': 'Add shadow-only late-join Asian Drift family with explicit missing-anchor reason, post-anchor drift score, profit-harvest MFE/MAE tracking, and no submit authority.', 'research_only': True, 'shadow_family_id': 'asian_drift_mgc_long_missing_18_00_et_session_anchor_context', 'shadow_only': True, 'strategy_family': 'asian_drift', 'submit_allowed': False}]`
- Sub-80 mining: `SUB80_MINING_DEFERRED_UNTIL_OVERFILTERING_AND_MISSING_REGIME_WORK`

## Over-Filtering Audit

- Near misses: `442`
- One-gate-away: `35`
- Two-gate-away: `407`

| Strategy | Near Misses | One Gate | Two Gate | Classification | Action |
| --- | ---: | ---: | ---: | --- | --- |
| `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1` | 126 | 25 | 101 | `OVERFILTERING_POSSIBLE_MISSED_WINNER` | `ADD_STRATEGY_LOCAL_SHADOW_FOR_FAILED_PREDICATE` |
| `asian_drift_v1` | 279 | 5 | 274 | `OVERFILTERING_POSSIBLE_ONE_GATE_AWAY` | `ADD_LATE_JOIN_VARIANT` |
| `ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1` | 20 | 5 | 15 | `OVERFILTERING_POSSIBLE_ONE_GATE_AWAY` | `LEAVE_STRICT_RULE_UNCHANGED_UNTIL_REVERSAL_REGIME` |
| `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` | 12 | 0 | 12 | `OVERFILTERING_POSSIBLE_TWO_GATE_AWAY` | `CONTINUE_OBSERVING` |
| `US_LATE_PAUSE_RESUME_LONG_V1` | 5 | 0 | 5 | `OVERFILTERING_POSSIBLE_TWO_GATE_AWAY` | `LEAVE_STRICT_RULE_UNCHANGED_UNTIL_REVERSAL_REGIME` |
| `FIRST_BEAR_SNAP_TURN_V1` | 0 | 0 | 0 | `NO_OVERFILTERING_SIGNAL` | `LEAVE_STRICT_RULE_UNCHANGED_UNTIL_REVERSAL_REGIME` |
| `FIRST_BULL_SNAP_TURN_V1` | 0 | 0 | 0 | `NO_OVERFILTERING_SIGNAL` | `LEAVE_STRICT_RULE_UNCHANGED_UNTIL_REVERSAL_REGIME` |
| `LONDON_LATE_PAUSE_RESUME_SHORT_V1` | 0 | 0 | 0 | `NO_OVERFILTERING_SIGNAL` | `CONTINUE_OBSERVING` |
| `MNQ_FIRST_BEAR_SNAP_TURN_V1` | 0 | 0 | 0 | `NO_OVERFILTERING_SIGNAL` | `LEAVE_STRICT_RULE_UNCHANGED_UNTIL_REVERSAL_REGIME` |
| `MNQ_FIRST_BULL_SNAP_TURN_V1` | 0 | 0 | 0 | `NO_OVERFILTERING_SIGNAL` | `LEAVE_STRICT_RULE_UNCHANGED_UNTIL_REVERSAL_REGIME` |
| `MNQ_US_DERIVATIVE_BEAR_TURN_V1` | 0 | 0 | 0 | `NO_OVERFILTERING_SIGNAL` | `CONTINUE_OBSERVING` |
| `US_DERIVATIVE_BEAR_TURN_V1` | 0 | 0 | 0 | `NO_OVERFILTERING_SIGNAL` | `CONTINUE_OBSERVING` |

## ATP / Trend Participation Activation

- Candidate configs: `25`
- Guarded roster active: `0`
- Interpretation: ATP/trend participation has implementation/config lineage, but current guarded PAPER runtime roster does not include its strategy ids. Treat activation as a Track B integration/shadow-roster task, not predicate loosening.

| Config Strategy | Symbol | Status | Why Inactive |
| --- | --- | --- | --- |
| `None` | `None` | `RESEARCH_LINEAGE_ONLY` | not_paper_only_track_b_guarded_candidate |
| `None` | `None` | `RESEARCH_LINEAGE_ONLY` | not_paper_only_track_b_guarded_candidate |
| `atp_companion_v1__live_entry_pilot_mgc_asia_us` | `MGC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `atp_companion_v1__paper_gc_asia_us` | `GC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `None` | `None` | `RESEARCH_LINEAGE_ONLY` | not_paper_only_track_b_guarded_candidate |
| `atp_companion_v1__benchmark_mgc_asia_us` | `MGC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `atp_companion_v1__benchmark_mgc_asia_us` | `MGC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `atp_companion_v1__benchmark_mgc_asia_us_5m` | `MGC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only` | `GC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_5m` | `GC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `atp_companion_v1__paper_gc_asia_us` | `GC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |
| `atp_companion_v1__paper_gc_asia_us` | `GC` | `IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG` | non_approved_or_research_candidate_config |

## Top Implementation Candidates

| Rank | Action | Title | Impact | Risk |
| ---: | --- | --- | --- | --- |
| 1 | `ACTIVATE_EXISTING_ATP_SHADOW` | Bring ATP/trend participation candidates into Track B shadow/diagnostic roster first | `HIGH` | Do not grant submit authority until Control Plane/Safe-State/generation-scoped path is proven for ATP. |
| 2 | `ADD_B_GRADE_SHADOW` | Promote Asian Drift late-join diagnostic into persistent B-grade shadow tracking | `HIGH` | Missing-anchor state remains non-authoritative. |
| 3 | `CREATE_NEW_CONTINUATION_FAMILY` | Create gap/drift continuation shadow family before sub-80 mining | `HIGH` | Continuation can chase; keep shadow-only until exits prove durable. |
| 4 | `TUNE_SPECIFIC_PREDICATE` | Instrument top one-gate/two-gate predicates as strategy-local shadows | `MEDIUM` | Predicate tuning without regime segmentation can add low-quality trade supply. |
| 5 | `DEFER_SUB80_MINING` | Scope 70-79 mining after over-filtering and ATP activation are measured | `MEDIUM` | Raw lower-score mining is likely to overfit and degrade quality. |

## Tollgate

Recommendation: `STOP_BEFORE_LIVE_RULE_CHANGES`

- Run all recommended variants as shadow/research only first.
- Collect forward MFE/MAE and exit-profile attribution by strategy family.
- Promote only explainable rule subsets, never raw lower score buckets.
- Keep Control Plane, Safe-State, guardian, lifecycle, and managed-exit gates authoritative.
