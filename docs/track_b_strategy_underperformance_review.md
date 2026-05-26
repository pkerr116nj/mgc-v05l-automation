# Track B Strategy Underperformance Review v1

Generated: `2026-05-26T12:15:53.089968+00:00`

This report is analysis-only. It does not change live rules, grant submit authority, mutate broker state, invoke paper_proof, or consume dashboard projections as authority.

## Top Root Causes

- **ANCHOR_POLICY_TOO_STRICT_FOR_LATE_JOIN_DRIFT** (HIGH): 5 strong late-join drift diagnostics; max score 4.747298570844022.
- **REGIME_COVERAGE_GAP** (HIGH): Observed regimes not covered by some strategies: {'TREND_PARTICIPATION': 3, 'CHOP_NO_TRADE': 3}.
- **STRICT_PREDICATE_PRESSURE** (MEDIUM): 442 one/two-gate-away or special near-miss candidates observed.
- **EXIT_AND_ORDER_MANAGEMENT_ATTRIBUTION_GAP** (HIGH): [{'issue': 'ENTRY_GOOD_ORDER_MANAGEMENT_BAD', 'count': 15}, {'issue': 'ENTRY_BAD', 'count': 2}]
- **INFRASTRUCTURE_SUPPRESSION** (MEDIUM): 16 infrastructure/safety rejects counted.

## Rejection And Near-Miss Attribution

- Total evaluations: `7218`
- Total near misses: `442`
- One-gate-away: `35`
- Two-gate-away: `407`
- Market-logic rejects: `7207`
- Infrastructure/safety rejects: `16`

| Strategy | Evaluations | Near misses | Top blockers | Recommendation |
| --- | ---: | ---: | --- | --- |
| `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1` | 643 | 126 | asia_early_or_gc_mgc_london_open, signal_retests_and_holds_breakout_level, breakout_bar_expansion_is_normal | `TUNE_PREDICATE` |
| `ASIA_EARLY_PAUSE_RESUME_SHORT_V1` | 643 | 12 | derivative_phase_asia_early, normalized_curvature_at_or_below_threshold, derivative_bear_close_weak | `TUNE_PREDICATE` |
| `ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1` | 572 | 20 | derivative_phase_asia_late, bull_snap_close_strong, pullback_range_expansion_below_threshold | `TUNE_PREDICATE` |
| `FIRST_BEAR_SNAP_TURN_V1` | 572 | 0 | bear_snap_turn_candidate, first_bear_snap_turn, bear_snap_raw | `KEEP_AS_IS` |
| `FIRST_BULL_SNAP_TURN_V1` | 572 | 0 | bull_snap_turn_candidate, first_bull_snap_turn, bull_snap_raw | `KEEP_AS_IS` |
| `LONDON_LATE_PAUSE_RESUME_SHORT_V1` | 572 | 0 | derivative_phase_london_late, normalized_slope_in_range, normalized_curvature_in_range | `KEEP_AS_IS` |
| `MNQ_FIRST_BEAR_SNAP_TURN_V1` | 643 | 0 | bear_snap_turn_candidate, first_bear_snap_turn, bear_snap_raw | `KEEP_AS_IS` |
| `MNQ_FIRST_BULL_SNAP_TURN_V1` | 643 | 0 | bull_snap_turn_candidate, first_bull_snap_turn, bull_snap_raw | `KEEP_AS_IS` |
| `MNQ_US_DERIVATIVE_BEAR_TURN_V1` | 572 | 0 | derivative_bear_window_ok, derivative_bear_phase_ok, normalized_curvature_below_threshold | `KEEP_AS_IS` |
| `US_DERIVATIVE_BEAR_TURN_V1` | 572 | 0 | derivative_bear_window_ok, derivative_bear_phase_ok, normalized_curvature_below_threshold | `KEEP_AS_IS` |
| `US_LATE_PAUSE_RESUME_LONG_V1` | 572 | 5 | session_us_late, derivative_phase_us_late, setup_bar_curvature_is_positive | `TUNE_PREDICATE` |
| `asian_drift_v1` | 642 | 279 | state_is_entry_eligible, entry_ready, entry_window_open | `ADD_SHADOW_VARIANT` |

## Regime Coverage

- Interpretation: Current live sessions show continuation/drift pressure that the roster only partially covers.
- Observed regimes: `{'MGC': {'primary_regime': 'TREND_PARTICIPATION', 'secondary_regimes': ['BREAKOUT_RETEST', 'TREND_PARTICIPATION'], 'bar_count': 17, 'net_change': -26.8, 'total_range': 37.3, 'directionality': 0.7185, 'pullback_count': 5, 'first_timestamp': '2026-05-26T10:50:00+00:00', 'latest_timestamp': '2026-05-26T12:10:00+00:00'}, 'MNQ': {'primary_regime': 'CHOP_NO_TRADE', 'secondary_regimes': ['BREAKOUT_RETEST'], 'bar_count': 17, 'net_change': -0.75, 'total_range': 66.0, 'directionality': 0.0114, 'pullback_count': 10, 'first_timestamp': '2026-05-26T10:50:00+00:00', 'latest_timestamp': '2026-05-26T12:10:00+00:00'}}`

## High-Quality Promotion Audit

- Primary match: `TRACK_B_ENTRY_ACCEPTANCE_ASIA_EARLY_BREAKOUT_RETEST_HOLD`
- Finding: The exact Entry Acceptance baseline is represented in guarded PAPER via ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1; the broad NEAR >=0.80 candidate universe is not active and remains research-only.

| Family | Status | Active | Key Evidence |
| --- | --- | --- | --- |
| `TRACK_B_ENTRY_ACCEPTANCE_ASIA_EARLY_BREAKOUT_RETEST_HOLD` | `TRACK_B_GUARDED_PAPER_ACTIVE_EXACT_BASELINE` | `True` | exact=856, near>=0.80=23943 |
| `ATP_COMPANION_V1_ACTIVE_TREND_PARTICIPATION` | `NOT_ACTIVE_IN_TRACK_B_GUARDED_PAPER_RUNTIME` | `False` | configs=25 |

## Threshold Pressure

- Authoritative recommendation: `KEEP_80_AUTHORITATIVE`
- Shadow recommendation: `ADD_70_79_SHADOW_INSTRUMENTATION_ONLY_AFTER_80_PLUS_REPAIR_OR_SEGMENTATION`
- Evidence: The broad >=0.80 near-match universe produced about 27.97x more episodes than exact baseline, but the available full-history proxy is negative after cost. That argues for structured shadows and segmentation, not live threshold lowering.

| Bucket | Count | Frequency Impact | Recommendation |
| --- | ---: | --- | --- |
| `AUTHORITATIVE_EXACT_BASELINE` | 856 | 1.0x | `KEEP_80_AUTHORITATIVE` |
| `80_89_OR_NEAR_GTE_0_80` | 23943 | 27.97x | `DO_NOT_LOWER_AUTHORITATIVE_THRESHOLD` |
| `70_79` | None | Expected to exceed NEAR >=0.80 supply, but no current bucket artifact was found. | `ADD_70_79_SHADOW_ONLY_IF_BUCKET_INSTRUMENTATION_EXISTS` |
| `60_69` | None | Unknown and likely large. | `NO_CHANGE` |

## Missed-Opportunity Discovery

- Classification: `MISSED_OPPORTUNITY_DISCOVERY_READY`
- ATP shadow: `{'candidate_count': 17, 'classification': 'ATP_TREND_PARTICIPATION_SHADOW_READY', 'classification_counts': {'ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT': 7, 'ATP_SHADOW_CONFIRMS_LIVE_SIGNAL': 3, 'ATP_SHADOW_LOW_CONFIDENCE': 4, 'ATP_SHADOW_NO_CANDIDATE': 3}, 'shadow_candidate_count': 10}`
- ATP lifecycle mapping shadow: `{'broker_mutation_allowed': False, 'candidate_count': 17, 'classification': 'ATP_LIFECYCLE_MAPPING_SHADOW_READY', 'lifecycle_authority': False, 'mapped_shadow_only_count': 3, 'status_counts': {'ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY': 3, 'ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE': 7, 'ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE': 7}, 'submit_allowed': False}`
- Near-miss scoring: `{'b_grade_shadow_candidate_count': 5, 'candidate_count': 52, 'classification': 'NEAR_MISS_SCORED_SHADOW_READY', 'grade_counts': {'B': 5, 'C': 47}}`
- Shadow candidate maturation: `{'candidate_count': 15, 'classification': 'SHADOW_CANDIDATE_MATURATION_READY', 'final_count': 5, 'partially_matured_count': 0, 'pending_count': 10, 'status_counts': {'FINAL': 5, 'PENDING': 10}}`
- Timestamp-locked evidence: `{'candidate_count': 15, 'classification': 'TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY', 'classification_counts': {'TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY': 5, 'UNCLEAR_FUTURE_BARS_NOT_YET_AVAILABLE': 10}}`
- Forward outcomes: `{'candidate_count': 15, 'classification': 'MISSED_OPPORTUNITY_FORWARD_OUTCOMES_READY', 'classification_counts': {'AVOIDED_LOSER': 1, 'MISSED_WINNER': 4, 'UNCLEAR': 10}}`
- Persistent shadow families: `[{'broker_mutation_allowed': False, 'candidate_count': 5, 'classification': 'ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_READY', 'dry_run_only': True, 'forward_outcome_counts': {'AVOIDED_LOSER': 1, 'MISSED_WINNER': 4}, 'live_money_route_allowed': False, 'not_lifecycle_authority': True, 'not_order_authority': True, 'paper_proof_allowed': False, 'promotion_allowed': False, 'promotion_gate_status': 'PROMOTION_PAUSED_COLLECT_FORWARD_EVIDENCE', 'research_only': True, 'shadow_only': True, 'shadow_strategy_id': 'ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1', 'submit_allowed': False, 'valid_forward_outcome_count': 5}]`
- B-grade missed-winner families: `[{'atp_overlap_candidate_count': 0, 'average_mae': -4.15, 'average_mfe': 7.15, 'average_near_miss_score': 0.88, 'behavioral_hypothesis': 'LATE_JOIN_DRIFT_CONTINUATION_BEHAVIOR', 'best_forward_window_counts': {'30m': 2, '60m': 2}, 'broker_mutation_allowed': False, 'candidate_count': 4, 'candidate_examples': [{'best_forward_mfe': 6.5, 'best_forward_net_movement': -0.2, 'best_forward_window': '60m', 'candidate_id': 'asian_drift_v1:2026-05-25T01:47:23.758641+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 4.081431836160131, 'instrument': 'MGC', 'mae': -5.0, 'mfe': 6.5, 'near_miss_score': 0.88, 'net_movement': -0.2, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T01:47:23.758641+00:00'}, {'best_forward_mfe': 6.5, 'best_forward_net_movement': -0.2, 'best_forward_window': '60m', 'candidate_id': 'asian_drift_v1:2026-05-25T01:48:24.319105+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 3.2379525893010483, 'instrument': 'MGC', 'mae': -5.0, 'mfe': 6.5, 'near_miss_score': 0.88, 'net_movement': -0.2, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T01:48:24.319105+00:00'}, {'best_forward_mfe': 7.8, 'best_forward_net_movement': -0.7, 'best_forward_window': '30m', 'candidate_id': 'asian_drift_v1:2026-05-25T02:21:27.207589+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 3.488982597521212, 'instrument': 'MGC', 'mae': -3.3, 'mfe': 7.8, 'near_miss_score': 0.88, 'net_movement': -1.4, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T02:21:27.207589+00:00'}, {'best_forward_mfe': 7.8, 'best_forward_net_movement': -0.7, 'best_forward_window': '30m', 'candidate_id': 'asian_drift_v1:2026-05-25T02:21:28.797010+00:00', 'direction': 'LONG', 'failed_critical_gates': [], 'failed_noncritical_gates': ['missing_18_00_et_session_anchor_context'], 'failed_predicates': ['missing_18_00_et_session_anchor_context'], 'harvest_exit_style': 'PROFIT_IMPULSE_HARVEST', 'hypothetical_score': 3.488982597521212, 'instrument': 'MGC', 'mae': -3.3, 'mfe': 7.8, 'near_miss_score': 0.88, 'net_movement': -1.4, 'passed_predicate_traits': ['near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed', 'directional_hypothesis_present', 'material_forward_mfe_observed'], 'session': None, 'source': 'GENERAL_NEAR_MISS_SCORED_SHADOW', 'source_authority_path': '/Users/patrick/Dev/MGC-v05l-automation/outputs/reports/entry_acceptance_research/full_history_batch/warehouse_historical_evaluator_partitions/MGC/2026Q2/datasets/derived_bars_5m/symbol=MGC/year=2026/shard_id=2026Q2/bars.parquet', 'strategy_id': 'asian_drift_v1', 'timestamp': '2026-05-25T02:21:28.797010+00:00'}], 'classification': 'B_GRADE_PROMISING_SHADOW', 'common_failed_predicates': ['missing_18_00_et_session_anchor_context'], 'common_passed_traits': ['directional_hypothesis_present', 'material_forward_mfe_observed', 'near_miss_score_threshold_passed', 'no_critical_safety_or_infrastructure_gate_failed', 'profit_harvest_forward_move_observed'], 'direction': 'LONG', 'dry_run_only': True, 'harvest_exit_style_counts': {'PROFIT_IMPULSE_HARVEST': 4}, 'instrument': 'MGC', 'live_money_route_allowed': False, 'live_rule_change_allowed': False, 'not_lifecycle_authority': True, 'not_order_authority': True, 'paper_proof_allowed': False, 'promotion_allowed': False, 'rank': 1, 'recommended_shadow_instrumentation': 'Add shadow-only late-join Asian Drift family with explicit missing-anchor reason, post-anchor drift score, profit-harvest MFE/MAE tracking, and no submit authority.', 'research_only': True, 'shadow_family_id': 'asian_drift_mgc_long_missing_18_00_et_session_anchor_context', 'shadow_only': True, 'strategy_family': 'asian_drift', 'submit_allowed': False}]`

## Forward Outcome Simulation

- Interpretation: Current strict rules avoided at least some weak forward outcomes.
- Outcome counts: `{'GOOD_REJECT': 31}`

## Exit / Position Management Attribution

- Closed trades scanned: `20`
- Attribution counts: `{'ENTRY_GOOD_ORDER_MANAGEMENT_BAD': 15, 'ENTRY_BAD': 2, 'STRATEGY_VALIDATION_INCONCLUSIVE': 3}`
- Top issues: `[{'issue': 'ENTRY_GOOD_ORDER_MANAGEMENT_BAD', 'count': 15}, {'issue': 'ENTRY_BAD', 'count': 2}]`

## Ranked Remediation Board

| Action | Title | Impact | Dependencies | Risk |
| --- | --- | --- | --- | --- |
| `ADD_SHADOW_VARIANT` | ASIAN_DRIFT_LATE_JOIN_B_GRADE_SHADOW_V1 | `HIGH` | shadow_event_history, forward_outcome_simulator | Shadow only; no submit authority. |
| `ADD_SHADOW_VARIANT` | GAP_DRIFT_CONTINUATION_B_GRADE_SHADOW_V1 | `HIGH` | shadow_event_history, forward_outcome_simulator | Shadow only; no submit authority. |
| `ADD_NEW_REGIME_FAMILY` | Gap/drift continuation strategy family, shadow first | `HIGH` | phase1_5m_regime_labels, B_grade_shadow_outcomes | Continuation entries can chase; require MFE/MAE and exit-profile evidence. |
| `IMPROVE_EXIT_PROFILE` | Upgrade exit profile evaluation and order-management attribution | `HIGH` | managed_exit_idempotency, modify_in_place_guardian, continuation_exit_history | Exit improvements must stay in exit roster/profile architecture. |
| `ADD_SHADOW_VARIANT` | RELAXED_BREAKOUT_WITHOUT_RETEST_B_GRADE_SHADOW_V1 | `MEDIUM` | shadow_event_history, forward_outcome_simulator | Shadow only; no submit authority. |

## Tollgate

Recommendation: `STOP_BEFORE_LIVE_RULE_CHANGES`

- Collect forward outcomes for B-grade shadows across multiple sessions.
- Require strategy-family review before any predicate loosening becomes A-grade authority.
- Keep all B-grade and new-regime variants dry-run only until sample size and exit attribution are adequate.
- Confirm broker position guardian and managed-exit idempotency stay clean during any guarded PAPER expansion.
