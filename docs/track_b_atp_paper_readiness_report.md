# Track B ATP PAPER Readiness Report

Generated: `2026-05-26T12:15:49.783653+00:00`

Research/shadow only. No broker authority, lifecycle authority, live-money route, or paper_proof path is created.

## Summary

- Classification: `ATP_PAPER_READINESS_READY`
- Candidate count: `17`
- Readiness counts: `{'PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING': 3, 'PAPER_CANDIDATE_NEEDS_EXIT_PROFILE': 7, 'SHADOW_ONLY': 4, 'REJECT_OR_DEFER': 3}`
- Instrument counts: `{'MGC': 6, 'GC': 8, 'PL': 3}`
- Lifecycle mapping shadow: `{'classification': 'ATP_LIFECYCLE_MAPPING_SHADOW_READY', 'candidate_count': 17, 'mapped_shadow_only_count': 3, 'status_counts': {'ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY': 3, 'ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE': 7, 'ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE': 7}, 'lifecycle_authority': False, 'submit_allowed': False, 'broker_mutation_allowed': False}`

## Ranked Candidates

| Rank | Candidate | Instrument | Direction | Readiness | Score | Outcomes | Missing Gates |
| ---: | --- | --- | --- | --- | ---: | --- | --- |
| 1 | `edge_v1` | `MGC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING` | 0.765909 | `{'UNCLEAR': 1}` | `['track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 2 | `promotion_1_075r_favorable_only` | `MGC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING` | 0.765909 | `{'UNCLEAR': 4}` | `['track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 3 | `promotion_1_075r_favorable_only` | `MGC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING` | 0.765909 | `{'UNCLEAR': 4}` | `['track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 4 | `atp_companion_v1_gc_asia_us_5m` | `GC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_EXIT_PROFILE` | 0.754505 | `{'UNCLEAR': 1}` | `['managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 5 | `atp_companion_v1_gc_asia_us_production_track` | `GC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_EXIT_PROFILE` | 0.754505 | `{'UNCLEAR': 1}` | `['managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 6 | `atp_companion_v1_gc_asia_us_production_track_5m` | `GC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_EXIT_PROFILE` | 0.754505 | `{'UNCLEAR': 1}` | `['managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 7 | `promotion_1_075r_favorable_only` | `GC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_EXIT_PROFILE` | 0.754505 | `{'UNCLEAR': 4}` | `['managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 8 | `promotion_1_075r_favorable_only` | `GC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_EXIT_PROFILE` | 0.754505 | `{'UNCLEAR': 4}` | `['managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 9 | `selective_v1` | `GC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_EXIT_PROFILE` | 0.754505 | `{'UNCLEAR': 2}` | `['managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 10 | `selective_v1` | `GC` | `SHORT` | `PAPER_CANDIDATE_NEEDS_EXIT_PROFILE` | 0.754505 | `{'UNCLEAR': 2}` | `['managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 11 | `atp_companion_v1_asia_us` | `MGC` | `LONG` | `SHADOW_ONLY` | 0.765909 | `{}` | `['shadow_candidate_not_currently_ready', 'paper_only_candidate_config_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 12 | `atp_companion_v1_asia_us` | `MGC` | `LONG` | `SHADOW_ONLY` | 0.765909 | `{}` | `['shadow_candidate_not_currently_ready', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 13 | `atp_companion_v1_asia_us_5m` | `MGC` | `LONG` | `SHADOW_ONLY` | 0.765909 | `{}` | `['shadow_candidate_not_currently_ready', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 14 | `atp_companion_v1_gc_asia_us` | `GC` | `LONG` | `SHADOW_ONLY` | 0.754505 | `{}` | `['shadow_candidate_not_currently_ready', 'paper_only_candidate_config_required', 'managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 15 | `atp_companion_v1_pl_asia_us_5m` | `PL` | `SHORT` | `REJECT_OR_DEFER` | 0.30819 | `{}` | `['shadow_candidate_not_currently_ready', 'managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 16 | `risk_shaped_v1` | `PL` | `SHORT` | `REJECT_OR_DEFER` | 0.30819 | `{}` | `['shadow_candidate_not_currently_ready', 'managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |
| 17 | `atp_companion_v1_pl_asia_us` | `PL` | `LONG` | `REJECT_OR_DEFER` | 0.05819 | `{}` | `['shadow_candidate_not_currently_ready', 'managed_exit_profile_required', 'track_b_lifecycle_mapping_required', 'managed_order_management_path_required', 'forward_outcome_sample_size_below_30', 'timestamp_locked_forward_outcomes_missing_or_incomplete']` |

## Recommendation

- First candidate to work next: `edge_v1` with `PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING`.
- Recommended action: Add Track B managed lifecycle ownership mapping for ATP candidate family, then continue shadow evidence.

## Next Slice

- `IMPLEMENT_ATP_LIFECYCLE_MAPPING_SHADOW_FIRST`
- Define generic ATP Track B lifecycle ownership metadata and map MGC/MNQ-compatible ATP candidates to existing diagnostic time-boxed exit profiles, still without broker authority.
