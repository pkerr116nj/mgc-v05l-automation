# Asia/London Instrumentation Contract

Output root:

- `/Users/patrick/Documents/MGC-v05l-automation/outputs/reports/strategy_activity_instrumentation`

Files:

- `asia_london_live_predicate_trace.jsonl`
- `asia_london_live_near_miss_trace.csv`
- `asia_london_score_bucket_live_observation.csv`

## JSONL row contract

Each row in `asia_london_live_predicate_trace.jsonl` contains:

- `observed_at`
- `timestamp`
- `lane_id`
- `instrument`
- `session_label`
- `strict_gate_pass`
- `strict_gate_fail_reason`
- `near_miss_score`
- `predicates_passed`
- `predicate_count`
- `candidate_score_bucket`
- `current_strict_candidate`
- `research_score_candidate`
- `entry_reason`
- `floor_reason`
- `predicate_results`
- `predicate_values`
- `forward_return_label_available`

Notes:

- `candidate_score_bucket` is observational only and is not used for trading.
- `research_score_candidate` is a research flag for `A+`, `A`, and `B+` buckets only.
- No forward-return labels are emitted from live runtime unless a replay/research context provides them without future leakage.

## CSV contracts

`asia_london_live_near_miss_trace.csv`

- `timestamp`
- `lane_id`
- `instrument`
- `session_label`
- `strict_gate_pass`
- `strict_gate_fail_reason`
- `predicates_passed`
- `predicate_count`
- `near_miss_score`
- `candidate_score_bucket`
- `current_strict_candidate`
- `research_score_candidate`
- `entry_reason`
- `floor_reason`
- `failed_predicates`

`asia_london_score_bucket_live_observation.csv`

- `timestamp`
- `lane_id`
- `instrument`
- `candidate_score_bucket`
- `near_miss_score`
- `predicates_passed`
- `predicate_count`
- `current_strict_candidate`
- `research_score_candidate`
