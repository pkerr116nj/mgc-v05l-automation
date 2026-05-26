# Track B Missed-Opportunity Discovery Layer

Generated: `2026-05-26T12:15:43.036581+00:00`

Shadow/diagnostic only. No live rules were loosened, no broker/lifecycle authority was created, and no paper_proof/live-money route was introduced.

## ATP / Trend Participation Shadow

- Classification: `ATP_TREND_PARTICIPATION_SHADOW_READY`
- Candidate count: `17`
- Shadow candidate count: `10`
- Counts: `{'ATP_SHADOW_LOW_CONFIDENCE': 4, 'ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT': 7, 'ATP_SHADOW_CONFIRMS_LIVE_SIGNAL': 3, 'ATP_SHADOW_NO_CANDIDATE': 3}`
- Lifecycle mapping shadow: `{'classification': 'ATP_LIFECYCLE_MAPPING_SHADOW_READY', 'candidate_count': 17, 'mapped_shadow_only_count': 3, 'status_counts': {'ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY': 3, 'ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE': 7, 'ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE': 7}, 'lifecycle_authority': False, 'submit_allowed': False, 'broker_mutation_allowed': False}`

## Near-Miss Scoring

- Classification: `NEAR_MISS_SCORED_SHADOW_READY`
- Candidate count: `52`
- B-grade candidate count: `5`
- Grade counts: `{'C': 47, 'B': 5}`

## Forward Outcomes

- Shadow candidate maturation: `{'classification': 'SHADOW_CANDIDATE_MATURATION_READY', 'candidate_count': 15, 'pending_count': 10, 'partially_matured_count': 0, 'final_count': 5, 'status_counts': {'PENDING': 10, 'FINAL': 5}}`
- Timestamp-locked evidence: `{'classification': 'TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY', 'candidate_count': 15, 'classification_counts': {'UNCLEAR_FUTURE_BARS_NOT_YET_AVAILABLE': 10, 'TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY': 5}}`
- Classification: `MISSED_OPPORTUNITY_FORWARD_OUTCOMES_READY`
- Candidate count: `15`
- Outcome counts: `{'UNCLEAR': 10, 'MISSED_WINNER': 4, 'AVOIDED_LOSER': 1}`

## Persistent PAPER Shadow Families

| Shadow Family | Classification | Candidates | Valid Forward Outcomes | Outcome Counts | Promotion Gate |
| --- | --- | ---: | ---: | --- | --- |
| `ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1` | `ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_READY` | 5 | 5 | `{'MISSED_WINNER': 4, 'AVOIDED_LOSER': 1}` | `PROMOTION_PAUSED_COLLECT_FORWARD_EVIDENCE` |

## B-Grade Missed-Winner Family Rankings

| Rank | Classification | Family | Instrument | Direction | Count | Failed Traits | Behavior | Exit Style |
| ---: | --- | --- | --- | --- | ---: | --- | --- | --- |
| 1 | `B_GRADE_PROMISING_SHADOW` | `asian_drift` | `MGC` | `LONG` | 4 | `['missing_18_00_et_session_anchor_context']` | `LATE_JOIN_DRIFT_CONTINUATION_BEHAVIOR` | `{'PROFIT_IMPULSE_HARVEST': 4}` |

## Promotion Gate Recommendations

| Recommendation | Title | Impact | Risk |
| --- | --- | --- | --- |
| `ADD_SHADOW_ONLY` | Run ATP/trend participation candidates as a Track B shadow cohort | `HIGH` | No submit authority; requires shared-services diagnostic-only wiring. |
| `ADD_SHADOW_ONLY` | Persist scored B-grade near-miss shadow candidates | `HIGH` | B-grade remains non-authoritative until forward evidence clears tollgate. |
| `COLLECT_MORE_FORWARD_EVIDENCE` | Prioritize late-join drift and gap/continuation forward evidence | `HIGH` | Continuation shadows can chase; require MFE/MAE by regime. |
| `KEEP_LIVE_RULES_UNCHANGED` | Keep A-grade live predicates unchanged | `HIGH` | Opportunity cost only; safety and attribution remain clean. |
| `DO_NOT_PROMOTE` | Do not promote raw sub-80 score buckets | `MEDIUM` | Raw mining likely increases false positives. |

## Tollgate

- Keep live A-grade predicates unchanged.
- Require forward MFE/MAE and exit attribution before promotion.
- Promote explainable shadows only; do not promote raw lower-score buckets.
