# Track B Session Changeover Overlay Research

Strategy-family agnostic, research-only overlay for 03:00 ET Asia-to-Europe and 07:00 ET Europe-to-US changeovers.

## Safety Scope

- Research only: true
- Live trading authority changed: false
- Active PAPER config changed: false
- Strategy thresholds changed: false
- Runtime restarted: false
- Broker mutation allowed: false
- paper_proof invoked: false

## Evidence

- Replay bar rows: 848733
- Replay symbols: GC, MGC
- Replay window: 2020-01-01T23:05:00+00:00 to 2026-05-25T06:45:00+00:00
- Normalized trade rows: 46435
- Trade source paths: 24

## Bar Behavior Summary

| Changeover | Window | Instrument | Inside bars | Adjacent bars | Avg range delta | Breakout delta | False-move delta |
|---|---:|---|---:|---:|---:|---:|---:|
| ASIA_TO_EUROPE_CHANGEOVER | tight | GC | 11175 | 9584 | 0.0139 | 0.0039 | 0.0038 |
| ASIA_TO_EUROPE_CHANGEOVER | tight | MGC | 10927 | 9356 | 0.0215 | 0.0053 | 0.0025 |
| ASIA_TO_EUROPE_CHANGEOVER | medium | GC | 20759 | 19140 | 0.0275 | 0.0105 | 0.0089 |
| ASIA_TO_EUROPE_CHANGEOVER | medium | MGC | 20283 | 18676 | 0.0184 | 0.0068 | 0.0038 |
| ASIA_TO_EUROPE_CHANGEOVER | wide | GC | 39899 | 38170 | 0.2060 | 0.0458 | 0.0144 |
| ASIA_TO_EUROPE_CHANGEOVER | wide | MGC | 38959 | 36558 | 0.1783 | 0.0387 | 0.0120 |
| EUROPE_TO_US_CHANGEOVER | tight | GC | 11144 | 9563 | 0.0394 | 0.0025 | 0.0027 |
| EUROPE_TO_US_CHANGEOVER | tight | MGC | 10856 | 9282 | 0.0305 | 0.0029 | 0.0030 |
| EUROPE_TO_US_CHANGEOVER | medium | GC | 20707 | 19126 | -0.0109 | -0.0079 | -0.0020 |
| EUROPE_TO_US_CHANGEOVER | medium | MGC | 20138 | 18617 | -0.0129 | -0.0066 | -0.0008 |
| EUROPE_TO_US_CHANGEOVER | wide | GC | 39833 | 38347 | -0.6475 | -0.1476 | -0.0567 |
| EUROPE_TO_US_CHANGEOVER | wide | MGC | 38755 | 37458 | -0.6531 | -0.1428 | -0.0514 |

## Strategy And Shadow Trade Overlay

| Changeover | Window | Source family | Inside trades | Inside win rate | Inside PF | Adjacent trades | Adjacent win rate | Adjacent PF |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| ASIA_TO_EUROPE_CHANGEOVER | tight | MNQ_FIRST_BULL_SNAP_TURN_V1 | 0 |  |  | 2 | 1.0000 |  |
| ASIA_TO_EUROPE_CHANGEOVER | tight | atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_5m | 2 |  |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | tight | gc_asia_early_normal_breakout_retest_hold_turn__GC | 24 | 0.0417 | 0.0040 | 10 | 0.1000 | 0.0155 |
| ASIA_TO_EUROPE_CHANGEOVER | tight | gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long | 4 | 1.0000 |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | tight | gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__london_early_long | 2 | 0.0000 | 0.0000 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | tight | mgc_asia_early_normal_breakout_retest_hold_turn__MGC | 24 | 0.0417 | 0.0031 | 9 | 0.0000 | 0.0000 |
| ASIA_TO_EUROPE_CHANGEOVER | tight | track_b_paper_execution_test_mule_v1__mnq | 3 | 0.0000 | 0.0000 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | medium | MNQ_FIRST_BULL_SNAP_TURN_V1 | 2 | 1.0000 |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | medium | atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_5m | 2 |  |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | medium | atp_companion_v1__production_track_gc_asia_us_selective_v1 | 0 |  |  | 2 | 0.0000 | 0.0000 |
| ASIA_TO_EUROPE_CHANGEOVER | medium | gc_asia_early_normal_breakout_retest_hold_turn__GC | 34 | 0.0588 | 0.0079 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | medium | gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long | 4 | 1.0000 |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | medium | gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__london_early_long | 2 | 0.0000 | 0.0000 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | medium | mgc_asia_early_normal_breakout_retest_hold_turn__MGC | 33 | 0.0303 | 0.0023 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | medium | track_b_paper_execution_test_mule_v1__mnq | 3 | 0.0000 | 0.0000 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1 | 0 |  |  | 1 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | MNQ_FIRST_BULL_SNAP_TURN_V1 | 2 | 1.0000 |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_5m | 2 |  |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | atp_companion_v1__production_track_gc_asia_us_selective_v1 | 2 | 0.0000 | 0.0000 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | gc_asia_early_normal_breakout_retest_hold_turn__GC | 34 | 0.0588 | 0.0079 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long | 4 | 1.0000 |  | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__london_early_long | 2 | 0.0000 | 0.0000 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | mgc_asia_early_normal_breakout_retest_hold_turn__MGC | 33 | 0.0303 | 0.0023 | 0 |  |  |
| ASIA_TO_EUROPE_CHANGEOVER | wide | track_b_paper_execution_test_mule_v1__mgc | 0 |  |  | 6 | 0.0000 | 0.0000 |
| ASIA_TO_EUROPE_CHANGEOVER | wide | track_b_paper_execution_test_mule_v1__mnq | 3 | 0.0000 | 0.0000 | 4 | 0.0000 | 0.0000 |
| EUROPE_TO_US_CHANGEOVER | tight | MNQ_FIRST_BEAR_SNAP_TURN_V1 | 0 |  |  | 6 | 1.0000 |  |
| EUROPE_TO_US_CHANGEOVER | tight | asia_london_participation_core_v1__mnq_1x_asia_london_participation__asia_london_long_v6 | 3 |  |  | 0 |  |  |
| EUROPE_TO_US_CHANGEOVER | tight | gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_long | 1 |  |  | 3 | 0.0000 | 0.0000 |
| EUROPE_TO_US_CHANGEOVER | tight | track_b_paper_execution_test_mule_v1__mgc | 4 | 0.0000 | 0.0000 | 0 |  |  |
| EUROPE_TO_US_CHANGEOVER | medium | MNQ_FIRST_BEAR_SNAP_TURN_V1 | 6 | 1.0000 |  | 0 |  |  |
| EUROPE_TO_US_CHANGEOVER | medium | asia_london_participation_core_v1__mnq_1x_asia_london_participation__asia_london_long_v6 | 3 |  |  | 0 |  |  |
| EUROPE_TO_US_CHANGEOVER | medium | gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_long | 4 | 0.0000 | 0.0000 | 0 |  |  |
| EUROPE_TO_US_CHANGEOVER | medium | track_b_paper_execution_test_mule_v1__mgc | 4 | 0.0000 | 0.0000 | 0 |  |  |
| EUROPE_TO_US_CHANGEOVER | wide | FIRST_BEAR_SNAP_TURN_V1 | 0 |  |  | 1 |  |  |
| EUROPE_TO_US_CHANGEOVER | wide | MNQ_FIRST_BEAR_SNAP_TURN_V1 | 6 | 1.0000 |  | 0 |  |  |
| EUROPE_TO_US_CHANGEOVER | wide | asia_london_participation_core_v1__mnq_1x_asia_london_participation__asia_london_long_v6 | 3 |  |  | 1 |  |  |
| EUROPE_TO_US_CHANGEOVER | wide | atp_companion_v1__production_track_gc_asia_us | 0 |  |  | 2 |  |  |
| EUROPE_TO_US_CHANGEOVER | wide | atp_companion_v1__production_track_gc_asia_us_5m | 0 |  |  | 2 |  |  |
| EUROPE_TO_US_CHANGEOVER | wide | atp_companion_v1__production_track_gc_asia_us_selective_v1 | 0 |  |  | 2 |  |  |
| EUROPE_TO_US_CHANGEOVER | wide | atp_companion_v1_pl_asia_us | 0 |  |  | 1 | 0.0000 |  |

## Candidate Implications For Entries

- ASIA_TO_EUROPE_CHANGEOVER: breakout_or_continuation_setups_may_need_changeover_context
- ASIA_TO_EUROPE_CHANGEOVER: breakout_or_continuation_setups_may_need_changeover_context

## Candidate Implications For Hold/Exit Policy

- ALL: No strong hold/exit implication detected yet.

## Session Label Recommendation

Add labels only as overlapping research/session-context tags until forward/live PAPER evidence supports using them in entries or exits.
- ASIA_TO_EUROPE_CHANGEOVER: ADD_RESEARCH_OVERLAY_LABEL_SHADOW_ONLY (5 evidence votes)
- EUROPE_TO_US_CHANGEOVER: ADD_RESEARCH_OVERLAY_LABEL_SHADOW_ONLY (2 evidence votes)

## Limitations

- This is a strategy-family agnostic observational overlay on existing replay/research artifacts, not a new backtest engine.
- Trade performance attribution depends on fields present in existing JSONL trade artifacts.
- Hold-through implications are observational unless an existing trade artifact spans the anchor.
