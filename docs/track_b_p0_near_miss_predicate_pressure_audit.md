# Track B P0 Near-Miss and Predicate Pressure Audit

Generated: `2026-05-25T05:48:34.854675+00:00`

## Summary
- Event rows analyzed: `31`
- Natural candidates: `0`
- Signals emitted: `0`
- Suppressed signals: `0`
- Primary classification: `P0_NO_TRADE_ANCHOR_POLICY_TOO_STRICT`
- Additional classifications: `P0_NO_TRADE_ANCHOR_POLICY_TOO_STRICT`, `P0_NO_TRADE_SESSION_WINDOW_TOO_NARROW`, `P0_NO_TRADE_NEEDS_NEW_DRIFT_STRATEGY`, `P0_NO_TRADE_VALID_SELECTIVITY`

The no-trade behavior is partly valid selectivity: snap-turn strategies did not see a reversal regime, and session strategies were outside/against their intended windows. The meaningful miss is coverage: Asian Drift observed strong late drift after the anchor window was missing, and the current P0 set has no submit-eligible late-join/gap-drift continuation strategy.

## Market Context
- MGC_5m: `2026-05-25T04:25:00+00:00` to `2026-05-25T05:45:00+00:00`, close delta `-6.8`, high `4567.3`, low `4553.8`
- MNQ_5m: `2026-05-25T04:25:00+00:00` to `2026-05-25T05:45:00+00:00`, close delta `18.0`, high `29995.0`, low `29959.0`

## Asian Drift
- Late-join strong drift count: `5`
- Max late-join score: `4.747298570844022`
- Missing anchor was sole late-join blocker: `True`
- Diagnostic-only late-join candidate would have appeared: `True`

## Per-Strategy Predicate Pressure
### ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1
- Classification: `SESSION_WINDOW_TOO_NARROW`
- Evaluations: `31`; signals: `0`; near-misses: `5`
- Top failed predicates: `asia_early_or_gc_mgc_london_open` (31), `breakout_bar_slope_is_flat` (27), `signal_retests_and_holds_breakout_level` (26), `breakout_breaks_prior_1_high` (24), `breakout_bar_expansion_is_normal` (21), `no_first_bull_snap_turn` (5)
- Latest: `ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION` - Asia Early normal breakout-retest-hold long v1 conditions did not pass: asia_early_or_gc_mgc_london_open, breakout_bar_slope_is_flat, breakout_bar_expansion_is_normal, signal_retests_and_holds_breakout_level
- Interpretation: Session/window predicates frequently blocked evaluation after the intended Asia-early regime.

### ASIA_EARLY_PAUSE_RESUME_SHORT_V1
- Classification: `SESSION_WINDOW_TOO_NARROW`
- Evaluations: `31`; signals: `0`; near-misses: `2`
- Top failed predicates: `derivative_phase_asia_early` (31), `setup_bar_curvature_is_flat` (29), `one_bar_rebound_before_signal` (19), `derivative_bear_close_weak` (16), `derivative_bear_stretch_ok` (16), `derivative_bear_body_ok` (14)
- Latest: `ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION` - Asia Early pause-resume short v1 conditions did not pass: session_asia, derivative_phase_asia_early, close_below_open, close_below_previous_close, derivative_bear_close_weak, derivative_bear_range_ok, derivative_bear_body_ok, normalized_curvature_at_or_below_threshold, setup_bar_curvature_is_flat, signal_breaks_prior_1_low, close_below_fast_ema
- Interpretation: Session/window predicates frequently blocked evaluation after the intended Asia-early regime.

### MNQ_FIRST_BEAR_SNAP_TURN_V1
- Classification: `VALID_SELECTIVITY`
- Evaluations: `31`; signals: `0`; near-misses: `5`
- Top failed predicates: `bear_snap_turn_candidate` (31), `first_bear_snap_turn` (31), `bear_snap_reversal_bar` (26), `bear_snap_raw` (26), `bear_snap_location_ok` (26), `bear_snap_range_ok` (23)
- Latest: `MNQ_FIRST_BEAR_SNAP_TURN_NO_SIGNAL_NO_MUTATION` - mnq_first_bear_snap_turn_v1 conditions did not pass: bear_snap_range_ok, bear_snap_body_ok, bear_snap_close_weak, bear_snap_velocity_ok, bear_snap_reversal_bar, bear_snap_raw, bear_snap_turn_candidate, first_bear_snap_turn
- Interpretation: Snap-turn predicates repeatedly required reversal/snap structure; observed regime behaved more like continuation/drift than snap reversal.

### MNQ_FIRST_BULL_SNAP_TURN_V1
- Classification: `VALID_SELECTIVITY`
- Evaluations: `31`; signals: `0`; near-misses: `0`
- Top failed predicates: `bull_snap_raw` (31), `bull_snap_turn_candidate` (31), `first_bull_snap_turn` (31), `bull_snap_close_strong` (30), `bull_snap_velocity_ok` (30), `bull_snap_reversal_bar` (30)
- Latest: `MNQ_FIRST_BULL_SNAP_TURN_NO_SIGNAL_NO_MUTATION` - mnq_first_bull_snap_turn_v1 conditions did not pass: bull_snap_downside_stretch_ok, bull_snap_close_strong, bull_snap_velocity_ok, bull_snap_reversal_bar, bull_snap_location_ok, bull_snap_raw, bull_snap_turn_candidate, first_bull_snap_turn
- Interpretation: Snap-turn predicates repeatedly required reversal/snap structure; observed regime behaved more like continuation/drift than snap reversal.

### asian_drift_v1
- Classification: `ANCHOR_POLICY_TOO_STRICT`
- Evaluations: `31`; signals: `0`; near-misses: `31`
- Top failed predicates: `state_is_entry_eligible` (31), `entry_ready` (31), `entry_window_open` (11)
- Latest: `ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION` - Asian Drift v1 conditions did not pass: state_is_entry_eligible, entry_ready, entry_window_open
- Interpretation: Strong drift was detected after startup, but the required 18:00 ET anchor was missing; diagnostic-only late join would have appeared.

## Recommendations
- Add a dry-run-only late-join Asian Drift research path that records hypothetical entry/continuation outcomes without submit authority.
- Add a gap/drift continuation strategy candidate or shadow relaxations; current P0 set is mostly anchor/session/reversal selective and did not cover the post-reopen drift regime.
- Keep submit-disabled loop available for natural candidate handoff, but do not proceed to guarded PAPER submit from this evidence alone.
- For session strategies, test dry-run-only relaxed session/window predicates against the captured Phase-1 candles.

Recommended next research/remediation slice: `late_join_asian_drift_research_plus_gap_drift_continuation_shadow`

## Safety
This audit was report-only. It did not restart runtime, mutate broker/order/lifecycle state, submit/cancel/close/replace/modify/flatten, invoke paper_proof, enable live money, or change strategy rules.
