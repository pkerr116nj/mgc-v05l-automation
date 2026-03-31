# Pattern Engine Promotions

## 2026-03-17: ASIA_EARLY SHORT pause_rebound_resume_short

- Branch name: `asiaEarlyPauseResumeShortTurn`
- Family: `pause_rebound_resume_short`
- Session: `ASIA_EARLY`
- Separator used: `setup curvature_state = CURVATURE_FLAT`
- Promotion status: `probationary baseline inclusion`

Why promoted:
- first promoted Pattern Engine v1 branch with positive replay economics
- added selective new short participation without disturbing existing short branches
- improved total net pnl, expectancy, and max drawdown on the research replay

Collision finding:
- one known same-window arbitration on `2025-12-01 18:50 ET`
- the promoted Asia short was already active and prevented an overlapping `firstBullSnapTurn` long
- that displaced long was a losing `LONG_INTEGRITY_FAIL` trade for `-21.0`
- this is recorded as an intentional same-position precedence outcome, not unexplained drift

Working interpretation:
- when the Asia-early short branch already occupies the position slot in that contested window,
  `firstBullSnapTurn` does not open the overlapping long
- promotion approved because the only detected interaction was a single, explainable,
  economically favorable same-window collision that removed a losing long without disturbing
  the broader branch structure

## 2026-03-17: US_LATE LONG pause_pullback_resume_long

- Branch name: `usLatePauseResumeLongTurn`
- Family: `pause_pullback_resume_long`
- Session: `US_LATE`
- Separator used: `setup curvature_state = CURVATURE_POS`
- Explicit operator rule: exclude `US_LATE` signals whose replay fill would occur on the
  `17:55 ET` post-break next-bar open
- Promotion status: `probationary baseline inclusion`

Why promoted:
- remained economically strong after removing the carryover-contaminated `17:55 ET` cases
- improved total net pnl, expectancy, and max drawdown versus the promoted-Asia baseline
- all added value remained cleanly in the US session after applying the carryover rule

Interaction result:
- cleaned branch added `+500.0` on `17` trades
- no existing family changed
- collision audit found no overlapping-family displacement or structural interaction

Working interpretation:
- promotion approved because the branch remained economically strong after removing
  `17:55 ET` post-break next-bar-open carryover cases, all added value remained cleanly
  in the US session, and no existing family displacement or collision effect appeared

## 2026-03-17: ASIA_EARLY LONG breakout_retest_hold

- Branch name: `asiaEarlyNormalBreakoutRetestHoldTurn`
- Family: `breakout_retest_hold`
- Session: `ASIA_EARLY`
- Qualifiers:
  - `breakout slope_state = SLOPE_FLAT`
  - `breakout expansion_state = NORMAL`
- Promotion status: `probationary baseline inclusion`

Why promoted:
- the containment version remained meaningfully additive after narrowing the broad Asia breakout lane
- improved total net pnl and expectancy versus the promoted baseline while keeping all added value in `ASIA`
- preserved the breakout-retest-hold shape instead of drifting into a different continuation/reversal subtype

Economic summary:
- `total_net_pnl: 2005.0 -> 2917.0` (`+912.0`)
- `expectancy: 12.0783 -> 13.6308`
- `trade_count: 166 -> 214`
- `long_side_pnl: 887.0 -> 1799.0` (`+912.0`)
- `ASIA_session_pnl: 491.0 -> 1403.0` (`+912.0`)

Interaction result:
- `firstBearSnapTurn`: unchanged exactly
- `firstBullSnapTurn`: unchanged exactly
- `asiaEarlyPauseResumeShortTurn`: unchanged exactly
- collision audit found `0` changed trades across the checked baseline families

Working interpretation:
- promotion approved because the containment step preserved substantial Asia-side value
  while removing the prior structural same-window overlap, leaving the branch isolated enough
  for probationary baseline inclusion

Tracked close-but-no note:
- keep the excluded expanded retest/hold traffic as a monitored research candidate, not a
  promoted branch
- the broader `ASIA_EARLY LONG breakout_retest_hold + breakout.slope_state = SLOPE_FLAT`
  variant produced much larger headline lift, but its expanded retest/hold traffic created
  structural same-window overlap with `firstBearSnapTurn`
- the `breakout expansion_state = NORMAL` containment rule is what made the promoted branch
  isolation-clean, so the excluded expanded traffic should only be revisited later with more data

Tracked close-but-no note:
- `ASIA_EARLY SHORT breakout_retest_hold + breakout->retest expansion_state = EXPANDED -> EXPANDED`
  was tested and failed direct replay A/B
- the separator was economically real on the Pattern Engine lane surface, but did not convert
  into executable additive value against the promoted baseline
- replay economics weakened modestly (`-38.0` pnl, lower expectancy, higher drawdown), so no
  collision or interaction audit was warranted
- keep it as a revisit-later candidate only: it still reads as genuine aggressive
  `breakout_retest_hold` behavior, not neighboring reversal-family contamination, but the current
  representation is replay-non-additive rather than structurally invalid

Tracked close-but-no note:
- `US_MIDDAY SHORT failed_move_reversal`
  with anchored lead `reversal.expansion_state = COMPRESSED`
  and refined test branch
  `reversal.expansion_state = COMPRESSED`
  plus
  `failed_move->reversal ema_location_state = ABOVE_BOTH_FAST_GT_SLOW -> REBOUND_ABOVE_SLOW`
  was tested and held below escalation quality
- the anchored lead failed direct replay A/B
- the refined paired branch improved replay modestly, but not strongly enough to justify
  promotion-track escalation
- pnl improved slightly and drawdown improved on the refined cut, but expectancy still slipped
  versus control, so no plainly strong executable additive edge was established
- existing promoted and baseline families remained unchanged at the summary level, so no
  collision or interaction audit was warranted
- keep it as a revisit-later candidate only: it still reads as genuine `failed_move_reversal`
  behavior rather than family contamination, but the current representation is borderline
  non-additive in executable replay

Tracked close-but-no note:
- `US_LATE LONG failed_move_reversal + failed_move.curvature_state = CURVATURE_POS`
  was tested and held below escalation quality
- the separator is economically real on the Pattern Engine lane surface
- direct replay A/B improved pnl and reduced drawdown, but expectancy still slipped versus
  control
- some of the improvement appears to come from internal late-long reassignment rather than
  clean isolated new value, so the gain was not clearly additive at the summary level
- no collision or interaction audit was warranted
- keep it as a revisit-later candidate only: it still reads as genuine
  `failed_move_reversal` behavior rather than family leakage, but the current representation
  is real without being strong enough in executable replay form

Tested but not currently actionable:
- `LONDON_OPEN SHORT failed_move_reversal`
  completed separator-only review without producing an A/B-ready first branch
- the lane still reads as genuine `failed_move_reversal` behavior
- containment quality was weak: no repeatable separator improved both directional behavior
  and `mfe/mae` together in a convincing way
- the lead cut stayed too broad and noisy for a narrow A/B, and the thin EMA-transition
  pocket was interesting but too small to prioritize
- do not run A/B on this lane now and do not force more mining inside this exact lane now;
  keep it as tested but not currently actionable
