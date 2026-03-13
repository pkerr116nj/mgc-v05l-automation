# MGC v0.5l External Automation Build - Phase 3B

## Implementation Rules Addendum

### 1. Purpose

Define non-negotiable implementation rules to eliminate drift from:
- bar indexing mistakes
- timing ambiguity
- session interpretation differences
- fill model assumptions
- VWAP interpretation differences
- state update mismatches

### 2. Locked Build Decisions

- replay first
- then paper
- live broker support only as interface/stub
- SQLite for v1
- single symbol only: MGC
- completed-bar evaluation only
- exactly one evaluation per finalized bar
- deterministic sequential processing

### 3. Explicit Locked Values

- TIMEFRAME = 5m
- SESSION_TIMEZONE = America/New_York
- REPLAY_FILL_POLICY = NEXT_BAR_OPEN
- VWAP_POLICY = SESSION_RESET
- REPLAY_DATA_COLUMNS = timestamp,open,high,low,close,volume

### 4. Bar Semantics

- no decisions on partial bars
- one evaluation per completed bar
- processed bar IDs must be persisted
- bars processed strictly in chronological order
- duplicates ignored
- out-of-order bars rejected and logged
- recommended bar_id format: MGC|5m|<bar_end_ts>

### 5. ThinkScript Indexing Translation Rules

- translate x[1], x[2] using finalized prior bars only
- preserve ThinkScript meaning, not syntax
- current bar, previous bar, two bars ago handled explicitly in history context
- swing confirmation semantics must match RC exactly
- do not substitute generic pivot libraries
- rec variables must be explicit persisted or rolling state:
  - lastSwingLow
  - lastSwingHigh
  - barsSinceBullSnap
  - barsSinceBearSnap
  - barsSinceAsiaReclaim
  - barsSinceAsiaVWAPSignal
  - barsSinceLongSetup
  - barsSinceShortSetup
  - longBEArmed
  - shortBEArmed
  - barsInTrade
  - strategySide
  - longEntryFamily

### 6. Session Logic Rules

- sessions are config-driven
- cross-midnight handling must match RC
- classify by bar end timestamp
- store session flags on each bar

### 7. Indicator Math Conventions

- ATR = Wilder's Average, not SMA ATR
- EMA semantics consistent with ThinkScript
- velocity = emaFast - emaSlow
- velocityDelta = velocity - prior_velocity
- volRatio = volume / avgVol, default 1 on zero denominator
- barRange = high - low
- bodySize = abs(close - open)
- preserve close location formulas exactly

### 8. VWAP Rules

- VWAP policy explicit: SESSION_RESET
- do not guess continuous vs session-reset
- persist reclaim bar low/high/VWAP on reclaim
- preserve 3-stage reclaim -> hold -> acceptance sequence

### 9. Entry Semantics

- long from firstBullSnapTurn or asiaVWAPLongSignal
- short from firstBearSnapTurn
- preserve anti-churn counters
- VWAP takes precedence if both long triggers occur on same bar
- one position at a time only

### 10. Stop, Risk, and Break-Even Rules

- preserve risk floor max(0.01, ...)
- K long stop base = lowest low of last 3 bars - stopATRMult * atr
- K short stop base = highest high of last 3 bars + stopATRMult * atr
- VWAP long stop base = reclaim low - vwapLongStopATRMult * atr
- active long stop base depends on family
- BE computed off live entry and current risk context
- VWAP long BE at vwapLongBreakevenAtR
- K long BE at breakevenAtR
- short BE at breakevenAtR
- after BE arm: long stop promoted to at least entry, short stop promoted to at most entry

### 11. Exit Semantics

- evaluate exits on every completed bar in position
- K long exits: stop breach, swing-low breach, integrity fail, max bars
- VWAP long exits: stop breach, VWAP loss if enabled, weak follow-through, max bars
- short exits: stop breach, swing-high breach, integrity fail, max bars

Primary reason priority:

K long:
1. LONG_STOP
2. LONG_SWING_FAIL
3. LONG_INTEGRITY_FAIL
4. LONG_TIME_EXIT

VWAP long:
1. LONG_STOP
2. VWAP_LOSS
3. VWAP_WEAK_FOLLOWTHROUGH
4. VWAP_TIME_EXIT

Short:
1. SHORT_STOP
2. SHORT_SWING_FAIL
3. SHORT_INTEGRITY_FAIL
4. SHORT_TIME_EXIT

### 12. Fill Model Rules

- replay fill policy must be explicit: NEXT_BAR_OPEN
- paper fill policy deterministic and configurable
- do not bury fill behavior in broker code

### 13. State Update Rules

- transitions only on confirmed fills, not intents
- bars_in_trade = 1 on entry fill, increment once per completed in-position bar, reset to 0 on exit
- flat reset clears side, qty, entry price, entry timestamp, entry bar id, long family, BE flags
- preserve RC behavior for reclaim anchors

### 14. Warm-Up Rules

- no entries before warm-up complete
- suggested formula:
  max(atrLen, turnSlowLen, turnStretchLookback + 2, belowVWAPLookback, volLen, 10)
- exits must still work if restoring into an open position during warm-up suppression

### 15. Invariants

Fault if:
- FLAT with nonzero qty
- FLAT with long_entry_family != NONE
- FLAT with entry_price not None
- LONG with entry_price None
- LONG with internal_position_qty <= 0
- LONG with long_entry_family = NONE
- SHORT with entry_price None
- SHORT with internal_position_qty <= 0
- bars_in_trade < 0
- cooldown counters negative
- simultaneous long and short state implied
- opposite-side pending orders simultaneously

### 16. Reconciliation Rules

- startup reconciliation required
- no new entries during unresolved uncertainty
- exits remain priority if state is degraded

### 17. Logging Rules

Every processed bar log:
- bar ID
- timestamp
- session flags
- features
- key signals
- state before
- state after
- any order intent created

Every order logs lifecycle:
- created
- submitted
- acknowledged
- filled/rejected/cancelled

Every fault logs:
- exact invariant or cause
- bar ID
- state snapshot
- order IDs if relevant
- reconciliation status

### 18. Codex Must Not Do

- replace session logic with generic market-hours library
- replace swing logic with generic pivot indicator
- replace Wilder ATR with SMA ATR
- infer family from later bars instead of entry source
- combine K and VWAP long exits into generic long exit
- update trade state on signal instead of fill
- silently choose timezone
- silently choose VWAP reset behavior
- silently choose replay fill behavior
- add multi-symbol support in v1
- add optimization logic in v1
- skip tests
