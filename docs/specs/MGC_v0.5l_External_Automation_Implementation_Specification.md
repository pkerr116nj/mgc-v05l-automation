# MGC v0.5l External Automation Implementation Specification

## 1. Document Control

- Document Title: MGC v0.5l External Automation Implementation Specification
- Strategy Name: MGC Strategy v0.5l
- Status: Approved baseline derived from finalized release candidate
- Target Instrument: MGC futures
- Execution Context: External automation system, replicating Thinkorswim strategy behavior outside platform-native automation
- Reference Source: Final ThinkScript release candidate labeled MGC Strategy v0.5l
- Version Baseline: v0.5l
- Primary Objective: Reproduce the strategy’s entry, trade-family tagging, risk logic, and exit behavior faithfully in an external automation environment

## 2. Purpose

This specification defines the formal behavior required to implement the MGC v0.5l strategy in an external automated execution framework.

The implementation must preserve the release candidate logic exactly enough that:
- live signals match the intended strategy design
- trade-family-specific management remains intact
- session gating and anti-churn protections are preserved
- external alerts, state handling, and execution logic can be built on a stable and testable contract

## 3. Strategy Summary

MGC v0.5l is a combined early-Asia and standard-session turn/reversal strategy with:
- Bull Snap K-family longs
- Bear Snap K-family shorts
- Early Asia VWAP reclaim longs
- separate, faster exit logic for VWAP longs
- Late Asia disabled beyond the configured Asia window
- London and US behavior unchanged relative to K-family logic

## 4. Scope

In Scope:
- session classification and gating
- ATR, EMA, VWAP, swing, volume, and bar-structure calculations
- Bull Snap long detection
- Bear Snap short detection
- Early Asia VWAP reclaim long detection
- anti-churn / cooldown logic
- trade-family tagging for long trades
- family-specific stop reference and break-even handling
- family-specific exit logic
- strategy-side state tracking
- order-intent generation for long entry, long exit, short entry, short exit
- debug/status outputs suitable for logs or monitoring

Out of Scope:
- broker-specific API implementation
- order routing details beyond order intent and side
- slippage model
- partial scaling logic
- position pyramiding
- multi-contract portfolio logic beyond a single strategy position model
- machine learning overlays
- adaptive parameter optimization
- later regime overlays unless explicitly added in a future version

## 5. Architectural Model

The external automation system shall implement the strategy as an event-driven bar-close evaluation model.

Required evaluation cadence:
- evaluate logic once per completed bar on the chosen chart timeframe

Core architectural components:
1. Market Data Layer
2. Indicator/Feature Layer
3. Signal Layer
4. Trade-State Layer
5. Execution Layer
6. Observability Layer

## 6. Configurable Inputs

Expose the following RC inputs:
- tradeSize
- enableBullSnapLongs
- enableBearSnapShorts
- enableAsiaVWAPLongs
- atrLen
- stopATRMult
- breakevenAtR
- maxBarsLong
- maxBarsShort
- allowAsia
- allowLondon
- allowUS
- asiaStart
- asiaEnd
- londonStart
- londonEnd
- usStart
- usEnd
- antiChurnBars
- useTurnFamily
- turnFastLen
- turnSlowLen
- turnSignalLen
- turnStretchLookback
- minSnapDownStretchATR
- minSnapBarRangeATR
- minSnapBodyATR
- minSnapCloseLocation
- minSnapVelocityDeltaATR
- snapCooldownBars
- useAsiaBullSnapThresholds
- asiaMinSnapBarRangeATR
- asiaMinSnapBodyATR
- asiaMinSnapVelocityDeltaATR
- useBullSnapLocationFilter
- bullSnapMaxCloseVsSlowEMAATR
- bullSnapRequireCloseBelowSlowEMA
- minBearSnapUpStretchATR
- minBearSnapBarRangeATR
- minBearSnapBodyATR
- maxBearSnapCloseLocation
- minBearSnapVelocityDeltaATR
- bearSnapCooldownBars
- useBearSnapLocationFilter
- bearSnapMinCloseVsSlowEMAATR
- bearSnapRequireCloseAboveSlowEMA
- belowVWAPLookback
- requireGreenReclaimBar
- reclaimCloseBufferATR
- minVWAPBarRangeATR
- useVWAPVolumeFilter
- minVWAPVolRatio
- requireHoldCloseAboveVWAP
- requireHoldNotBreakReclaimLow
- requireAcceptanceCloseAboveReclaimHigh
- requireAcceptanceCloseAboveVWAP
- vwapLongStopATRMult
- vwapLongBreakevenAtR
- vwapLongMaxBars
- useVWAPHardLossExit
- vwapWeakCloseLookbackBars
- volLen
- showDebugLabels

## 7. Session Logic

Derived booleans:
- asiaAllowed = allowAsia and isAsia
- londonAllowed = allowLondon and isLondon
- usAllowed = allowUS and isUS
- sessionAllowed = asiaAllowed or londonAllowed or usAllowed
- nonAsiaAllowed = londonAllowed or usAllowed

Behavioral intent:
- Bull Snap longs are allowed in early Asia and non-Asia sessions
- VWAP reclaim longs are only allowed in Asia
- Bear Snap shorts are allowed in any enabled session

## 8. Core Calculations

- tr = TrueRange(high, close, low)
- atr = Wilder’s Average(tr, atrLen)
- barRange = high - low
- bodySize = abs(close - open)
- avgVol = Average(volume, volLen)
- volRatio = volume / avgVol, defaulting to 1 if denominator is zero
- turnEmaFast = EMA(close, turnFastLen)
- turnEmaSlow = EMA(close, turnSlowLen)
- velocity = turnEmaFast - turnEmaSlow
- velocityDelta = velocity - velocity[1]
- vwapVal = VWAP()
- vwapBuffer = reclaimCloseBufferATR * atr

Swing state:
- swingLowConfirmed = low[1] < low[2] and low[1] < low
- swingHighConfirmed = high[1] > high[2] and high[1] > high
- persist lastSwingLow and lastSwingHigh until replaced

## 9. Bull Snap Long Logic

Purpose:
- detect a downside-stretched reversal bar with improving turn velocity

Downside stretch:
- downsideStretch = Highest(high[1], turnStretchLookback) - close

Conditions:
- useTurnFamily = yes
- downside stretch >= minSnapDownStretchATR * atr
- bullish reversal bar with required range, body, and close location
- velocity delta >= applicable threshold
- close > close[1]
- location filter passes if enabled

Session allowance:
- only valid when asiaAllowed or nonAsiaAllowed
- Bull Snap longs enabled

Cooldown:
- valid only if prior Bull Snap candidate count exceeds snapCooldownBars

Final event:
- firstBullSnapTurn

## 10. Early Asia VWAP Reclaim Long Logic

Purpose:
- capture a reclaim-and-acceptance sequence above VWAP during Asia

Reclaim bar requires:
- asiaAllowed
- Asia VWAP longs enabled
- at least one close below VWAP within belowVWAPLookback
- close > vwapVal + vwapBuffer
- reclaim bar color rule passes
- range threshold passes
- volume rule passes if enabled
- close > close[1]

Persist when reclaim occurs:
- reclaim bar low
- reclaim bar high
- reclaim bar VWAP

Hold bar:
- next bar after reclaim
- may require close above reclaim VWAP and low not breaking reclaim low

Acceptance bar:
- following bar after hold
- may require close above reclaim high
- may require close above reclaim VWAP
- prior hold must be valid

Anti-churn:
- fresh VWAP signal only if prior Asia VWAP signal older than antiChurnBars

Final event:
- asiaVWAPLongSignal

## 11. Bear Snap Short Logic

Purpose:
- detect an upside-stretched bearish reversal with deteriorating turn velocity

Upside stretch:
- upsideStretch = close - Lowest(low[1], turnStretchLookback)

Conditions:
- useTurnFamily = yes
- upside stretch >= minBearSnapUpStretchATR * atr
- bearish reversal bar with required range, body, and close location
- velocity delta <= negative threshold
- close < close[1]
- location filter passes if enabled

Session allowance:
- valid only when sessionAllowed
- Bear Snap shorts enabled

Cooldown:
- valid only if prior Bear Snap candidate count exceeds bearSnapCooldownBars

Final event:
- firstBearSnapTurn

## 12. Entry Resolution

- longEntryRaw = firstBullSnapTurn or asiaVWAPLongSignal
- shortEntryRaw = firstBearSnapTurn

Track:
- barsSinceLongSetup
- barsSinceShortSetup
- recentLongSetup
- recentShortSetup

Final long entry:
- longEntryRaw true and anti-churn conditions satisfied

Final short entry:
- shortEntryRaw true and anti-churn conditions satisfied

## 13. Position State and Trade Family Tagging

Long trade families:
- 0 = none/flat
- 1 = K long
- 2 = VWAP long

Tagging rules:
- if long entry comes from asiaVWAPLongSignal, tag 2
- if long entry comes from firstBullSnapTurn, tag 1
- if both occur together, VWAP family takes precedence
- reset to 0 when flat

## 14. Risk and Stop Reference Logic

- K long stop base = Lowest(low, 3) - stopATRMult * atr
- K short stop base = Highest(high, 3) + stopATRMult * atr
- VWAP long stop base = asiaReclaimBarLow - vwapLongStopATRMult * atr
- active long stop base = VWAP stop if long family is VWAP, else K long stop
- long risk = max(0.01, entryPrice - activeLongStopRefBase)
- short risk = max(0.01, kShortStopRefBase - entryPrice)

## 15. Break-Even Logic

Long BE:
- VWAP long: arm when high reaches entry + vwapLongBreakevenAtR * longRisk
- K long: arm when high reaches entry + breakevenAtR * longRisk

Short BE:
- arm when low reaches entry - breakevenAtR * shortRisk

Effective stop after BE:
- long stop = max(activeLongStopRefBase, entryPrice)
- short stop = min(kShortStopRefBase, entryPrice)

## 16. Bars-in-Trade Logic

- set to 1 on entry
- increment by one while in position
- reset to 0 when flat

## 17. Exit Logic

K long exits on:
- close < longStopRef
- close < lastSwingLow
- integrity failure
- barsInTrade >= maxBarsLong

VWAP long exits on:
- close < longStopRef
- VWAP hard-loss exit if enabled and close < vwapVal
- weak follow-through
- barsInTrade >= vwapLongMaxBars

Short exits on:
- close > shortStopRef
- close > lastSwingHigh
- integrity failure
- barsInTrade >= maxBarsShort

## 18. Position Side State

Persist:
- 1 = long
- -1 = short
- 0 = flat

Transitions:
- set on entry
- reset to flat on exit
- otherwise persist prior value

## 19. External Order Intent Specification

Logical intents:
- BUY_TO_OPEN on longEntry
- SELL_TO_CLOSE on longExit
- SELL_TO_OPEN on shortEntry
- BUY_TO_CLOSE on shortExit

External implementation:
- evaluate at completed bar close
- submit executable order at next live opportunity
- document live vs ThinkScript fill differences

## 20. Logging and Reason Codes

Minimum log fields:
- timestamp
- session classification
- OHLCV
- ATR
- VWAP
- EMA fast/slow
- velocity
- velocity delta
- Bull Snap raw/final
- Bear Snap raw/final
- reclaim/hold/accept states
- final longEntry/shortEntry
- longEntryFamily
- inPosition
- strategySide
- barsInTrade
- stop references
- BE armed flags
- longExit/shortExit
- exit reason code

## 21. Acceptance Criteria

Must demonstrate:
- signal parity
- family tagging parity
- stop reference parity
- break-even parity
- exit behavior parity
- cooldown and anti-churn preservation

## 22. Known Sensitivities

- ThinkScript rec statefulness must be explicit
- EntryPrice/inPosition semantics must be implemented explicitly
- ThinkScript next-bar-open fill assumption must be documented
- VWAP stop anchoring depends on persisted reclaim low
- VWAP precedence when both long triggers fire
- session clock consistency is critical

## 23. Recommended External Module Breakdown

- session_engine
- indicator_engine
- swing_tracker
- bull_snap_detector
- bear_snap_detector
- asia_vwap_reclaim_detector
- trade_state_manager
- risk_manager
- exit_engine
- execution_adapter
- audit_logger
