# MGC v0.5l External Automation Build - Phase 2.5

## Concrete Technical Design

### 1. Goal

Turn architecture into a build-ready design with:
- concrete components
- explicit state schema
- order lifecycle rules
- failure handling behavior
- persistence model
- first implementation blueprint

### 2. Proposed Runtime Topology

Use a single strategy service with internal modules, plus external dependencies:
- broker/API
- database
- notification channel

### 3. Component Diagram

A. Market Data Gateway
- receives MGC trade/bar data
- outputs normalized bar-close events

B. Session Clock / Time Normalizer
- determines Asia/London/US membership
- attaches session flags

C. Feature Engine
- computes ATR, EMA fast/slow, velocity, velocity delta, VWAP, average volume, volume ratio, swing confirmation, bar body/range/location metrics

D. Signal Engine
- evaluates Bull Snap long
- Bear Snap short
- Asia reclaim/hold/acceptance
- anti-churn and cooldown
- final long/short entries

E. Strategy State Engine
- strategy mode
- position state
- entry family
- reclaim anchor state
- bars in trade
- BE flags
- cooldown counters
- last signal and order timestamps

F. Exit / Risk Engine
- active stop refs
- risk now
- break-even arming
- family-specific exits
- reason code priority

G. Execution Engine
- receives intents
- deduplicates
- converts to broker orders
- updates state only after actual confirmation

H. Reconciliation Engine
- compares internal state vs broker state vs open orders
- repairs or faults

I. Persistence Layer
- stores bars, features, signals, state, orders, fills, reconciliation, faults

J. Monitoring / Operator Layer
- current session
- current side
- latest signal
- position/family
- stop refs
- health flags
- disable / halt entries / flatten and halt / recover from fault

### 4. First-Pass Code Structure

```text
mgc_v05l/
  config/
    settings.yaml
    production.yaml
    paper.yaml
    replay.yaml
  app/
    main.py
    runner.py
  domain/
    models.py
    enums.py
    events.py
    state_machine.py
  market_data/
    gateway.py
    bar_builder.py
    session_clock.py
  indicators/
    feature_engine.py
    vwap_engine.py
    swing_tracker.py
  signals/
    bull_snap.py
    bear_snap.py
    asia_vwap_reclaim.py
    entry_resolver.py
  strategy/
    strategy_engine.py
    trade_state.py
    risk_engine.py
    exit_engine.py
    reconcile.py
  execution/
    execution_engine.py
    broker_adapter.py
    order_router.py
  persistence/
    db.py
    repositories.py
    state_store.py
  monitoring/
    logger.py
    alerts.py
    health.py
  tests/
    ...
```

### 5. Core Domain Model

Enums:
- StrategyStatus
- PositionSide
- LongEntryFamily
- OrderIntentType
- ExitReason
- HealthStatus

Primary state object fields:
- strategy_status
- position_side
- broker_position_qty
- internal_position_qty
- entry_price
- entry_timestamp
- entry_bar_id
- long_entry_family
- bars_in_trade
- long_be_armed
- short_be_armed
- last_swing_low
- last_swing_high
- asia_reclaim_bar_low
- asia_reclaim_bar_high
- asia_reclaim_bar_vwap
- bars_since_bull_snap
- bars_since_bear_snap
- bars_since_asia_reclaim
- bars_since_asia_vwap_signal
- bars_since_long_setup
- bars_since_short_setup
- last_signal_bar_id
- last_order_intent_id
- open_broker_order_id
- entries_enabled
- exits_enabled
- operator_halt
- reconcile_required
- fault_code
- updated_at

### 6. Bar Model

Bar schema fields:
- bar_id
- symbol
- timeframe
- start_ts
- end_ts
- open
- high
- low
- close
- volume
- is_final
- session_asia
- session_london
- session_us
- session_allowed

Rule:
- one and only one strategy evaluation per bar_id

### 7. Feature Packet

Fields:
- bar_id
- tr
- atr
- bar_range
- body_size
- avg_vol
- vol_ratio
- turn_ema_fast
- turn_ema_slow
- velocity
- velocity_delta
- vwap
- vwap_buffer
- swing_low_confirmed
- swing_high_confirmed
- last_swing_low
- last_swing_high
- downside_stretch
- upside_stretch
- bull_close_strong
- bear_close_weak

### 8. Signal Packet

Fields include:
- Bull Snap subfields
- Bear Snap subfields
- VWAP reclaim/hold/acceptance subfields
- long_entry_raw
- short_entry_raw
- recent_long_setup
- recent_short_setup
- long_entry
- short_entry
- long_entry_source
- short_entry_source

### 9. State Machine Rules

Allowed states:
- DISABLED
- READY
- IN_LONG_K
- IN_LONG_VWAP
- IN_SHORT_K
- RECONCILING
- FAULT

Illegal transitions:
- direct long -> short
- direct short -> long
- any in-position -> another in-position without flatten
- READY -> READY with new entry fill while broker already has position

### 10. Strategy Engine Evaluation Order

While flat:
1. load state
2. verify health
3. verify warm-up complete
4. verify no pending unresolved order
5. compute features
6. update sessions
7. compute signals
8. update counters/anti-churn
9. decide entry
10. persist pre-order snapshot
11. send order intent
12. on fill update live state

While in position:
1. load state
2. verify health
3. compute features
4. update swing state
5. compute exit state
6. compute stop refs and BE
7. decide exit
8. persist pre-order snapshot
9. send exit order if needed
10. on fill transition to READY

### 11. Warm-Up Logic

No entries until enough history exists for:
- ATR
- slow EMA
- stretch lookback
- below-VWAP lookback
- swing confirmation
- average volume

Suggested:

```text
warmup_bars_required = max(
  atrLen,
  turnSlowLen,
  turnStretchLookback + 2,
  belowVWAPLookback,
  volLen,
  10
)
```

### 12. Persistence Schema

Tables:
- bars
- features
- signals
- strategy_state
- order_intents
- fills
- reconciliation_events
- fault_events

### 13. Order Lifecycle

Entry:
- generate intent
- submit
- ack
- fill
- persist fill and updated state

Exit:
- same flow, then reset state to flat on fill

Pending-order protection:
- no second entry while entry pending
- no second exit while exit pending
- no opposite-side order while pending unresolved order exists

### 14. Broker Adapter Contract

Required methods:
- connect
- disconnect
- is_connected
- get_latest_bars
- subscribe_bars
- submit_order
- cancel_order
- get_order_status
- get_open_orders
- get_position
- get_account_health

### 15. Recommended Order Policy for v1

- market order or aggressive marketable order after bar close
- market order for exits
- prioritize protective behavior over price improvement

### 16. Reconciliation Design

Startup reconciliation:
- load latest state
- query broker position/open orders
- compare and resolve or halt

Periodic reconciliation:
- on reconnect
- on rejected order
- on missing fill ack
- on timer
- after manual operator action

Outcomes:
- safe repair
- unsafe ambiguity -> halt entries / RECONCILING / FAULT

### 17. Failure Matrix

Market data failures:
- missing bar
- duplicate bar
- out-of-order bar
- stale feed

Broker failures:
- order rejected
- ack missing
- fill missing
- disconnected flat
- disconnected in position

Persistence failures:
- cannot write state
- cannot read state at startup

Invariant failures:
- both long and short active
- long family set while flat
- inconsistent bars_in_trade

### 18. Operator Commands

- enable_strategy
- disable_strategy
- halt_entries
- resume_entries
- flatten_and_halt
- force_reconcile
- acknowledge_fault

### 19. Health Model

Composite health from:
- market data connectivity
- broker connectivity
- persistence availability
- reconciliation cleanliness
- invariant integrity

### 20. Testing Blueprint

- unit tests
- stateful tests
- integration tests
- replay tests

### 21. Build Sequence

1. domain models/enums
2. session clock, feature engine, signal engine
3. replay harness
4. state engine and persistence
5. exit/risk engine
6. execution engine and broker adapter
7. reconciliation
8. monitoring/operator controls
9. paper soak test

### 22. Remaining decisions already locked for this build

- exact timeframe = 5m
- timezone basis = America/New_York
- replay fill convention = NEXT_BAR_OPEN
- VWAP policy = SESSION_RESET
