# MGC v0.5l External Automation Build - Phase 2

## System Architecture and Execution Design

### 1. Objective

Build a system that can:
- ingest market data
- evaluate MGC v0.5l logic on completed bars
- maintain persistent trade state
- generate executable order intents
- manage risk and exits
- survive disconnects and restarts
- produce a verifiable audit trail

### 2. Design Principles

- Bar-close deterministic evaluation
- Single source of truth for position state
- Stateful, restart-safe architecture
- Separation of concerns
- Fail closed, not fail open

### 3. High-Level Architecture

1. Market Data Adapter
2. Bar Builder / Time Normalizer
3. Indicator and Feature Engine
4. Signal Engine
5. Trade State Engine
6. Risk and Exit Engine
7. Execution Engine
8. Broker Adapter
9. Persistence Layer
10. Monitoring and Alerting Layer
11. Operator Console / Control Layer

Processing order per completed bar:
1. Receive raw market data
2. Build or confirm bar close
3. Update indicators and persistent features
4. Evaluate session state
5. Evaluate entry candidates
6. Evaluate active trade management and exits
7. Resolve allowed action
8. Send order intent if required
9. Persist state and logs
10. Notify operator if action or fault occurred

### 4. Runtime Model

- fixed bar timeframe matching release candidate test timeframe
- evaluate once per completed bar
- compute signals after bar close
- submit order as soon as possible after close confirmation
- record actual broker timestamp and fill
- document backtest/live fill drift

### 5. Core Modules

#### 5.1 Market Data Adapter

Responsibilities:
- subscribe to MGC market data
- ingest tick or bar feed
- provide OHLCV and timestamp data
- provide session-consistent clock basis
- detect stale feed or missing bars

Requirements:
- normalize timestamps to one timezone
- reject or quarantine out-of-order data
- prevent duplicate bars from duplicate evaluations
- detect missing bars

Failure behavior:
- stop new entries on stale/incomplete/inconsistent data
- continue logging
- allow protective exits if needed

#### 5.2 Bar Builder / Time Normalizer

Responsibilities:
- assemble bars if using tick feed, or validate bars if using broker bars
- define bar-close events
- classify bars into Asia/London/US windows

Requirements:
- exact session rules must match implementation spec
- cross-midnight handling must be explicit
- one and only one evaluation event per completed bar

#### 5.3 Indicator and Feature Engine

Computes and persists:
- true range
- ATR
- EMA fast/slow
- velocity
- velocity delta
- VWAP
- average volume
- volume ratio
- swing highs/lows
- bar structure metrics
- reclaim-state anchors

Requirements:
- deterministic and reproducible calculations
- warm-up enforced before signals allowed
- all indicator values logged per decision bar

#### 5.4 Signal Engine

Evaluates:
- Bull Snap long candidate
- Bear Snap short candidate
- Asia VWAP reclaim sequence
- cooldowns
- anti-churn windows
- final long and short entry conditions

Outputs:
- structured signal packet

#### 5.5 Trade State Engine

Tracks:
- flat/long/short state
- current position quantity
- average entry price
- active long entry family
- bars in trade
- BE armed status
- active stop references
- reclaim anchors for VWAP longs
- cooldown counters
- last signal timestamps
- last execution timestamps

Canonical states:
- FLAT
- LONG_K
- LONG_VWAP
- SHORT_K

Restart rule:
- restore persisted state before evaluating next bar

#### 5.6 Risk and Exit Engine

Responsibilities:
- compute live stop references
- compute risk-now
- arm break-even logic
- evaluate K long exits
- evaluate VWAP long exits
- evaluate short exits
- produce exit reason codes

Priority rule:
- close position once
- log all true exit conditions
- assign one primary reason by priority

#### 5.7 Execution Engine

Responsibilities:
- transform logical intents into executable actions
- de-duplicate submissions
- enforce one-position-at-a-time
- prevent opposite-side entries without flattening
- track acknowledgement/fills
- handle timeouts and retries

#### 5.8 Broker Adapter

Responsibilities:
- connect to broker API
- submit orders
- receive acknowledgements/fills
- query positions and open orders
- reconcile execution status

Requirement:
- freeze new entries if broker state disagrees with internal state

#### 5.9 Persistence Layer

Persist:
- strategy state
- bar history
- signal history
- order intents
- execution events
- reconciliation events
- faults

Recommended tables:
- bars
- indicator_values
- signal_events
- strategy_state
- order_intents
- broker_orders
- fills
- reconciliation_events
- fault_events

#### 5.10 Monitoring and Alerting Layer

Notify operator of:
- entries
- exits
- rejected orders
- stale data
- broker disconnects
- reconciliation mismatches
- state restore failures
- session transitions if useful

#### 5.11 Operator Console / Control Layer

Minimum controls:
- enable/disable strategy
- flatten and halt
- halt new entries only
- resume after review
- inspect current state
- latest signals
- latest logs/faults

### 6. State Machine Design

Operating states:
- DISABLED
- READY
- IN_LONG_K
- IN_LONG_VWAP
- IN_SHORT_K
- FAULT
- RECONCILING

Transition rules:
- DISABLED -> READY after enable + broker connected + data healthy + warm-up complete + state loaded
- READY -> IN_LONG_K on Bull Snap long fill
- READY -> IN_LONG_VWAP on VWAP long fill
- READY -> IN_SHORT_K on Bear Snap short fill
- in-position -> READY after exit fill confirms flat
- any state -> RECONCILING on restart/mismatch/uncertain open orders
- any state -> FAULT on persistence failure, unrecoverable broker issue, or state ambiguity

### 7. Entry and Exit Sequencing Rules

Entry:
1. compute signals
2. confirm READY
3. confirm flat
4. confirm no unresolved mismatch
5. confirm no duplicate order for bar
6. submit entry order
7. persist order intent
8. wait for fill/ack
9. update live state only on fill

Exit:
1. compute exits
2. if exit true, submit exit order
3. persist exit intent with reason codes
4. wait for fill/ack
5. set flat only on actual fill

### 8. Reconciliation Logic

Triggers:
- startup
- broker reconnect
- rejected order
- missing fill acknowledgement
- scheduled heartbeat
- internal vs broker position mismatch

Checks:
- internal qty vs broker qty
- internal side vs broker side
- internal avg price vs broker avg price
- open orders internal vs broker
- last fill timestamp internal vs broker

Resolution:
- safe repair if explainable
- otherwise halt new entries and move to RECONCILING or FAULT

### 9. Failure Handling and Safeguards

Data failures:
- no new entries
- continue monitoring
- allow exits if needed

Broker failures:
- freeze new entries
- attempt reconciliation
- escalate if open risk exists

Persistence failures:
- do not continue normal trading
- enter FAULT

Logic invariant violations:
- critical log
- freeze strategy
- enter FAULT

### 10. Audit Trail Requirements

For each bar log:
- bar timestamp
- OHLCV
- session flags
- indicators
- raw/final signals
- active state
- exit conditions true
- whether an order was generated

For each order log:
- order intent ID
- strategy bar ID
- side
- order type
- quantity
- submit timestamp
- broker order ID
- ack status
- fill status
- fill timestamp
- fill price
- reason code

For reconciliation log:
- trigger source
- internal snapshot
- broker snapshot
- mismatch classification
- repair action

### 11. Deployment Environments

- Research/replay
- Paper/simulation
- Production

### 12. Validation Plan

Prove:
- signal parity
- restart safety
- broker mismatch handling
- data integrity handling
- operator controls

### 13. Recommended Build Sequence

Phase 2A:
- bar ingestion
- indicator engine
- signal engine
- replay harness

Phase 2B:
- persistent state
- state machine
- restart restore
- logging

Phase 2C:
- broker adapter
- execution engine
- reconciliation

Phase 2D:
- monitoring
- operator controls
- fail-safe handling

Phase 2E:
- paper trade soak test

### 14. Recommended Technology Posture

- one always-on strategy process
- durable database
- structured logging
- broker adapter abstraction
- deterministic replay
- config-driven parameters
- explicit environment separation

Suggested first implementation:
- Python service
- strategy engine process
- database
- broker adapter module
- simple dashboard or command console
