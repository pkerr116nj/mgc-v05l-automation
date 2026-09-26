# Index Options Compression Research & Execution Program — Initial Project Plan

Status: **GOVERNING PROGRAM PLAN**
Branch: `codex/ndxp-multidte-credit-research`
Program scope: research, replay, paper execution, and eventual controlled live execution
Initial instruments: **NDXP, SPX/SPXW, QQQ, RUT/RUTW**

## 1. Program objective

Determine whether rich-credit, near-ATM/ITM short vertical spreads exhibit a durable and executable compression-scalp opportunity across major index/index-adjacent options, and if so, turn the validated opportunity into a causal, machine-observable, replayable, paper-tradable, and eventually controlled-live execution system.

The program is not limited to proving a single NDXP backtest. It must answer:

1. Does the compression phenomenon exist structurally?
2. Does it survive higher-resolution path analysis?
3. Is it realistically executable with resting and reactive complex orders?
4. What market-state features distinguish the best entries and worst failures?
5. Is the effect portable across NDXP, SPX, QQQ, and RUT?
6. What management rules improve expectancy without destroying the edge?
7. Can the behavior be reproduced prospectively by a deterministic engine?
8. What capital, drawdown, concurrency, and tail-risk profile results?

## 2. Core economic hypothesis

A rich-credit short vertical may have favorable economics because:

- a larger opening credit sharply reduces maximum remaining economic loss;
- 2–3DTE (and later nearby maturities) gives the directional thesis time to work;
- fast opening moves can create large, temporary changes in spread value;
- resting profit-taking orders may capture brief intraday sweeps that one-minute sampled data cannot model cleanly;
- the opportunity may be a repeatable compression process rather than an expiration-hold strategy.

The program must test, not assume, each part of this hypothesis.

## 3. Instruments

Analyze separately first:

- **NDXP** — initial laboratory and current richest historical work.
- **SPX/SPXW** — expected benchmark for liquidity and institutional complex-order behavior.
- **QQQ** — Nasdaq-linked ETF options control with different settlement/exercise characteristics and very high liquidity.
- **RUT/RUTW** — additional cash-settled index-options control.

Do not pool instruments into one sample until instrument-specific behavior is understood.

## 4. Evidence hierarchy

Every claim must be labeled by evidence level:

### Level 1 — Structural
Minute-level history shows a recurring pattern.

### Level 2 — Path
Second-level history confirms the path and timing of compression/adverse excursion.

### Level 3 — Executable
Event-level quote/trade data supports plausible complex-order fills.

### Level 4 — Conditional
Causal entry-state features improve results out of sample.

### Level 5 — Replay
A deterministic engine reproduces the strategy from historical event streams.

### Level 6 — Prospective paper
Unseen live markets reproduce the setup and execution behavior in real time without capital.

### Level 7 — Controlled live
Small-capital deployment validates actual broker/exchange execution assumptions.

A lower evidence level must never be described as if it proved a higher one.

## 5. Resolution architecture

Use a layered data strategy.

### Long-run structural layer
Use lower-frequency historical data, preferably back to the earliest practical coverage, to test:
- DTE;
- credit;
- width;
- moneyness;
- year/regime stability;
- broad target behavior.

### High-resolution path layer
For periods where second-level OPRA history is available, reconstruct:
- opening spread dynamics;
- target crossings;
- adverse excursions;
- time-to-target;
- transient sweeps;
- opening-window behavior.

### Event-level microstructure layer
Use targeted event-level data around:
- opening entries;
- spread levels near target;
- adverse excursions;
- important target crossings.

Do **not** acquire full-market event-level OPRA indiscriminately.

## 6. Initial strategy surface

The current NDXP benchmark remains frozen for confirmatory work:

- short put verticals;
- 10-point width;
- literal 2DTE and 3DTE;
- approximately $5/$6/$7 opening credit;
- spread-mid valuation;
- 20-contract translation for P/L examples.

Broaden only in controlled research phases to:
- 0–5DTE;
- credit continuum roughly $3–$8.50;
- 5/10/20-point widths;
- calls as well as puts;
- moneyness/delta/IV;
- entry timing.

Do not optimize all dimensions simultaneously.

## 7. Entry-state research

For every candidate entry, preserve continuous causal features before creating labels.

Potential features:

- overnight return/range;
- prior close to open gap;
- opening 5s/15s/30s/60s/2m/5m returns where data permit;
- acceleration/deceleration;
- distance from overnight high/low;
- opening-range position;
- VWAP relationship;
- realized volatility / ATR-like normalization;
- VIX/volatility state;
- quote width/age;
- spread rate-of-change;
- complex-order availability if observable;
- day of week;
- event-day flags.

Primary research question:

> What distinguishes clean compression, adverse-then-success, and outright failure?

## 8. Path research

For each trade reconstruct:

- fixed-horizon spread values;
- first-passage times;
- maximum favorable/adverse excursion;
- time underwater;
- $7/$8/$9-before-$3 incidence;
- target touch vs persistence;
- next-event and next-minute behavior;
- overnight/weekend recovery behavior;
- final intrinsic/settlement outcome where applicable.

Path research should support management-rule generation, not merely post-hoc description.

## 9. Execution research

Treat **entry and exit execution separately**.

### Entry
Study:
- complex-order midpoint behavior;
- opening quote width;
- fill probability near target credit;
- improvement/chasing requirements;
- speed of spread repricing.

### Exit
Study:
- resting BTC limit behavior;
- reactive BTC behavior;
- transient target sweeps;
- duration at/through target;
- quote/trade evidence around the target;
- next-event continuation/reversal.

Execution models should include:
- optimistic/plausible/conservative fill classes;
- explicit slippage stresses;
- target persistence stresses;
- resting-order vs reactive-order logic.

Do not use independent leg natural prices as the primary complex-order execution model.

## 10. Cross-instrument comparison

After each instrument has an internally valid study, compare:

- hit rates;
- path shapes;
- slippage sensitivity;
- liquidity/execution evidence;
- regime stability;
- drawdown;
- capital efficiency;
- strategy availability frequency.

Purpose:

> Determine whether compression is instrument-specific, index-options structural, or broadly generic.

## 11. Validation discipline

Use frozen chronological partitions for each instrument before conditional analysis.

For existing NDXP work:
- Development: 2021-09-24 to 2024-12-31
- Validation: 2025-01-01 to 2025-12-31
- Final holdout: 2026-01-01 to 2026-09-24

The 2026 NDXP period is not perfectly pristine because aggregate results were already observed; from this point forward, do not use it for rule tuning.

For newly added instruments, define development/validation/final-holdout partitions before viewing conditional results.

## 12. Automation track

Research and execution should develop in parallel.

Shared primitives should include:
- instrument/chain loader;
- spread constructor;
- candidate selector;
- causal feature calculator;
- spread-state tracker;
- target manager;
- risk-state machine;
- deterministic logging;
- replay interface.

Progression:

**Research → specification → replay → paper execution → shadow-live → controlled live**

No live order authority is authorized by this plan.

The execution system should first function as a **prospective observation engine**, recording every setup it would have traded and simulating resting/modified orders in live markets.

## 13. Risk research

Measure:

- max economic loss;
- mark-to-market drawdown;
- adverse excursion;
- daily and weekly drawdown;
- consecutive losses;
- concurrent positions;
- correlated exposure across instruments;
- repeated same-day entries;
- capital utilization;
- stop-trading rules;
- tail-event behavior;
- weekend/overnight carry risk.

Do not rely on realized-sequence drawdown alone.

## 14. Data governance

- Raw cached data are immutable.
- Derived data go into versioned output namespaces.
- Every large acquisition requires a manifest and cost estimate first.
- Reuse local data before acquiring more.
- No hidden or automatic paid downloads.
- Hash important raw/derived inputs.
- Avoid thousands of serial metadata/API calls.
- Local reruns should not incur additional Databento cost.
- Log data-quality warnings explicitly.

## 15. Databento upgrade policy

Do not upgrade solely because deeper data might be useful.

Before any upgrade:
1. identify exact schemas/date ranges/instruments needed;
2. estimate one-time usage-based cost;
3. compare against subscription/plan economics;
4. document retention/reuse value;
5. choose the cheaper/faster path consistent with the research plan.

## 16. Program phases

### Phase A — NDXP deep-dive completion
- Finish path analysis.
- Add entry morphology.
- Add high-resolution execution validation.
- Verify settlement behavior.
- Build first replayable strategy definition.

### Phase B — Data architecture expansion
- Inventory local/historical coverage for SPX, QQQ, RUT.
- Define symbol parents, expirations, settlement mechanics, and acquisition plans.
- Build common cross-instrument schema.

### Phase C — Cross-instrument structural studies
- Run minute-level structural studies independently.
- Freeze per-instrument baseline families.
- Compare behavior.

### Phase D — High-resolution execution studies
- Acquire second-level/event-level targeted data.
- Calibrate realistic resting/reactive fill models.
- Build instrument-specific execution assumptions.

### Phase E — Entry-state and management models
- Development-only feature analysis.
- Candidate rule generation.
- Validation and holdout.

### Phase F — Replay and prospective paper engine
- Deterministic historical replay.
- Live observation.
- Paper orders.
- Daily research logging.

### Phase G — Controlled live pilot
Only after explicit review and separate authorization.

## 17. Decision gates

At each phase ask:

- Does the edge survive?
- Is the effect stable?
- Is it executable?
- Is the edge large relative to friction and uncertainty?
- Is the drawdown acceptable?
- Is the rule machine-observable?
- Does validation support it?
- Is automation reducing or adding operational risk?

The program should stop or narrow any hypothesis that fails these gates.

## 18. Immediate next step

The next assignment is **Phase 3A: data inventory + acquisition manifest + cost study**.

No purchase is authorized yet.

The task is to determine exactly what historical data we already possess and what additional NDXP/SPX/QQQ/RUT data would be needed for:
- long-run structural studies;
- second-level path studies;
- targeted event-level executability studies.

The output must include request counts, expected storage, likely cost, and a recommended acquisition sequence.
