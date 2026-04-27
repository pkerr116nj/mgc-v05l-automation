# US Open Follow-Through / Failed Move Research Design

## Purpose
This document defines the next controlled probabilistic research family after Asia Drift.

The objective is to study what happens after the U.S. cash open without creating trading rules, optimizing thresholds, or modifying execution logic.

The first phase should answer:
- when the opening move continues
- when the opening move fails and reverses
- whether `NQ/MNQ` behave differently from `ES/MES`
- whether `VIX` regime, overnight direction, and Asia/London context materially change the distribution

This is a research design only. It is not a signal specification.

## Scope

### Instruments
Initial universe:
- `ES`
- `MES`
- `NQ`
- `MNQ`

Deferred:
- `GC`
- `MGC`

### Data
Available inputs:
- canonical `1m` data from `2020-01-01` through `2026-04-21`
- Phase A warehouse:
  - `1m`
  - `5m`
  - `15m`
  - `60m`
  - `240m`
  - `daily`
- `VIX` regime layer
- existing Asia Drift research framework patterns

### Session windows
- premarket context: `04:00-09:30 ET`
- opening drive: `09:30-10:00 ET`
- confirmation window: `10:00-10:30 ET`

### Outcome windows
- `30m`
- `60m`
- `120m`
- `15:30 ET`
- `session close`

## Core Research Framing
The first family should be framed as a post-open observational study, not as a signal family.

We should start from a fixed decision timestamp and measure:
- continuation in the direction of the opening move
- reversal against the opening move
- forward return distributions
- MFE/MAE paths
- time-to-peak favorable move
- time-to-peak adverse move

This should stay closer to Asia Drift Pass 1 than to a replay/entry study.

## Candidate Definitions

### Candidate Lens A: Opening Drive Continuation
Decision timestamp:
- `10:00 ET`

Base candidate:
- every instrument/session where the `09:30-10:00 ET` opening drive has a non-zero signed net move from `09:30 open` to `10:00 close`

Direction:
- `UP` if `10:00 close > 09:30 open`
- `DOWN` if `10:00 close < 09:30 open`

Interpretation:
- this is the cleanest smallest first lens
- it avoids threshold optimization
- it lets us measure continuation versus reversal from a fixed post-open decision point

### Candidate Lens B: Opening Drive Failed Move
Decision timestamp:
- `10:30 ET`

Base candidate:
- sessions already labeled by Lens A
- then classify whether the `10:00-10:30 ET` confirmation window invalidates the opening drive

Predeclared failure markers for research only:
- move back through `09:30 open`
- move back through `10:00 VWAP state`
- move back through opening-drive midpoint

Important:
- Lens B should not be the first executable pass
- it should be built after Lens A establishes baseline continuation/reversal structure

## Smallest First Executable Pass
The smallest first pass should be:

### Pass UO-1
`US Open Continuation / Failure Baseline`

Design:
- universe: `ES,MES,NQ,MNQ`
- decision timestamp: `10:00 ET`
- candidate set: all directional opening-drive sessions
- no threshold tuning
- no filtering for “best” open moves
- measure both:
  - continuation probability
  - reversal probability

Outputs:
- pooled continuation / reversal tables
- per-instrument continuation / reversal tables
- `ES/MES` cluster versus `NQ/MNQ` cluster comparison
- forward return by horizon
- MFE/MAE by horizon
- time-to-peak favorable/adverse
- regime-conditioned continuation/failure by:
  - overnight drift direction
  - `60m` trend agreement
  - `VIX` level / change bucket
  - prior session range regime
  - cross-index confirmation

Reason for starting here:
- it is the least assumption-heavy design
- it keeps the decision timestamp fixed
- it naturally supports a later failed-move study without prematurely turning confirmation behavior into an entry rule

## Feature Set For UO-1
All features should be descriptive and predeclared.

### Opening-drive structure
- `open_drive_direction`
- `open_drive_return_points`
- `open_drive_return_pct_of_prior_day_range`
- `open_drive_range_points`
- `open_drive_range_vs_premarket_range`
- `open_drive_close_location_in_range`
- `open_drive_vwap_state_at_10:00`

### Post-open structure available at decision time
- `5m slope at 10:00`
- `15m slope at 10:00`
- `60m trend agreement`
- `240m trend agreement`
- `open_above_or_below_daily_reference`

### Overnight / session context
- overnight drift direction:
  - `18:00 prior session open` to `09:30 cash open`
- overnight drift magnitude bucket
- premarket range size
- Asia direction
- London direction
- whether Asia and London align with the opening drive
- whether overnight drift aligns with the opening drive

### Regime context
- `VIX` level bucket
- `VIX` change bucket
- combined `VIX` regime bucket
- prior session range regime
- prior session trend/chop regime

### Cross-index context
- `ES/MES` confirmation state
- `NQ/MNQ` confirmation state
- cross-index aligned versus divergent at `10:00`

## Outcome Definitions
All outcomes should be computed relative to the opening-drive direction.

### Primary outcomes
- `forward_return_30m`
- `forward_return_60m`
- `forward_return_120m`
- `forward_return_1530`
- `forward_return_close`

### Directional probabilities
- `continuation_success_horizon_X`
  - signed forward return in the opening-drive direction > `0`
- `reversal_success_horizon_X`
  - signed forward return opposite the opening-drive direction > `0`

### Path outcomes
- `mfe_horizon_X`
- `mae_horizon_X`
- `time_to_peak_favorable`
- `time_to_peak_adverse`
- `close_to_entry_outcome`

## Train / Holdout Split
To remain consistent with Asia Drift:
- development: through `2024-12-31`
- holdout: `2025-01-01` through `2026-04-21`

This split should remain fixed across the initial U.S. open passes.

## Expected Artifacts

### Pass UO-1 artifacts
Candidate dataset:
- `features/us_open_probabilistic_pass1_candidates.csv`
- `features/us_open_probabilistic_pass1_candidates.parquet`

Outcome dataset:
- `signals/us_open_probabilistic_pass1_outcomes.csv`
- `signals/us_open_probabilistic_pass1_outcomes.parquet`

Reports:
- `reports/us_open_probabilistic_pass1_pooled_summary.csv`
- `reports/us_open_probabilistic_pass1_per_instrument_probability.csv`
- `reports/us_open_probabilistic_pass1_cluster_summary.csv`
- `reports/us_open_probabilistic_pass1_forward_return_table.csv`
- `reports/us_open_probabilistic_pass1_mfe_mae_distribution.csv`
- `reports/us_open_probabilistic_pass1_time_to_peak.csv`
- `reports/us_open_probabilistic_pass1_regime_summary.csv`
- `reports/us_open_probabilistic_pass1_summary.json`
- `reports/us_open_probabilistic_pass1_summary.md`

### Likely code layout
- `src/mgc_v05l/research/us_open_follow_through/probabilistic_pass1.py`
- `src/mgc_v05l/app/us_open_probabilistic_pass1.py`
- `tests/unit/test_us_open_probabilistic_pass1.py`

## Candidate Definition Details To Freeze Before Execution
Before implementing UO-1, freeze:

1. Decision timestamp:
- `10:00 ET`

2. Opening-drive direction:
- `09:30 open` to `10:00 close`

3. Non-flat filter:
- include any non-zero net move
- do not add minimum-magnitude thresholds in the first pass

4. Continuation definition:
- positive signed forward return in drive direction

5. Reversal definition:
- positive signed forward return against drive direction

6. Symbol scope:
- `ES,MES,NQ,MNQ` only

## Overfitting Avoidance Rules

### Hard constraints
- no threshold search
- no parameter sweep
- no symbol-specific tuned cutoffs
- no adding new windows after looking at outcomes
- no replay optimization
- no signal generation

### Research discipline
- keep the first pass observational
- predeclare all windows and features
- use the existing development / holdout split
- report trust/sample flags on thin buckets
- pooled and cluster results come first
- instrument-specific findings are secondary unless sample size is robust

### Minimum trust policy
Suggested initial trust policy:
- pooled rows below `100` -> `DO_NOT_TRUST`
- per-instrument rows below `50` -> `DO_NOT_TRUST`

Those are reporting controls, not strategy rules.

## Recommended Phase Sequence

### Phase 1
Implement and run `UO-1` baseline.

Questions answered:
- does the `10:00` opening-drive continuation effect exist at all?
- how often does reversal dominate instead?
- how different is `NQ/MNQ` versus `ES/MES`?

### Phase 2
If Phase 1 is informative, add:
- explicit `10:00-10:30` confirmation taxonomy
- failed-move observational slicing
- `VIX` regime simplification similar to Asia Drift Pass 6/7

### Phase 3
Only after stable observational results:
- decision-layer state mapping
- then later execution-layer research

## What Not To Do Yet
- do not mine broad pattern space
- do not add GC/MGC in Pass 1
- do not create entry rules
- do not optimize confirmation thresholds
- do not touch execution code
- do not alter Asia Drift

## Success Criteria For UO-1
The first pass is successful if it gives a clean descriptive answer to:
- whether the post-open move tends to continue or fail
- whether `NQ/MNQ` differ materially from `ES/MES`
- whether `VIX`, overnight direction, and cross-index alignment explain the split

It does not need to produce a tradable setup yet.
