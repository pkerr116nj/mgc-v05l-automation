# NDXP 2DTE/3DTE Rich-Credit Research Program — Deep-Dive Specification

Status: **ACTIVE RESEARCH SPEC**
Branch: `codex/ndxp-multidte-credit-research`
Primary owner: research engineering / Codex
Research question owner: Patrick + ChatGPT
Scope: research only; no live order authority

## 1. Objective

Determine whether short 10-point NDXP put verticals entered near the cash open with approximately $5/$6/$7 of credit possess a durable, exploitable edge when given 2 or 3 calendar DTE, and identify:

1. the **ideal entry morphology** — what the tape looks like when the best entries occur;
2. the **path morphology** — how winning and losing spreads evolve after entry;
3. the **management edge** — whether early path information can improve exits or reduce tail loss;
4. the **execution robustness** — whether the result survives realistic spread-level slippage and target persistence;
5. the **regime stability** — whether the effect is persistent across years and market states rather than concentrated in one period.

The current hypothesis is not “hold rich ITM spreads until expiration.” The strongest preliminary result is “sell rich premium and monetize substantial compression, often around a $3 buyback.”

## 2. Existing empirical baseline

Current cached research inputs on Mars:

- Candidate file:
  `output/ndxp_multidte/candidates_2021-09-24_2026-09-24.csv`
- Selected option path file:
  `output/ndxp_multidte/paths_2021-09-24_2026-09-24.csv`
- Discovery cache:
  `output/ndxp_multidte/discovery_cache/`
- Selected-path cache:
  `output/ndxp_multidte/path_cache/`
- Existing baseline output:
  `output/ndxp_multidte/backtest_mid_5c/`

Dataset characteristics already established:

- 1,254 successful opening-discovery sessions across the 5-year window.
- 3,738 selected candidate spreads.
- 915 qualifying path sessions requested; 913 downloaded.
- 8,979,954 minute-level selected-contract path rows.
- The two path failures are 2026-09-22 and 2026-09-23 because Databento had not yet published the full 2026-09-25 path at acquisition time; they are not historical data-quality failures.
- Known Databento condition notices include 2024-06-03 degraded and 2025-10-22 degraded. Preserve and flag these dates.

Current baseline execution assumptions:

- 20 contracts.
- $10-wide vertical.
- entry price = spread midpoint - $0.05.
- exit price = spread midpoint + $0.05.
- fee model = $1.324 per contract per side.
- candidate opening-credit buckets = $5, $6, $7.
- exit targets = $3, $2, $1, $0.50.
- Spread-level midpoint is the execution benchmark. Do **not** treat independent short-leg bid minus long-leg ask as a realistic complex-order fill envelope.

Initial smell-test results show strong $3-target behavior. Examples:

- 2DTE $5 -> $3: ~86.6% win rate, PF ~3.46, avg P/L ~$3,068.
- 2DTE $6 -> $3: ~78.7% win rate, PF ~3.47, avg P/L ~$3,982.
- 2DTE $7 -> $3: ~69.5% win rate, PF ~3.57, avg P/L ~$4,617.
- 3DTE $5 -> $3: ~85.5% win rate, PF ~3.31, avg P/L ~$3,047.
- 3DTE $6 -> $3: ~78.5% win rate, PF ~3.55, avg P/L ~$4,100.
- 3DTE $7 -> $3: ~68.8% win rate, PF ~3.50, avg P/L ~$4,547.

These are exploratory results, not production claims.

## 3. Research discipline / anti-overfitting rules

The goal is to **characterize the landscape**, not to maximize in-sample P/L.

Do not search arbitrary parameter combinations until something looks best. Do not produce a “winning rule” from the full sample.

Freeze chronological partitions now:

- **Development:** 2021-09-24 through 2024-12-31.
- **Validation:** 2025-01-01 through 2025-12-31.
- **Final holdout:** 2026-01-01 through 2026-09-24.

Important limitation: the final holdout contributed to the previously viewed aggregate smell-test result. It is therefore not perfectly pristine. From this point forward, however, do not use 2026 conditional/regime/feature results to choose rules. Treat it as untouched for all new feature selection and rule refinement.

Workflow:

1. explore and formulate candidate rules using development only;
2. freeze each candidate rule before viewing validation performance;
3. use validation to reject or retain;
4. only after the rule set is frozen, reveal final-holdout performance;
5. clearly distinguish exploratory findings from pre-specified tests.

Any machine-learning model must be interpretable and secondary to descriptive analysis. No black-box model should be used to declare a trade rule.

## 4. Data governance

### 4.1 Raw data are immutable

Never overwrite, mutate, normalize in place, or delete:

- DBN caches;
- candidate CSVs;
- selected-path CSVs;
- prior backtest outputs.

Derived data must be written to a new research namespace, recommended:

`output/ndxp_multidte/deep_dive_v1/`

### 4.2 Reuse before acquisition

Before requesting any new Databento data:

1. inventory local files and existing futures/index research stores;
2. determine whether required NQ/MNQ/NDX/VIX data already exist;
3. document exact gaps;
4. if a paid acquisition is genuinely needed, run a bounded cost estimate and STOP for operator approval.

Do not launch a large paid download autonomously.

### 4.3 Time conventions

All event alignment must be explicit in America/New_York.

No look-ahead:
- an entry feature at time T may use only data timestamped at or before T;
- future spread path information belongs only in labels/outcomes.

## 5. Core trade definition held fixed during first deep-dive

Do not broaden the trade universe yet.

Hold fixed:

- instrument: NDXP;
- side: short put vertical;
- width: 10 points;
- calendar DTE: literal 2 or 3 calendar days where such expiration exists;
- opening-credit targets: approximately $5/$6/$7;
- default size for P/L translation: 20 contracts;
- baseline spread-level entry execution: mid - $0.05;
- baseline spread-level exit execution: mid + $0.05.

Do not yet optimize:
- call spreads;
- 1DTE/4DTE/5DTE;
- different widths;
- SPX/RUT;
- arbitrary exact credit levels.

Those are future extensions only after the core phenomenon is understood.

## 6. Workstream A — Entry morphology

Build one causal feature row per candidate observation.

### 6.1 Opening-time candidate snapshots

Reconstruct candidate spread values at or as close as data permit to:

- 09:31
- 09:32
- 09:33
- 09:35
- 09:40
- 09:45
- 10:00

Do not silently forward-fill across long gaps. Record actual quote timestamp/age.

For each time point record:
- spread midpoint;
- distance from target credit;
- short/long strikes;
- expiration;
- calendar DTE;
- option-leg quote widths as diagnostics;
- spread change from prior snapshot.

### 6.2 Underlying/tape features

Inventory existing local historical data first. Prefer NDX cash when available; NQ/MNQ is an acceptable and useful continuous tape proxy.

Required causal features where data exist:

- prior cash close to opening gap;
- overnight NQ return;
- overnight high-low range;
- pre-open 60m / 30m / 15m / 5m return;
- 09:30-09:31 return;
- 09:30-09:31 range;
- 09:30-09:32, 09:35, 09:40, 09:45 cumulative return;
- realized range / ATR-like normalized opening move;
- price relative to VWAP;
- slope/direction of VWAP if supportable causally;
- short-horizon momentum and acceleration/deceleration;
- distance from overnight high/low;
- opening gap sign and standardized magnitude;
- simple trend-vs-chop measures that are continuous first, labels second;
- VIX level/change if locally available;
- day-of-week;
- known data-quality flag.

Do not start by forcing “red / green / chop” labels. First preserve continuous measurements. Labels may be derived later using transparent thresholds.

### 6.3 Morphology questions

Answer at minimum:

- Are successful $6->$3 trades associated with red, green, reversal, or neutral openings?
- Does an adverse move immediately before entry improve sale price without damaging eventual success?
- Do screaming-green openings still provide adequate $6-$7 candidates simply by selecting deeper ITM strikes?
- Does opening acceleration matter more than opening direction?
- Does gap-and-go behave differently from gap-and-reverse?
- Is entry quality mostly determined by tape shape, or does tape shape have weak discriminatory power?

## 7. Workstream B — Path morphology

For each selected spread reconstruct the minute path from entry through expiration/last available quote.

Produce path features including:

### 7.1 Fixed-horizon values

Spread midpoint and P/L at:
- +1m
- +2m
- +5m
- +10m
- +15m
- +30m
- +60m
- +120m
- cash close each day if relevant
- final available minute before expiration

### 7.2 Threshold timing

For each trade calculate first passage time to:
- $5.50
- $5
- $4
- $3
- $2
- $1
- $0.50

Also first passage to adverse levels where applicable:
- entry + $0.50
- entry + $1
- $7
- $8
- $9
- $9.50

Record “never hit” explicitly.

### 7.3 Adverse-before-success anatomy

For each target, especially $3:

- maximum debit observed before first $3 hit;
- time spent above entry before $3 hit;
- whether $7/$8/$9 was touched first;
- time underwater;
- number of sign changes around entry price;
- best improvement achieved before subsequent failure;
- whether success occurred after a deep adverse excursion.

This is a primary deliverable. A 79% eventual target-hit rate is insufficient without understanding the path required to achieve it.

### 7.4 Persistence tests

A target should be tested under:
- one-minute touch;
- 2 consecutive qualifying minutes;
- 3 consecutive qualifying minutes.

Also test exit at the next minute after first target touch to detect single-bar quote noise.

## 8. Workstream C — Execution robustness

Run a fixed, pre-specified slippage matrix.

Per-side spread-level adverse slippage:

- $0.00
- $0.05
- $0.10
- $0.15
- $0.25

Do not use independent leg-natural pricing as the primary execution model.

For each combination report:
- win rate;
- target-hit rate;
- mean/median P&L;
- profit factor;
- expected return on max economic risk;
- worst trade;
- max drawdown by chronological sequence;
- longest losing streak.

The purpose is sensitivity, not parameter selection.

## 9. Workstream D — Failure anatomy and early-abort research

Concentrate initially on the $6->$3 family, retaining 2DTE and 3DTE separately.

Compare:
- winners vs losers;
- fast winners vs slow winners;
- winners with severe adverse excursion vs clean winners;
- matched entries with similar initial tape but different outcomes.

At checkpoints +5m, +15m, +30m, +60m, +120m, calculate conditional probability of later hitting $3 based only on information available at that checkpoint.

Explore simple pre-specified management questions:

- If spread has not improved by 30/60/120m, how does eventual success rate change?
- If spread first widens to $7/$8/$9, what is later $3-hit probability?
- If underlying invalidates the opening directional move, is there a consistent loss-reduction opportunity?
- Does early favorable compression predict continued success strongly enough to justify taking profits earlier?

Do not invent an optimized stop from the full sample. Characterize first; propose candidate rules second.

## 10. Workstream E — Regime stability

Report all core results by:

- calendar year;
- development / validation / final-holdout partition;
- 2DTE vs 3DTE;
- $5/$6/$7 entry target;
- opening direction quantiles;
- gap-size quantiles;
- realized-volatility quantiles;
- VIX regime if available;
- day of week.

Require minimum sample-size disclosure for every subgroup.

Do not treat tiny subgroup performance as evidence.

## 11. Matched-pair analysis

Create a nearest-neighbor or transparent distance-based matched analysis on **development data only**.

Goal: find pairs of candidate entries that look similar at entry but have different outcomes.

Use standardized causal features only. Do not include future path variables in matching.

For representative matched pairs produce compact path tables or plots showing:
- underlying path;
- spread midpoint path;
- first $3 hit or failure;
- adverse excursion;
- key entry features.

This is intended to reveal whether differentiation exists at entry or emerges only after entry.

## 12. Entry timing experiment

The existing 09:31 snapshot is an arbitrary baseline, not a sacred entry rule.

Without changing the trade family, compare pre-specified candidate entry times:

- 09:31
- 09:32
- 09:33
- 09:35
- 09:40
- 09:45

For each entry time select the spread nearest each target credit using only information then available.

Study:
- candidate availability;
- realized entry credit;
- later $3-hit probability;
- average P/L;
- immediate 1/2/5-minute compression;
- whether adverse opening impulses produce meaningfully richer fills.

Do not choose a single “best minute” from the full sample. Characterize the curve on development data, then validate any simple timing hypothesis.

## 13. Settlement / terminal-value rigor

Replace “last available option quote” as the terminal economic truth wherever possible.

For expiration outcomes:
- use correct NDXP PM settlement/intrinsic-value mechanics;
- document the settlement source;
- distinguish quote-based last observation from settlement-based payoff.

Do not allow stale final option quotes to masquerade as expiration P/L.

## 14. Required implementation modules

Codex should implement or extend clean modules under the existing research package. Suggested structure:

- `ndxp_multidte_entry_features.py`
- `ndxp_multidte_path_features.py`
- `ndxp_multidte_robustness.py`
- `ndxp_multidte_regime_analysis.py`
- `ndxp_multidte_validation.py`
- `ndxp_multidte_deep_dive.py` as orchestration/report entry point if useful

Reuse existing parsers/helpers instead of duplicating logic.

## 15. Required outputs

Write all derived artifacts under:

`output/ndxp_multidte/deep_dive_v1/`

At minimum:

- `entry_features.csv`
- `path_features.csv`
- `candidate_timeline.csv` or equivalent compact snapshot table
- `execution_robustness.csv`
- `yearly_stability.csv`
- `regime_breakdown.csv`
- `failure_anatomy.csv`
- `matched_pairs.csv`
- `partition_results.csv`
- `data_quality_report.json`
- `research_report.md`
- machine-readable `report.json`

Charts may be generated locally into the same directory, but every chart must be reproducible from a committed script.

## 16. Tests and acceptance criteria

Add unit/integration tests covering at minimum:

1. no feature uses timestamps after entry;
2. 2DTE/3DTE are literal calendar DTE;
3. spread midpoint is computed consistently;
4. slippage direction is adverse on entry and exit;
5. threshold first-passage timing is correct;
6. target persistence logic is correct;
7. no future-path fields leak into entry-feature table;
8. chronological partition assignment is deterministic;
9. holdout rows are excluded from development feature/rule selection;
10. degraded/missing Databento dates are flagged;
11. settlement payoff for a 10-point put vertical is bounded [0, 10];
12. raw cached inputs are never mutated.

Run the relevant existing test suite plus new tests.

## 17. Runtime and API-cost constraints

This project has already experienced avoidable long-running Databento workflows.

Before any loop that calls a remote API:
- calculate expected number of requests;
- state it in code comments or report output;
- batch, cache, or parallelize responsibly;
- prefer local analysis once data are present.

No research step should make thousands of serial metadata calls.

A rerun of derived analysis from already-cached data should be local and should not incur Databento cost.

## 18. Deliverable sequence

Codex should execute in phases, committing after each coherent phase.

### Phase 1 — Inventory and integrity
- inventory local inputs;
- verify row counts / dates / candidate coverage;
- identify exact two failed recent path sessions;
- produce data-quality report;
- do not download new paid data.

### Phase 2 — Path feature store
- build all path features and persistence logic;
- reproduce baseline results;
- test slippage matrix;
- produce first path-anatomy report.

### Phase 3 — Entry feature store
- inventory local NDX/NQ/MNQ/VIX data;
- integrate available causal tape features;
- identify gaps before any new acquisition;
- if acquisition is needed, estimate cost and stop for approval.

### Phase 4 — Morphology and regime analysis
- development sample only for exploratory conditional work;
- matched pairs;
- entry-timing curves;
- failure anatomy;
- candidate management hypotheses.

### Phase 5 — Validation
- freeze hypotheses;
- run 2025 validation;
- reject/retain without retuning on validation.

### Phase 6 — Final holdout
- only after rules are frozen;
- run 2026 final holdout;
- report without further tuning.

## 19. What Codex must NOT do

- Do not modify live trading/execution code.
- Do not enable broker order submission.
- Do not delete or overwrite raw research data.
- Do not silently buy additional market data.
- Do not optimize hundreds of arbitrary combinations and present the best.
- Do not use final-holdout results to tune rules.
- Do not assume a midpoint touch guarantees a practical fill; use the explicit persistence/slippage stress tests.
- Do not collapse 2DTE and 3DTE into one sample without also reporting them separately.
- Do not hide failed dates, degraded data, or sample-count changes.
- Do not infer causal claims from simple correlations.

## 20. Definition of success

This phase succeeds even if the strategy ultimately fails.

Success means we can answer, with reproducible evidence:

1. What does a high-quality entry actually look like?
2. Does opening direction matter, or is directional acceleration/deceleration more important?
3. How quickly do successful rich-credit spreads tend to compress?
4. How much adverse excursion is normally endured before success?
5. Can failures be recognized early enough to reduce losses without destroying winners?
6. Does the result survive realistic spread-level execution friction?
7. Is the effect stable across years and regimes?
8. Does it survive chronological validation and final holdout?
9. Is the edge primarily entry selection, trade management, or both?

Only after those questions are answered should the project consider broadening into other DTEs, widths, call spreads, or indices.
