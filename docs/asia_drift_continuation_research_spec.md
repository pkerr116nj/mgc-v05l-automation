# Asia Drift Continuation Research Spec

## 1. Data Foundation Used

- Universe: `GC`, `MGC`, `ES`, `MES`, `NQ`, `MNQ`
- Canonical `1m` coverage: `2020-01-01` through `2026-04-21`
- Phase A parquet warehouse:
  - `raw_bars_1m`
  - `derived_bars_5m`
  - `derived_bars_15m`
  - `derived_bars_60m`
  - `derived_bars_240m`
  - `derived_bars_daily`
- Data integrity state before research:
  - final audit healthy
  - `analysis_allowed=true`
- Regime enrichment:
  - realized daily regime proxy
  - external official Cboe `vix_daily`
  - derived `vol_regime_daily` with as-of joins

## 2. Setup Definition

- Setup family: `Asia Drift Continuation`
- Research intent:
  - measure conditional continuation behavior after directional Asia drift
  - remain observational and probabilistic, not rule-generating
- Scope of current execution-oriented research:
  - index contracts only: `ES`, `MES`, `NQ`, `MNQ`
  - decision timeframe: `5m`

## 3. Evidence Stack

- Base evidence inputs:
  - VWAP separation
  - `5m` slope state
  - `15m` slope state
  - `60m` directional agreement
  - `240m` directional agreement
  - compression state
  - disorder state
  - cross-asset confirmation flag
  - session timing bucket
  - prior daily regime context
- Current highest-confidence stack from the controlled passes:
  - `60m AGREE`
  - cross-asset confirmation
  - `NOT_COMPRESSED`
  - timing bucket emphasis on `LATE_ASIA`

## 4. Regime Decision Layer

- `TRADE_FAVORABLE`
  - environments classified as `EDGE_ON`
  - best current expression is concentrated in `LATE_ASIA`
- `TRADE_NEUTRAL`
  - mixed or lower-conviction environments
  - still positive in some slices, but less stable
- `DO_NOT_TRADE`
  - environments classified as `EDGE_OFF`
  - weaker expectancy, lower quality win rate, and worse adverse path

The decision layer is a research classification wrapper only. It is not live execution logic.

## 5. Current Provisional Execution Framework

- Only consider:
  - `TRADE_FAVORABLE`
  - `LATE_ASIA`
- Current base holding framework:
  - `120m` base time stop
- Instrument-family read:
  - `ES/MES`
    - lower-dispersion continuation profile
    - smaller expectancy
    - materially smaller adverse excursion envelope
  - `NQ/MNQ`
    - higher-expectancy continuation profile
    - materially larger `MAE`
    - stronger upside tail, but much larger path risk
- Current interpretation:
  - `60m` is informative
  - `120m` is the cleanest provisional extension
  - `240m` is still interesting, especially for `NQ/MNQ`, but degrades in quality
  - full session-end holds are not currently favored

## 6. Stop/Target What-If Findings

- Research-only, non-optimized structures studied:
  - `ES/MES`: `6/12`
  - `NQ/MNQ`: `20/40`
- These were stronger than the smaller alternative structures in the current what-if pass:
  - `ES/MES`: stronger than `4/8`
  - `NQ/MNQ`: stronger than `15/30`
- These findings are descriptive only:
  - not optimized
  - not walk-forward tuned
  - not approved for live execution

## 7. Key Avoid Conditions

- `PRE_LONDON` remains weak as a continuation-quality regime
- Simplified avoid environments from the regime passes included:
  - `PRE_LONDON|LOW_VOL|LOW|NOT_UP`
  - `PRE_LONDON|HIGH_VOL|NOT_LOW|NOT_UP`
  - `LATE_ASIA|HIGH_VOL|NOT_LOW|UP`
  - `LATE_ASIA|LOW_VOL|NOT_LOW|UP`
- Earlier regime work also flagged weak conditions around:
  - `EARLY_ASIA` after large prior-session range
  - weak or noisy low-vol `PRE_LONDON`

## 8. Known Caveats

- Regime sensitivity is real:
  - `2025` and `2026` do not look identical
  - `NQ/MNQ` are more regime-sensitive than `ES/MES`
- Sample limitations remain:
  - some regime buckets become sparse quickly
  - combined regime slicing can fragment holdout samples
- Not yet live:
  - no live execution logic has been created
  - no promotion to paper/live trading is implied
- No optimized parameters:
  - no parameter search
  - no stop/target optimization
  - no threshold fitting for live deployment

## 9. Open Research Items

- trailing / partial-taking design
- `240m` extension study for `NQ/MNQ`
- live VIX / TradeStation regime data integration
- walk-forward validation across decision-state and regime layers
- paper / shadow trading design for controlled forward validation
