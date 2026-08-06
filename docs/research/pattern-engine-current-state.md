# Pattern Engine Current State

## Status

Research baseline and terminology reference.

This document is descriptive research state. It is not runtime authority, not broker authority, not a production trading gate, and not an approval to promote, demote, enable, or disable any strategy.

Any change to baseline-family status, timeframe authority, default universe, or promotion rules must use the appropriate Research Question and Architecture Track where applicable.

## Purpose

Record the current Pattern Engine, replay/operator baseline families, research boundaries, session terminology, timeframe hierarchy, and deferred research branches as durable project knowledge.

The goal is to prevent future work from reopening settled questions or confusing research concepts with production baseline families.

## Current Replay And Operator Baseline Families

The active replay/operator baseline families are:

- `firstBearSnapTurn`
- `firstBullSnapTurn`
- `usDerivativeBearTurn`
- `usDerivativeBearAdditiveTurn`
- `usMiddayPauseResumeShortTurn`

These are concrete replay/source family labels used in the current operator baseline. They should not be treated as generic Pattern Engine concept names.

## Retained But Not Baseline-Ready Research Family

`usMiddayPauseResumeLongTurn` remains retained research rather than an operator baseline family.

Its retained winner uses slope floor `-0.10`.

Do not widen the slope floor beyond `-0.10` without a new research decision. This family should not be silently promoted into production or baseline use.

## Leading Pattern-Engine-Derived Branch Before Promotion Audit

The leading Pattern-Engine-derived research branch before promotion audit was:

- Branch: `asiaEarlyPauseResumeShortTurn`
- Session: `ASIA_EARLY`
- Direction: `SHORT`
- Pattern concept: `pause_rebound_resume_short`
- Setup curvature: `CURVATURE_FLAT`

Immediate decision path:

- It required a `firstBullSnapTurn` interaction audit.
- The purpose was to determine whether the interaction was incidental or structural.
- Promotion into the operator baseline must not occur until that interaction question is resolved.

## Session Terminology

Project-specific research/session labels:

- `ASIA_EARLY`: early Asia pocket.
- `ASIA_LATE`: later Asia.
- `LONDON_OPEN`: London open.
- `LONDON_LATE`: later London.
- `US_MIDDAY`: US midday.
- `US_LATE`: later US session.

These labels are project-specific research/session categories and should be reused consistently.

## Pattern Concepts Versus Replay And Source Labels

Pattern Engine concepts are abstract research concepts:

- `pause_pullback_resume_long`
- `pause_rebound_resume_short`
- `breakout_retest_hold`
- `failed_move_reversal`

Replay/source labels are concrete family names, such as:

- `usMiddayPauseResumeShortTurn`
- `asiaEarlyPauseResumeShortTurn`

Do not treat Pattern Engine concept names and replay/source family names as interchangeable.

## Naming Conventions

- `SnapTurn` is the legacy baseline turn family.
- `DerivativeBearTurn` is the widened/research short family.
- `DerivativeBearAdditiveTurn` is the additive short lane attached to derivative-bear work.
- `PauseResume`, `PausePullbackResume`, and `PauseReboundResume` describe phase-based family concepts.
- `Turn` suffixes in replay-family names are concrete replay/source labels rather than generic concept definitions.

## Timeframe Hierarchy

The `5m` timeframe is the lead decision surface.

The `3m` timeframe is parallel research only. Do not promote `3m` back to the lead surface without a separate approved research decision.

Native timeframe ladder:

- `1m`
- `5m`
- `10m`
- `15m`
- `30m`
- `1440m`

Derived timeframe ladder:

- `3m`
- `60m`
- `120m`
- `240m`
- `360m`
- `720m`

## Default Validated Research Universe

The default validated refresh universe is:

- `MGC`
- `GC`
- `MES`
- `ES`
- `MNQ`
- `NQ`
- `ZT`
- `ZF`
- `ZN`
- `ZB`
- `6E`
- `6J`
- `6B`
- `6A`
- `HG`
- `PL`
- `CL`
- `NG`
- `MBT`
- `YM`

Exclusions:

- `QC` remains excluded from default research screens.
- `ZQ` remains excluded from default research screens.

Exclusion from the default screen does not mean permanent prohibition. It means the instrument is outside the current validated default universe.

## Execution And Reference Pairs

Explicit execution/reference pairs:

- `MGC` / `GC`
- `MES` / `ES`
- `MNQ` / `NQ`

Execution and reference instruments must remain explicit rather than inferred.

## Research Discipline

Settled constraints:

- Do not reopen broad data-foundation work unless new evidence shows the foundation is inadequate.
- Do not promote `3m` back to the lead timeframe.
- Do not revisit London-late first.
- Do not widen the midday-long slope floor beyond `-0.10`.
- Prefer narrow A/B tests or separator-only passes before promotion.
- Do not silently promote retained research into operator baseline status.
- Promotion requires a bounded audit and explicit decision.

## Trend Continuation Overlay

The future sandbox extension is named Trend Continuation Overlay.

It should be evaluated only after the core Regime Dashboard/model is established.

It is not current baseline logic and should not be treated as already approved.

## Trend Participation Engine Preference

High trade frequency is intentionally a feature for v1.

The strategy is intended to trade actively by design. Frequency may be reduced later if evidence shows overtrading.

Do not prematurely optimize for low trade count.

## MicroTrendParticipationLayer And Directional Participation Layer Direction

This is an important future design direction, not current production authority.

Preferred input posture:

- Prefer candle-based and time-based completed OHLCV inputs.
- Use `5m` completed candles as the lead surface.
- Suggested v1 horizons:
  - fast: last 2 completed `5m` candles.
  - medium: last 4 completed `5m` candles.
  - slow: last 6 completed `5m` candles.
- Avoid a raw fixed-N tick counter as the first implementation because tick counts create instrument/session/liquidity bias.

Authority boundary:

- The layer is a market-quality/state producer, not a strategy.
- It has no trade authority, broker mutation, hidden submit, or live-money eligibility.
- It may later inform entry quality, hold quality, or exit urgency only through an approved integration decision.
- It should distinguish impulse from persistent pressure.
- It must consume runtime hot-path data with provenance and freshness checks, never research-captured artifacts as live truth.

Candidate output states:

- `BULLISH_PERSISTENT_PRESSURE`
- `BEARISH_PERSISTENT_PRESSURE`
- `BULLISH_IMPULSE_ONLY`
- `BEARISH_IMPULSE_ONLY`
- `BULLISH_IMPULSE_DECAYING`
- `BEARISH_IMPULSE_DECAYING`
- `BALANCED_PRESSURE`
- `CHOP`
- `THIN_OR_STALE`
- `LOW_CONFIDENCE`

## Opposite-Side Thesis Discipline

When reviewing a setup or regime, valid opposite-side theses should be raised proactively.

This is intended to reduce frame lock and improve research quality. It does not mean forcing a countertrend trade.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_PRINCIPLES.md`
- `ENGINEERING_PROCESS.md`
- `docs/architecture/track-b-architectural-invariants.md`
- `docs/architecture/narratives/AN-003-trading-platform-evolution.md`
- `docs/architecture/narratives/AN-004-research-platform-evolution.md`
- `docs/PATTERN_ENGINE_PROMOTIONS.md`
- `docs/TREND_PARTICIPATION_ENGINE.md`
