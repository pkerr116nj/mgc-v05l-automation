# Active Trend Participation Engine v1 Baseline Overlay

## 1. Purpose

This document defines the ATP Engine v1 baseline overlay that has been earned by replay/paper evidence so far.

It is a companion baseline, not a replacement baseline.

The legacy v0.5l benchmark documents and legacy benchmark settings remain authoritative for the original parity path.

ATP v1 is a separate overlay that:
- preserves the legacy replay-first and safety posture,
- keeps the legacy path intact,
- formalizes ATP-specific context and execution assumptions without rewriting the original v0.5l baseline as if ATP were the same system.

## 1A. Current Promoted Benchmark

The current promoted ATP companion benchmark is:
- `ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only`
- internal label: `ATP_COMPANION_V1_ASIA_US`

Reference benchmark note:
- [ATP Companion Baseline v1 Benchmark](./ATP_COMPANION_BASELINE_V1_BENCHMARK.md)

Named companion config:
- `config/atp_companion_baseline_v1_asia_us.yaml`

Tracked paper strategy posture:
- benchmark is now surfaced as a tracked paper strategy in the app-facing paper surface
- paper tracking remains observational and restart-safe
- this does not imply live execution authority

## 2. Separation Rule

Two baselines now coexist in the repository:

1. Legacy v0.5l locked baseline
- governs the original legacy benchmark implementation path
- remains tied to legacy completed-bar strategy semantics and legacy replay assumptions

2. ATP v1 baseline overlay
- governs ATP replay/paper research semantics only
- defines how ATP interprets context, timing, and entry-price quality
- does not widen live submit scope
- does not replace the legacy strategy families

If a behavior is not explicitly promoted into the ATP baseline overlay, it should not be inferred as part of ATP baseline semantics.

## 3. Retained From Legacy Baseline

ATP v1 currently retains the following baseline assumptions from the legacy baseline:

- replay-first development posture
- single-symbol `MGC` scope
- SQLite-backed replay/paper research workflow
- session-reset VWAP policy
- fill-driven state transitions
- deterministic sequential processing
- one-position-at-a-time enforcement
- legacy replay fill policy remains unchanged for the legacy path
- legacy Bull Snap / Asia VWAP reclaim / Bear Snap semantics remain intact on the legacy path

## 4. ATP Baseline Overlay Assumptions

The following assumptions are now formalized as ATP baseline overlay semantics.

### 4.1 Context And Timing Split

- `5m` bars define ATP context only.
- `1m` bars define ATP executable timing only.
- ATP does not move the whole engine to `1m`.
- ATP remains a two-timeframe design:
  - `5m` for bias, pullback state, pullback envelope, and readiness
  - `1m` for executable continuation timing and entry-price quality

### 4.2 Completed-Bar Context Still Governs ATP State

- ATP bias, pullback classification, and entry readiness are still determined from completed `5m` bar context.
- ATP does not replace that context layer with intrabar noise.
- The `1m` layer activates only after `5m` ATP context is already armed.

### 4.3 VWAP Price-Quality Baseline

- `VWAP_FAVORABLE` is the preferred ATP executable class.
- `VWAP_NEUTRAL` is allowed only under stricter constraints.
- `VWAP_CHASE_RISK` remains blocked.

Current ATP evidence-supported posture:
- favorable executions are the preferred baseline ATP outcome,
- neutral executions are tolerated only in a narrower band than the original ATP timing bridge,
- chase-risk executions are not baseline ATP entries.

### 4.4 ATP Scope Constraints That Remain Baseline

- ATP remains replay/paper first.
- ATP does not widen live submit scope in the current baseline.
- ATP does not change broker or control-plane safety semantics in the current baseline.
- ATP does not change manual-live stock code in the current baseline.
- ATP does not change the global legacy replay fill policy in the current baseline.
- ATP executable participation is currently baseline-enabled in `ASIA` and `US`.
- ATP executable participation is currently baseline-disabled in `LONDON` until a better London-specific continuation model is earned.

## 5. ATP Behaviors Explicitly Not Yet Baseline

The following ideas are intentionally not yet promoted to ATP baseline status:

- promotion / add logic
- staged participation beyond one-position-at-a-time enforcement
- exit redesign
- London re-entry into ATP executable baseline coverage
- clustering controls or anti-cluster rules
- multi-symbol widening
- timeframe widening beyond the current `5m` context plus `1m` timing bridge
- live ATP submit expansion

These may become later ATP decisions, but they are not baseline assumptions yet.

## 6. Baseline Interpretation Matrix

### Legacy v0.5l locked baseline

- `baseline_parity_mode` keeps `5m` as the sole decision/evaluation timeframe for the legacy benchmark lane
- completed-bar-only evaluation
- replay fills at `NEXT_BAR_OPEN` in the baseline-parity lane
- session-reset VWAP
- legacy entry-family semantics

### ATP v1 baseline overlay

- `5m` completed-bar context remains the source of truth for ATP setup state
- `1m` timing is used only for ATP executable entry timing in replay/paper
- session-reset VWAP remains retained
- ATP executable session scope is currently `ASIA` plus `US`, with `LONDON` excluded
- VWAP execution quality is explicit:
  - preferred: `VWAP_FAVORABLE`
  - constrained: `VWAP_NEUTRAL`
  - blocked: `VWAP_CHASE_RISK`
- one-position-at-a-time remains in force for now

## 7. Operational Implication

Repository contributors should not treat ATP v1 as proof that the legacy benchmark lane has changed.

Instead:
- use the legacy benchmark documents for v0.5l parity behavior,
- use this overlay for ATP replay/paper behavior,
- promote future ATP changes only when they are intentionally documented as ATP baseline decisions.
