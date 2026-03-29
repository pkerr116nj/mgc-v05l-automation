# Research-First Strategy Platform Charter

## Status

This charter supersedes the old parity-first objective as the governing framework for new strategy research and platform development.

## Purpose

The original objective was to reproduce strategies first developed in Thinkorswim and validate whether they appeared to offer positive returns. That objective was useful at the beginning, but it is no longer the governing purpose of this project.

Thinkorswim is now treated as:
- a source of early ideas
- a reference point
- a legacy benchmark lane

It is not treated as the platform whose limitations should define our research environment.

## Governing Objective

Build a research-first trading strategy platform that can:
- discover new candidate strategies
- test and compare them honestly
- evaluate them across longer history and broader regimes
- support richer execution assumptions than Thinkorswim allowed
- preserve trustworthy evidence and provenance
- promote only those candidates that survive disciplined review

## Core Principle

Baseline parity is now a reference function, not the primary mission.

The platform's job is not:
"match the old Thinkorswim behavior as faithfully as possible."

The platform's job is:
"find, test, compare, and harden economically robust strategies using better data, better tooling, and clearer evidence than Thinkorswim allowed."

## I. Rule Hierarchy

The platform now operates under three classes of rules:

### 1. Legacy Baseline Assumptions

These preserve old benchmark or reference behavior for comparison. They are not global truth.

Examples:
- legacy `5m`-only behavior
- replay-specific fill conventions such as `NEXT_BAR_OPEN`
- historical session restrictions previously adopted for benchmark purposes
- old assumptions imported from Thinkorswim limitations

These assumptions must remain available as an explicit benchmark lane, but they no longer govern all new research.

### 2. Research Platform Defaults

These are the default assumptions for new strategy research unless a study says otherwise.

Research defaults should favor:
- economic robustness
- broader history
- broader regime coverage
- explicit session comparison
- explicit execution-model comparison
- explicit provenance
- honest metrics and evidence-quality labeling

Research defaults may change over time as the platform improves.

### 3. Production Safety Invariants

These remain strict even as research becomes more flexible.

Examples:
- explicit provenance by lane
- explicit lifecycle-truth labeling
- fill-driven state transitions where applicable
- no fake metrics
- no silent fallback that hides weaker evidence
- no silent rewriting of benchmark results
- explicit separation of baseline, research, paper, and production evidence

## II. Baseline Parity Is Demoted, Not Deleted

The old `v0.5l` or ATP baseline and similar legacy strategy behavior remain important.

They are now classified as:
- reference benchmarks
- comparison lanes
- historical baselines

They are not treated as the governing target for all future strategy work.

Any old "locked implementation decisions" should be reclassified as:
- Legacy Baseline Assumptions

unless they are truly Production Safety Invariants.

## III. Research-First Platform Rules

### 1. Research may challenge old assumptions

No prior result from Thinkorswim or the initial port is to be treated as final truth.

This includes assumptions about:
- timeframe
- session inclusion or exclusion
- entry timing
- execution timing
- exit timing
- add or promotion logic
- regime definitions
- lookback length
- data source limitations

### 2. Longer-history research is required

Do not let short broker windows or limited platform history define the research universe.

The platform must be designed to support materially longer backfilled history so strategy conclusions are not anchored to a narrow sample.

### 3. Sessions are hypotheses, not doctrine

Asia, London, and US session behavior must be treated as research questions, not permanent law.

Examples:
- London exclusion in a frozen benchmark does not mean London is globally bad
- negative US contribution in one candidate does not mean US is globally invalid
- any session constraint should be treated as lane-specific unless repeatedly validated

### 4. Timeframe is not universal truth

Timeframe assumptions must be explicit and lane-specific.

Possible distinctions include:
- structural signal timeframe
- execution timeframe
- reporting timeframe

A legacy benchmark may remain `5m`.
A research lane may use different structural and execution timeframes.
The platform must not pretend these are the same thing.

### 5. Candidate testing must be explicit

All new strategy changes must be tested as named candidates against a frozen benchmark.

Do not bury semantic changes inside infrastructure work.
Do not modify the frozen benchmark silently.
Every candidate must preserve clear before/after comparison.

### 6. Keep failed candidates

Research failures are not trash.
Retain weaker candidates and failed variants as parked research branches when they may still be useful for later optimization or understanding.

Do not discard useful dead ends.

## IV. Evidence Rules

### 1. Provenance is mandatory

Every result must preserve its source lane explicitly.

Examples:
- `BENCHMARK_REPLAY`
- `PAPER_RUNTIME`
- `HISTORICAL_PLAYBACK`
- `RESEARCH_EXECUTION`
- `PRODUCTION`

### 2. Truth quality must be explicit

Do not present all studies, runs, or strategies as equally authoritative.

Lifecycle truth and evidence quality must be labeled honestly, for example:
- `FULL_LIFECYCLE_TRUTH`
- `HYBRID_ENTRY_BASELINE_EXIT_TRUTH`
- `BASELINE_ONLY`
- `UNSUPPORTED`

### 3. Missing data stays missing

If a metric is unavailable because source truth is incomplete:
- do not infer it casually
- do not synthesize it to make reports look cleaner
- show it as unavailable with a reason

### 4. Benchmark results must remain reproducible

Legacy baseline lanes must remain reproducible and inspectable even after the broader framework becomes more flexible.

## V. Platform Architecture Direction

### 1. The platform is strategy-centric, not file-centric

Users should be able to:
- choose strategy
- choose lane, run, or candidate
- inspect results
- compare outcomes

They should not need to navigate raw storage first.

### 2. The platform must support multiple lanes cleanly

At minimum, the platform should support:
- legacy baseline parity lanes
- research execution lanes
- paper-runtime lanes
- future production lanes

### 3. Shared infrastructure should not force semantic sameness

Shared framework code should make comparability easier.
It should not flatten meaningful differences between strategies, sessions, timeframes, or truth quality.

## VI. Promotion Rules

### 1. Promotion requires explicit evidence

A strategy or candidate is not promoted because it is interesting.
It is promoted because it shows clean, durable improvement against the frozen benchmark.

### 2. Gross P/L alone is not enough

Candidates must be evaluated on:
- net P/L
- average trade
- profit factor where supported
- drawdown where supported
- session contribution
- trade-family contribution
- evidence quality

### 3. No forced promotion

If no candidate is good enough, the correct answer is:
- keep the benchmark
- retain the candidate as research-only
- continue studying

## VII. Immediate Implementation Consequences

Effective immediately:

1. Old "locked implementation decisions" are no longer global law.
2. They must be reclassified into:
   - Legacy Baseline Assumptions
   - Research Platform Defaults
   - Production Safety Invariants
3. New research branches may challenge:
   - timeframe assumptions
   - session assumptions
   - lookback assumptions
   - execution-model assumptions
4. Frozen benchmarks must remain intact and comparable.
5. Provenance and evidence-quality honesty remain strict.

## VIII. What Must Stay Strict

These principles remain non-negotiable:
- no silent semantic drift in frozen benchmarks
- no fake metrics
- no provenance collapse
- no hiding weaker evidence behind polished summaries
- no deleting failed but informative research branches
- no claiming parity where parity has not been proven

## IX. Final Statement

This project is no longer a Thinkorswim-matching exercise.

It is now a research-first strategy platform whose purpose is to:
- use stronger data
- test more honestly
- compare more rigorously
- discover better strategies than the original constrained environment could support

Legacy strategies remain valuable benchmarks.
They do not define the limits of the platform.
