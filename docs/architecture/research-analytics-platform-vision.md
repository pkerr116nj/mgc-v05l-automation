# Research Analytics Platform Vision

## Status And Scope

This document is a long-term architectural and product vision for the research analytics platform.

It is diagnostic and research-facing. It is not an implementation plan, not a database decision, not a UI decision, not a trading gate, not production authority, and not approval for machine learning, automatic optimization, or autonomous strategy changes.

Knowledge half-life: long. Implementations may change, but the research principles and authority boundaries should remain durable unless superseded through governance.

## Purpose

Describe the intended future research environment: a reproducible, traceable, evidence-centered platform that turns completed PAPER trades into reliable findings without creating production authority.

## Why The Platform Exists

Autonomous PAPER trading is valuable because it produces structured observations.

The research platform exists to convert those observations into reproducible evidence. It should help answer what works, when it works, why it works, when it fails, and what should be investigated next.

The platform should replace memory, anecdote, and after-the-fact chart interpretation with deterministic analysis. Research quality is more important than report quantity.

## Core Research Spine

The intended research spine is:

```text
Broker-backed completed trade evidence
-> Canonical Trade Records
-> Canonical Research Record
-> CTOL outcome metrics
-> CTOE context enrichment
-> RA8 finalized path evidence
-> RA7 canonical path summaries
-> RA3 attribution
-> CAE queries and reusable analytics
-> REF bounded experiments
-> Investigation / Evidence / Claim / Conclusion workflow
-> validated findings and advisory outputs
```

This spine consolidates existing layers rather than replacing them.

## Canonical Research Record Role

The Canonical Research Record (CRR) is the stable research-facing access contract.

CRR is derived and never authoritative. It exposes source provenance and join quality, makes exact, tolerance, missing, and broken joins distinguishable, and reduces repeated artifact-path and join rediscovery.

Materialized fields are reconciled caches only. CRR should not become a second analytics engine or shadow source of truth.

## Research Record Levels

The platform should keep these levels conceptually distinct:

### Execution Evidence

- orders
- fills
- broker acknowledgements
- commissions where available

### Completed Trade Identity

- canonical trade identity
- entry and exit anchors
- strategy and lane context

### Outcome Evidence

- realized P&L proxy
- duration
- MFE
- MAE
- giveback
- path coverage status

### Context Evidence

- session
- regime
- volatility
- VWAP/AVWAP
- opening-range position
- GRE/CRFD
- strategy/setup labels

### Research Conclusions

- cohort statistics
- experiments
- findings
- limitations
- recommendations

These levels should remain traceable and should not be collapsed into one opaque record.

## First-Class Research Questions

The platform should support questions such as:

- Which strategies, symbols, sessions, and regimes have positive expectancy?
- Which entry characteristics distinguish winners from losers?
- Which exits surrender excessive MFE?
- Which trades should have been filtered out?
- Which context fields materially improve expectancy?
- Which results survive out-of-sample and walk-forward validation?
- Which findings are driven by sparse or contaminated samples?
- Which strategies trade frequently because of design versus because of overtrading?
- Which opposite-side theses explain failures in the dominant interpretation?

These examples are not a fixed research backlog.

## Research Workflow

Expected workflow:

```text
Observation
-> Research Question
-> population definition
-> data-quality gate
-> bounded experiment
-> in-sample result
-> out-of-sample or walk-forward validation
-> finding
-> limitations
-> conclusion
-> roadmap or design action if justified
```

A result is not a conclusion until population, coverage, validity, and limitations are recorded.

## Reproducibility

Every analysis should preserve, where applicable:

- query or experiment identifier
- source artifact set
- source schema versions
- generated timestamps
- code or commit version
- population definition
- filters
- exclusions
- sample size
- coverage
- join-quality distribution
- validity state
- output fingerprint
- random seed if randomness exists

The same source set and code version should produce the same result.

## Evidence Quality

Research outputs should expose:

- sample size
- exact versus tolerance joins
- missing and broken joins
- path coverage
- data freshness
- session and instrument coverage
- outlier sensitivity
- partial-fill or scale complexity
- contamination
- in-sample versus out-of-sample status
- confidence intervals or uncertainty where appropriate

Unknown should remain Unknown.

Sparse evidence must not be presented as a precise conclusion.

## CAE Role

CAE should provide reusable grouped analytics, saved queries, execution provenance, result diffs, and deterministic insight summaries.

CAE should make common research questions easy to rerun. CAE should not become production authority.

CAE summaries must remain traceable to underlying populations and source records. AI-generated explanation may summarize evidence but may not replace deterministic calculations.

## REF Role

REF should support bounded offline experiments.

Experiments should declare:

- hypothesis
- population
- features
- comparison
- success criteria
- data-quality limits
- validation method

REF should keep experimental logic isolated from runtime.

An experiment result does not promote a rule or strategy automatically. Failed and inconclusive experiments are valuable evidence and should remain discoverable.

## Investigation / Evidence / Claim / Conclusion Model

Investigation is the container for a research problem.

Evidence is a traceable observation or dataset.

Claim is a testable interpretation.

Conclusion is the bounded disposition after evidence and validation.

Claims and conclusions should retain supporting and contradictory evidence. Conclusions should identify what would invalidate them.

A conclusion may be:

- supported
- partially supported
- unsupported
- inconclusive
- superseded

## Path And Exit Research

RA8 and RA7 are central to MFE, MAE, giveback, hold-time, and counterfactual exit research.

Path coverage status must be checked before path-derived conclusions are trusted. Legacy trades with incomplete paths must remain identifiable.

One strong move is not equivalent to persistent pressure.

Exit research should distinguish:

- bad entry
- bad hold
- bad exit
- unavailable evidence

Counterfactuals must remain bounded and should not pretend to recreate unavailable market microstructure.

## Entry And Setup Research

Structured setup labels should improve over time.

Entry research should compare context, not merely winner-versus-loser labels.

Important future features include:

- opening-range position
- VWAP/AVWAP relationship
- session
- regime
- trend/participation quality
- setup family
- slope/curvature
- volatility

Features should be captured structurally where possible rather than reconstructed from logs.

## Partial Fills, Scaling, And Position Cycles

The platform must eventually represent:

- multiple entry fills
- scale-ins
- partial exits
- scale-outs
- reversals
- overlapping strategy-scoped positions where permitted

Research representation must preserve sequence and ownership.

Do not force complex execution into a simplistic one-entry/one-exit model when evidence shows otherwise. Completed-trade and position-cycle definitions must remain explicit.

## Analytics Application Vision

A future internal analytics application may expose the research platform.

It should consume precomputed, validated research records rather than recompute everything on every page load.

Useful views may include:

- expectancy by strategy, symbol, session, regime, and setup
- entry-quality analysis
- exit giveback
- MFE/MAE distributions
- holding-time analysis
- path-coverage quality
- join-quality warnings
- experiment results
- trade drill-down

The first application should prioritize research utility over visual polish. The UI remains diagnostic and advisory.

No database, framework, host, or frontend choice is approved by this vision document.

## Storage And Materialization Direction

CRR v1 uses versioned JSONL as accepted by ADR-001.

Future DuckDB or Parquet materialization may be justified by scale and analytical query needs.

PostgreSQL or another server database should not be introduced without a clear operational requirement.

Storage choices should preserve deterministic rebuildability and source lineage. Research datasets should be rebuildable from source artifacts where practical.

Active research working sets and long-term archive retention should remain distinct.

## Automation And Refresh

Research refreshes may run manually, on demand, or on a bounded schedule.

Heavy rebuilds should avoid interfering with critical runtime windows. The analytics surface should read prepared outputs.

Research failure should not affect trading runtime.

No continuous real-time recomputation is required by default.

## Relationship To Decision Intelligence

Research Analytics produces evidence and validated findings.

Decision Intelligence interprets, compares, prioritizes, and presents advisory choices.

Research Analytics should remain more deterministic and calculation-centered. Decision Intelligence may use research outputs but must preserve provenance and uncertainty.

Neither changes production behavior without governance.

## Relationship To Mission Control

Mission Control focuses on current operational truth.

Research Analytics focuses on historical and empirical evidence.

Operational state and research findings should remain visually and conceptually distinct. A future shared interface may link them, but display proximity does not merge authority.

## Research Boundaries

Research artifacts are not runtime truth.

Research does not submit, cancel, close, or mutate broker state.

Research does not enable live money.

Research does not promote strategies automatically.

Research does not alter Managed Exit, Guardian, Safe-State, readiness, or trading gates.

Any production integration requires the Architecture Track.

## Failure Modes To Avoid

- parallel research systems with conflicting definitions
- dashboards hiding data-quality limits
- tolerance joins treated as exact
- experiments that cannot be reproduced
- winner-only reporting
- p-hacking or repeated parameter widening without discipline
- overfitting thin samples
- silently excluding losing or incomplete trades
- treating logs as the canonical dataset
- mixing live runtime data with stale research snapshots
- allowing generated prose to become the evidence
- rebuilding existing RA/CTOL/CTOE/CAE/REF layers unnecessarily

## Desired Future State

The desired future platform is one where:

- every completed trade is reconstructable to the degree evidence permits
- missing evidence is explicit
- core research questions are repeatable
- experiments are bounded and versioned
- conclusions preserve limitations
- analytics are fast enough for practical operator use
- research improvements compound over time
- runtime remains isolated and safe
- findings can inform governed engineering and trading decisions

## Near-Term Implications

This vision supports, but does not itself authorize:

- implementing CRR v1
- eliminating tolerance joins
- improving path coverage
- enriching completed trades with opening-range/VWAP/AVWAP/GRE/CRFD fields
- improving setup and exit attribution
- defining the first analytics application
- formalizing experiment/result catalogs

These are directional implications, not commitments unless already present in the roadmap.

## Boundaries And Governance

This document is not executable policy.

No runtime consumer may use it as trading logic.

No analytics result changes production automatically.

No framework, database, host, or UI is selected here.

Material implementation must follow the appropriate governance track.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `ENGINEERING_PROCESS.md`
- `docs/architecture/track-b-architectural-invariants.md`
- `docs/architecture/decision-intelligence-vision.md`
- `docs/architecture/narratives/AN-004-research-platform-evolution.md`
- `docs/research/pattern-engine-current-state.md`
- `docs/epics/EPIC-002-research-platform.md`
- `docs/architecture/baselines/BA-001-current-research-platform.md`
- `docs/architecture/proposals/DP-001-canonical-research-record.md`
- `docs/architecture/reviews/RR-001-DP-001-canonical-research-record.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
- `outputs/reports/research_data_inventory/research_data_inventory.md`
- `outputs/reports/research_data_inventory/research_data_flow_diagram.md`
- `docs/roadmap/NEXT.md`
- `docs/roadmap/PARKING_LOT.md`
