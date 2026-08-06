# BA-002: Prospective Market Context Coverage

Status: Draft

## Purpose

Record current market-context field availability for prospective NQ cohort validation without changing runtime capture or research semantics.

## Current Baseline

The prospective monitor consumes CRR v1, CRR validation, research eligibility, INV-005 peer definitions, and INV-006 period definitions. It does not consume live runtime or broker state.

## Available Fields

- Strategy/setup family is available through CRR entry anchors.
- Session is available through CTOE context validity summaries materialized into CRR.
- Regime validity state is available as context validity metadata, but not as a rich raw regime explanation.
- GRE and CRFD validity classifications are available where CTOE provides them.

## Available Only As Research Artifacts

- RA8/path evidence is optional and remains coverage-limited.
- INV-005 and INV-006 cohort definitions are available as generated research artifacts and are frozen for prospective validation.

## Absent Or Insufficient Fields

- VWAP relationship
- AVWAP relationship
- Opening-range position
- Trend state
- Participation state beyond strategy/setup family
- Slope
- Curvature
- Volatility state

These fields should not be inferred or reconstructed when source evidence is missing.

## Runtime-Hot-Path Considerations

Fields that require observation at decision time may require a runtime producer change. Such work is outside this baseline and should proceed only through the appropriate engineering process.

## Historical Reconstruction Limits

Historical reconstruction is not defensible for fields absent from CRR or its upstream source artifacts. Missing evidence remains missing.

## Authority Boundary

This baseline introduces no broker, runtime, Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, research authority, production recommendation, or trading gate.
