# CAE1 Canonical Analytics Engine Design

Generated: 2026-07-03

## Purpose

The Canonical Analytics Engine is a reusable, diagnostic-only aggregation layer for Track B analytics. It accepts records plus explicit dimensions, metrics, filters, and context-validity rules, then returns grouped descriptive analytics.

It is not an execution engine, strategy optimizer, trading gate, or runtime authority.

## MVP Contract

Inputs:

- records: sequence of mapping-like analytics rows
- dimensions: field names used to form group keys
- filters: JSON-friendly predicates such as `eq`, `in`, `exists`, `gt`, and `lte`
- validity rules: provider-independent context validity predicates, currently centered on `VALID`
- metrics: reusable metric specifications

Outputs:

- schema version
- request metadata
- input and matched row counts
- grouped metrics
- diagnostic guardrail flags

## Initial Metric Pack

The MVP includes the common expectancy metric pack:

- count
- sample class and sample rank
- win rate
- average and median P&L proxy
- average and median realized points
- P&L proxy percentiles
- best and worst trade references
- average and median hold seconds
- data-quality flag counts
- enrichment data-quality flag counts

## First Client

`track_b_core_expectancy_analytics.py` now delegates grouped expectancy aggregation to the Canonical Analytics Engine while preserving its output schema.

Existing views migrated in CAE1:

- strategy/lane expectancy
- session expectancy
- instrument expectancy
- side expectancy
- exit-policy expectancy
- VIX context expectancy
- valid GRE context expectancy

## Guardrails

The engine emits:

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`

It has no broker imports, runtime imports, strategy imports, order authority, or mutation authority.

## Future Clients

Future phases can migrate these consumers onto the same engine:

- context-aware trade analytics
- trade outcome scorecards
- research discovery candidate generation
- shadow research policy summaries
- regime validation scorecards
