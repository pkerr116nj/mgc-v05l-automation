# CAE2 Query Contract

Generated: 2026-07-03

## Purpose

CAE2 turns grouped analytics into declarative queries. A client supplies a `CanonicalAnalyticsQuery`; the Canonical Analytics Engine resolves registered dimensions, metrics, filters, and validity requirements, then returns a `CanonicalAnalyticsResult`.

## Query Fields

- `name`: stable query identifier
- `dimensions`: registered dimension names or raw field names
- `metrics`: registered metric names
- `filters`: explicit field predicates
- `named_filters`: registered filters such as `valid_gre_only`
- `validity_requirements`: registered validity requirements
- `order_by`: grouped-row output fields used for sorting
- `min_sample_size`: minimum grouped sample count
- `min_sample_class`: minimum grouped sample class
- `missing_value`: replacement for null/blank dimension values

## Result Fields

- `schema_version`
- `query`
- `generated_at`
- `grouped_rows`
- `summary`
- `validity_metadata`
- `provenance`
- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`

## First Migrated Client

Core Expectancy Analytics now builds CAE queries for grouped views instead of owning independent grouping logic.

## Safety

CAE2 is diagnostic/research only. It has no broker, order, runtime, Managed Exit, strategy, or gate authority.
