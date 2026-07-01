# D4A Research Data Universe Contract

Generated: 2026-07-01

## Purpose

Replace the hardcoded Databento research backfill ticker allowlist with an explicit research-data universe contract.

The universe contract is research-only. It is not broker authority, runtime authority, strategy authority, Managed Exit authority, or a trading gate.

## Config

Path:

`config/research_data_universe.json`

The config declares:

- schema version,
- default symbols,
- enabled research symbols,
- Databento continuous symbols,
- family classification,
- micro-to-reference relationships,
- routine refresh limits,
- estimated row/storage assumptions,
- authority boundaries.

## Enabled D4A Universe

Supported:

- `GC`
- `MGC`
- `ES`
- `MES`
- `NQ`
- `MNQ`
- `ZT`
- `ZF`
- `ZN`
- `ZB`
- `PL`

## Guardrails

The backfill CLI now:

- loads configured research universe,
- rejects unknown symbols,
- requires explicit `--end-date`,
- reports estimated bars/storage,
- reports whether operator approval is required,
- fails closed for non-dry-run large refreshes without `--operator-approved-large-refresh`,
- preserves research-only/no-runtime/no-broker/no-submit fields.

## Routine Refresh Policy

Configured default:

- maximum routine range without approval: 10 calendar days,
- estimated 1m bars per calendar day: 1,380,
- estimated Parquet bytes per 1m bar: 220.

Large catch-up ranges may be dry-run, but real download/write execution requires explicit approval.

## D4A Non-Goals

D4A does not:

- download Databento data,
- mutate replay DB,
- schedule weekly refresh,
- alter runtime,
- alter broker behavior,
- change strategies,
- implement the replay DB integrity checker.

