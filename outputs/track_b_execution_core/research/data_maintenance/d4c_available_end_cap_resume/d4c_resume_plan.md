# D4C Resume Plan

- generated_at: `2026-07-01T09:45:00-04:00`
- phase: `D4C`
- source partial run: `D4B`
- resume_download_executed: `false`

## D4B Partial State

D4B wrote April, May, and June 2026 partitions for all configured symbols:

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

D4B failed only the July 1 partial-day chunk for each symbol because the requested end exceeded Databento available end.

## D4C Dry-Run Command Shape

The safe resume dry-run used:

- start date: `2026-04-22`
- end date: `2026-07-01`
- end-boundary mode: `intraday-available-end`
- provider available end: `2026-07-01T13:10:00Z`
- dry-run: `true`

## D4C Dry-Run Result

- final verdict: `RESEARCH_MINUTE_BACKFILL_DRY_RUN_APPROVAL_REQUIRED`
- effective requested end: `2026-07-01T13:10:00+00:00`
- planned fetch end: `2026-07-01T13:10:00+00:00`
- cap reason: `PROVIDER_AVAILABLE_END`
- existing complete partitions skipped: `33`
- partitions that would fetch: `11`
- provider errors: `0`

Partition statuses:

- `SKIPPED_EXISTING_COMPLETE`: `33`
- `DRY_RUN_WOULD_FETCH`: `11`

## Safe Resume Recommendation

If approved later, run the same command without `--dry-run` and with `--operator-approved-large-refresh`, capped to the provider available end or a newer verified provider available end.

Do not run the resume download until explicitly approved.

## Research Regeneration

CRFD/GRE regeneration should wait until the resume completes without provider errors, or be explicitly scoped to the last complete coverage boundary.

