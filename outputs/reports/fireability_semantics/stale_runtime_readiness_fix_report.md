# Stale Runtime Readiness Fix Report

- Effective runtime staleness is no longer treated the same as waiting for the next 3-minute decision bar.
- Softening rule: if the runtime is healthy and updated within the larger of two decision bars or four poll intervals, `stale_runtime` is suppressed as a hard block.

## Current Snapshot

- stale runtime observed rows: `0`
- stale runtime suppressed rows: `0`
- stale runtime effective blocks: `0`

This keeps operator readiness from collapsing to zero just because the next completed bar has not arrived yet.