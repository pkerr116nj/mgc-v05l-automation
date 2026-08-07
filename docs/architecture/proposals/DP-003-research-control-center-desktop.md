# DP-003: Research Control Center Desktop

## Status

Draft revised after repository-grounded architecture fit audit.

This proposal authorizes architecture direction only. It does not approve real artifact reads, runtime integration, broker integration, strategy changes, Mission Control authority changes, or trading gates.

## Problem Statement

Track B now has durable research evidence, but that evidence is distributed across Canonical Research Record outputs, research eligibility, investigation records, prospective NQ checkpoint artifacts, and governance documents. The repository remains the durable institutional memory and source-backed research authority. The Research Control Center is a read-only presentation surface over producer-authored research records.

The product need is not another page of raw generated HTML. The product need is a small desktop surface that presents existing research objects, confidence, contradictions, missing evidence, and prospective validation progress without becoming a new research or production authority.

## Repository-Grounded Object Model

RCC v1 uses an object-model-over-projections architecture. Pages are projections over bounded read-models; pages are not the authority.

### V1 Real Objects

- Investigation: existing structured Investigation Records under `outputs/track_b_execution_core/research_analytics/investigations/INV-*/investigation.json` and durable summaries under `docs/research/investigations/`.
- Question: embedded in Investigation under `investigation_id` in v1.
- Evidence references: source paths and fingerprints in Investigation, CRR, eligibility, and prospective artifacts.
- NQ-specific Checkpoint: existing prospective NQ checkpoint rows keyed by `cohort_id` plus `checkpoint_trade_count`.

### Deferred Objects

- standalone Question and `question_id`;
- Claim schema;
- governed Contradiction schema;
- structured Roadmap producer;
- generic multi-instrument Checkpoint contract;
- stable Evidence IDs.

Claim remains conceptual and is not synthesized by RCC v1. Contradictory evidence may be displayed but not resolved by the desktop.

## Producer Authority Rules

- Confidence comes directly from producer-authored artifacts.
- `conclusion_status` comes directly from producer-authored Investigation artifacts.
- The desktop may not compute confidence.
- The desktop may not infer, resolve, or suppress contradictions.
- The desktop may not synthesize Claims from findings.
- Missing evidence remains missing.
- Roadmap is `NOT_READY` until a governed structured roadmap producer exists.

## Bounded Read-Models

RCC uses bounded read-models instead of one broad consolidated payload.

| Read-model | Disposition | V1 source stance |
| --- | --- | --- |
| `research_questions_and_investigations_v1` | `READY_WITH_SMALL_PRODUCER_EXTENSION` | v1 may ship with Question embedded in Investigation. |
| `research_evidence_coverage_v1` | `READY_FROM_EXISTING_ARTIFACTS` | first real model candidate after fixture-only Phase 1. |
| `research_checkpoints_v1` | `READY_WITH_SMALL_PRODUCER_EXTENSION` | v1 uses existing NQ-specific checkpoints keyed by `cohort_id` + `checkpoint_trade_count`. |
| `research_roadmap_v1` | `REQUIRES_NEW_GOVERNED_PRODUCER` | real model is not approved; show `NOT_READY` placeholder only. |

## First Screen: Triage

The first screen should be a triage view, not a broad dashboard.

Show:

- What changed: newest Investigation updates and latest NQ checkpoint deltas.
- What needs attention: producer-declared `STALE`, `MISSING`, or `INVALID` evidence and producer-authored contradictory evidence.
- What remains credible: investigations grouped by producer-authored confidence and conclusion state.
- Roadmap: visible `NOT_READY` placeholder with reason `GOVERNED_ROADMAP_PRODUCER_NOT_AVAILABLE`.

Primary drill-down destinations:

- Investigation Library.
- Evidence Coverage.
- Checkpoint History.

Roadmap remains a placeholder destination until a governed roadmap producer exists.

## Adapter Contract

### Permitted

- formatting;
- sorting;
- filtering;
- pagination;
- counting existing enumerated statuses;
- selecting producer-designated latest records;
- joining on producer-defined stable IDs only;
- preserving source paths and fingerprints;
- passing through producer-declared stale, missing, invalid, confidence, and conclusion states.

### Prohibited

- computing confidence;
- creating Claims;
- resolving contradictions;
- inferring roadmap completion;
- inventing stale thresholds;
- recomputing CRR joins;
- performing research analytics;
- reading broker or runtime state;
- mutations of any kind.

Anything that requires new research judgment belongs upstream in a governed producer, not in the desktop adapter.

## Renderer Contract

The renderer is presentation only.

- No direct filesystem research reads.
- No research computation.
- No operational IPC reuse.
- No mutation controls.
- No broker, runtime, strategy, Guardian, Safe-State, Managed Exit, readiness, reconciliation, or trading-gate authority.

## Desktop Integration

RCC should be a separate read-only Electron window within the existing Electron/React/Vite desktop app.

- Reuse the existing React/Vite renderer shell and styling patterns.
- Use main-process read-model adapters.
- Expose bounded RCC IPC channels only.
- Preserve context isolation.
- Keep cross-platform path handling in the main process.
- Target macOS first.
- Remain Linux-compatible by contract.
- Do not unnecessarily prevent Windows packaging.

The RCC window must be clearly distinct from Mission Control/operator controls.

## Fixture-Only Phase 1

Phase 1 may implement only:

- separate RCC BrowserWindow;
- restricted RCC preload bridge;
- fixture-only IPC endpoints for the four bounded read-models;
- deterministic fixture read-models;
- static renderer triage, Investigation Library, Evidence Coverage, Checkpoint History, and Roadmap placeholder views;
- fail-soft behavior for invalid, missing, stale, and unsupported-version fixture states;
- tests proving no filesystem reads, no operational authority, and no broker/runtime imports.

Phase 1 must not read real research artifacts, CRR artifacts, durable candles, runtime state, broker state, or roadmap documents.

## V1 Read-Model Fixture Requirements

`research_questions_and_investigations_v1` fixture includes multiple Investigations, embedded questions, producer-authored confidence, producer-authored conclusion status, contradictory evidence, source paths, and source fingerprints.

`research_evidence_coverage_v1` fixture includes `HEALTHY`, `VALID_WITH_WARNINGS`, `STALE`, `MISSING`, and `INVALID` cases.

`research_checkpoints_v1` fixture includes zero prospective data, 10-trade checkpoint, 20-trade checkpoint, and checkpoint delta examples.

`research_roadmap_v1` fixture contains only `NOT_READY` with reason `GOVERNED_ROADMAP_PRODUCER_NOT_AVAILABLE`.

## Fail-Soft Behavior

- One invalid read-model must not break other views.
- Unsupported schema versions show explicit error.
- Missing models show `NOT_READY` or `MISSING` state.
- Stale models remain visible with stale indication.
- No synthetic fallback data is created.

## Security And IPC

- Renderer has no direct filesystem access.
- RCC preload exposes no mutation-capable operational APIs.
- RCC IPC allowlist is explicit.
- Context isolation remains enabled.
- Business logic has no broker/runtime imports.

## Cross-Platform Contract

No RCC business logic may contain:

- hard-coded `/Users` paths;
- macOS-only open commands;
- platform-specific path separators;
- platform-specific artifact locations.

Platform behavior stays behind main-process adapters.

## Testing Requirements

- separate RCC window creation;
- no operational mutation IPC exposed;
- bounded read-model schema parsing;
- unsupported-version rejection;
- model-level fail-soft behavior;
- Investigation confidence pass-through unchanged;
- no Claim synthesis;
- contradictory evidence pass-through unchanged;
- Roadmap `NOT_READY` behavior;
- checkpoint fixture rendering;
- renderer no filesystem access;
- deterministic fixture payloads;
- cross-platform-safe path handling;
- no broker/runtime/strategy imports;
- no recommendation language.

## Deferred Producer Work

- standalone Question schema / `question_id`;
- stable Evidence IDs;
- Claim schema;
- Contradiction schema;
- `research_roadmap_status_v1` producer;
- generic Checkpoint contract.

## Non-Goals

- No trading controls.
- No broker data.
- No live runtime control.
- No strategy changes.
- No research conclusion generation.
- No AI recommendations.
- No new database.
- No broad framework replacement.
- No Mission Control authority coupling.
- No real artifact reads in Phase 1.

## First Blocker

The first blocker for real artifact integration is the lack of governed structured producers for standalone Claims, governed contradictions, and roadmap status. Fixture-only Phase 1 can proceed because it proves the desktop boundary without depending on those missing producers.

## Related Documents

- `../mission-control-philosophy.md`
- `../decisions/ADR-001-canonical-research-record.md`
- `../decisions/ADR-002-prospective-market-context-capture.md`
- `../../research/NQ_PROSPECTIVE_COHORT_VALIDATION_CONTRACT.md`
- `../../research/investigations/INVESTIGATION_RECORD_CONTRACT.md`
- `../../epics/EPIC-002-research-platform.md`
- `../implementation-plans/IP-002-research-evidence-explorer-v1.md`
