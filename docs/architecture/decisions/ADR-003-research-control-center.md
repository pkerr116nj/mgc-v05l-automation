# ADR-003: Research Control Center

## Status

Accepted for fixture-only Phase 1.

Real artifact reads, roadmap inference, Claim synthesis, contradiction resolution, broker/runtime access, and mutation controls are not approved by this ADR.

## Decision Date

2026-08-06

## Context

The Track B research platform now has Canonical Research Record, research eligibility, Investigation Records, NQ prospective validation, and architecture governance. The evidence exists, but it is distributed across producer artifacts and generated reports.

A desktop surface is useful only if it presents producer-authored research state without becoming a new authority layer. Repository audit showed that Investigation is the strongest current structured object, while standalone Question, Claim, governed Contradiction, generic Checkpoint, and structured Roadmap remain upstream gaps.

## Decision

Adopt the Research Control Center architecture for fixture-only Phase 1.

The accepted architecture is object-model-over-page-model. Pages are projections over bounded read-models, not authority sources.

Accepted bounded read-models:

- `research_questions_and_investigations_v1`
- `research_evidence_coverage_v1`
- `research_checkpoints_v1`
- `research_roadmap_v1`

RCC will be a separate read-only Electron window in the existing desktop app.

## V1 Object Boundary

- Investigation is the primary structured v1 object.
- Question remains embedded in Investigation.
- Evidence is represented by source paths and fingerprints.
- NQ-specific Checkpoints are allowed for v1.
- NQ checkpoint identity is `cohort_id` plus `checkpoint_trade_count`.
- Confidence and `conclusion_status` are producer-authored only.
- Roadmap is `NOT_READY` until a governed structured roadmap producer exists.
- Contradictory evidence may be displayed but not resolved.

## Adapter Contract

Permitted:

- formatting;
- sorting;
- filtering;
- pagination;
- counting existing enumerated statuses;
- selecting producer-designated latest records;
- joining on producer-defined stable IDs only;
- preserving source paths/fingerprints;
- passing through producer-declared stale, missing, invalid, confidence, and conclusion states.

Prohibited:

- computing confidence;
- creating Claims;
- resolving contradictions;
- inferring roadmap completion;
- inventing stale thresholds;
- recomputing CRR joins;
- performing research analytics;
- reading broker/runtime state;
- mutations of any kind.

## Renderer And IPC Boundary

The renderer must not read research files directly, compute research, or receive operational mutation APIs.

The RCC window uses a restricted preload bridge with bounded read-model IPC only. Context isolation remains enabled. Platform-specific behavior and path handling stay in the main process.

## Cross-Platform Contract

Mac desktop is the first target. Linux compatibility is required by contract. Windows must not be unnecessarily prevented.

RCC business logic must avoid hard-coded `/Users` paths, macOS-only commands, platform-specific separators, and platform-specific artifact locations.

## Fixture-Only Phase 1 Approval

Phase 1 may implement:

- separate RCC BrowserWindow;
- restricted RCC preload bridge;
- fixture-only read-model endpoints;
- deterministic fixture payloads;
- static renderer triage, Investigation Library, Evidence Coverage, Checkpoint History, and Roadmap placeholder views;
- fail-soft model display;
- tests for boundary, schemas, deterministic fixtures, no Claim synthesis, contradiction pass-through, and no broker/runtime imports.

Phase 1 must not read real artifacts.

## Not Approved

- Real artifact reads.
- Roadmap inference.
- Claim synthesis.
- Contradiction resolution.
- Broker access.
- Runtime access.
- Mission Control authority coupling.
- Mutation controls.
- Production recommendations.
- Trading gates.

## Consequences

Positive:

- RCC can start with a safe desktop boundary.
- Investigation evidence becomes easier to review without changing authority.
- The architecture prevents UI-generated research confidence.
- Missing upstream producers remain visible instead of being hidden by UI inference.

Costs:

- Phase 1 is fixture-only and does not yet improve real artifact access.
- Real RCC value requires later producer work and real read-model integration.
- Roadmap remains `NOT_READY` until governed structure exists.

## Deferred Producer Work

- standalone Question schema / `question_id`;
- stable Evidence IDs;
- Claim schema;
- Contradiction schema;
- `research_roadmap_status_v1` producer;
- generic multi-instrument Checkpoint contract.

## Links

- `../proposals/DP-003-research-control-center-desktop.md`
- `../decisions/ADR-001-canonical-research-record.md`
- `../decisions/ADR-002-prospective-market-context-capture.md`
- `../../research/investigations/INVESTIGATION_RECORD_CONTRACT.md`
- `../../research/NQ_PROSPECTIVE_COHORT_VALIDATION_CONTRACT.md`
- `../../epics/EPIC-002-research-platform.md`
