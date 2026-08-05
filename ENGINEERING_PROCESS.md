# Engineering Process

This repository uses Git as the authoritative project memory. Conversations generate ideas; durable plans, designs, decisions, and conclusions live under `docs/`. Generated artifacts remain under `outputs/`.

Research must remain isolated from production authority unless explicitly approved. Missing evidence may not be inferred or fabricated.

## Lifecycle

Idea -> Baseline Assessment -> Research Question, when applicable -> Design Proposal -> Independent Architecture Review for major decisions -> Review Resolution -> Architecture Decision Record -> Implementation -> Validation -> Closeout

This is the full architecture lifecycle. Routine implementation changes may use a lighter path when they do not meet the major-change threshold below.

## Change Tracks

### Routine Track

Use the routine track for narrow changes that do not alter authority boundaries, accepted ADRs, public schemas, cross-subsystem contracts, runtime behavior, broker behavior, Managed Exit behavior, strategy behavior, or generated artifact meaning.

Routine track: Implementation -> Validation -> Closeout.

Closeout should still identify evidence, tests, residual risk, and follow-up work.

### Architecture Track

Use the full lifecycle for major changes.

A change is major when it:

- Changes or introduces an authority boundary.
- Changes an accepted ADR or creates a new architectural contract.
- Crosses subsystem boundaries.
- Changes public artifact schemas or durable join semantics.
- Changes runtime, broker, Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, or trading-gate behavior.
- Changes how research evidence can influence future decisions.
- Creates material migration, rollback, or data-quality risk.

### Emergency Track

Use the emergency track only for urgent safety or operability defects that cannot reasonably wait for the full architecture lifecycle.

Emergency track: documented issue -> minimal fix -> validation -> closeout -> follow-up planning item.

Emergency changes must:

- Fix the first proven defect only.
- Preserve existing authority boundaries unless the emergency is the authority defect itself.
- Record why the routine or architecture track was not appropriate before the fix.
- Create follow-up work in `docs/roadmap/NEXT.md` when architectural review or broader cleanup is still needed.

## Research Question Trigger

Create a Research Question when the decision depends on empirical evidence, trade populations, market context, outcome comparisons, or data-quality limits.

Skip RQ when the work is purely structural, editorial, or already justified by a factual baseline and does not require new empirical analysis.

## Independent Review Outcomes

Independent Architecture Review should produce one of these outcomes:

- No blocking concerns: proceed to Review Resolution and ADR.
- Approve with minor revisions: resolve findings in RR, revise the DP, then proceed to ADR if the revisions are complete.
- Needs substantial redesign: return to DP before ADR.
- Rejected: stop the proposal or create a new proposal with a different direction.

An independent reviewer must be separate from the proposal author for the review task and must not be the same reasoning path that produced the design. For a solo project, that can be a separate review session or reviewer role with explicit challenge criteria.

## Artifacts

### Baseline Assessment (BA)

Purpose: document the current factual state before choosing a design.

Completion criteria:

- Identifies source evidence.
- Separates facts from recommendations.
- States existing authority boundaries.
- Lists known gaps, risks, and constraints.
- Recommends whether design work should proceed.

### Research Question (RQ)

Purpose: frame an empirical question that can be answered with available or explicitly requested data.

Completion criteria:

- States the question and population.
- Defines required measurements.
- Defines data-quality limits.
- Avoids production recommendations unless explicitly authorized.

### Design Proposal (DP)

Purpose: propose a change or contract before implementation.

Completion criteria:

- Defines the problem, non-goals, proposed shape, alternatives, and acceptance criteria.
- Preserves established authority boundaries.
- Identifies migration and validation needs.

### Independent Architecture Review

Purpose: challenge major designs before implementation.

Completion criteria:

- Reviews assumptions, authority boundaries, failure modes, migration risk, and testability.
- Produces actionable findings or explicitly states no blocking concerns.

### Review Resolution (RR)

Purpose: record how review findings were resolved.

Completion criteria:

- Links to the reviewed proposal.
- Lists each finding and resolution.
- Identifies any design changes before implementation.

### Architecture Decision Record (ADR)

Purpose: make an accepted architecture decision durable.

Completion criteria:

- States context, decision, consequences, and rejected alternatives.
- Links to the baseline, proposal, review, and resolution.
- Identifies the expected implementation boundary.
- Updates or explicitly leaves unchanged `SYSTEM_OVERVIEW.md`, `PROJECT_PRINCIPLES.md`, and active epics when the decision changes authority hierarchy, subsystem lists, or enduring principles.

### Implementation

Purpose: make the approved change at the correct authority boundary.

Completion criteria:

- Stays within the accepted design.
- Avoids unrelated refactors.
- Preserves runtime, broker, Managed Exit, and strategy authority unless explicitly approved.
- Adds focused tests for the changed behavior.

### Validation

Purpose: prove the implementation behaves as intended.

Completion criteria:

- Runs focused tests and relevant safety checks.
- Runs format checks such as JSON parse, markdown sanity, `git diff --check`, or compile checks when applicable.
- Verifies that research/runtime isolation, authority boundaries, and missing-evidence handling remain intact when the change touches those areas.
- Reports residual risk and data-quality limits.

### Closeout

Purpose: finish the work as a traceable project increment.

Completion criteria:

- Summarizes what changed.
- Links committed code, docs, and reports.
- Records validation evidence.
- Identifies follow-up work without broadening the completed scope.
- Places follow-up planning items in `docs/roadmap/NEXT.md` or `docs/roadmap/PARKING_LOT.md` as appropriate.
- Updates or explicitly leaves unchanged `SYSTEM_OVERVIEW.md`, `PROJECT_PRINCIPLES.md`, and active epics when the completed work changes authority hierarchy, subsystem lists, or enduring principles.
