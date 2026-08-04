# Engineering Process

This repository uses Git as the authoritative project memory. Conversations generate ideas; durable plans, designs, decisions, and conclusions live under `docs/`. Generated artifacts remain under `outputs/`.

Research must remain isolated from production authority unless explicitly approved. Missing evidence may not be inferred or fabricated.

## Lifecycle

Idea -> Baseline Assessment -> Research Question, when applicable -> Design Proposal -> Independent Architecture Review for major decisions -> Review Resolution -> Architecture Decision Record -> Implementation -> Validation -> Closeout

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
- Reports residual risk and data-quality limits.

### Closeout

Purpose: finish the work as a traceable project increment.

Completion criteria:

- Summarizes what changed.
- Links committed code, docs, and reports.
- Records validation evidence.
- Identifies follow-up work without broadening the completed scope.

