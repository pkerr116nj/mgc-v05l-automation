# Project Principles

These principles capture the durable engineering posture of the MGC-v05l automation project. They are not implementation rules, runbooks, or configuration instructions.

Track B-specific invariants are recorded in `docs/architecture/track-b-architectural-invariants.md`.

The engineering lessons behind the current evidence-first posture are summarized in `docs/architecture/evidence-driven-engineering-lessons.md`.

The human-AI engineering collaboration methodology is summarized in `docs/architecture/human-ai-engineering-methodology.md`.

Operational retention and archive policy is recorded in `docs/operations/data-retention-and-archive-policy.md`.

## Authority Is Singular

Every important operational fact should have one controlling source.

Broker truth controls current exposure and open orders. Canonical Trade Records control completed-trade identity. CTOL, CTOE, RA8, RA7, and other layers add analytics, enrichment, and evidence, but they do not replace their upstream authorities.

## Derived Artifacts Never Replace Authoritative Sources

Derived artifacts exist to make evidence easier to use. They must remain traceable to their sources and must not become hidden control planes.

## Evidence Is Never Fabricated

Missing evidence remains missing. Unknown joins, sparse data, absent context, and incomplete path coverage must be reported rather than inferred.

## Runtime And Research Remain Isolated

Research can explain, compare, and propose experiments. It must not influence broker authority, Managed Exit, runtime readiness, strategy logic, Guardian, Safe-State, or trading gates unless explicitly approved through a separate process.

## Simplicity Beats Unnecessary Complexity

The preferred design is the smallest one that preserves authority, evidence, safety, and future maintainability.

## Incremental Evolution Beats Wholesale Replacement

The platform has valuable existing layers. New work should consolidate and evolve them rather than replace them with parallel systems.

## Major Architectural Changes Receive Independent Review

Material decisions should move through baseline assessment, design proposal, independent review, review resolution, and architecture decision before implementation. `ENGINEERING_PROCESS.md` defines the threshold for major, routine, and emergency changes.

## Documentation Records Reasoning

Runbooks explain how to operate the system. Architecture documents explain why the system is shaped the way it is.

## Git Is Project Memory

Conversations generate ideas. Durable plans, designs, decisions, principles, and conclusions belong in version-controlled documentation.
