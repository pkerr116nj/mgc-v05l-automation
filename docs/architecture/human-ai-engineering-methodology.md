# Human-AI Engineering Methodology

## Status And Scope

This document records the engineering collaboration methodology used to design, review, implement, validate, document, and preserve the MGC-v05l platform.

It is an engineering methodology.

It is not:

- runtime logic
- trading logic
- an AI prompt library
- an implementation guide
- a coding standard
- an authority document

Knowledge half-life: long. The discipline should survive changes in specific tools.

## Purpose

Capture how engineering work is conceived, challenged, implemented, validated, documented, and preserved.

The methodology is about responsibilities and evidence, not any single AI product.

## Engineering Philosophy

Core principles:

- Architecture before implementation.
- Evidence before opinion.
- Independent review improves quality.
- Small generalized fixes outperform repeated local patches.
- Durable reasoning belongs in Git.
- Preserve knowledge with long half-life.
- Favor reusable engineering invariants.

The goal is not process theater. The goal is safer, faster, more coherent engineering.

## Human Responsibilities

Patrick is the product owner and final decision-maker.

Human responsibilities include:

- vision
- priorities
- acceptance
- architectural direction
- final authority

Human judgment decides what matters, what risk is acceptable, and when a proposed direction is worth pursuing.

## Architecture Partner Role

The architecture partner role helps turn ambiguous goals into coherent designs.

Responsibilities include:

- architectural reasoning
- consistency checking
- identifying reusable abstractions
- preserving long-term coherence
- identifying adjacent risks
- proposing future direction

This role should help the project see around corners without prematurely expanding scope.

## Independent Reviewer Role

Independent architectural review challenges major proposals before implementation.

Its purpose is to:

- challenge assumptions
- identify hidden coupling
- identify future technical debt
- expose alternative viewpoints

Independence means evaluating the proposal rather than defending it. The goal is improvement, not consensus or competition.

## Implementation Role

The implementation role changes the repository inside the accepted boundary.

Responsibilities include:

- bounded implementation
- repository changes
- validation
- publication
- preserving architecture
- avoiding undocumented redesign

Implementation should respect the agreed authority boundary and stop before broadening into adjacent work.

## Typical Engineering Workflow

Full workflow:

```text
Idea
-> Baseline
-> Research Question, when appropriate
-> Design Proposal
-> Independent Review
-> Review Resolution
-> ADR
-> Implementation
-> Validation
-> Closeout
-> Knowledge Capture
-> Commit
```

Not every routine change requires every stage. The full lifecycle applies when the change is material, cross-subsystem, or authority-sensitive.

## Knowledge Capture Specification

Knowledge capture preserves durable reasoning after meaningful work.

It should capture:

- significant completed work
- architectural decisions
- infrastructure evolution
- operational lessons
- durable philosophy

It should not capture:

- transient debugging
- ephemeral observations
- low-value detail

The test is whether the knowledge would be expensive, risky, or frustrating to rediscover later.

## Git As Institutional Memory

Conversation creates ideas.

Git preserves decisions.

Repository documents should allow future contributors to understand:

- what changed
- why it changed
- which tradeoffs were accepted
- where the project is going next

without replaying conversations.

## Independent Review Philosophy

Major architectural work benefits from review by reasoning independent of the proposal.

The goal is improvement.

It is not consensus.

It is not competition.

A strong review should make the resulting design clearer, safer, and more durable.

## AI Collaboration

AI collaboration should be described by responsibility rather than by product.

Possible collaborative roles include:

- architecture
- critique
- implementation
- summarization
- documentation
- validation

Responsibilities matter more than specific tools. Tools can change while the engineering discipline remains.

## Evidence Hierarchy

Engineering discussion should distinguish:

- evidence
- inference
- hypothesis
- opinion
- future vision

These categories must not become interchangeable.

Evidence can justify a conclusion. A hypothesis can guide investigation. Opinion can shape taste and priorities. Future vision can orient direction. Mixing them carelessly creates false certainty.

## Documentation Philosophy

Document only knowledge that is expensive to rediscover.

Avoid documenting every temporary implementation detail.

Architecture documents should explain why the system is shaped as it is. Runbooks should explain how to operate it. Generated artifacts should preserve factual outputs.

## Engineering Economics

Engineering effort should consider return on investment.

Useful filters include:

- first blocker
- reusable fixes
- bounded investigations
- avoiding unnecessary compute
- preserving engineering velocity

Correctness, safety, and authority boundaries remain non-negotiable. Within those constraints, the project should prefer work that improves reliability, autonomy, research quality, future speed, or economic understanding.

## Completion

A project increment is not complete until implementation, validation, documentation, and knowledge capture are complete.

For small routine changes, documentation may be a concise closeout. For major changes, it may require baselines, proposals, reviews, ADRs, narratives, or operational closeouts.

## Future Evolution

This methodology may evolve through governance.

No tool is permanent.

The engineering discipline should remain.

Future changes should preserve the same core posture: evidence, explicit authority, review where material, validation, and durable memory.

## Boundaries

This document grants no runtime authority.

It grants no broker authority.

It grants no production authority.

It grants no implementation approval.

It records engineering methodology only.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `ENGINEERING_PROCESS.md`
- `docs/architecture/evidence-driven-engineering-lessons.md`
- `docs/architecture/decision-intelligence-vision.md`
- `docs/architecture/research-analytics-platform-vision.md`
- `docs/architecture/narratives/AN-006-engineering-governance-evolution.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
- `docs/architecture/reviews/RR-001-DP-001-canonical-research-record.md`
- `docs/architecture/baselines/BA-001-current-research-platform.md`
- `docs/operations/closeouts/`
- `docs/roadmap/NEXT.md`
- `docs/roadmap/PARKING_LOT.md`
