# Decision Intelligence Vision

## Status And Scope

This document is a long-term architectural vision.

It is advisory and directional. It is not an implementation plan, not a product specification, not a trading gate, not a source of broker authority, not a source of runtime authority, not a source of research authority, not a source of operator authority, and not approval for autonomous self-modification.

Knowledge half-life: long. The concepts here are expected to remain relevant for more than five years unless superseded through governance.

## Purpose

Explain the platform's north star: turning trustworthy runtime evidence into better human and system decisions while preserving explicit authority boundaries.

## Why Decision Intelligence Exists

The trading runtime generates actions and evidence.

The research platform converts completed trades and market context into reproducible analysis.

Analytics organizes that evidence into findings.

Decision Intelligence should help convert validated findings into clearer choices.

The purpose is to improve the quality, consistency, traceability, and timing of decisions.

The purpose is not to remove human judgment or allow unreviewed research outputs to control trading.

## Core Flow

Conceptual flow:

```text
Runtime and broker evidence
-> Canonical and enriched research records
-> Reproducible analytics and bounded experiments
-> Findings with provenance and confidence limits
-> Advisory recommendations
-> Human review and governance
-> Approved implementation or operating decisions
```

The loop is evidence-driven and governed.

## Human Authority

Patrick remains the final product owner and decision-maker.

Major architectural, production, and live-capital decisions remain human-approved.

The operator must be able to understand:

- what is recommended
- why it is recommended
- which evidence supports it
- what uncertainty remains
- which authority boundary applies

Decision Intelligence should reduce frame lock and surface valid opposite-side theses. It should challenge assumptions without forcing action.

Human override and refusal remain valid outcomes.

## Machine-Assisted Decisions

Potential future assistance may include:

- ranking research questions
- identifying data-quality gaps
- detecting recurring failure patterns
- comparing entry and exit cohorts
- surfacing regime/context relationships
- explaining why results changed
- identifying contradictory evidence
- proposing bounded experiments
- prioritizing engineering work by expected ROI
- summarizing operational risk and freshness
- presenting alternative interpretations

Assistance remains diagnostic or advisory unless separately promoted through approved governance.

## Decisions That Must Not Become Automatic By Default

The following must not silently become machine-controlled:

- live-money eligibility
- broker submit authority
- strategy promotion
- position sizing changes
- authority hierarchy changes
- Managed Exit authority
- Guardian or Safe-State overrides
- research-to-runtime promotion
- architecture changes
- self-modifying strategy logic

Any promotion toward automation requires:

- explicit architecture work
- independent review
- validation
- clear rollback
- preserved provenance
- human approval

## Relationship To Mission Control

Mission Control is the principal operator-facing surface for current operational truth.

Decision Intelligence may eventually appear within Mission Control as advisory context.

Mission Control continues to report current state and authority. Decision Intelligence explains, compares, and recommends.

Neither gains broker or trading authority merely by being displayed together.

Advisory outputs must be visibly separated from authoritative operational state.

## Relationship To The Research Platform

The Canonical Research Record should provide a stable research-facing contract.

CTOL and CTOE remain outcome and enrichment layers.

RA7 and RA8 remain path evidence layers.

RA3 remains attribution evidence.

CAE, REF, and future analytics remain consumers of documented evidence.

Decision Intelligence should consume validated research outputs rather than raw, ambiguous artifacts.

Missing, broken, tolerance, and exact joins must remain distinguishable.

Research conclusions must preserve population, sample size, coverage, validity, and uncertainty.

## Relationship To Pattern Engine And Market-Quality Layers

Pattern Engine outputs and future quality/state layers are evidence inputs, not direct authority.

Retained research families are not silently promoted.

Future layers such as the MicroTrendParticipationLayer may inform quality, patience, or urgency only after explicit integration decisions.

Decision Intelligence may compare or interpret these outputs but does not turn them into hidden trading predicates.

The 5-minute lead surface and current research boundaries remain controlling unless separately changed.

## Evidence Quality And Confidence

Decision Intelligence should distinguish confidence in evidence from confidence in a market prediction.

It should expose:

- sample size
- coverage
- freshness
- provenance
- join quality
- missing data
- conflicting evidence
- regime dependence
- out-of-sample status

It must not convert weak evidence into precise-looking recommendations.

Unknown must remain Unknown.

Recommendations should be proportional to evidence quality.

## Explainability And Provenance

Every advisory output should be traceable to:

- source artifacts
- source versions
- generated timestamps
- experiment or query identifiers
- population definition
- relevant assumptions
- validation status

The system should be able to answer:

- Why is this recommendation present?
- What evidence supports it?
- What evidence contradicts it?
- What changed since the prior conclusion?
- What would invalidate it?

## Governance Path From Evidence To Production

Intended promotion path:

```text
Research observation
-> Research Question
-> bounded experiment
-> validated finding
-> Design Proposal if architecture or runtime behavior changes
-> independent review
-> Review Resolution
-> ADR
-> implementation
-> validation
-> closeout
```

No analytics result alone changes production behavior.

## Engineering Economics

Decision Intelligence should also help prioritize engineering effort.

Recommendations should consider:

- reliability improvement
- autonomy
- profitability contribution
- risk reduction
- future token/time savings
- implementation cost

It should distinguish:

- Infrastructure ROI
- Product Development
- Perfection Work

It should prefer first-blocker analysis and reusable fixes over broad, expensive investigations.

## Failure Modes To Avoid

- Advisory outputs becoming shadow authority.
- Opaque scoring with no evidence trail.
- Research artifacts treated as live truth.
- Overconfident recommendations from thin samples.
- Dashboards mixing status and recommendation without clear labeling.
- Automated promotion of strategies or rules.
- Self-reinforcing conclusions based on contaminated data.
- Optimizing for activity rather than decision quality.
- Allowing AI-generated prose to substitute for deterministic evidence.

## Desired Future State

The desired future platform is one where:

- runtime remains safe and evidence-rich
- research remains reproducible
- analytics remains traceable
- Mission Control remains operationally authoritative only within its reporting role
- Decision Intelligence helps the operator decide what to investigate, trust, change, defer, or reject
- human governance remains the bridge between evidence and production behavior
- AI improves judgment without silently taking authority

## Near-Term Implications

This vision suggests, but does not itself authorize:

- completing CRR v1
- strengthening analytics and investigation workflows
- improving evidence quality and join coverage
- exposing freshness and provenance in Mission Control
- building advisory research summaries
- improving experiment and conclusion traceability

These are directional implications, not implementation commitments unless they already exist in the roadmap.

## Boundaries

No runtime component may consume this document as decision logic.

No AI or analytics component gains submit authority.

No live-money eligibility is created.

No strategy or quality layer is promoted.

No broker mutation is authorized.

Any material implementation of this vision must follow the appropriate governance track.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `ENGINEERING_PROCESS.md`
- `docs/architecture/track-b-architectural-invariants.md`
- `docs/architecture/research-analytics-platform-vision.md`
- `docs/architecture/mission-control-philosophy.md`
- `docs/architecture/narratives/AN-003-trading-platform-evolution.md`
- `docs/architecture/narratives/AN-004-research-platform-evolution.md`
- `docs/architecture/narratives/AN-005-mission-control-evolution.md`
- `docs/research/pattern-engine-current-state.md`
- `docs/epics/EPIC-002-research-platform.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
- `docs/operations/data-retention-and-archive-policy.md`
- `docs/roadmap/NEXT.md`
- `docs/roadmap/PARKING_LOT.md`
