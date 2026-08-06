# Evidence-Driven Engineering Lessons

## Status And Scope

This document records durable engineering lessons learned across Track B platform development, leak testing, debugging, reconciliation, migration, and research.

It summarizes recurring patterns learned across many incidents and experiments. It is not a substitute for ADRs, operational closeouts, source artifacts, or validation reports.

It is not an implementation guide, not a trading recommendation, and does not grant runtime or broker authority.

Knowledge half-life: long. These lessons should remain useful even if the implementation, hosts, strategies, or artifact formats change.

## Purpose

Explain the important lessons that changed how the project is designed and operated.

Central theme:

Reliable decisions require trustworthy evidence, explicit authority, and disciplined interpretation.

## From Action-First To Evidence-First

Early automation focused primarily on whether the system could observe, decide, and act.

Experience showed that action without durable evidence was not enough. The platform needed to explain:

- what decided
- what submitted
- what the broker accepted
- what filled
- what position existed
- what closed it
- what evidence remained afterward

That lesson led to the emphasis on canonical records, broker truth, lifecycle evidence, reconciliation, attribution, and closeout.

## Broker Truth Became Authoritative

Local intent, lifecycle records, dashboards, and strategy state can become stale or incomplete.

Current exposure and open orders must be resolved from fresh broker truth. Derived artifacts may explain or corroborate broker state, but they must not override it.

Reconciliation exists because local and broker evidence can temporarily diverge. Ambiguity should block authority rather than be smoothed over.

## Authority Must Be Explicit

Hidden or implied authority creates dangerous coupling.

Entry authority, close-only authority, submit authority, broker mutation, research authority, and UI authority must remain distinct.

A component that reports a state does not thereby gain authority over that state. A dashboard button, research score, or legacy monitor artifact must not become an accidental control plane.

Authority should fail closed when prerequisites are stale, contradictory, or unresolved.

## PAPER Testing Revealed Real Architecture Defects

PAPER losses are economically harmless but operationally informative.

Real broker-backed PAPER fills exposed lifecycle, ownership, exit, adoption, reconciliation, and close-persistence defects that unit tests alone did not reveal.

The value of leak testing is not pretend-money P&L; it is discovery of reusable platform defects.

Temporary PAPER exposure can remain open when useful for diagnosis, provided authority and broker state remain controlled. Tolerance for PAPER loss never means tolerance for uncontrolled state or unclear authority.

## Generalized Fixes Outperform Local Patches

A defect observed in one lane, strategy, contract, or trade may reflect a broader resolver, lifecycle, authority, data-plane, or observability problem.

The preferred response is to identify and repair the reusable invariant. Lane-specific patches may hide the symptom while preserving the defect class.

The project therefore prefers:

- central ownership resolution
- common reconciliation
- reusable authority gates
- shared lifecycle semantics
- generalized freshness rules

Local fixes remain appropriate only when the problem is genuinely local.

## First-Blocker Debugging

Complex failures often produce many downstream symptoms.

Broad audits can waste time and compute before the first blocking defect is understood.

Preferred method:

- establish current broker truth
- establish current authority state
- identify the first inconsistent contract or artifact
- stop at the first proven blocker
- fix and revalidate before chasing later symptoms

Broader investigation should occur only when the first blocker does not explain the observed behavior or evidence supports a wider defect.

## Research Data Is Not Runtime Truth

Research snapshots, replay datasets, and historical captures are valuable for offline analysis.

Their durability does not make them current.

Runtime components require explicit hot-path producers, freshness, completed-candle status, provenance, source category, and staleness validation.

Bridging research data into runtime state silently is a category error. When live evidence is unavailable, runtime must fail closed.

## Provenance Matters As Much As Values

A value without source identity, generation time, schema context, or resolution method is difficult to trust.

Research and operational conclusions should preserve:

- source artifact
- generated_at
- schema version
- input mode
- instrument
- timeframe
- resolution method
- validation state

Reproducibility depends on preserving lineage, not merely saving output values.

## Missing Evidence Must Remain Missing

Inferring a plausible answer is not equivalent to preserving evidence.

Missing fills, incomplete paths, absent context, unresolved joins, stale snapshots, and contradictory ownership should remain explicitly classified.

Unknown, missing, stale, broken, and tolerance-joined are different states.

The platform should not manufacture certainty for convenience.

## Exit Behavior Required Separate Authority And Evidence

Entry and exit are not symmetric operational problems.

Entry can be blocked while close-only authority remains necessary for risk reduction.

Exit failures exposed lifecycle ownership, managed-position resolution, stale projection, and close-persistence gaps.

Exit research must distinguish:

- bad entry
- bad hold
- bad exit
- unavailable evidence

Managed Exit remains an authority surface, while exit analytics remain research.

## Runtime Restarts Must Follow State, Not Convenience

A validated code fix alone is not enough to restart a submit-capable runtime.

Restart readiness depends on:

- broker state
- order state
- lifecycle state
- reconciliation
- Safe-State
- Control Plane coherence
- exposure resolution

When those states are clean, unnecessary watch-only delays reduce development efficiency. When those states are ambiguous, restart must remain blocked.

## Observation Surfaces Must Not Become Authority

Mission Control, dashboards, research summaries, and monitors exist to reduce uncertainty.

They must show stale, missing, contradictory, or unknown state visibly. They must not infer health from absence of errors.

UI submit authority and hidden mutation paths remain prohibited unless explicitly designed and approved.

Display proximity does not merge roles or authority.

## Infrastructure Lessons

Service placement affects reliability and operator responsiveness.

Jupiter became more responsive after coupled market-data and Regime Monitor services moved to Atlas. Coupled services sharing active state should be considered together during migration.

Reboot persistence, firewall checks, cross-host access, dashboard routing, and rollback are part of a complete cutover.

Operational logs must remain bounded. Infrastructure should support the trading platform rather than become an unbounded source of maintenance work.

## Logs Versus Evidence

Repetitive logs are useful for short-term diagnosis.

They are not the research database.

Important trade facts should be captured as structured artifacts. Retention should be purposeful.

Large unmanaged logs and artifacts can degrade system responsiveness without adding durable research value.

## Empirical Research Changed The Development Model

Strategy and exit ideas should be tested against completed-trade evidence.

Walk-forward and out-of-sample validation are more reliable than retrospective narrative.

Negative and inconclusive results are valuable. Parameter widening should stop when evidence weakens.

Retained research should not be silently promoted. Research results should change production only through governance.

## Opposite-Side Theses Reduce Frame Lock

Strong attachment to one interpretation can obscure valid counterevidence.

The platform and research process should surface credible opposite-side theses.

The goal is not to force countertrend trades. The goal is to test the dominant explanation and improve decision quality.

## Engineering Economics Matters

Correctness and safety remain non-negotiable.

Beyond that, engineering effort should be judged by expected return:

- reliability
- autonomy
- risk reduction
- profitability contribution
- future time/token savings
- implementation cost

The project distinguishes:

- Infrastructure ROI
- Product Development
- Perfection Work

Avoid repeated expensive investigation of harmless PAPER-only quirks unless they reveal reusable lessons.

## Git Became Institutional Memory

Important reasoning cannot remain only in conversations.

Baselines, proposals, reviews, resolutions, ADRs, narratives, closeouts, principles, and operational policies now preserve that reasoning.

The goal is not maximum documentation. The goal is to preserve knowledge that would be expensive or dangerous to rediscover.

Significant completed work should trigger a knowledge-capture decision.

## Human And AI Roles

Patrick remains product owner and final decision-maker.

Architecture work may be developed collaboratively. Independent review challenges major decisions. Codex performs repository implementation and publication. Git records the result.

AI-generated prose or recommendations never substitute for deterministic evidence or human approval.

A dedicated collaboration-methodology document may describe these roles in more detail.

## Failure Patterns To Avoid

- treating local state as broker truth
- allowing stale artifacts to authorize action
- patching one lane instead of fixing a shared invariant
- converting diagnostic state into submit authority
- hiding ambiguity behind a green dashboard
- treating PAPER P&L as more important than architectural learning
- using research snapshots as runtime inputs
- retaining logs instead of structured evidence
- widening parameters until a result appears
- restarting runtime before state is reconciled
- allowing conversational memory to remain the sole record of important decisions

## Desired Future Posture

The desired future posture is a platform where:

- current truth is authoritative and fresh
- research is reproducible
- defects become generalized lessons
- ambiguity blocks authority
- operator surfaces are clear
- infrastructure remains bounded
- implementation follows approved architecture
- knowledge capture closes the loop after significant work

## Boundaries

This document does not supersede incident reports or ADRs.

It does not define runtime logic.

It does not authorize trading or broker mutation.

It does not convert lessons into automatic policy unless separately implemented.

Future changes to these principles should follow the Architecture Track where material.

## Related Documents

- `SYSTEM_OVERVIEW.md`
- `PROJECT_HISTORY.md`
- `PROJECT_PRINCIPLES.md`
- `ENGINEERING_PROCESS.md`
- `docs/architecture/track-b-architectural-invariants.md`
- `docs/architecture/decision-intelligence-vision.md`
- `docs/architecture/research-analytics-platform-vision.md`
- `docs/architecture/mission-control-philosophy.md`
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md`
- `docs/architecture/narratives/AN-003-trading-platform-evolution.md`
- `docs/architecture/narratives/AN-004-research-platform-evolution.md`
- `docs/architecture/narratives/AN-006-engineering-governance-evolution.md`
- `docs/operations/data-retention-and-archive-policy.md`
- `docs/research/pattern-engine-current-state.md`
- `docs/roadmap/NEXT.md`
- `docs/roadmap/PARKING_LOT.md`
