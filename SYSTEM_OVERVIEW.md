# System Overview

MGC-v05l automation is a PAPER-first trading and research platform for developing, supervising, and analyzing autonomous futures strategy behavior.

## Purpose

The repository supports deterministic PAPER trading, managed exits, operational safety, evidence preservation, completed-trade analytics, offline research experiments, and engineering governance.

## Major Subsystems

- Trading runtime: evaluates strategy lanes and enters PAPER trades when authority permits.
- Managed Exit: supervises managed positions and preserves close-only authority.
- Broker truth and order truth: provide the controlling view of current exposure and open orders.
- Guardian and Safe-State: classify operational safety and startup/submission authority.
- Canonical Trade Records: preserve completed-trade identity.
- CTOL and CTOE: provide completed-trade outcomes and enrichment.
- RA and REF layers: provide path evidence, attribution, diagnostics, hypotheses, and offline experiments.
- CAE: provides reusable analytics queries and result interpretation.
- Mission Control: provides operational awareness; its philosophy is recorded in `docs/architecture/mission-control-philosophy.md`.
- Governance docs: preserve reasoning, decisions, and direction.

The operator workstation and trading-application role separation is summarized in `docs/operations/trading-workstation-and-operator-workflow.md`.

The human-AI engineering methodology is summarized in `docs/architecture/human-ai-engineering-methodology.md`.

## Authority Hierarchy

1. Broker truth is authoritative for current exposure and open orders.
2. Canonical Trade Records are authoritative for completed-trade identity.
3. CTOL and CTOE provide completed-trade analytics and enrichment.
4. RA8 finalized capture and RA7 canonical paths provide path research evidence.
5. RA3 provides subordinate attribution evidence.
6. Operational artifacts provide safety and provenance evidence.

Derived research artifacts do not control runtime, broker, Managed Exit, Guardian, Safe-State, readiness, reconciliation, strategy, or trading gates.

Track B architectural invariants are summarized in `docs/architecture/track-b-architectural-invariants.md`.

Durable engineering lessons behind those invariants are summarized in `docs/architecture/evidence-driven-engineering-lessons.md`.

## Current Epics

The active research-platform epic is `docs/epics/EPIC-002-research-platform.md`.

Its current direction is to implement the Canonical Research Record v1 as a bounded, derived research contract over existing artifacts.

Current Pattern Engine and replay/operator research terminology is summarized in `docs/research/pattern-engine-current-state.md`.

The long-term Research Analytics Platform vision is recorded in `docs/architecture/research-analytics-platform-vision.md`.

The long-term Decision Intelligence vision is recorded in `docs/architecture/decision-intelligence-vision.md`.

## Current Direction

The project is moving toward a coherent research and operations platform:

- Keep PAPER trading operationally safe and evidence-rich.
- Consolidate completed-trade research around documented authority boundaries.
- Improve path, context, attribution, and join quality over time.
- Use Git-backed governance for major architectural changes.
- Preserve runtime and research isolation.
