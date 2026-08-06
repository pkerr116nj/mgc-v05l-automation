# MGC-v05l Automation

MGC-v05l automation is a PAPER-first trading and research platform for developing, supervising, and analyzing autonomous futures strategy behavior.

The repository combines deterministic PAPER trading, Managed Exit supervision, broker-truth safety, completed-trade evidence, research analytics, and Git-backed engineering governance.

This README is the entry point for a new engineer. It points to the durable project knowledge without duplicating the underlying documents.

## Recommended Reading Order

1. `SYSTEM_OVERVIEW.md` explains the platform purpose, major subsystems, authority hierarchy, and current direction.
2. `PROJECT_PRINCIPLES.md` records the durable engineering principles that future work should preserve.
3. `ENGINEERING_PROCESS.md` defines the governance lifecycle for routine, architecture, and emergency changes.
4. `docs/architecture/track-b-architectural-invariants.md` summarizes the non-negotiable Track B authority, safety, and research/runtime boundaries.
5. `docs/architecture/evidence-driven-engineering-lessons.md` explains why the project emphasizes broker truth, provenance, generalized fixes, bounded experiments, and governed promotion.
6. `docs/architecture/decisions/ADR-001-canonical-research-record.md` records the accepted Canonical Research Record architecture decision.
7. `docs/epics/EPIC-002-research-platform.md` describes the active research-platform epic and next implementation milestone.
8. `docs/roadmap/NOW.md`, `docs/roadmap/NEXT.md`, and `docs/roadmap/PARKING_LOT.md` show current work, next work, and deferred ideas.

## Documentation Organization

### Governance

- `ENGINEERING_PROCESS.md` defines the project lifecycle from idea through validation and closeout.
- `docs/architecture/human-ai-engineering-methodology.md` explains the collaboration model for product ownership, architecture, review, implementation, validation, and knowledge capture.
- `docs/architecture/IMPLEMENTATION_QUEUE.md` lists accepted architecture work awaiting implementation planning.

### Architecture

- `SYSTEM_OVERVIEW.md` is the concise map of the current platform.
- `PROJECT_PRINCIPLES.md` records durable principles.
- `docs/architecture/track-b-architectural-invariants.md` records Track B-specific invariants.
- `docs/architecture/research-analytics-platform-vision.md` describes the long-term research analytics platform after CRR v1.
- `docs/architecture/decision-intelligence-vision.md` describes the long-term advisory decision-support direction.
- `docs/architecture/mission-control-philosophy.md` records the purpose and authority boundaries of Mission Control.
- `docs/architecture/evidence-driven-engineering-lessons.md` summarizes lessons that shaped the current engineering posture.

### Operations

- `docs/operations/current-infrastructure-baseline.md` describes current host responsibilities from an ownership perspective.
- `docs/operations/trading-workstation-and-operator-workflow.md` describes the current operator environment and workstation role separation.
- `docs/operations/data-retention-and-archive-policy.md` records retention and archive direction for logs, artifacts, and research evidence.
- `docs/operations/closeouts/atlas-regime-monitor-migration-closeout.md` closes out the Atlas Regime Monitor migration.

### Research

- `docs/epics/EPIC-002-research-platform.md` is the active epic for consolidating completed-trade research.
- `docs/research/pattern-engine-current-state.md` records the current Pattern Engine and operator-baseline terminology.
- `outputs/reports/research_data_inventory/research_data_inventory.md` inventories persistent data produced by the paper trading system.
- `outputs/reports/research_data_inventory/research_data_flow_diagram.md` shows the research data-flow relationships.

### Narratives

- `docs/architecture/narratives/AN-001-physical-network-evolution.md` explains physical network evolution.
- `docs/architecture/narratives/AN-002-infrastructure-evolution.md` explains infrastructure ownership evolution.
- `docs/architecture/narratives/AN-003-trading-platform-evolution.md` explains the move toward deterministic PAPER execution.
- `docs/architecture/narratives/AN-004-research-platform-evolution.md` explains the research platform evolution.
- `docs/architecture/narratives/AN-005-mission-control-evolution.md` explains Mission Control evolution.
- `docs/architecture/narratives/AN-006-engineering-governance-evolution.md` explains the governance evolution.

### Roadmaps

- `docs/roadmap/NOW.md` identifies active governance and implementation focus.
- `docs/roadmap/NEXT.md` lists near-term planning and implementation candidates.
- `docs/roadmap/PARKING_LOT.md` keeps useful deferred work visible without interrupting current priorities.

### Decisions

- `docs/architecture/baselines/BA-001-current-research-platform.md` records the factual pre-design research-platform baseline.
- `docs/architecture/proposals/DP-001-canonical-research-record.md` is the accepted proposal superseded by ADR-001 for implementation governance.
- `docs/architecture/reviews/RR-001-DP-001-canonical-research-record.md` resolves the independent review of DP-001.
- `docs/architecture/decisions/ADR-001-canonical-research-record.md` is the accepted CRR architecture decision.

### Closeouts

- `docs/operations/closeouts/atlas-regime-monitor-migration-closeout.md` records completion, validation, and lessons from the Atlas Regime Monitor migration.

## Documentation Taxonomy

| Document Or Area | Category | Why Read It |
| --- | --- | --- |
| `PROJECT_PRINCIPLES.md` | WHY | Understand the durable principles behind the project. |
| `docs/architecture/evidence-driven-engineering-lessons.md` | WHY | Understand the hard-won engineering lessons behind the current posture. |
| `docs/architecture/research-analytics-platform-vision.md` | WHY | Understand the intended research platform direction after CRR v1. |
| `docs/architecture/decision-intelligence-vision.md` | WHY | Understand future advisory decision support without authority drift. |
| `docs/architecture/mission-control-philosophy.md` | WHY | Understand what Mission Control should and should not become. |
| `docs/architecture/human-ai-engineering-methodology.md` | WHY | Understand how collaborative engineering work is conceived, challenged, implemented, and preserved. |
| `docs/architecture/narratives/` | WHY | Understand subsystem evolution and architectural reasoning over time. |
| `SYSTEM_OVERVIEW.md` | WHAT | Understand the current platform, subsystems, and authority hierarchy. |
| `docs/architecture/track-b-architectural-invariants.md` | WHAT | Understand the current non-negotiable Track B invariants. |
| `docs/architecture/baselines/` | WHAT | Understand factual current state before design work. |
| `docs/epics/` | WHAT | Understand active multi-step product and architecture objectives. |
| `docs/research/` | WHAT | Understand current research terminology and retained research state. |
| `docs/roadmap/` | WHAT | Understand current, next, and deferred work. |
| `ENGINEERING_PROCESS.md` | HOW | Understand the governance lifecycle and change tracks. |
| `docs/architecture/proposals/` | HOW | Understand proposed designs before acceptance. |
| `docs/architecture/reviews/` | HOW | Understand independent review findings and resolution. |
| `docs/architecture/decisions/` | HOW | Understand accepted decisions and implementation boundaries. |
| `docs/operations/` | HOW | Understand operational baselines, policies, closeouts, and workstation practices. |
| `docs/specs/` and legacy runbooks | HOW | Understand older or subsystem-specific implementation/runbook material when needed. |

## Documentation Dependency Map

```text
PROJECT_PRINCIPLES
  -> SYSTEM_OVERVIEW
  -> Track B Architectural Invariants
  -> Evidence-Driven Engineering Lessons

ENGINEERING_PROCESS
  -> Human-AI Engineering Methodology
  -> Baselines
  -> Design Proposals
  -> Review Resolutions
  -> ADRs
  -> Closeouts

SYSTEM_OVERVIEW
  -> Architecture visions and invariants
  -> Operations baselines and policies
  -> Research epic and research state
  -> Roadmaps

ADR-001 Canonical Research Record
  <- BA-001 baseline
  <- DP-001 proposal
  <- RR-001 review resolution
  -> EPIC-002 implementation planning
  -> Research Analytics Platform Vision

Operations documents
  -> Current infrastructure baseline
  -> Operator workflow
  -> Retention and archive policy
  -> Migration closeouts

Research documents
  -> Pattern Engine current state
  -> Research data inventory outputs
  -> EPIC-002
  -> CRR v1 planning
```

## Repository Health Report

### Inventory

| Family | Count | Notes |
| --- | ---: | --- |
| Architecture narratives | 6 | AN-001 through AN-006 are present. |
| Baselines | 1 | BA-001 covers the current research platform. |
| ADRs | 1 | ADR-001 accepts the Canonical Research Record. |
| Design Proposals | 1 | DP-001 is accepted and superseded by ADR-001 for implementation governance. |
| Review Resolutions | 1 | RR-001 is complete. |
| Epics | 1 | EPIC-002 is active for the research platform. |
| Operational policies and baselines | 4 | Current infrastructure, operator workflow, retention/archive, and Atlas closeout. |
| Research docs under `docs/research/` | 1 | Pattern Engine current state; question/experiment/finding folders are ready for future use. |
| Roadmaps | 3 | NOW, NEXT, and PARKING_LOT are present. |
| Top-level architecture docs | 7 | Implementation queue, invariants, Mission Control, Decision Intelligence, Research Analytics, engineering lessons, and methodology. |

### Empty Folders

No empty documentation folders were found. Placeholder `.gitkeep` files remain in intentionally reserved folders such as `docs/research/questions/`, `docs/research/experiments/`, `docs/research/findings/`, `docs/operations/`, and `docs/architecture/decisions/`.

### Orphaned Or Low-Inbound Documents

No broken Markdown links were found.

Some older legacy runbooks, specs, and Track B design documents are intentionally not woven into the new governance spine. They remain useful historical or subsystem-specific references, but the new entry path should start from this README, `SYSTEM_OVERVIEW.md`, `PROJECT_PRINCIPLES.md`, and `ENGINEERING_PROCESS.md`.

### Duplicate Docs Or Concepts

No duplicate governance documents were identified in the Knowledge Foundation set.

Some concepts intentionally recur across documents:

- Broker truth as current exposure authority.
- Research/runtime isolation.
- Missing evidence remains missing.
- Git as institutional memory.

These recurrences are deliberate cross-cutting principles rather than duplicate definitions.

### Superseded Artifacts

`docs/architecture/proposals/DP-001-canonical-research-record.md` is accepted and superseded as the active implementation-governance record by `docs/architecture/decisions/ADR-001-canonical-research-record.md`.

No other superseded governance artifact was identified during this pass.

## Commit Preparation

Suggested commit title:

```text
docs: prepare knowledge foundation milestone
```

Suggested commit body:

```text
Create the repository documentation entry point for the Knowledge Foundation milestone.

Summarize recommended reading order, documentation organization, WHY/WHAT/HOW taxonomy, dependency map, and repository documentation health.

Keep the work documentation-only and preserve existing runtime, trading, service, and generated-output boundaries.
```

Suggested Git tag:

```text
knowledge-foundation-v1
```

Suggested milestone name:

```text
Knowledge Foundation v1
```

Files intentionally excluded from this milestone should include unrelated application/runtime changes, generated outputs, and research scripts that are not part of the documentation foundation.
