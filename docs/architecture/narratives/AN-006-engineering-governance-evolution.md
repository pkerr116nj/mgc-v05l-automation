# AN-006: Engineering Governance Evolution

## Purpose

Describe the transition from conversation-driven development to governed engineering with durable project memory.

## Where We Were

Many important ideas originally lived in conversations. That was natural while the system was changing quickly, but it made architectural continuity fragile. Reasoning could be lost, repeated, or accidentally contradicted by later implementation.

The project needed a way to preserve not just what changed, but why it changed.

## What We Decided

Git became the authoritative project memory.

The governance model introduced:

- Baseline Assessments for factual current state.
- Design Proposals for proposed changes.
- Independent Architecture Reviews for major decisions.
- Review Resolutions for finding-by-finding disposition.
- Architecture Decision Records for accepted decisions.
- Architecture Narratives for subsystem evolution.
- Codex implementation only after the appropriate design boundary is clear.

The goal is not process for its own sake. The goal is to keep future development anchored in documented reasoning instead of conversational recall.

## What Changed

Engineering became repeatable.

The first completed governance cycle established BA-001, DP-001, RR-001, and ADR-001 for the Canonical Research Record. That cycle proved the shape: assess the current platform, propose a bounded change, review it independently, resolve the review, then adopt an explicit decision before implementation.

## Where We Are Going

The direction is sustainable long-term governance.

Future major work should:

- Start with documented baseline facts.
- Make authority boundaries explicit.
- Receive independent review when the design is material.
- Keep generated artifacts under `outputs/`.
- Keep human-maintained plans, decisions, and conclusions under `docs/`.
- Preserve the distinction between research evidence and production authority.

## Related Documents

- `ENGINEERING_PROCESS.md`
- `PROJECT_PRINCIPLES.md`
- `docs/architecture/human-ai-engineering-methodology.md`
- `docs/architecture/evidence-driven-engineering-lessons.md`
- `docs/architecture/baselines/BA-001-current-research-platform.md`
- `docs/architecture/proposals/DP-001-canonical-research-record.md`
- `docs/architecture/reviews/RR-001-DP-001-canonical-research-record.md`
- `docs/architecture/decisions/ADR-001-canonical-research-record.md`
