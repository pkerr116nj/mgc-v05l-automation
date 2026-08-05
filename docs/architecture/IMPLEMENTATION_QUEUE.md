# Architecture Implementation Queue

This queue lists accepted architecture decisions that are ready for bounded implementation planning.

It is not a runtime plan, service manifest, trading configuration, or generated-output index.

## Approved Work Awaiting Implementation Planning

### ADR-001: Canonical Research Record

- Decision: `docs/architecture/decisions/ADR-001-canonical-research-record.md`
- Status: accepted.
- Next step: create a bounded CRR v1 implementation plan.
- Implementation boundary: research artifact generation and validation only.
- Required constraints:
  - No runtime behavior changes.
  - No broker behavior changes.
  - No Managed Exit behavior changes.
  - No strategy logic changes.
  - No trading gates.
  - No production recommendations.
  - Reuse existing canonical join and resolution logic where available.
  - Preserve explicit join-quality, provenance, and reconciliation reporting.

## Empty Queue Policy

When an ADR implementation cycle is completed, remove it from this queue or move its follow-up work to `docs/roadmap/NEXT.md` or `docs/roadmap/PARKING_LOT.md`.

