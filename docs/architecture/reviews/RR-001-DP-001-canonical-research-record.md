# RR-001: DP-001 Canonical Research Record Review Resolution

## Status

Complete.

DP-001 was revised after this review, and ADR-001 records the accepted architecture decision.

## Reviewed Proposal

- Proposal: `docs/architecture/proposals/DP-001-canonical-research-record.md`
- Baseline: `docs/architecture/baselines/BA-001-current-research-platform.md`
- Epic: `docs/epics/EPIC-002-research-platform.md`

## Reviewer Role And Review Date/Context

- Reviewer role: independent architecture reviewer.
- Review context: architecture-governance review of DP-001 before any implementation work.
- Review date/context: documented in this Git-backed review resolution after the independent review was completed.
- Independent review recommendation: "Approve with minor revisions."

## Summary Of Review

The independent review found no blocking architectural concerns with DP-001's direction. The proposal is consistent with the current platform baseline as long as the Canonical Research Record remains a derived, deterministic research-facing contract over existing artifacts.

The review emphasized that CRR must not introduce a new authority layer, duplicate source-of-truth semantics, silently treat tolerance joins as exact, or allow research artifacts to influence production authority.

## Finding Disposition Table

| Finding | Disposition | Rationale | Required DP Change |
|---|---|---|---|
| Add explicit design principles for CRR. | Accepted | DP-001 needs durable constraints so future implementation cannot drift into a new authority layer. | Add Design Principles: CRR is derived and never authoritative; every field traces to an authoritative source; missing evidence remains missing; tolerance joins are marked and treated as migration debt; research artifacts do not influence production authority; CRR simplifies access to existing evidence rather than replacing or duplicating authority layers. |
| Clarify CRR as a stable deterministic research-facing contract over existing artifacts. | Accepted | This prevents the proposal from being read as a replacement for Canonical Trade Records, CTOL, CTOE, RA8, RA7, or operational truth. | State that CRR does not introduce a new authority layer. |
| Add per-source artifact/schema version provenance. | Accepted | CRR schema version alone is insufficient to reproduce fields derived from multiple upstream artifacts. | Add source artifact path, source schema version, source generated_at, and source fingerprint/hash where available for every upstream source. |
| Define missing join versus broken join precisely. | Accepted | Research quality depends on distinguishing absent evidence from inconsistent evidence. | Define missing join and broken join and include one worked example of each. |
| Expand authority hierarchy for RA3 and CTOE context validity. | Accepted | RA3 and CTOE are important evidence layers, but their authority boundaries must be explicit. | Add RA3 as deterministic attribution evidence subordinate to Canonical Trade Records and CTOL/CTOE identity/outcome authority. Add CTOE context validity as authority for enrichment validity, not raw trade identity or fills. |
| Treat tolerance joins as migration debt. | Accepted | Tolerance joins are useful transitional tools but dangerous if normalized as exact joins. | Require explicit documentation for every tolerance join, including why exact linkage is unavailable and how it will be reduced over time. |
| Avoid independently reimplementing existing canonical join semantics. | Accepted | Reusing CTOL, RA8, RA7, and established resolvers avoids divergence. | State that CRR should consume existing canonical resolution logic where available. |
| Add reconciliation requirement for materialized authoritative fields. | Accepted | Materialized fields can improve performance, but source artifacts remain authoritative. | Require validation that materialized copies reproduce authoritative source values; mismatches must fail validation or be explicitly reported. |
| Decide whether CRR v1 should omit attribution_ref, context_summary, and operational_provenance_refs. | Deferred | These references may be useful if lightweight and traceable, but they could become embedded competing data if mishandled. | Revised DP-001 must justify inclusion as lightweight references or defer them from v1. |
| Decide whether entry/exit fields should be references only or materialized. | Deferred | Materialization can be acceptable as a deterministic reconciled cache, but never as authority. | Revised DP-001 must specify whether v1 uses references only or materialized cache fields, with reconciliation requirements. |
| Decide exact materialization format and storage location. | Deferred | Storage format and location are implementation-level design questions and should be decided in revised DP-001, not in this review resolution. | Revised DP-001 should identify options and select one only if needed for approval. |

## Accepted Changes

The following changes are accepted and required for DP-001 revision:

1. Add explicit Design Principles:
   - CRR is derived and never authoritative.
   - Every field must trace to an authoritative source.
   - Missing evidence remains missing.
   - Tolerance joins are explicitly marked and treated as migration debt.
   - Research artifacts do not influence production authority.
   - CRR simplifies access to existing evidence rather than replacing or duplicating authority layers.
2. Clarify that CRR is a stable, deterministic research-facing contract over existing artifacts and does not introduce a new authority layer.
3. Add per-source artifact/schema version provenance in addition to the CRR schema version.
4. Define missing join versus broken join:
   - Missing join: required source evidence is absent or unavailable.
   - Broken join: expected referential evidence exists but cannot be resolved consistently or violates the declared join contract.
5. Include a worked example of a missing join and a broken join.
6. Expand the authority hierarchy:
   - RA3 is deterministic attribution evidence subordinate to Canonical Trade Records and CTOL/CTOE identity/outcome authority.
   - CTOE context validity is authority for whether enrichment fields are valid, not for raw trade identity or fills.
7. Treat tolerance joins as tracked migration debt:
   - No new tolerance join without explicit documentation.
   - Each tolerance join must identify why exact linkage is unavailable.
   - The roadmap should seek to reduce tolerance joins over time.
8. Avoid independently reimplementing join semantics where CTOL, RA8, RA7, or another established layer already provides reusable canonical resolution logic.
9. Add a reconciliation requirement:
   - Any materialized copy of an authoritative field must be reproducible from its source.
   - Validation must compare materialized values against authoritative artifacts.
   - Mismatches must fail validation or be explicitly reported.

## Deferred Questions

The following questions are deferred pending DP-001 revision discussion:

1. Whether CRR v1 should omit `attribution_ref`, `context_summary`, and `operational_provenance_refs`.
   - Current view: references may remain if they are lightweight, traceable, and not embedded competing data. The revised proposal should explicitly justify inclusion or defer them.
2. Whether authoritative entry/exit fields should be references only or may be materialized for analytical performance.
   - Current view: materialization may be allowed only as a deterministic, reconciled cache with clear source provenance. It must never become authoritative.
3. Exact materialization format and storage location.
   - These remain design questions for the revised DP and should not be decided in RR-001.

## Rejected Recommendations

None.

## Blocking Concerns

There are no blocking architectural concerns.

DP-001 was revised after this review, and ADR-001 records the accepted decision.

## Recommendation For Next Action

Proceed from ADR-001 to bounded CRR v1 implementation planning. Do not implement CRR outside the ADR-001 implementation boundary.
