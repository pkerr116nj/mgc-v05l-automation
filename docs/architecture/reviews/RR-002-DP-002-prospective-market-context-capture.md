# RR-002: DP-002 Prospective Market Context Capture Review Resolution

## Status

Complete after focused rereview resolution. DP-002 received `APPROVE WITH MINOR REVISIONS`; ADR-002 may be created after the six required revisions in this review are incorporated.

## Reviewed Proposal

- Proposal: `docs/architecture/proposals/DP-002-prospective-market-context-capture.md`
- Baseline: `docs/architecture/baselines/BA-002-prospective-market-context-coverage.md`
- Related contract: `docs/research/NQ_PROSPECTIVE_COHORT_VALIDATION_CONTRACT.md`

## Reviewer Role And Review Date/Context

- Reviewer role: independent architecture reviewer.
- Review context: architecture review of prospective market-context capture before producer implementation.
- Review date/context: documented in this Git-backed review resolution after the independent review was completed.
- Initial independent review recommendation: `REVISE AND RESUBMIT`.
- Focused rereview recommendation: `APPROVE WITH MINOR REVISIONS`.

## Summary Of Review

The initial review found that DP-002's direction was useful and bounded, but the original proposal left too many semantics implementation-defined. The focused rereview accepted the revised architecture direction with six minor required revisions before ADR-002 and fixture-only Phase 1.

## Finding Disposition Table

| Finding | Disposition | Rationale | Required DP Change |
| --- | --- | --- | --- |
| Define one authoritative `decision_timestamp`. | Accepted required revision | Context capture cannot choose among multiple timestamps heuristically. | Select CRR `entry_anchor.entry_time`, document semantics, timezone, precision, and failure behavior. |
| Replace single candle cutoff with per-timeframe and field-level cutoffs. | Accepted required revision | 1m and 5m evidence have different no-look-ahead boundaries. | Add `completed_candle_cutoffs`, exact boundary semantics, and off-by-one tests. |
| Define market-data identity and contract-roll policy. | Accepted required revision | Cross-contract calculations would contaminate context evidence. | Require literal contract-specific candles and explicit contract/roll statuses. |
| Define VWAP methodology. | Accepted required revision | “Session VWAP” was ambiguous. | Add session-specific VWAP identifiers, anchor/reset, price/volume inputs, cutoff, threshold, units, and calendar handling. |
| Define opening range per session. | Accepted required revision | One generic opening range would hide session differences. | Add session-specific range identifiers and state semantics. |
| Define trend-state classifier. | Accepted required revision | Slope cannot be hidden as implementation detail. | Add 5m OLS slope classifier, window, thresholds, and boundary behavior. |
| Define volatility-state classifier. | Accepted required revision | Thresholds cannot be implementation-defined. | Add 5m ATR-ratio classifier, windows, thresholds, and history rules. |
| Standardize statuses. | Accepted required revision | Missing, insufficient, stale, and not-applicable evidence mean different things. | Add common status model and field applicability. |
| Resolve CRR cache policy. | Accepted required revision | Optional caches could become competing authority. | Make v1 CRR integration reference-only. |
| Define schema versioning. | Accepted required revision | Prospective checkpoints must remain tied to exact producer/classifier/source versions. | Add producer/classifier/source versioning and consumer behavior. |
| Tighten runtime-anchor wording. | Accepted required revision | The proposal must not imply new runtime-hot-path emission. | State v1 reads existing durable anchors only and stops if they are insufficient. |
| Update schema example. | Accepted required revision | The example must reflect the revised contract. | Add timestamp provenance, cutoffs, contract identity, classifier versions, statuses, roll status, and source fingerprints. |
| Add implementation gates. | Accepted required revision | Real-candle and CRR work must remain blocked until approval. | Limit Phase 1 to fixture-only work and gate later phases. |
| Add acceptance criteria. | Accepted required revision | Approval needs explicit validation targets. | Add deterministic, no-look-ahead, contract identity, reproducibility, and authority-boundary criteria. |

## Accepted Required Revisions

1. Authoritative decision timestamp is CRR `entry_anchor.entry_time`.
2. Per-timeframe cutoffs are required for 1m and 5m evidence.
3. Literal contract-specific candle identity is required for v1.
4. VWAP must be session-specific and versioned.
5. Opening range must be session-specific and versioned.
6. Trend state must use a documented 5m reproducible classifier.
7. Volatility state must use a documented 5m reproducible classifier.
8. Field statuses must be standardized.
9. CRR v1 integration must be reference-only.
10. Schema and classifier versions must be explicit.
11. The proposal must state that v1 reads existing durable anchors only.
12. The schema example must include provenance, cutoffs, contract identity, classifier versions, statuses, roll status, and source fingerprints.
13. Implementation phases must gate real-candle population and CRR integration.
14. Acceptance criteria must cover deterministic regeneration, no-look-ahead, contract identity, missing evidence, no backfill, and authority isolation.

## Focused Rereview Required Revisions

The focused rereview accepted DP-002 with these required minor revisions:

1. Add causal durability proof for every candle used at or before the decision timestamp.
2. State OLS candle ordering explicitly and add a hand-computed trend fixture.
3. Define true range exactly and prohibit cross-contract predecessor closes.
4. Correct volatility history from 84 to 85 completed 5m candles and clarify non-overlapping windows.
5. Remove unresolved London opening-range support from v1; London returns `NOT_APPLICABLE` until governed separately.
6. Define `INVALID_DECISION_TIMESTAMP` as a record-level status distinct from field statuses.

## Accepted Optional Revisions

The revised proposal also clarifies:

- P0 remains unchanged.
- Slope is retained as method evidence inside trend state, not as a separate P0 field.
- Previously published checkpoints remain tied to exact versions and source fingerprints.

## Deferred Questions

1. Whether CRR should ever materialize context cache values after v1.
2. Whether AVWAP, GRE, CRFD, and richer regime fields should become P1 after the first prospective checkpoint.
3. Whether a future pre-fill decision anchor should replace the first-opening-fill proxy.
4. Whether a governed continuous NQ series should be introduced for research context after literal-contract v1.

## Rejected Findings

None.

## Blocking Concerns

Real-candle population and CRR integration remain blocked until:

- ADR-002 is accepted;
- exact decision-anchor audit passes;
- literal contract candle identity is proven;
- no-look-ahead boundary tests pass.

## Whether Fixture-Only Phase 1 May Proceed

Fixture-only Phase 1 may proceed after the six focused rereview revisions are incorporated and ADR-002 is created, if it is limited to schema, classifier definitions, fixture-only producer behavior, and no-look-ahead tests.

Phase 1 must not read real durable candles, integrate with CRR, alter runtime producers, or create any broker/runtime/strategy authority.

## Recommendation For Next Action

Create ADR-002 and proceed only with fixture-only Phase 1. Do not proceed to real-candle population or CRR integration until the later gates are satisfied.
