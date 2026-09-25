# Codex Assignment — NDXP Rich-Credit Deep Dive V1

You are working in repository `pkerr116nj/mgc-v05l-automation` on branch `codex/ndxp-multidte-credit-research`.

Read and obey:

`docs/specs/NDXP_MULTIDTE_RICH_CREDIT_DEEP_DIVE_V1.md`

Treat that document as the governing research specification.

## Immediate assignment

Begin **Phase 1 and Phase 2 only**.

Do not jump ahead to feature acquisition or new paid data.

### Phase 1
1. Inventory the existing local NDXP research artifacts under `output/ndxp_multidte/`.
2. Verify candidate/path row counts, date coverage, DTE labels, and the two known recent path failures.
3. Inspect the known Databento degraded/missing-date warnings.
4. Produce `output/ndxp_multidte/deep_dive_v1/data_quality_report.json`.
5. Do not modify raw cached files.

### Phase 2
1. Build a reusable path-feature store for every candidate.
2. Reproduce the existing $5/$6/$7 × 2DTE/3DTE baseline.
3. Add fixed-horizon path values, first-passage times, adverse-before-success metrics, time-underwater metrics, and target persistence tests.
4. Run the pre-specified spread-level slippage matrix: 0/5/10/15/25 cents adverse per side.
5. Focus interpretive reporting on the $6->$3 family first, while retaining the complete $5/$6/$7 matrix.
6. Produce chronological drawdown and losing-streak statistics.
7. Add tests required by the governing spec for all implemented logic.
8. Write a concise Phase-2 report identifying what the path says about clean winners, adverse-then-success winners, and failures.

## Research controls

- No live trading code changes.
- No broker actions.
- No new market-data purchase in this assignment.
- No arbitrary parameter optimization.
- No final-holdout tuning.
- Preserve raw inputs.
- Use spread midpoint as the primary complex-order valuation benchmark and apply explicit slippage/persistence stress tests.
- If a required input is missing, document the gap; do not silently acquire it.

## Required completion response

When finished, report:

1. exact files created/changed;
2. tests run and pass/fail counts;
3. input row/session counts verified;
4. headline path findings for $6->$3;
5. any data-quality caveats;
6. anything blocking Phase 3;
7. commit SHA.

Commit your completed Phase 1/2 work to the current branch.
