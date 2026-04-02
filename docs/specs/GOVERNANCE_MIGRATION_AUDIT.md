# Governance Migration Audit

## Purpose

This audit records the first-pass migration from a parity-first platform framing to a research-first platform framing while preserving legacy benchmark reproducibility.

Primary governing reference:
- [Research-First Strategy Platform Charter](./RESEARCH_FIRST_STRATEGY_PLATFORM_CHARTER.md)

## Reclassification Table

| Old rule / assumption | Prior location(s) | New classification | Action | Rationale |
| --- | --- | --- | --- | --- |
| "Locked implementation decisions" acted like repo-wide law | `docs/specs/LOCKED_IMPLEMENTATION_DECISIONS.md`, `docs/PROJECT_STATUS.md`, `README.md` | Split across all three classes | Rewrite | The old label implied universal truth. The repo now needs explicit separation between benchmark-only assumptions, research defaults, and safety invariants. |
| `5m` treated as the platform's default truth | `config/base.yaml`, `src/mgc_v05l/config/settings.yaml`, `docs/specs/MGC_v0.5l_External_Automation_Implementation_Specification.md`, `docs/specs/MGC_v0.5l_External_Automation_Build_Phase_3B_Implementation_Rules_Addendum.md` | Legacy Baseline Assumption | Keep and reframe | `5m` remains required for frozen benchmark reproduction, but research lanes may separate structural, execution, and artifact timeframes. |
| Replay fill convention `NEXT_BAR_OPEN` read as universal execution truth | `config/base.yaml`, `src/mgc_automation/settings.py`, `docs/specs/MGC_v0.5l_External_Automation_Implementation_Specification.md`, `docs/specs/BLOCKING_AMBIGUITIES.md`, `docs/specs/MGC_v0.5l_External_Automation_Build_Phase_3B_Implementation_Rules_Addendum.md` | Legacy Baseline Assumption | Keep and reframe | The replay fill model is still required for benchmark reproducibility, but research and paper lanes must state their own execution truth explicitly. |
| Thinkorswim parity read as the main objective | `README.md`, `docs/specs/MGC_v0.5l_External_Automation_Implementation_Specification.md`, `docs/specs/RESEARCH_FIRST_STRATEGY_PLATFORM_CHARTER.md` | Retired as governing objective; legacy parity remains a benchmark function | Rewrite | The platform now exists to discover and validate robust strategies, with legacy parity retained for comparison rather than mission control. |
| Session restrictions inferred as permanent law | legacy benchmark and ATP benchmark docs, baseline configs, lane-specific research notes | Research Platform Default unless frozen into a benchmark lane | Rewrite | Session inclusion and exclusion must be treated as hypotheses outside frozen benchmark lanes. |
| Strategy changes could be discussed without explicit benchmark comparison discipline | project docs and ad hoc research surfaces | Research Platform Default | Introduce | Candidate naming, frozen-benchmark comparison, and retained failed branches are now explicit research workflow defaults. |
| Provenance separation across replay, research, paper, and future production | `docs/STRATEGY_ANALYSIS_SURFACE.md`, `src/mgc_v05l/app/strategy_analysis.py`, operator dashboard payloads, execution-truth helpers | Production Safety Invariant | Keep and elevate | Provenance discipline is central to operator trust and evidence quality. |
| Lifecycle truth had to be labeled honestly instead of implied | `docs/STRATEGY_ANALYSIS_SURFACE.md`, `src/mgc_v05l/app/execution_truth.py`, `src/mgc_v05l/app/strategy_study.py` | Production Safety Invariant | Keep and elevate | Mixed or partial truth must remain explicit so research, paper, and benchmark evidence are not overstated. |
| Fill-driven state transitions where lifecycle truth depends on actual fills | `src/mgc_v05l/strategy/*`, `src/mgc_v05l/execution/*`, `docs/specs/MGC_v0.5l_External_Automation_Build_Phase_3B_Implementation_Rules_Addendum.md` | Production Safety Invariant | Keep | This protects state correctness and prevents intent-driven false positions. |
| No fake or silently backfilled metrics | `docs/specs/RESEARCH_FIRST_STRATEGY_PLATFORM_CHARTER.md`, `docs/STRATEGY_ANALYSIS_SURFACE.md`, dashboard provenance notes | Production Safety Invariant | Keep and elevate | The platform must prefer unavailable-with-reason over polished but dishonest synthesis. |
| Standalone strategy identity per strategy root + instrument | `README.md`, `docs/specs/LOCKED_IMPLEMENTATION_DECISIONS.md`, `docs/specs/MGC_v0.5l_External_Automation_Implementation_Specification.md` | Legacy Baseline Assumption plus operational platform default | Keep and clarify | This remains part of current runtime behavior and benchmark reproducibility, while also supporting broader strategy-centric analysis. |
| Replay-first wording read as the whole platform architecture | `README.md`, `docs/PROJECT_STATUS.md`, `docs/DEVELOPER_RUNBOOK.md` | Legacy Baseline Assumption for benchmark lane; Research Platform Default favors multi-lane framing | Rewrite | Replay is still important, but it is no longer the only architectural lens the repo should present. |
| ATP benchmark docs referred back to "locked" legacy language without classification | `docs/specs/ATP_COMPANION_BASELINE_V1_BENCHMARK.md`, `docs/specs/ACTIVE_TREND_PARTICIPATION_ENGINE_V1_BASELINE_OVERLAY.md` | Mixed: legacy baseline assumptions plus ATP benchmark-specific assumptions | Rewrite | ATP benchmark docs should preserve historical context while clearly naming which assumptions are benchmark-only. |
| Operator and pilot docs did not clearly distinguish benchmark, research, paper, and production lanes | `docs/DEVELOPER_RUNBOOK.md`, `docs/OPERATOR_RUNBOOK_PILOT_V1.md`, `docs/PILOT_V1_STATE.md` | Research Platform Default for framing; Production Safety Invariant for operational boundaries | Rewrite | Runbooks should help users understand which lane they are in and what evidence model or operational scope applies. |

## Updated Documents In This Pass

- `README.md`
- `docs/README.md`
- `docs/PROJECT_STATUS.md`
- `docs/specs/RESEARCH_FIRST_STRATEGY_PLATFORM_CHARTER.md`
- `docs/specs/LOCKED_IMPLEMENTATION_DECISIONS.md`
- `docs/specs/MGC_v0.5l_External_Automation_Implementation_Specification.md`
- `docs/specs/MGC_v0.5l_External_Automation_Build_Phase_3B_Implementation_Rules_Addendum.md`
- `docs/specs/BLOCKING_AMBIGUITIES.md`
- `docs/specs/ATP_COMPANION_BASELINE_V1_BENCHMARK.md`
- `docs/specs/ACTIVE_TREND_PARTICIPATION_ENGINE_V1_BASELINE_OVERLAY.md`
- `docs/DEVELOPER_RUNBOOK.md`
- `docs/OPERATOR_RUNBOOK_PILOT_V1.md`
- `docs/PILOT_V1_STATE.md`
- `config/base.yaml`
- `src/mgc_v05l/config/settings.yaml`
- `src/mgc_automation/settings.py`

## Second-Pass Cleanup Decisions

### Removed As Redundant Duplicates

- `docs/DEVELOPER_RUNBOOK 2.md`
- `docs/SCHWAB_MARKET_DATA_ADAPTER 2.md`
- `docs/PROJECT_STATUS 2.md`
- `config/base 2.yaml`
- `config/live 2.yaml`
- `config/paper 2.yaml`
- `config/replay 2.yaml`

Rationale:
- these files were unreferenced duplicate surfaces
- they created ambiguity about which docs or configs were current
- canonical replacements already exist in the repo, and git history preserves the removed copies if later inspection is needed

### Retained With Stable Machine Names

- `baseline_parity_mode`
- `BASELINE_PARITY_ONLY`
- `baseline_parity_emitter`

Rationale:
- these are compatibility-sensitive lane or artifact identifiers
- changing them in this pass would create unnecessary churn and risk artifact or test breakage
- human-facing labels now prefer `Legacy Benchmark` where appropriate

## Benchmark-Only Assumptions After First Pass

- `baseline_parity_mode` as the frozen legacy benchmark lane
- `5m` benchmark chart, structural, execution, and artifact truth where the lane says so
- replay fill convention `NEXT_BAR_OPEN` for frozen benchmark reproduction
- Thinkorswim-derived baseline semantics retained for the legacy `v0.5l` benchmark path
- ATP benchmark constraints that are explicitly frozen into ATP benchmark docs

## Research Platform Defaults After First Pass

- treat session behavior as a testable hypothesis outside frozen benchmark lanes
- allow structural, execution, and artifact timeframes to diverge when labeled explicitly
- favor longer-history backfill and broader regime coverage
- require candidate-versus-frozen-benchmark comparison for semantic strategy changes
- preserve weaker or failed candidate branches when they remain informative
- present the platform as strategy-centric and multi-lane rather than replay-only

## Production Safety Invariants After First Pass

- explicit provenance by lane
- explicit lifecycle-truth labeling
- fill-driven state transitions where the lifecycle contract depends on actual fills
- no fake metrics
- no silent fallback that hides weaker evidence
- no silent mutation of frozen benchmark behavior or benchmark results
- explicit separation of benchmark, research, paper, and future production evidence

## Remaining Inconsistencies For Later Cleanup

- many historical research reports in `outputs/` intentionally preserve old vocabulary and should be treated as historical artifacts unless explicitly reissued
- code-level enum names such as `baseline_parity_mode` remain valid lane identifiers, but surrounding comments and UI language may still need selective cleanup
