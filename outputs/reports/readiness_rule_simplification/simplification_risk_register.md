# Simplification Risk Register

## Classification
`READINESS_RULE_SIMPLIFICATION_PLAN_READY`

## Risks
| Risk | Impact | Why It Matters | Mitigation | Tests Needed |
|---|---|---|---|---|
| Over-simplifying paper readiness and dropping a real safety gate | High | Could allow paper submits without monitor/reconciliation/exposure safety | Keep bridge preflight as final authority; simplify labels before removing logic | bridge preflight tests; monitor contract tests |
| Removing `PaperBroker` too broadly | Medium | Replay, validation, and internal-only paths still need deterministic local fills | Deprecate only for submit-capable supported lanes | replay integration tests; runtime route tests |
| Merging advisory and blocking statuses incorrectly | Medium | Could reintroduce false blocks or hide real faults | Preserve explicit severity taxonomy | operator_surface tests; Electron triage tests |
| Treating snapshot fallback as equivalent to attached live state | High | Operators may act on stale information | Keep source mode visible and fail closed for attached-only actions | desktop attachment/fallback tests |
| Collapsing session labels into one field | Medium | Loses useful debugging context for phase gaps | Keep both `broad_session` and `detected_phase_label` | session classification tests |
| Renaming fields without updating all frontends | Medium | Browser and Electron can drift again | Define one canonical field map and use renderer mapping tests | frontend payload mapping tests |
| Clearing stale/runtime faults too aggressively | Medium | Could hide unresolved runtime problems | Separate recovered/advisory faults from active blocking faults | runtime fault lifecycle tests |
| Simplification increases hidden coupling between paper and live paths | High | Future fixes could accidentally weaken live protections | Keep distinct paper/live authority contracts | authority split tests; live-path policy tests |

## Safe Migration Order
1. Standardize names and classifications.
2. Keep all current hard gates intact.
3. Remove false-block contributors.
4. Deprecate silent fallback paths.
5. Only then delete redundant implementations.

## Audit Constraint
This plan intentionally does **not** rewrite or remove working code in this pass. It is a sequencing and ownership map so the next cleanup passes can be small and safe.
