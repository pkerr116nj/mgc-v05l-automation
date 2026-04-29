# Immediate Cleanup Recommendations

Classification context: `READINESS_RULE_SIMPLIFICATION_PLAN_READY`

## Highest-Value Simplifications
1. Make `operator_dashboard.py:_paper_readiness_payload` the only authoritative paper lane-readiness model.
   - Reason: it already computes session eligibility, waiting-for-bar, no-setup, actionable-now, route-ready, and blocked classifications.
   - Impact: removes semantic drift between operator surfaces.

2. Keep `operator_surface.py:_build_runtime_readiness` as a compact transport contract, not a second decision engine.
   - Reason: it should summarize counts and runtime fault posture, not reinterpret per-lane gating.

3. Restrict `operatorTriage.ts` to top-level paper vs live authority only.
   - Reason: it is useful for “can this mode act?” but should not become a second lane fireability model.

4. Enforce explicit route labeling everywhere fills or intents appear.
   - Required labels:
     - `IBKR_ROUTED`
     - `IBKR_SUBMIT_CAPABLE_NO_ACTION`
     - `INTERNAL_ONLY_DIAGNOSTIC`
     - `LEGACY_LOCAL_PAPER`
     - `BLOCKED_NOT_SENT_TO_BROKER`

5. Continue removing silent `PaperBroker` usage from submit-capable supported families.
   - Safe target: only replay, soak validation, and explicitly internal-only paths may synthesize local `paper-*` fills.

## Specific Problem Areas To Clean Next
### 1. Live authority affecting paper
- Status: recently fixed
- Recommendation: keep separate `paper_trade_authority` and `live_trade_authority` fields everywhere

### 2. WATCH rows counted as blocking faults
- Status: recently fixed
- Recommendation: codify severity policy:
  - `BLOCKING` -> hard block
  - `WATCH/REVIEW/INFO` -> advisory only

### 3. Stale snapshot/fallback overriding live state
- Recommendation:
  - attached live truth wins
  - snapshot fallback remains visible but non-authoritative for readiness

### 4. Legacy `PaperBroker` paths for submit-capable lanes
- Recommendation:
  - deprecate from supported broker-path families
  - preserve for replay/tests/internal-only diagnostics

### 5. Route-ready vs actionable-now confusion
- Recommendation:
  - `route_ready` means infrastructure path exists
  - `session_eligible` means lane can evaluate in-window
  - `waiting_for_completed_bar` means normal cadence wait
  - `actionable_now` means immediate broker-path candidate

### 6. Fine-grained `UNCLASSIFIED` vs broad session eligibility
- Recommendation:
  - never imply lane blockage from `UNCLASSIFIED` alone
  - show both fields side-by-side

### 7. Schwab readiness as global prerequisite
- Status: previously fixed
- Recommendation: keep Schwab confined to provider-specific fallback status only

### 8. Stale market-data gates after feed recovery
- Recommendation:
  - clear stale markers on successful refresh
  - distinguish recovered-but-recently-stale from currently stale

### 9. Duplicated server dashboard vs Electron dashboard semantics
- Recommendation:
  - share one readiness field taxonomy across both surfaces
  - avoid separate naming for the same backend fields

## Proposed Naming Contract
- `paper_monitor_ready`
- `data_ready`
- `session_eligible`
- `waiting_for_completed_bar`
- `setup_evaluated`
- `no_setup_present`
- `actionable_now`
- `governance_allowed`
- `exposure_allowed`
- `broker_reconciled`
- `bridge_submit_allowed`
- `paper_trade_authority`
- `live_trade_authority`

## What Not To Do
- Do not delete bridge preflight gates.
- Do not weaken live-money checks.
- Do not reintroduce silent local fill fallback for supported broker-path lanes.
- Do not collapse waiting-for-bar into blocked/not-ready.
