`PAPER_BLOCK_STALE_RUNTIME_FAULTS_FIXED`

**Observed bad state**
- snapshot: `outputs/operator_dashboard/dashboard_api_snapshot.json`
- generated_at before restart: `2026-04-29T17:30:34.234223+00:00`
- `runtime_readiness.blocking_faults_count = 9`
- `runtime_readiness.blocking_faults_active = true`
- `runtime_recovery_state = RUNNING`
- `runtime_recovery_attempts = 0`
- `runtime_recovery_attempt_budget = 3`

**Why that was wrong**
- The nine rows came from `outputs/operator_dashboard/paper_exceptions_snapshot.json`
- severity rollup was:
  - `BLOCKING=0`
  - `WATCH=9`
- all rows were `DECISION_WITHOUT_INTENT`
- these are review signals, not unrecovered runtime faults

**Current published state after fix + dashboard restart**
- snapshot: `outputs/operator_dashboard/dashboard_api_snapshot.json`
- generated_at after restart: `2026-04-29T17:31:53.661924+00:00`
- `blocking_faults_count = 0`
- `advisory_faults_count = 9`
- `blocking_faults_active = false`
- `runtime_recovery_state = RUNNING`
- `runtime_recovery_attempts = 0`
- `runtime_recovery_attempt_budget = 3`

**Interpretation**
- `runtime_recovery=RUNNING` is healthy here, not a degradation by itself
- `restart_budget=0/3` means no restart attempts have been consumed in-window
- the old hard block came from severity misclassification, not from active runtime failure

**What still remains visible**
- the nine review rows remain visible as advisory/operator-review items
- they no longer hard-block paper runtime readiness

**Current separate paper-side nuance**
- after restart, the runtime-readiness payload currently publishes `market_data=STALE`
- if paper remains blocked after refresh, that is now the real gate to inspect
- it is no longer correct to blame `faults=9` or missing live authority
