# Track B Manual Remediation Inventory

Track B manual remediation tools must treat `execution_core` authority artifacts
as safety evidence. Dashboard/operator artifacts are projections only and must
not be consumed for routing, restart, broker, or lifecycle authority.

| Path | Category | Current status | Notes |
| --- | --- | --- | --- |
| `src/mgc_v05l/app/track_b_paper_lifecycle_close_cleanup.py` | local lifecycle cleanup | migrated first | Requires fresh shared-truth evidence in the audit and blocks on open/suspicious order truth, managed-order truth, supervisor manual-review states, or active shared position evidence that disagrees with the exact target. |
| `src/mgc_v05l/app/track_b_paper_lifecycle_adoption.py` | broker-backed lifecycle adoption | migrated | Requires fresh shared-truth evidence in the audit and blocks on open/suspicious order truth, managed-order warnings, runtime supervisor manual-review states, unsafe reconciliation/lease states, or active shared position evidence that conflicts with the exact adoption target. |
| `src/mgc_v05l/app/track_b_managed_exit_cancel_replace.py` | targeted cancel/replace CLI | migrated via execution_core | Thin CLI over the migrated execution_core guarded cancel/replace service. |
| `src/mgc_v05l/execution_core/track_b_managed_exit_cancel_replace.py` | cancel/replace planning/execution support | migrated | Requires fresh shared-truth evidence and exact Managed Order Registry / Order Adjustment Planner target identity before any cancel/replace action. Blocks on suspicious/duplicate/unknown order truth, planner review/do-not-replace states, runtime-supervisor review states, unsafe reconciliation/lease, and active position evidence that conflicts with the exact target. Replacement remains gated behind terminal cancel confirmation of the old order. |
| `src/mgc_v05l/app/ibkr_unattended_paper_close.py` / `src/mgc_v05l/execution/ibkr_unattended_paper_close.py` | guarded broker close | migrated | Requires fresh shared-truth evidence before the harness can connect/submit and blocks on unsafe Open Order Truth, Managed Order Registry, Order Adjustment Planner, Runtime Supervisor Authority, reconciliation/lease, or Position/Managed Position target mismatch. Existing duplicate-close and broker-position-before-close guards remain in place. |
| `src/mgc_v05l/app/ibkr_unattended_paper_rest_cancel.py` | targeted broker cancel | high-risk broker mutation | Needs Managed Order Registry / Order Adjustment Planner agreement and explicit operator authorization. |
| `src/mgc_v05l/app/ibkr_post_manual_close_reconciliation.py` | post-manual cleanup evidence | reads raw truth, diagnostic/local cleanup | Should emit shared-truth evidence alongside read-only IBKR proof before any local lifecycle repair consumes it. |
| `src/mgc_v05l/app/track_b_paper_malformed_ledger_cleanup.py` | malformed ledger cleanup | migrated | Requires fresh shared-truth evidence before appending any ledger reconciliation record. Blocks on active broker/managed exposure, unsafe Open Order Truth, Managed Order Registry, Runtime Supervisor Authority review states, unsafe reconciliation/lease, or non-clean Position Truth / Managed Position Registry. Historical broker-backed malformed rows are superseded as manual-reconciled artifacts, not silently voided as stale/test rows. |
| `src/mgc_v05l/app/track_b_maintenance_repair_executor.py` | repair executor | high-risk orchestrator | Should become a thin consumer of Runtime Supervisor Authority and Self-Recover Rules before executing any repair. |
| `src/mgc_v05l/execution_core/track_b_artifact_reconciliation_cli.py` | artifact reconciliation | diagnostic/local repair | Keep read-only by default; require shared-truth snapshot for apply modes. |
| `src/mgc_v05l/app/ibkr_position_reconciliation.py` | broker truth diagnostic | reads raw truth, diagnostic | Useful evidence producer; should not be treated as shared authority unless wrapped by execution_core services. |
| `src/mgc_v05l/app/baseline_closeout.py` | legacy/manual closeout | historical/high-risk | Keep out of Track B PAPER control plane unless explicitly migrated behind shared authority gates. |

Design rule: new or migrated remediation code should call or consume
`execution_core` authority services first, then use local raw artifacts only as
exact identity evidence for the scoped repair.
