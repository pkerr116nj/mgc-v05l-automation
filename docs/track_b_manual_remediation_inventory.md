# Track B Manual Remediation Inventory

Track B manual remediation tools must treat `execution_core` authority artifacts
as safety evidence. Dashboard/operator artifacts are projections only and must
not be consumed for routing, restart, broker, or lifecycle authority.

| Path | Category | Current status | Notes |
| --- | --- | --- | --- |
| `src/mgc_v05l/app/track_b_paper_lifecycle_close_cleanup.py` | local lifecycle cleanup | migrated first | Requires fresh shared-truth evidence in the audit and blocks on open/suspicious order truth, managed-order truth, supervisor manual-review states, or active shared position evidence that disagrees with the exact target. |
| `src/mgc_v05l/app/track_b_paper_lifecycle_adoption.py` | broker-backed lifecycle adoption | migrated | Requires fresh shared-truth evidence in the audit and blocks on open/suspicious order truth, managed-order warnings, runtime supervisor manual-review states, unsafe reconciliation/lease states, or active shared position evidence that conflicts with the exact adoption target. |
| `src/mgc_v05l/app/track_b_managed_exit_cancel_replace.py` | targeted cancel/replace | high-risk broker mutation | Should consume Managed Order Registry and Order Adjustment Planner before any operator-authorized cancel/replace. |
| `src/mgc_v05l/execution_core/track_b_managed_exit_cancel_replace.py` | cancel/replace planning/execution support | partly shared-truth aligned | Planning is shared-service oriented; execution-facing paths still need a formal shared-evidence precheck before mutation. |
| `src/mgc_v05l/app/ibkr_unattended_paper_close.py` | guarded broker close | high-risk broker mutation | Needs Managed Order Registry, Open Order Truth, Managed Position Registry, and broker-position-before-close evidence as mandatory inputs. |
| `src/mgc_v05l/app/ibkr_unattended_paper_rest_cancel.py` | targeted broker cancel | high-risk broker mutation | Needs Managed Order Registry / Order Adjustment Planner agreement and explicit operator authorization. |
| `src/mgc_v05l/app/ibkr_post_manual_close_reconciliation.py` | post-manual cleanup evidence | reads raw truth, diagnostic/local cleanup | Should emit shared-truth evidence alongside read-only IBKR proof before any local lifecycle repair consumes it. |
| `src/mgc_v05l/app/track_b_paper_malformed_ledger_cleanup.py` | malformed ledger cleanup | local artifact repair | Needs shared Position Truth and reconciliation context before mutating ledger outputs. |
| `src/mgc_v05l/app/track_b_maintenance_repair_executor.py` | repair executor | high-risk orchestrator | Should become a thin consumer of Runtime Supervisor Authority and Self-Recover Rules before executing any repair. |
| `src/mgc_v05l/execution_core/track_b_artifact_reconciliation_cli.py` | artifact reconciliation | diagnostic/local repair | Keep read-only by default; require shared-truth snapshot for apply modes. |
| `src/mgc_v05l/app/ibkr_position_reconciliation.py` | broker truth diagnostic | reads raw truth, diagnostic | Useful evidence producer; should not be treated as shared authority unless wrapped by execution_core services. |
| `src/mgc_v05l/app/baseline_closeout.py` | legacy/manual closeout | historical/high-risk | Keep out of Track B PAPER control plane unless explicitly migrated behind shared authority gates. |

Design rule: new or migrated remediation code should call or consume
`execution_core` authority services first, then use local raw artifacts only as
exact identity evidence for the scoped repair.
