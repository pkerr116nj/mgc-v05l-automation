"""Strategy-side reconciliation placeholder."""

from datetime import datetime

from ..domain.models import StrategyState


class StrategyReconciler:
    """Coordinates startup and periodic reconciliation from the strategy layer."""

    def force_reconcile(self, state: StrategyState, occurred_at: datetime) -> StrategyState:
        del occurred_at
        return state
