"""Health model helpers."""

from ..domain.enums import HealthStatus
from ..domain.models import HealthSnapshot


def derive_health_status(snapshot: HealthSnapshot) -> HealthStatus:
    """Return the composite health classification from explicit subsystem flags."""
    if snapshot.market_data_ok and snapshot.broker_ok and snapshot.persistence_ok and snapshot.reconciliation_clean and snapshot.invariants_ok:
        return HealthStatus.HEALTHY
    if snapshot.persistence_ok and snapshot.invariants_ok:
        return HealthStatus.DEGRADED
    return HealthStatus.FAULT
