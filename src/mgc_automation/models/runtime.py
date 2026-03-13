"""Runtime and health models derived from the Phase 2 architecture."""

from dataclasses import dataclass

from .enums import BrokerConnectionStatus, DataHealthStatus, DeploymentEnvironment, OperatingState


@dataclass(frozen=True)
class RuntimeStatus:
    environment: DeploymentEnvironment
    operating_state: OperatingState
    broker_connection_status: BrokerConnectionStatus
    data_health_status: DataHealthStatus
    strategy_enabled: bool
    new_entries_allowed: bool
    warmup_complete: bool
    state_loaded: bool
