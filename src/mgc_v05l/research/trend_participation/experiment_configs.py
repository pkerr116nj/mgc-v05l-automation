"""Structured ATP experiment config objects with stable hashing."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from ..platform import stable_hash

ATP_EXPERIMENT_CONFIG_VERSION = "atp_experiment_config_v1"


@dataclass(frozen=True)
class SessionScopeConfig:
    allowed_sessions: tuple[str, ...]


@dataclass(frozen=True)
class EarlyInvalidationConfig:
    session_scope: str
    window_bars: int
    min_favorable_excursion_r: float
    adverse_excursion_abort_r: float
    logic_mode: str = "all"


@dataclass(frozen=True)
class DrawdownGovernanceConfig:
    threshold_cash: float
    mode: str


@dataclass(frozen=True)
class ExitDrawdownOverlayConfig:
    exit_mode: str = "none"
    daily_loss_halt_multiple: float | None = None
    session_loser_limit: int | None = None
    disable_us_adds: bool = False


@dataclass(frozen=True)
class ExecutionRealismConfig:
    fee_per_fill: float = 0.0
    slippage_per_fill: float = 0.0
    confirm_halt_next_bar: bool = False


@dataclass(frozen=True)
class FullHistoryReviewConfig:
    mode: str = "optimized"
    required_timeframes: tuple[str, ...] = ("1m", "5m")
    publish_registry: bool = True
    publish_analytics: bool = True


@dataclass(frozen=True)
class AtpPackageConfig:
    package_id: str
    session_scope: SessionScopeConfig
    early_invalidation: EarlyInvalidationConfig | None = None
    drawdown_governance: DrawdownGovernanceConfig | None = None
    execution_realism: ExecutionRealismConfig | None = None


def config_hash(config: Any) -> str:
    payload = {
        "artifact_version": ATP_EXPERIMENT_CONFIG_VERSION,
        "config": asdict(config) if hasattr(config, "__dataclass_fields__") else config,
    }
    return stable_hash(payload, length=24)


def config_payload(config: Any) -> Mapping[str, Any]:
    if hasattr(config, "__dataclass_fields__"):
        return {
            "artifact_version": ATP_EXPERIMENT_CONFIG_VERSION,
            "config": asdict(config),
            "config_hash": config_hash(config),
        }
    return {
        "artifact_version": ATP_EXPERIMENT_CONFIG_VERSION,
        "config": config,
        "config_hash": config_hash(config),
    }
