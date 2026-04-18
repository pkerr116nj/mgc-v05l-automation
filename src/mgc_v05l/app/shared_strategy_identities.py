"""Canonical shared strategy-lane identities used during de-specialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SharedStrategyIdentity:
    identity_id: str
    lane_id: str
    display_name: str
    internal_label: str
    strategy_family: str
    strategy_identity_root: str
    symbol: str
    allowed_sessions: tuple[str, ...]
    session_restriction: str

    def runtime_row_defaults(self) -> dict[str, Any]:
        return {
            "lane_id": self.lane_id,
            "display_name": self.display_name,
            "symbol": self.symbol,
            "strategy_family": self.strategy_family,
            "strategy_identity_root": self.strategy_identity_root,
            "allowed_sessions": list(self.allowed_sessions),
            "session_restriction": self.session_restriction,
        }


ATP_COMPANION_V1_ASIA_US = SharedStrategyIdentity(
    identity_id="ATP_COMPANION_V1_ASIA_US",
    lane_id="atp_companion_v1_asia_us",
    display_name="ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
    internal_label="ATP_COMPANION_V1_ASIA_US",
    strategy_family="active_trend_participation_engine",
    strategy_identity_root="ATP_COMPANION_V1_ASIA_US",
    symbol="MGC",
    allowed_sessions=("ASIA", "US"),
    session_restriction="ASIA/US",
)


ATP_COMPANION_V1_GC_ASIA_US = SharedStrategyIdentity(
    identity_id="ATP_COMPANION_V1_GC_ASIA_US",
    lane_id="atp_companion_v1_gc_asia_us",
    display_name="ATP Companion Candidate v1 — GC / Asia + US Executable, London Diagnostic-Only",
    internal_label="ATP_COMPANION_V1_GC_ASIA_US",
    strategy_family="active_trend_participation_engine",
    strategy_identity_root="ATP_COMPANION_V1",
    symbol="GC",
    allowed_sessions=("ASIA", "US"),
    session_restriction="ASIA/US",
)


ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK = SharedStrategyIdentity(
    identity_id="ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK",
    lane_id="atp_companion_v1_gc_asia_us_production_track",
    display_name="ATP Companion Production-Track Candidate v1 — GC / Asia + US / US_LATE Safeguard / Halt-Only 3000",
    internal_label="ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK",
    strategy_family="active_trend_participation_engine",
    strategy_identity_root="ATP_COMPANION_V1",
    symbol="GC",
    allowed_sessions=("ASIA", "US"),
    session_restriction="ASIA/US",
)


def shared_strategy_identities() -> tuple[SharedStrategyIdentity, ...]:
    return (
        ATP_COMPANION_V1_ASIA_US,
        ATP_COMPANION_V1_GC_ASIA_US,
        ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK,
    )


def get_shared_strategy_identity(identity_id: str) -> SharedStrategyIdentity:
    normalized = str(identity_id or "").strip()
    for identity in shared_strategy_identities():
        if identity.identity_id == normalized:
            return identity
    raise KeyError(f"Unknown shared strategy identity: {identity_id}")
