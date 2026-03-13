"""Locked implementation settings for the engine."""

from dataclasses import dataclass

from .models.enums import (
    DatabaseBackend,
    ReplayFillPolicy,
    RunMode,
    SessionTimezone,
    SymbolScope,
    Timeframe,
    VwapPolicy,
)

LOCKED_REPLAY_DATA_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


@dataclass(frozen=True)
class LockedSettings:
    timeframe: Timeframe
    session_timezone: SessionTimezone
    replay_fill_policy: ReplayFillPolicy
    vwap_policy: VwapPolicy
    initial_run_mode: RunMode
    database_backend: DatabaseBackend
    symbol_scope: SymbolScope
    replay_data_columns: tuple[str, ...]
    symbol: str


LOCKED_SETTINGS = LockedSettings(
    timeframe=Timeframe.FIVE_MINUTES,
    session_timezone=SessionTimezone.AMERICA_NEW_YORK,
    replay_fill_policy=ReplayFillPolicy.NEXT_BAR_OPEN,
    vwap_policy=VwapPolicy.SESSION_RESET,
    initial_run_mode=RunMode.REPLAY_FIRST,
    database_backend=DatabaseBackend.SQLITE,
    symbol_scope=SymbolScope.SINGLE_SYMBOL_MGC,
    replay_data_columns=LOCKED_REPLAY_DATA_COLUMNS,
    symbol="MGC",
)
