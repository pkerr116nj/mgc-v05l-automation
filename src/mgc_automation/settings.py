"""Automation settings defaults and legacy benchmark snapshots.

This module no longer treats the original MGC/5m/replay-first prototype values
as repo-wide locks. Instead it exposes:

- platform defaults that remain broadly applicable across lanes
- a legacy baseline snapshot preserved only for reproducible benchmark paths
"""

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

DEFAULT_REPLAY_DATA_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


@dataclass(frozen=True)
class AutomationPlatformDefaults:
    """Repo-wide defaults that do not hard-code the old prototype lane."""

    session_timezone: SessionTimezone
    replay_fill_policy: ReplayFillPolicy
    vwap_policy: VwapPolicy
    database_backend: DatabaseBackend
    replay_data_columns: tuple[str, ...]


@dataclass(frozen=True)
class LegacyBaselineSettings:
    """Legacy single-lane benchmark snapshot preserved for reproducibility only."""
    timeframe: Timeframe
    session_timezone: SessionTimezone
    replay_fill_policy: ReplayFillPolicy
    vwap_policy: VwapPolicy
    initial_run_mode: RunMode
    database_backend: DatabaseBackend
    symbol_scope: SymbolScope
    replay_data_columns: tuple[str, ...]
    symbol: str


AUTOMATION_PLATFORM_DEFAULTS = AutomationPlatformDefaults(
    session_timezone=SessionTimezone.AMERICA_NEW_YORK,
    replay_fill_policy=ReplayFillPolicy.NEXT_BAR_OPEN,
    vwap_policy=VwapPolicy.SESSION_RESET,
    database_backend=DatabaseBackend.SQLITE,
    replay_data_columns=DEFAULT_REPLAY_DATA_COLUMNS,
)


LEGACY_BASELINE_SETTINGS = LegacyBaselineSettings(
    timeframe=Timeframe.FIVE_MINUTES,
    session_timezone=SessionTimezone.AMERICA_NEW_YORK,
    replay_fill_policy=ReplayFillPolicy.NEXT_BAR_OPEN,
    vwap_policy=VwapPolicy.SESSION_RESET,
    initial_run_mode=RunMode.REPLAY_FIRST,
    database_backend=DatabaseBackend.SQLITE,
    symbol_scope=SymbolScope.SINGLE_SYMBOL_MGC,
    replay_data_columns=DEFAULT_REPLAY_DATA_COLUMNS,
    symbol="MGC",
)
