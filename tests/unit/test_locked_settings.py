"""Tests for locked implementation decisions."""

from mgc_automation.settings import LOCKED_REPLAY_DATA_COLUMNS, LOCKED_SETTINGS
from mgc_automation.models.enums import (
    DatabaseBackend,
    ReplayFillPolicy,
    RunMode,
    SessionTimezone,
    SymbolScope,
    Timeframe,
    VwapPolicy,
)


def test_locked_settings_match_user_provided_decisions() -> None:
    assert LOCKED_SETTINGS.timeframe is Timeframe.FIVE_MINUTES
    assert LOCKED_SETTINGS.session_timezone is SessionTimezone.AMERICA_NEW_YORK
    assert LOCKED_SETTINGS.replay_fill_policy is ReplayFillPolicy.NEXT_BAR_OPEN
    assert LOCKED_SETTINGS.vwap_policy is VwapPolicy.SESSION_RESET
    assert LOCKED_SETTINGS.initial_run_mode is RunMode.REPLAY_FIRST
    assert LOCKED_SETTINGS.database_backend is DatabaseBackend.SQLITE
    assert LOCKED_SETTINGS.symbol_scope is SymbolScope.SINGLE_SYMBOL_MGC
    assert LOCKED_SETTINGS.symbol == "MGC"


def test_locked_replay_data_columns_match_exact_order() -> None:
    assert LOCKED_REPLAY_DATA_COLUMNS == (
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )
