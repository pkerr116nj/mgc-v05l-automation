"""Tests for automation defaults and legacy baseline settings."""

from mgc_automation.settings import (
    AUTOMATION_PLATFORM_DEFAULTS,
    DEFAULT_REPLAY_DATA_COLUMNS,
    LEGACY_BASELINE_SETTINGS,
)
from mgc_automation.models.enums import (
    DatabaseBackend,
    ReplayFillPolicy,
    RunMode,
    SessionTimezone,
    SymbolScope,
    Timeframe,
    VwapPolicy,
)


def test_automation_platform_defaults_do_not_hard_code_old_prototype_lane() -> None:
    assert AUTOMATION_PLATFORM_DEFAULTS.session_timezone is SessionTimezone.AMERICA_NEW_YORK
    assert AUTOMATION_PLATFORM_DEFAULTS.replay_fill_policy is ReplayFillPolicy.NEXT_BAR_OPEN
    assert AUTOMATION_PLATFORM_DEFAULTS.vwap_policy is VwapPolicy.SESSION_RESET
    assert AUTOMATION_PLATFORM_DEFAULTS.database_backend is DatabaseBackend.SQLITE
    assert AUTOMATION_PLATFORM_DEFAULTS.replay_data_columns == DEFAULT_REPLAY_DATA_COLUMNS

    assert not hasattr(AUTOMATION_PLATFORM_DEFAULTS, "timeframe")
    assert not hasattr(AUTOMATION_PLATFORM_DEFAULTS, "initial_run_mode")
    assert not hasattr(AUTOMATION_PLATFORM_DEFAULTS, "symbol_scope")
    assert not hasattr(AUTOMATION_PLATFORM_DEFAULTS, "symbol")


def test_legacy_baseline_settings_preserve_original_benchmark_snapshot_only() -> None:
    assert LEGACY_BASELINE_SETTINGS.timeframe is Timeframe.FIVE_MINUTES
    assert LEGACY_BASELINE_SETTINGS.session_timezone is SessionTimezone.AMERICA_NEW_YORK
    assert LEGACY_BASELINE_SETTINGS.replay_fill_policy is ReplayFillPolicy.NEXT_BAR_OPEN
    assert LEGACY_BASELINE_SETTINGS.vwap_policy is VwapPolicy.SESSION_RESET
    assert LEGACY_BASELINE_SETTINGS.initial_run_mode is RunMode.REPLAY_FIRST
    assert LEGACY_BASELINE_SETTINGS.database_backend is DatabaseBackend.SQLITE
    assert LEGACY_BASELINE_SETTINGS.symbol_scope is SymbolScope.SINGLE_SYMBOL_MGC
    assert LEGACY_BASELINE_SETTINGS.symbol == "MGC"


def test_default_replay_data_columns_match_exact_order() -> None:
    assert DEFAULT_REPLAY_DATA_COLUMNS == (
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )
