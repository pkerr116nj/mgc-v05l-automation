"""Tests for the probationary shadow runtime scaffolding."""

from __future__ import annotations

import csv
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select

import mgc_v05l.app.probationary_runtime as probationary_runtime_module
from mgc_v05l.app.strategy_study import build_strategy_study_v3
from mgc_v05l.app.probationary_runtime import (
    ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
    ATP_COMPANION_PRODUCTION_TRACK_OVERLAY_ID,
    ATPE_EXIT_POLICY_HARD_TARGET,
    ATPE_EXIT_POLICY_TARGET_CHECKPOINT,
    GC_MGC_ACCEPTANCE_RUNTIME_KIND,
    PAPER_EXECUTION_CANARY_MODE,
    PAPER_EXECUTION_CANARY_ENTRY_REASON,
    PAPER_EXECUTION_CANARY_EXIT_REASON,
    ProbationaryAdaptedPaperLaneRuntime,
    ProbationaryLaneStructuredLogger,
    ProbationaryPaperLaneRuntime,
    ProbationaryPaperLaneMetrics,
    ProbationaryPaperRiskRuntimeState,
    _apply_probationary_paper_risk_controls,
    _apply_probationary_supervisor_operator_control,
    _build_probationary_paper_lane_settings,
    _build_probationary_strategy_engine,
    _ensure_probationary_paper_risk_state_session,
    _load_probationary_same_underlying_entry_holds,
    _load_probationary_paper_lane_specs,
    _write_probationary_paper_risk_artifacts,
    _write_probationary_supervisor_operator_status,
    _reconcile_paper_runtime,
    _restore_paper_runtime_state,
    _apply_probationary_operator_control,
    _active_probationary_paper_lane_specs,
    _atp_us_late_overlay_abort_reasons,
    _build_probationary_paper_soak_validation_runtime,
    _build_probationary_paper_soak_validation_settings,
    build_probationary_paper_readiness,
    build_probationary_paper_runner,
    _build_exit_parity_summary,
    _run_probationary_live_timing_validation,
    _paper_soak_validation_bars,
    run_probationary_paper_soak_validation,
    _run_probationary_paper_soak_extended,
    _run_probationary_paper_soak_unattended,
    generate_probationary_daily_summary,
    generate_probationary_parity_report,
    inspect_probationary_shadow_session,
    simulate_atpe_exit_policy_on_bars,
    submit_probationary_operator_control,
)
from mgc_v05l.domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
from mgc_v05l.config_models import RuntimeMode, load_settings_from_files
from mgc_v05l.config_models.settings import EnvironmentMode, ExecutionTimeframeRole
from mgc_v05l.execution.execution_engine import ExecutionEngine
from mgc_v05l.execution.live_strategy_broker import LiveStrategyPilotBroker
from mgc_v05l.execution.order_models import FillEvent
from mgc_v05l.execution.paper_broker import PaperBroker
from mgc_v05l.domain.models import Bar, SignalPacket
from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.market_data.live_feed import LivePollingService, _latest_completed_bar_end
from mgc_v05l.market_data.session_clock import classify_sessions
from mgc_v05l.market_data.schwab_adapter import SchwabMarketDataAdapter
from mgc_v05l.market_data.schwab_models import (
    SchwabAuthConfig,
    SchwabBarFieldMap,
    SchwabLivePollRequest,
    SchwabMarketDataConfig,
    SchwabPriceHistoryFrequency,
    TimestampSemantics,
)
from mgc_v05l.monitoring.alerts import AlertDispatcher
from mgc_v05l.monitoring.logger import StructuredLogger
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import signals_table
from mgc_v05l.strategy.strategy_engine import StrategyEngine
from mgc_v05l.app.approved_quant_lanes.engine import ApprovedQuantStrategyEngine
from mgc_v05l.app.gc_mgc_london_open_acceptance_continuation_runtime import (
    GC_MGC_LONDON_OPEN_ACCEPTANCE_SOURCE,
    GcMgcLondonOpenAcceptanceContinuationStrategyEngine,
)
from mgc_v05l.research.trend_participation.models import AtpEntryState, AtpTimingState, ConflictOutcome, ResearchBar


def _build_probationary_settings(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path}"',
                "live_poll_lookback_minutes: 60",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            override_path,
        ]
    )


def _research_bar_1m(index: int, *, instrument: str = "GC", close: str = "100") -> ResearchBar:
    end_ts = datetime(2026, 4, 17, 13, 0, tzinfo=timezone.utc) + timedelta(minutes=index + 1)
    start_ts = end_ts - timedelta(minutes=1)
    price = Decimal(close)
    return ResearchBar(
        instrument=instrument,
        timeframe="1m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=100,
        session_label="US",
        session_segment="US",
        source="test",
        provenance="unit_test",
    )


def test_resample_research_bars_supports_3m_context() -> None:
    bars_1m = [_research_bar_1m(index, close=str(100 + index)) for index in range(6)]

    resampled = probationary_runtime_module._resample_research_bars(  # noqa: SLF001
        bars_1m,
        target_timeframe="3m",
    )

    assert len(resampled) == 3
    assert all(bar.timeframe == "3m" for bar in resampled)
    assert [bar.end_ts for bar in resampled] == [
        datetime(2026, 4, 17, 13, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 17, 13, 3, tzinfo=timezone.utc),
        datetime(2026, 4, 17, 13, 6, tzinfo=timezone.utc),
    ]


def test_latest_atpe_feature_state_uses_configured_context_timeframe(monkeypatch: pytest.MonkeyPatch) -> None:
    bars_1m = [_research_bar_1m(index, close=str(100 + index)) for index in range(6)]
    captured: dict[str, object] = {}

    def _fake_build_feature_states(*, bars_5m, bars_1m):
        captured["timeframes"] = [bar.timeframe for bar in bars_5m]
        captured["count"] = len(bars_5m)
        return [SimpleNamespace(instrument="GC")]

    monkeypatch.setattr(probationary_runtime_module, "build_feature_states", _fake_build_feature_states)

    feature = probationary_runtime_module._latest_atpe_feature_state_from_bars(  # noqa: SLF001
        bars_1m=bars_1m,
        instrument="GC",
        context_timeframe="3m",
    )

    assert feature is not None
    assert captured["count"] == 2
    assert captured["timeframes"] == ["3m", "3m"]


def _build_probationary_paper_settings(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "paper_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            override_path,
        ]
    )


def _build_live_strategy_pilot_settings(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "live_strategy_pilot_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.live.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "live_artifacts"}"',
                "live_poll_lookback_minutes: 60",
                "live_strategy_pilot_enabled: true",
                "live_strategy_pilot_submit_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            override_path,
        ]
    )


def _build_probationary_paper_settings_with_canary(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "paper_canary_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_execution_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            override_path,
        ]
    )


def _build_probationary_paper_settings_with_atpe_canary(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "paper_atpe_canary_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_atpe_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            Path("config/probationary_pattern_engine_paper_atpe_canary.yaml"),
            override_path,
        ]
    )


def _build_probationary_paper_settings_with_gc_mgc_acceptance(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "paper_gc_mgc_acceptance_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_gc_mgc_acceptance_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            Path("config/probationary_pattern_engine_paper_gc_mgc_acceptance.yaml"),
            override_path,
        ]
    )


def _build_bar(end_ts: datetime) -> Bar:
    return Bar(
        bar_id=f"MGC|5m|{end_ts.astimezone(ZoneInfo('UTC')).isoformat()}",
        symbol="MGC",
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def _seed_strategy_warmup(strategy_engine: StrategyEngine, final_bar: Bar) -> None:
    required = strategy_engine._settings.warmup_bars_required()  # noqa: SLF001
    history: list[Bar] = []
    for index in range(required):
        prior_end_ts = final_bar.end_ts - timedelta(minutes=5 * (required - index))
        history.append(_build_bar(prior_end_ts))
    strategy_engine._bar_history = history  # noqa: SLF001
    strategy_engine._feature_history = [strategy_engine._compute_feature_packet(bar) for bar in history]  # noqa: SLF001


def _build_atp_study_fixture_bars() -> tuple[list[Bar], list[Bar]]:
    source_bars: list[Bar] = []
    start = datetime.fromisoformat("2026-03-13T18:00:00+00:00")
    price = 100.0
    for index in range(120):
        bar_start = start + timedelta(minutes=index)
        open_price = price
        if index < 40:
            close_price = price + 0.12
        elif index < 55:
            close_price = price - 0.08
        elif index < 75:
            close_price = price + 0.16
        elif index < 90:
            close_price = price - 0.04
        else:
            close_price = price + 0.10
        high_price = max(open_price, close_price) + 0.06
        low_price = min(open_price, close_price) - 0.06
        price = close_price
        source_bars.append(
            Bar(
                bar_id=f"MGC|1m|{(bar_start + timedelta(minutes=1)).isoformat()}",
                symbol="MGC",
                timeframe="1m",
                start_ts=bar_start,
                end_ts=bar_start + timedelta(minutes=1),
                open=Decimal(str(round(open_price, 4))),
                high=Decimal(str(round(high_price, 4))),
                low=Decimal(str(round(low_price, 4))),
                close=Decimal(str(round(close_price, 4))),
                volume=100,
                is_final=True,
                session_asia=False,
                session_london=False,
                session_us=True,
                session_allowed=True,
            )
        )
    playback_bars: list[Bar] = []
    for index in range(0, len(source_bars), 5):
        chunk = source_bars[index : index + 5]
        if len(chunk) < 5:
            break
        playback_bars.append(
            Bar(
                bar_id=f"MGC|5m|{chunk[-1].end_ts.isoformat()}",
                symbol="MGC",
                timeframe="5m",
                start_ts=chunk[0].start_ts,
                end_ts=chunk[-1].end_ts,
                open=chunk[0].open,
                high=max(bar.high for bar in chunk),
                low=min(bar.low for bar in chunk),
                close=chunk[-1].close,
                volume=sum(bar.volume for bar in chunk),
                is_final=True,
                session_asia=False,
                session_london=False,
                session_us=True,
                session_allowed=True,
            )
        )
    return source_bars, playback_bars


def _run_atp_companion_benchmark_paper_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    exit_bearing: bool = False,
    allow_pre_5m_context_participation: bool = False,
) -> SimpleNamespace:
    def _clean_reconciliation_payload() -> dict[str, object]:
        return {
            "clean": True,
            "classification": "clean",
            "reason": "test_stubbed_clean_reconciliation",
            "mismatches": [],
            "repair_actions": [],
            "recommended_action": "No action needed.",
            "resulting_strategy_status": "READY",
            "resulting_fault_code": None,
            "resulting_entries_enabled": True,
            "entries_frozen": False,
            "fault_code": None,
        }

    lane_spec = {
        "lane_id": "atp_companion_v1_asia_us",
        "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
        "symbol": "MGC",
        "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
        "short_sources": [],
        "session_restriction": "ASIA/US",
        "allowed_sessions": ["ASIA", "US"],
        "point_value": "10",
        "trade_size": 1,
        "catastrophic_open_loss": "-500",
        "lane_mode": "ATP_COMPANION_BENCHMARK",
        "strategy_family": "active_trend_participation_engine",
        "strategy_identity_root": "ATP_COMPANION_V1_ASIA_US",
        "runtime_kind": ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
        "live_poll_lookback_minutes": 1440,
        "observed_instruments": ["MGC"],
        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        "experimental_status": "tracked_paper_benchmark",
        "paper_only": True,
        "non_approved": True,
        "observer_variant_id": "trend_participation.pullback_continuation.long.conservative",
        "observer_side": "LONG",
        "allow_pre_5m_context_participation": allow_pre_5m_context_participation,
        "artifacts_dir": str(tmp_path / "paper_artifacts" / "lanes" / "atp_companion_v1_asia_us"),
        "database_url": f"sqlite:///{tmp_path / 'probationary.paper__atp_companion_v1_asia_us.sqlite3'}",
    }
    override_path = tmp_path / "paper_atp_companion_runtime.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                f"probationary_paper_lanes_json: '{json.dumps([lane_spec])}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def _bar(end_ts: datetime) -> Bar:
        return Bar(
            bar_id=f"MGC|1m|{end_ts.isoformat()}",
            symbol="MGC",
            timeframe="1m",
            start_ts=end_ts - timedelta(minutes=1),
            end_ts=end_ts,
            open=Decimal("100.0"),
            high=Decimal("100.6"),
            low=Decimal("99.9"),
            close=Decimal("100.5"),
            volume=10,
            is_final=True,
            session_asia=True,
            session_london=False,
            session_us=False,
            session_allowed=True,
        )

    first_bar = _bar(datetime(2026, 3, 29, 19, 1, tzinfo=ZoneInfo("America/New_York")))
    duplicate_bar = _bar(datetime(2026, 3, 29, 19, 1, tzinfo=ZoneInfo("America/New_York")))
    second_bar = _bar(datetime(2026, 3, 29, 19, 2, tzinfo=ZoneInfo("America/New_York")))
    bar_sequence = [first_bar, duplicate_bar, second_bar]
    if exit_bearing:
        third_bar = Bar(
            bar_id=f"MGC|1m|{datetime(2026, 3, 29, 19, 3, tzinfo=ZoneInfo('America/New_York')).isoformat()}",
            symbol="MGC",
            timeframe="1m",
            start_ts=datetime(2026, 3, 29, 19, 2, tzinfo=ZoneInfo("America/New_York")),
            end_ts=datetime(2026, 3, 29, 19, 3, tzinfo=ZoneInfo("America/New_York")),
            open=Decimal("100.5"),
            high=Decimal("101.6"),
            low=Decimal("100.4"),
            close=Decimal("101.4"),
            volume=10,
            is_final=True,
            session_asia=True,
            session_london=False,
            session_us=False,
            session_allowed=True,
        )
        fourth_bar = Bar(
            bar_id=f"MGC|1m|{datetime(2026, 3, 29, 19, 4, tzinfo=ZoneInfo('America/New_York')).isoformat()}",
            symbol="MGC",
            timeframe="1m",
            start_ts=datetime(2026, 3, 29, 19, 3, tzinfo=ZoneInfo("America/New_York")),
            end_ts=datetime(2026, 3, 29, 19, 4, tzinfo=ZoneInfo("America/New_York")),
            open=Decimal("101.4"),
            high=Decimal("101.7"),
            low=Decimal("101.2"),
            close=Decimal("101.5"),
            volume=10,
            is_final=True,
            session_asia=True,
            session_london=False,
            session_us=False,
            session_allowed=True,
        )
        fifth_bar = Bar(
            bar_id=f"MGC|1m|{datetime(2026, 3, 29, 19, 5, tzinfo=ZoneInfo('America/New_York')).isoformat()}",
            symbol="MGC",
            timeframe="1m",
            start_ts=datetime(2026, 3, 29, 19, 4, tzinfo=ZoneInfo("America/New_York")),
            end_ts=datetime(2026, 3, 29, 19, 5, tzinfo=ZoneInfo("America/New_York")),
            open=Decimal("101.5"),
            high=Decimal("101.6"),
            low=Decimal("101.3"),
            close=Decimal("101.4"),
            volume=10,
            is_final=True,
            session_asia=True,
            session_london=False,
            session_us=False,
            session_allowed=True,
        )
        bar_sequence.extend([third_bar, fourth_bar, fifth_bar])

    class _DummyLivePollingService:
        def __init__(self, bars):
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            if not self._bars:
                return []
            return [self._bars.pop(0)]

    service_by_lane: dict[str, _DummyLivePollingService] = {}

    def _fake_build_live_polling_service(settings, repositories, schwab_config_path):
        del repositories, schwab_config_path
        lane_id = settings.probationary_paper_lane_id
        if lane_id not in service_by_lane:
            service_by_lane[lane_id] = _DummyLivePollingService(bar_sequence)
        return service_by_lane[lane_id]

    def _fake_build_feature_states(*, bars_5m, bars_1m):
        del bars_5m
        latest = bars_1m[-1]
        return [
            SimpleNamespace(
                instrument="MGC",
                decision_ts=latest.end_ts,
                session_date=latest.end_ts.date(),
                session_label="ASIA_EARLY",
                session_segment="ASIA",
            )
        ]

    def _fake_runtime_feature_row(feature, spec):
        return {
            "lane_id": spec.lane_id,
            "symbol": feature.instrument,
            "feature_timestamp": feature.decision_ts.isoformat(),
            "session_segment": feature.session_segment,
        }

    def _fake_classify_entry_states(*, feature_rows, allowed_sessions, runtime_ready, position_flat, one_position_rule_clear):
        del allowed_sessions
        feature = feature_rows[-1]
        return [
            AtpEntryState(
                instrument="MGC",
                decision_ts=feature.decision_ts,
                session_date=feature.session_date,
                session_segment="ASIA",
                family_name="atp_v1_long_pullback_continuation",
                bias_state="LONG_BIAS",
                pullback_state="NORMAL_PULLBACK",
                continuation_trigger_state="CONTINUATION_TRIGGER_CONFIRMED",
                entry_state="ENTRY_ELIGIBLE",
                blocker_codes=(),
                primary_blocker=None,
                raw_candidate=True,
                trigger_confirmed=True,
                entry_eligible=True,
                session_allowed=True,
                warmup_complete=True,
                runtime_ready=runtime_ready,
                position_flat=position_flat,
                one_position_rule_clear=one_position_rule_clear,
                setup_signature="benchmark-setup",
                setup_state_signature="benchmark-state",
                setup_quality_score=0.9,
                setup_quality_bucket="HIGH",
                feature_snapshot={
                    "decision_bar_low": 99.9,
                    "decision_bar_high": 100.6,
                    "average_range": 0.5,
                    "setup_signature": "benchmark-setup",
                    "setup_quality_bucket": "HIGH",
                },
            )
        ]

    def _fake_classify_timing_states(*, entry_states, bars_1m, allow_pre_5m_context_participation=False):
        state = entry_states[-1]
        latest_bar = bars_1m[-1]
        executable = len(bars_1m) == 1
        return [
            AtpTimingState(
                instrument="MGC",
                decision_ts=state.decision_ts,
                session_date=state.session_date,
                session_segment="ASIA",
                family_name=state.family_name,
                context_entry_state="ENTRY_BLOCKED" if executable and allow_pre_5m_context_participation else state.entry_state,
                timing_state=(
                    "ATP_TIMING_EARLY_PARTICIPATION"
                    if executable and allow_pre_5m_context_participation
                    else "ATP_TIMING_CONFIRMED"
                    if executable
                    else "ATP_TIMING_WAITING"
                ),
                vwap_price_quality_state="VWAP_FAVORABLE",
                blocker_codes=() if executable else ("ATP_TIMING_CONFIRMATION_NOT_REACHED",),
                primary_blocker=None if executable else "ATP_TIMING_CONFIRMATION_NOT_REACHED",
                setup_armed=True,
                timing_confirmed=executable,
                executable_entry=executable,
                invalidated_before_entry=False,
                setup_armed_but_not_executable=False,
                entry_executed=False,
                timing_bar_ts=latest_bar.end_ts if executable else None,
                entry_ts=latest_bar.end_ts if executable else None,
                entry_price=100.5 if executable else None,
                feature_snapshot={
                    "decision_bar_low": 99.9,
                    "decision_bar_high": 100.6,
                    "average_range": 0.5,
                    "setup_signature": "benchmark-setup",
                    "setup_quality_bucket": "HIGH",
                },
            )
        ]

    monkeypatch.setattr(probationary_runtime_module, "_build_live_polling_service", _fake_build_live_polling_service)
    monkeypatch.setattr(probationary_runtime_module, "_reconcile_paper_runtime", lambda **kwargs: _clean_reconciliation_payload())
    monkeypatch.setattr(
        probationary_runtime_module,
        "_run_reconciliation_heartbeat",
        lambda *, settings, strategy_engine, execution_engine, heartbeat_status, occurred_at=None: (
            {
                **dict(heartbeat_status or {}),
                "enabled": True,
                "cadence_seconds": int(settings.reconciliation_heartbeat_interval_seconds),
                "last_attempted_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
                "last_result_clean": True,
                "last_result_classification": "clean",
                "next_due_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
            },
            _clean_reconciliation_payload(),
            True,
        ),
    )
    monkeypatch.setattr(probationary_runtime_module, "build_feature_states", _fake_build_feature_states)
    monkeypatch.setattr(probationary_runtime_module, "_runtime_feature_row", _fake_runtime_feature_row)
    monkeypatch.setattr(probationary_runtime_module, "classify_entry_states", _fake_classify_entry_states)
    monkeypatch.setattr(probationary_runtime_module, "classify_timing_states", _fake_classify_timing_states)
    monkeypatch.setattr(probationary_runtime_module, "latest_atp_state_summary", lambda feature: {"bias_state": "LONG_BIAS"} if feature else {})
    monkeypatch.setattr(
        probationary_runtime_module,
        "latest_atp_entry_state_summary",
        lambda state: {"entry_state": state.entry_state, "primary_blocker": state.primary_blocker} if state else {},
    )
    monkeypatch.setattr(
        probationary_runtime_module,
        "latest_atp_timing_state_summary",
        lambda state: {
            "timing_state": state.timing_state,
            "primary_blocker": state.primary_blocker,
            "entry_executed": state.entry_executed,
            "vwap_price_quality_state": state.vwap_price_quality_state,
        }
        if state
        else {},
    )
    monkeypatch.setattr(
        probationary_runtime_module,
        "_run_probationary_runtime_market_data_transport_probe",
        lambda *args, **kwargs: None,
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            override_path,
        ],
        Path("config/schwab.local.json"),
    )
    lane = next(lane for lane in runner._lanes if lane.spec.lane_id == "atp_companion_v1_asia_us")  # noqa: SLF001
    for _ in range(len(bar_sequence)):
        runner.run(poll_once=True)

    operator_status = json.loads((lane.settings.probationary_artifacts_path / "operator_status.json").read_text(encoding="utf-8"))
    runtime_state = json.loads((lane.settings.probationary_artifacts_path / "runtime_state.json").read_text(encoding="utf-8"))
    trade_rows = [
        json.loads(line)
        for line in (lane.settings.probationary_artifacts_path / "trades.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    processed_bars = [
        json.loads(line)
        for line in (lane.settings.probationary_artifacts_path / "processed_bars.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    return SimpleNamespace(
        runner=runner,
        lane=lane,
        first_bar=first_bar,
        second_bar=second_bar,
        operator_status=operator_status,
        runtime_state=runtime_state,
        trade_rows=trade_rows,
        processed_bars=processed_bars,
        order_intents=lane.repositories.order_intents.list_all(),
        fills=lane.repositories.fills.list_all(),
    )


def _shadow_broker_truth_snapshot(
    *,
    reconciliation_status: str = "clear",
    broker_reachable: bool = True,
    auth_ready: bool = True,
    account_selected: bool = True,
    orders_fresh: bool = True,
    positions_fresh: bool = True,
    open_rows: list[dict[str, object]] | None = None,
    positions: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "status": "ready" if reconciliation_status == "clear" and broker_reachable else "degraded",
        "detail": "ok",
        "connection": {"selected_account_hash": "hash-123"},
        "accounts": {"selected_account_hash": "hash-123"},
        "health": {
            "broker_reachable": {"ok": broker_reachable, "label": "BROKER REACHABLE" if broker_reachable else "BROKER DEGRADED"},
            "auth": {"ok": auth_ready, "label": "AUTH READY" if auth_ready else "AUTH DEGRADED"},
            "account_selected": {"ok": account_selected, "label": "ACCOUNT SELECTED" if account_selected else "ACCOUNT NOT SELECTED"},
            "orders_fresh": {"ok": orders_fresh, "label": "ORDERS FRESH" if orders_fresh else "ORDERS STALE"},
            "positions_fresh": {"ok": positions_fresh, "label": "POSITIONS FRESH" if positions_fresh else "POSITIONS STALE"},
        },
        "reconciliation": {
            "status": reconciliation_status,
            "label": reconciliation_status.upper(),
            "detail": "clear" if reconciliation_status == "clear" else "blocked",
            "mismatch_count": 0 if reconciliation_status == "clear" else 1,
        },
        "orders": {"open_rows": list(open_rows or [])},
        "portfolio": {"positions": list(positions or [])},
    }


def _shadow_broker_truth_snapshot_auth_healthy(
    *,
    reconciliation_status: str = "clear",
    broker_reachable: bool = True,
    auth_ready: bool = True,
    account_selected: bool = True,
    orders_fresh: bool = True,
    positions_fresh: bool = True,
    open_rows: list[dict[str, object]] | None = None,
    positions: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    payload = _shadow_broker_truth_snapshot(
        reconciliation_status=reconciliation_status,
        broker_reachable=broker_reachable,
        auth_ready=auth_ready,
        account_selected=account_selected,
        orders_fresh=orders_fresh,
        positions_fresh=positions_fresh,
        open_rows=open_rows,
        positions=positions,
    )
    health = dict(payload.get("health") or {})
    auth_row = dict(health.pop("auth", {}) or {})
    health["auth_healthy"] = auth_row
    payload["health"] = health
    return payload


def _live_strategy_truth_snapshot_from_broker(
    broker: PaperBroker,
    *,
    reconciliation_status: str = "clear",
    broker_reachable: bool = True,
    auth_ready: bool = True,
    account_selected: bool = True,
    orders_fresh: bool = True,
    positions_fresh: bool = True,
) -> dict[str, object]:
    open_rows: list[dict[str, object]] = []
    for row in broker.get_open_orders():
        broker_order_id = str(row.get("broker_order_id") if isinstance(row, dict) else row)
        status_payload = broker.get_order_status(broker_order_id)
        open_rows.append(
            {
                "broker_order_id": broker_order_id,
                "symbol": "MGC",
                "status": status_payload.get("status"),
                "instruction": "BUY",
                "quantity": 1,
            }
        )
    position = broker.get_position()
    positions: list[dict[str, object]] = []
    quantity = int(getattr(position, "quantity", 0) or 0)
    if quantity != 0:
        positions.append(
            {
                "symbol": "MGC",
                "quantity": quantity,
                "side": "LONG" if quantity > 0 else "SHORT",
                "average_cost": str(getattr(position, "average_price", None)) if getattr(position, "average_price", None) is not None else None,
            }
        )
    return _shadow_broker_truth_snapshot(
        reconciliation_status=reconciliation_status,
        broker_reachable=broker_reachable,
        auth_ready=auth_ready,
        account_selected=account_selected,
        orders_fresh=orders_fresh,
        positions_fresh=positions_fresh,
        open_rows=open_rows,
        positions=positions,
    )


def _build_standard_lane_restart_fixture(tmp_path: Path):
    settings = _build_probationary_paper_settings(tmp_path)
    spec = next(spec for spec in _load_probationary_paper_lane_specs(settings) if spec.lane_id == "mgc_us_late_pause_resume_long")
    lane_settings = _build_probationary_paper_lane_settings(settings, spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    lane_logger = StructuredLogger(lane_settings.probationary_artifacts_path)
    root_logger = StructuredLogger(tmp_path / "root")

    class FakeLivePollingService:
        def poll_bars(self, *args, **kwargs):
            return []

    def build_runtime() -> tuple[ProbationaryPaperLaneRuntime, StrategyEngine, ExecutionEngine]:
        execution_engine = ExecutionEngine(broker=PaperBroker())
        strategy_engine = StrategyEngine(
            settings=lane_settings,
            repositories=repositories,
            execution_engine=execution_engine,
            structured_logger=lane_logger,
            alert_dispatcher=AlertDispatcher(lane_logger),
        )
        lane_runtime = ProbationaryPaperLaneRuntime(
            spec=spec,
            settings=lane_settings,
            repositories=repositories,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            live_polling_service=FakeLivePollingService(),
            structured_logger=ProbationaryLaneStructuredLogger(
                lane_id=spec.lane_id,
                symbol=spec.symbol,
                root_logger=root_logger,
                lane_logger=lane_logger,
            ),
            alert_dispatcher=AlertDispatcher(lane_logger),
        )
        return lane_runtime, strategy_engine, execution_engine

    return settings, spec, lane_settings, repositories, lane_logger, build_runtime


def _blank_signal_packet(bar_id: str) -> SignalPacket:
    return SignalPacket(
        bar_id=bar_id,
        bull_snap_downside_stretch_ok=False,
        bull_snap_range_ok=False,
        bull_snap_body_ok=False,
        bull_snap_close_strong=False,
        bull_snap_velocity_ok=False,
        bull_snap_reversal_bar=False,
        bull_snap_location_ok=False,
        bull_snap_raw=False,
        bull_snap_turn_candidate=False,
        first_bull_snap_turn=False,
        below_vwap_recently=False,
        reclaim_range_ok=False,
        reclaim_vol_ok=False,
        reclaim_color_ok=False,
        reclaim_close_ok=False,
        asia_reclaim_bar_raw=False,
        asia_hold_bar=False,
        asia_hold_close_vwap_ok=False,
        asia_hold_low_ok=False,
        asia_hold_bar_ok=False,
        asia_acceptance_bar=False,
        asia_acceptance_close_high_ok=False,
        asia_acceptance_close_vwap_ok=False,
        asia_acceptance_bar_ok=False,
        asia_vwap_long_signal=False,
        midday_pause_resume_long_turn_candidate=False,
        us_late_pause_resume_long_turn_candidate=False,
        us_late_failed_move_reversal_long_turn_candidate=False,
        us_late_breakout_retest_hold_long_turn_candidate=False,
        asia_early_breakout_retest_hold_long_turn_candidate=False,
        asia_early_normal_breakout_retest_hold_long_turn_candidate=False,
        asia_late_pause_resume_long_turn_candidate=False,
        asia_late_flat_pullback_pause_resume_long_turn_candidate=False,
        asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate=False,
        bear_snap_up_stretch_ok=False,
        bear_snap_range_ok=False,
        bear_snap_body_ok=False,
        bear_snap_close_weak=False,
        bear_snap_velocity_ok=False,
        bear_snap_reversal_bar=False,
        bear_snap_location_ok=False,
        bear_snap_raw=False,
        bear_snap_turn_candidate=False,
        first_bear_snap_turn=False,
        derivative_bear_slope_ok=False,
        derivative_bear_curvature_ok=False,
        derivative_bear_turn_candidate=False,
        derivative_bear_additive_turn_candidate=False,
        midday_compressed_failed_move_reversal_short_turn_candidate=False,
        midday_compressed_rebound_failed_move_reversal_short_turn_candidate=False,
        midday_expanded_pause_resume_short_turn_candidate=False,
        midday_compressed_pause_resume_short_turn_candidate=False,
        midday_pause_resume_short_turn_candidate=False,
        london_late_pause_resume_short_turn_candidate=False,
        asia_early_expanded_breakout_retest_hold_short_turn_candidate=False,
        asia_early_compressed_pause_resume_short_turn_candidate=False,
        asia_early_pause_resume_short_turn_candidate=False,
        long_entry_raw=False,
        short_entry_raw=False,
        recent_long_setup=False,
        recent_short_setup=False,
        long_entry=False,
        short_entry=False,
        long_entry_source=None,
        short_entry_source=None,
    )


def test_probationary_paper_lane_specs_include_engine_backed_quant_rows(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)

    specs = _load_probationary_paper_lane_specs(settings)
    quant_specs = [spec for spec in specs if spec.runtime_kind == "approved_quant_strategy_engine"]

    assert len(quant_specs) == 9
    assert {spec.lane_id for spec in quant_specs} >= {
        "breakout_metals_us_unknown_continuation__GC",
        "breakout_metals_us_unknown_continuation__MGC",
        "failed_move_no_us_reversal_short__CL",
        "failed_move_no_us_reversal_short__6E",
    }
    assert all(spec.live_poll_lookback_minutes is not None for spec in quant_specs)
    assert all(spec.allowed_sessions for spec in quant_specs)


def test_probationary_paper_lane_specs_include_atpe_canary_rows_when_enabled(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings_with_atpe_canary(tmp_path)

    specs = _load_probationary_paper_lane_specs(settings)
    atpe_specs = [spec for spec in specs if spec.runtime_kind == "atpe_canary_observer"]

    assert {spec.lane_id for spec in atpe_specs} == {
        "atpe_long_medium_high_canary__MES",
        "atpe_long_medium_high_canary__MNQ",
        "atpe_short_high_only_canary__MES",
        "atpe_short_high_only_canary__MNQ",
    }
    assert all(spec.paper_only for spec in atpe_specs)
    assert all(spec.non_approved for spec in atpe_specs)
    assert {spec.observed_instruments for spec in atpe_specs} == {("MES",), ("MNQ",)}
    assert {spec.quality_bucket_policy for spec in atpe_specs} == {"MEDIUM_HIGH_ONLY", "HIGH_ONLY"}


def test_probationary_paper_lane_specs_include_gc_mgc_acceptance_rows_when_enabled(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings_with_gc_mgc_acceptance(tmp_path)

    specs = _load_probationary_paper_lane_specs(settings)
    acceptance_specs = [spec for spec in specs if spec.runtime_kind == GC_MGC_ACCEPTANCE_RUNTIME_KIND]

    assert {spec.lane_id for spec in acceptance_specs} == {
        "gc_mgc_london_open_acceptance_continuation_long__GC",
        "gc_mgc_london_open_acceptance_continuation_long__MGC",
    }
    assert all(spec.paper_only for spec in acceptance_specs)
    assert all(spec.non_approved for spec in acceptance_specs)
    assert all(spec.experimental_status == "experimental_temp_paper" for spec in acceptance_specs)
    assert all(spec.observer_side == "LONG" for spec in acceptance_specs)


def test_probationary_atpe_lane_settings_preserve_isolated_canary_artifact_paths(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings_with_atpe_canary(tmp_path)
    atpe_spec = next(
        spec
        for spec in _load_probationary_paper_lane_specs(settings)
        if spec.lane_id == "atpe_long_medium_high_canary__MES"
    )

    lane_settings = _build_probationary_paper_lane_settings(settings, atpe_spec)

    assert "outputs/probationary_quant_canaries/active_trend_participation_engine/lanes/atpe_long_medium_high_canary__MES" in str(
        lane_settings.probationary_artifacts_path
    )
    assert lane_settings.database_url.endswith("atpe_long_medium_high_canary__MES.sqlite3")


def test_build_probationary_paper_runner_includes_atpe_observer_lanes_when_overlay_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    override_path = tmp_path / "paper_runner_atpe_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_atpe_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class _DummyLivePollingService:
        def poll_bars(self, *args, **kwargs):
            return []

    monkeypatch.setattr(probationary_runtime_module, "_build_live_polling_service", lambda *args, **kwargs: _DummyLivePollingService())
    monkeypatch.setattr(
        probationary_runtime_module,
        "_run_probationary_runtime_market_data_transport_probe",
        lambda *args, **kwargs: None,
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            Path("config/probationary_pattern_engine_paper_atpe_canary.yaml"),
            override_path,
        ],
        schwab_config_path="config/schwab.local.json",
    )

    lane_ids = {lane.spec.lane_id for lane in getattr(runner, "_lanes", [])}

    assert {
        "atpe_long_medium_high_canary__MES",
        "atpe_long_medium_high_canary__MNQ",
        "atpe_short_high_only_canary__MES",
        "atpe_short_high_only_canary__MNQ",
    } <= lane_ids


def test_build_probationary_paper_runner_includes_atp_companion_benchmark_lane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lane_spec = {
        "lane_id": "atp_companion_v1_asia_us",
        "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
        "symbol": "MGC",
        "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
        "short_sources": [],
        "session_restriction": "ASIA/US",
        "allowed_sessions": ["ASIA", "US"],
        "point_value": "10",
        "trade_size": 1,
        "catastrophic_open_loss": "-500",
        "lane_mode": "ATP_COMPANION_BENCHMARK",
        "strategy_family": "active_trend_participation_engine",
        "strategy_identity_root": "ATP_COMPANION_V1_ASIA_US",
        "runtime_kind": ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
        "live_poll_lookback_minutes": 1440,
        "observed_instruments": ["MGC"],
        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        "experimental_status": "tracked_paper_benchmark",
        "paper_only": True,
        "non_approved": True,
        "observer_variant_id": "trend_participation.pullback_continuation.long.conservative",
        "observer_side": "LONG",
        "artifacts_dir": str(tmp_path / "paper_artifacts" / "lanes" / "atp_companion_v1_asia_us"),
        "database_url": f"sqlite:///{tmp_path / 'probationary.paper__atp_companion_v1_asia_us.sqlite3'}",
    }
    override_path = tmp_path / "paper_atp_companion_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                f"probationary_paper_lanes_json: '{json.dumps([lane_spec])}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class _DummyLivePollingService:
        def poll_bars(self, *args, **kwargs):
            return []

    monkeypatch.setattr(probationary_runtime_module, "_build_live_polling_service", lambda *args, **kwargs: _DummyLivePollingService())
    monkeypatch.setattr(
        probationary_runtime_module,
        "_run_probationary_runtime_market_data_transport_probe",
        lambda *args, **kwargs: None,
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            override_path,
        ],
        schwab_config_path="config/schwab.local.json",
    )

    assert runner.__class__.__name__ == "ProbationaryPaperSupervisor"
    lane = next(lane for lane in runner._lanes if lane.spec.lane_id == "atp_companion_v1_asia_us")  # noqa: SLF001
    assert isinstance(lane, ProbationaryAdaptedPaperLaneRuntime)
    assert lane.spec.runtime_kind == ATP_COMPANION_BENCHMARK_RUNTIME_KIND
    assert lane.spec.symbol == "MGC"
    assert lane.spec.allowed_sessions == ("ASIA", "US")
    assert lane.spec.observed_instruments == ("MGC",)
    assert lane.settings.probationary_paper_lane_id == "atp_companion_v1_asia_us"


def test_atp_companion_exclusive_runtime_config_ignores_stale_paper_config_in_force(tmp_path: Path) -> None:
    lane_spec = {
        "shared_strategy_identity": "ATP_COMPANION_V1_ASIA_US",
        "lane_id": "atp_companion_v1_asia_us",
        "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
        "symbol": "MGC",
        "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
        "short_sources": [],
        "session_restriction": "ASIA/US",
        "allowed_sessions": ["ASIA", "US"],
        "point_value": "10",
        "trade_size": 1,
        "catastrophic_open_loss": "-500",
        "lane_mode": "ATP_COMPANION_BENCHMARK",
        "strategy_family": "active_trend_participation_engine",
        "strategy_identity_root": "ATP_COMPANION_V1_ASIA_US",
        "runtime_kind": ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
        "live_poll_lookback_minutes": 1440,
        "observed_instruments": ["MGC"],
        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        "observer_variant_id": "trend_participation.pullback_continuation.long.conservative",
        "observer_side": "LONG",
        "artifacts_dir": str(tmp_path / "paper_artifacts" / "lanes" / "atp_companion_v1_asia_us"),
        "database_url": f"sqlite:///{tmp_path / 'probationary.paper__atp_companion_v1_asia_us.sqlite3'}",
    }
    override_path = tmp_path / "override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_runtime_exclusive_config: true",
                f"probationary_paper_lanes_json: '{json.dumps([lane_spec])}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            override_path,
        ]
    )
    runtime_dir = settings.probationary_artifacts_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "runtime_kind": "strategy_engine",
                    },
                    {
                        "lane_id": "atpe_long_medium_high_canary__MES",
                        "display_name": "ATPE Long Medium+High Canary / MES",
                        "symbol": "MES",
                        "runtime_kind": "atpe_canary_observer",
                    },
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )

    specs = _active_probationary_paper_lane_specs(settings)

    assert [spec.lane_id for spec in specs] == ["atp_companion_v1_asia_us"]
    assert specs[0].runtime_kind == ATP_COMPANION_BENCHMARK_RUNTIME_KIND


def test_atp_companion_operator_control_queues_shared_lane_target(tmp_path: Path) -> None:
    control_path = tmp_path / "paper_artifacts" / "runtime" / "operator_control.json"
    override_path = tmp_path / "paper_atp_companion_control_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_runtime_exclusive_config: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_paths = [
        Path("config/base.yaml"),
        Path("config/live.yaml"),
        Path("config/probationary_pattern_engine.yaml"),
        Path("config/probationary_pattern_engine_paper.yaml"),
        override_path,
    ]

    settings = load_settings_from_files(config_paths)
    result = submit_probationary_operator_control(
        config_paths,
        action="resume_entries",
        shared_strategy_identity="ATP_COMPANION_V1_ASIA_US",
    )

    assert settings.resolved_probationary_operator_control_path == control_path
    assert result.control_path == str(control_path)
    assert control_path.exists()
    payload = json.loads(control_path.read_text(encoding="utf-8"))
    assert payload["action"] == "resume_entries"
    assert payload["status"] == "pending"
    assert payload["control_scope"] == "lane"
    assert payload["lane_id"] == "atp_companion_v1_asia_us"
    assert payload["shared_strategy_identity"] == "ATP_COMPANION_V1_ASIA_US"


def test_atp_companion_production_track_config_queues_shared_lane_target(tmp_path: Path) -> None:
    control_path = tmp_path / "paper_artifacts" / "runtime" / "operator_control.json"
    override_path = tmp_path / "paper_atp_companion_gc_production_track_control_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_runtime_exclusive_config: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_paths = [
        Path("config/base.yaml"),
        Path("config/live.yaml"),
        Path("config/probationary_pattern_engine.yaml"),
        Path("config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml"),
        override_path,
    ]

    settings = load_settings_from_files(config_paths)
    result = submit_probationary_operator_control(
        config_paths,
        action="halt_entries",
        shared_strategy_identity="ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK",
    )

    assert settings.resolved_probationary_operator_control_path == control_path
    assert result.control_path == str(control_path)
    payload = json.loads(control_path.read_text(encoding="utf-8"))
    assert payload["action"] == "halt_entries"
    assert payload["lane_id"] == "atp_companion_v1_gc_asia_us_production_track"
    assert payload["shared_strategy_identity"] == "ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK"


def test_lane_targeted_operator_control_uses_resolved_runtime_path_even_when_not_exclusive(tmp_path: Path) -> None:
    control_path = tmp_path / "paper_artifacts" / "runtime" / "atp_gc_control.json"
    override_path = tmp_path / "paper_atp_companion_gc_production_track_nonexclusive_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                f'probationary_operator_control_path: "{control_path}"',
                "probationary_paper_runtime_exclusive_config: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_paths = [
        Path("config/base.yaml"),
        Path("config/live.yaml"),
        Path("config/probationary_pattern_engine.yaml"),
        Path("config/probationary_pattern_engine_paper.yaml"),
        Path("config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml"),
        override_path,
    ]

    settings = load_settings_from_files(config_paths)
    result = submit_probationary_operator_control(
        config_paths,
        action="clear_risk_halts",
        shared_strategy_identity="ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK",
    )

    assert settings.probationary_paper_runtime_exclusive_config is False
    assert settings.resolved_probationary_operator_control_path == control_path
    assert result.control_path == str(control_path)
    payload = json.loads(control_path.read_text(encoding="utf-8"))
    assert payload["action"] == "clear_risk_halts"
    assert payload["lane_id"] == "atp_companion_v1_gc_asia_us_production_track"
    assert payload["shared_strategy_identity"] == "ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK"


def test_lane_targeted_operator_control_writes_only_to_shared_control_path(tmp_path: Path) -> None:
    shared_control_path = tmp_path / "paper_artifacts" / "runtime" / "operator_control.json"
    legacy_control_path = tmp_path / "paper_artifacts" / "runtime" / "atp_companion_v1_operator_control.json"
    override_path = tmp_path / "paper_atp_companion_gc_production_track_shared_runtime_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                f'probationary_operator_control_path: "{shared_control_path}"',
                "probationary_paper_runtime_exclusive_config: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_paths = [
        Path("config/base.yaml"),
        Path("config/live.yaml"),
        Path("config/probationary_pattern_engine.yaml"),
        Path("config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml"),
        override_path,
    ]

    result = submit_probationary_operator_control(
        config_paths,
        action="resume_entries",
        shared_strategy_identity="ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK",
    )

    assert result.control_path == str(shared_control_path)
    shared_payload = json.loads(shared_control_path.read_text(encoding="utf-8"))
    assert shared_payload["status"] == "pending"
    assert shared_payload["lane_id"] == "atp_companion_v1_gc_asia_us_production_track"
    assert shared_payload["shared_strategy_identity"] == "ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK"
    assert legacy_control_path.exists() is False


def test_atp_companion_production_track_overlay_triggers_only_when_both_conditions_fail() -> None:
    bars = [
        SimpleNamespace(high=Decimal("100.20"), low=Decimal("99.20")),
        SimpleNamespace(high=Decimal("100.22"), low=Decimal("99.10")),
    ]

    reasons = _atp_us_late_overlay_abort_reasons(
        entry_fill_price=100.0,
        risk_points=1.0,
        bars=bars,
        min_favorable_excursion_r=0.25,
        adverse_excursion_abort_r=0.65,
        logic_mode="all",
    )

    assert reasons == ["no_traction", "adverse_excursion"]

    safe_bars = [
        SimpleNamespace(high=Decimal("100.40"), low=Decimal("99.60")),
        SimpleNamespace(high=Decimal("100.80"), low=Decimal("99.55")),
    ]
    safe_reasons = _atp_us_late_overlay_abort_reasons(
        entry_fill_price=100.0,
        risk_points=1.0,
        bars=safe_bars,
        min_favorable_excursion_r=0.25,
        adverse_excursion_abort_r=0.65,
        logic_mode="all",
    )

    assert safe_reasons == []

def test_atp_companion_benchmark_runtime_processes_live_1m_bars_and_suppresses_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _run_atp_companion_benchmark_paper_fixture(tmp_path, monkeypatch)

    assert len(fixture.processed_bars) == 2
    assert len(fixture.order_intents) == 1
    assert len(fixture.fills) == 1
    assert fixture.operator_status["tracked_strategy_id"] == "atp_companion_v1_asia_us"
    assert fixture.operator_status["runtime_attached"] is True
    assert fixture.operator_status["duplicate_bar_suppression_count"] == 1
    assert fixture.operator_status["last_processed_bar_end_ts"] == fixture.second_bar.end_ts.isoformat()
    assert fixture.operator_status["execution_timeframe"] == "1m"
    assert fixture.operator_status["context_timeframes"] == ["5m"]
    assert fixture.operator_status["last_execution_bar_evaluated_at"] == fixture.second_bar.end_ts.isoformat()
    assert fixture.operator_status["last_completed_context_bars_at"] == {"5m": "2026-03-29T19:00:00-04:00"}
    assert fixture.operator_status["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert fixture.operator_status["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert fixture.operator_status["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"]
    assert fixture.operator_status["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert fixture.operator_status["authoritative_intrabar_available"] is True
    assert fixture.operator_status["authoritative_entry_truth_available"] is True
    assert fixture.operator_status["authoritative_exit_truth_available"] is False
    assert fixture.operator_status["authoritative_trade_lifecycle_available"] is True
    assert fixture.operator_status["authoritative_trade_lifecycle_records"] == []
    assert fixture.operator_status["lifecycle_records"] == []
    assert fixture.operator_status["pnl_truth_basis"] == "PAPER_RUNTIME_LEDGER"
    assert fixture.operator_status["lifecycle_truth_class"] == "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"
    assert fixture.operator_status["truth_provenance"]["runtime_context"] == "PAPER"
    assert fixture.operator_status["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"
    assert fixture.runtime_state["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert fixture.runtime_state["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert fixture.runtime_state["authoritative_trade_lifecycle_records"] == []
    assert fixture.runtime_state["lifecycle_records"] == []
    assert fixture.runtime_state["pnl_truth_basis"] == "PAPER_RUNTIME_LEDGER"
    assert fixture.runtime_state["lifecycle_truth_class"] == "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"
    assert fixture.runtime_state["truth_provenance"]["artifact_context"] == "ATP_COMPANION_PAPER_RUNTIME_STATE"


def test_atp_companion_runtime_can_arm_entry_from_early_participation_timing_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _run_atp_companion_benchmark_paper_fixture(
        tmp_path,
        monkeypatch,
        allow_pre_5m_context_participation=True,
    )

    assert fixture.lane.spec.allow_pre_5m_context_participation is True
    assert len(fixture.order_intents) == 1
    assert len(fixture.fills) == 1
    assert fixture.operator_status["allow_pre_5m_context_participation"] is True
    assert fixture.runtime_state["allow_pre_5m_context_participation"] is True


def test_atp_replay_and_paper_artifacts_expose_shared_lifecycle_contract_with_explicit_lane_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _run_atp_companion_benchmark_paper_fixture(tmp_path, monkeypatch)
    source_bars, playback_bars = _build_atp_study_fixture_bars()
    replay_settings = load_settings_from_files([Path("config/base.yaml")]).model_copy(
        update={
            "symbol": "MGC",
            "timeframe": "5m",
            "environment_mode": EnvironmentMode.RESEARCH_EXECUTION,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "1m",
            "artifact_timeframe": "5m",
            "execution_timeframe_role": ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY,
        }
    )
    replay_study = build_strategy_study_v3(
        repositories=RepositorySet(build_engine(f"sqlite:///{tmp_path / 'atp_replay_compare.sqlite3'}")),
        settings=replay_settings,
        bars=playback_bars,
        source_bars=source_bars,
        point_value=Decimal("10"),
        standalone_strategy_id="atp_companion_v1_asia_us",
        strategy_family="ACTIVE_TREND_PARTICIPATION",
        instrument="MGC",
        run_metadata={"mode": "REPLAY", "run_stamp": "atp-replay-compare"},
    )

    contract_keys = [
        "active_entry_model",
        "execution_truth_emitter",
        "authoritative_intrabar_available",
        "authoritative_entry_truth_available",
        "authoritative_exit_truth_available",
        "authoritative_trade_lifecycle_available",
        "pnl_truth_basis",
        "lifecycle_truth_class",
        "truth_provenance",
    ]
    for key in contract_keys:
        assert key in replay_study["meta"]
        assert key in fixture.operator_status
        assert key in fixture.runtime_state

    assert replay_study["meta"]["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert fixture.operator_status["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert fixture.runtime_state["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert replay_study["meta"]["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert fixture.operator_status["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert fixture.runtime_state["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert replay_study["meta"]["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"]
    assert fixture.operator_status["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"]
    assert fixture.runtime_state["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"]
    assert replay_study["meta"]["authoritative_intrabar_available"] is True
    assert fixture.operator_status["authoritative_intrabar_available"] is True
    assert fixture.runtime_state["authoritative_intrabar_available"] is True
    assert replay_study["meta"]["authoritative_entry_truth_available"] is True
    assert fixture.operator_status["authoritative_entry_truth_available"] is True
    assert fixture.runtime_state["authoritative_entry_truth_available"] is True
    assert replay_study["meta"]["authoritative_trade_lifecycle_available"] is True
    assert fixture.operator_status["authoritative_trade_lifecycle_available"] is True
    assert fixture.runtime_state["authoritative_trade_lifecycle_available"] is True
    assert replay_study["meta"]["truth_provenance"]["run_lane"] == "BENCHMARK_REPLAY"
    assert fixture.operator_status["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"
    assert fixture.runtime_state["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"
    assert replay_study["meta"]["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert fixture.operator_status["lifecycle_truth_class"] == "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"
    assert fixture.runtime_state["lifecycle_truth_class"] == "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"
    assert fixture.operator_status["lifecycle_truth_class"] != "BASELINE_PARITY_ONLY"
    assert fixture.runtime_state["lifecycle_truth_class"] != "BASELINE_PARITY_ONLY"
    assert replay_study["meta"]["pnl_truth_basis"] == "ENRICHED_EXECUTION_TRUTH"
    assert fixture.operator_status["pnl_truth_basis"] == "PAPER_RUNTIME_LEDGER"
    assert fixture.runtime_state["pnl_truth_basis"] == "PAPER_RUNTIME_LEDGER"


def test_atp_exit_bearing_paper_runtime_artifacts_emit_trade_record_lifecycle_parity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _run_atp_companion_benchmark_paper_fixture(tmp_path, monkeypatch, exit_bearing=True)
    source_bars, playback_bars = _build_atp_study_fixture_bars()
    replay_settings = load_settings_from_files([Path("config/base.yaml")]).model_copy(
        update={
            "symbol": "MGC",
            "timeframe": "5m",
            "environment_mode": EnvironmentMode.RESEARCH_EXECUTION,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "1m",
            "artifact_timeframe": "5m",
            "execution_timeframe_role": ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY,
        }
    )
    replay_study = build_strategy_study_v3(
        repositories=RepositorySet(build_engine(f"sqlite:///{tmp_path / 'atp_replay_exit_compare.sqlite3'}")),
        settings=replay_settings,
        bars=playback_bars,
        source_bars=source_bars,
        point_value=Decimal("10"),
        standalone_strategy_id="atp_companion_v1_asia_us",
        strategy_family="ACTIVE_TREND_PARTICIPATION",
        instrument="MGC",
        run_metadata={"mode": "REPLAY", "run_stamp": "atp-replay-exit-compare"},
    )

    assert fixture.trade_rows
    assert fixture.operator_status["authoritative_exit_truth_available"] is True
    assert fixture.runtime_state["authoritative_exit_truth_available"] is True
    assert fixture.operator_status["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert fixture.runtime_state["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert fixture.operator_status["authoritative_trade_lifecycle_records"]
    assert fixture.runtime_state["authoritative_trade_lifecycle_records"]
    assert replay_study["lifecycle_records"]

    required_record_keys = {
        "trade_id",
        "decision_id",
        "decision_ts",
        "entry_ts",
        "exit_ts",
        "entry_price",
        "exit_price",
        "primary_exit_reason",
        "decision_context_linkage_available",
        "decision_context_linkage_status",
        "entry_model",
        "pnl_truth_basis",
        "lifecycle_truth_class",
        "truth_provenance",
    }
    replay_record = replay_study["lifecycle_records"][0]
    paper_record = fixture.operator_status["authoritative_trade_lifecycle_records"][0]
    runtime_record = fixture.runtime_state["authoritative_trade_lifecycle_records"][0]
    assert required_record_keys.issubset(replay_record)
    assert required_record_keys.issubset(paper_record)
    assert required_record_keys.issubset(runtime_record)
    assert paper_record["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert runtime_record["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert replay_record["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert paper_record["decision_id"]
    assert runtime_record["decision_id"]
    assert paper_record["decision_ts"]
    assert runtime_record["decision_ts"]
    assert paper_record["decision_context_linkage_available"] is True
    assert runtime_record["decision_context_linkage_available"] is True
    assert paper_record["decision_context_linkage_status"] == "AVAILABLE"
    assert runtime_record["decision_context_linkage_status"] == "AVAILABLE"
    assert paper_record["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"
    assert runtime_record["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"
    assert replay_record["truth_provenance"]["run_lane"] == "BENCHMARK_REPLAY"
    assert paper_record["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert runtime_record["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert replay_record["decision_context_linkage_available"] is True


def test_probationary_atpe_lane_emits_live_paper_intents_and_fills(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    override_path = tmp_path / "paper_atpe_canary_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_atpe_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class _DummyLivePollingService:
        def __init__(self, bars):
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            if not self._bars:
                return []
            return [self._bars.pop(0)]

    def _build_1m_bar(symbol: str, end_ts: datetime, *, open_px: str, high_px: str, low_px: str, close_px: str) -> Bar:
        local_time = end_ts.astimezone(ZoneInfo("America/New_York")).time()
        return Bar(
            bar_id=f"{symbol}|1m|{end_ts.isoformat()}",
            symbol=symbol,
            timeframe="1m",
            start_ts=end_ts - timedelta(minutes=1),
            end_ts=end_ts,
            open=Decimal(open_px),
            high=Decimal(high_px),
            low=Decimal(low_px),
            close=Decimal(close_px),
            volume=10,
            is_final=True,
            session_asia=local_time >= datetime(2026, 3, 24, 18, 0, tzinfo=ZoneInfo("America/New_York")).time(),
            session_london=datetime(2026, 3, 24, 3, 0, tzinfo=ZoneInfo("America/New_York")).time() <= local_time < datetime(2026, 3, 24, 8, 30, tzinfo=ZoneInfo("America/New_York")).time(),
            session_us=datetime(2026, 3, 24, 9, 0, tzinfo=ZoneInfo("America/New_York")).time() <= local_time < datetime(2026, 3, 24, 17, 0, tzinfo=ZoneInfo("America/New_York")).time(),
            session_allowed=True,
        )

    mes_bars = [
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 1, tzinfo=ZoneInfo("America/New_York")), open_px="100.0", high_px="100.2", low_px="99.9", close_px="100.1"),
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 2, tzinfo=ZoneInfo("America/New_York")), open_px="100.1", high_px="100.2", low_px="100.0", close_px="100.1"),
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 3, tzinfo=ZoneInfo("America/New_York")), open_px="100.1", high_px="100.3", low_px="100.0", close_px="100.2"),
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 4, tzinfo=ZoneInfo("America/New_York")), open_px="100.2", high_px="100.4", low_px="100.1", close_px="100.3"),
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 5, tzinfo=ZoneInfo("America/New_York")), open_px="100.3", high_px="100.4", low_px="100.2", close_px="100.3"),
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 6, tzinfo=ZoneInfo("America/New_York")), open_px="100.3", high_px="100.8", low_px="100.2", close_px="100.7"),
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 7, tzinfo=ZoneInfo("America/New_York")), open_px="100.8", high_px="103.0", low_px="100.7", close_px="102.8"),
        _build_1m_bar("MES", datetime(2026, 3, 24, 3, 8, tzinfo=ZoneInfo("America/New_York")), open_px="102.8", high_px="102.9", low_px="102.6", close_px="102.7"),
    ]
    service_by_lane: dict[str, _DummyLivePollingService] = {}

    def _fake_build_live_polling_service(lane_settings, repositories, schwab_config_path):
        lane_id = lane_settings.probationary_paper_lane_id
        if lane_id not in service_by_lane:
            service_by_lane[lane_id] = _DummyLivePollingService(mes_bars if lane_id == "atpe_long_medium_high_canary__MES" else [])
        return service_by_lane[lane_id]

    emitted_decisions: set[str] = set()

    def _fake_generate_signal_decisions(*, feature_rows, variants, higher_priority_signals):
        if not feature_rows:
            return []
        feature = feature_rows[-1]
        variant = variants[0]
        decision_id = f"{feature.instrument}:{feature.decision_ts.isoformat()}:{variant.variant_id}"
        if feature.instrument != "MES" or emitted_decisions:
            return []
        emitted_decisions.add(decision_id)
        return [
            SimpleNamespace(
                instrument=feature.instrument,
                variant_id=variant.variant_id,
                family=variant.family,
                side="LONG",
                decision_ts=feature.decision_ts,
                session_date=feature.session_date,
                session_segment=feature.session_segment,
                decision_id=decision_id,
                setup_quality_bucket="MEDIUM",
                setup_quality_score=0.8,
                conflict_outcome=ConflictOutcome.NO_CONFLICT,
                block_reason=None,
                setup_signature="mes-atpe-test",
                setup_state_signature="mes-atpe-state",
                feature_snapshot={"test": True},
                average_range=1.0,
                decision_bar_high=100.4,
                decision_bar_low=99.9,
                decision_bar_open=100.0,
                decision_bar_close=100.2,
            )
        ]

    monkeypatch.setattr(probationary_runtime_module, "_build_live_polling_service", _fake_build_live_polling_service)
    monkeypatch.setattr(probationary_runtime_module, "generate_signal_decisions", _fake_generate_signal_decisions)
    monkeypatch.setattr(
        probationary_runtime_module,
        "_latest_atpe_feature_state_from_bars",
        lambda *, bars_1m, instrument: SimpleNamespace(
            trend_state="FLAT",
            momentum_persistence="MIXED",
            mtf_agreement_state="MIXED",
            bar_anatomy="UPPER_REJECTION",
            reference_state="MID_RANGE",
            expansion_state="COMPRESSED",
            direction_bias="SHORT_BIAS",
        ),
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            Path("config/probationary_pattern_engine_paper_atpe_canary.yaml"),
            override_path,
        ],
        Path("config/schwab.local.json"),
    )

    atpe_lane = next(lane for lane in runner._lanes if lane.spec.lane_id == "atpe_long_medium_high_canary__MES")  # noqa: SLF001

    for _ in range(len(mes_bars)):
        runner.run(poll_once=True)

    order_intents = atpe_lane.repositories.order_intents.list_all()
    fills = atpe_lane.repositories.fills.list_all()
    trades_path = atpe_lane.settings.probationary_artifacts_path / "trades.jsonl"
    trades_rows = [json.loads(line) for line in trades_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    operator_status = json.loads((atpe_lane.settings.probationary_artifacts_path / "operator_status.json").read_text(encoding="utf-8"))

    assert [row["intent_type"] for row in order_intents] == [
        OrderIntentType.BUY_TO_OPEN.value,
        OrderIntentType.SELL_TO_CLOSE.value,
    ]
    assert len(fills) == 2
    assert len(trades_rows) == 1
    assert operator_status["intent_count"] == 2
    assert operator_status["fill_count"] == 2
    assert operator_status["position_side"] == "FLAT"


def test_atpe_target_checkpoint_policy_allows_healthy_winner_to_extend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = "MES"
    start = datetime(2026, 3, 24, 3, 1, tzinfo=ZoneInfo("America/New_York"))

    def _bar(index: int, *, open_px: float, high_px: float, low_px: float, close_px: float) -> ResearchBar:
        end_ts = start + timedelta(minutes=index)
        return ResearchBar(
            instrument=instrument,
            timeframe="1m",
            start_ts=end_ts - timedelta(minutes=1),
            end_ts=end_ts,
            open=open_px,
            high=high_px,
            low=low_px,
            close=close_px,
            volume=10,
            session_label="LONDON",
            session_segment="LONDON_OPEN",
        )

    bars = [
        _bar(0, open_px=100.0, high_px=100.2, low_px=99.9, close_px=100.1),
        _bar(1, open_px=100.1, high_px=100.2, low_px=100.0, close_px=100.1),
        _bar(2, open_px=100.1, high_px=100.3, low_px=100.0, close_px=100.2),
            _bar(3, open_px=100.2, high_px=100.4, low_px=100.1, close_px=100.3),
            _bar(4, open_px=100.3, high_px=100.4, low_px=100.2, close_px=100.3),
            _bar(5, open_px=100.3, high_px=101.4, low_px=100.2, close_px=101.2),
            _bar(6, open_px=101.5, high_px=101.9, low_px=101.4, close_px=101.8),
            _bar(7, open_px=101.7, high_px=102.1, low_px=101.6, close_px=102.0),
        _bar(8, open_px=102.0, high_px=102.1, low_px=101.8, close_px=101.9),
        _bar(9, open_px=102.0, high_px=102.1, low_px=101.9, close_px=102.0),
    ]
    variant = SimpleNamespace(
        variant_id="trend_participation.pullback_continuation.long.conservative",
        family="pullback_continuation",
        side="LONG",
        entry_window_bars_1m=3,
        max_hold_bars_1m=10,
        stop_atr_multiple=0.7,
        target_r_multiple=1.2,
        trigger_reclaim_band_multiple=0.0,
    )
    emitted_decisions: set[str] = set()

    def _fake_generate_signal_decisions(*, feature_rows, variants, higher_priority_signals):
        if not feature_rows:
            return []
        feature = feature_rows[-1]
        decision_id = f"{feature.instrument}:{feature.decision_ts.isoformat()}:{variant.variant_id}"
        if feature.instrument != instrument or emitted_decisions:
            return []
        emitted_decisions.add(decision_id)
        return [
            SimpleNamespace(
                instrument=feature.instrument,
                variant_id=variant.variant_id,
                family=variant.family,
                side="LONG",
                decision_ts=feature.decision_ts,
                session_date=feature.session_date,
                session_segment=feature.session_segment,
                decision_id=decision_id,
                setup_quality_bucket="MEDIUM",
                setup_quality_score=0.8,
                conflict_outcome=ConflictOutcome.NO_CONFLICT,
                block_reason=None,
                setup_signature="mes-atpe-target-test",
                setup_state_signature="mes-atpe-target-state",
                feature_snapshot={"test": True},
                average_range=1.0,
                decision_bar_high=100.4,
                decision_bar_low=99.9,
                decision_bar_open=100.0,
                decision_bar_close=100.2,
            )
        ]

    def _feature_for(ts: datetime):
        if ts <= bars[7].end_ts:
            return SimpleNamespace(
                trend_state="STRONG_UP",
                momentum_persistence="PERSISTENT_UP",
                mtf_agreement_state="ALIGNED_UP",
                bar_anatomy="BULL_IMPULSE",
                reference_state="NEAR_RECENT_HIGH",
                expansion_state="EXPANDED",
                direction_bias="LONG_BIAS",
            )
        return SimpleNamespace(
            trend_state="FLAT",
            momentum_persistence="MIXED",
            mtf_agreement_state="MIXED",
            bar_anatomy="UPPER_REJECTION",
            reference_state="MID_RANGE",
            expansion_state="COMPRESSED",
            direction_bias="SHORT_BIAS",
        )

    monkeypatch.setattr(probationary_runtime_module, "generate_signal_decisions", _fake_generate_signal_decisions)
    monkeypatch.setattr(
        probationary_runtime_module,
        "_latest_atpe_feature_state_from_bars",
        lambda *, bars_1m, instrument: _feature_for(sorted(bars_1m, key=lambda item: item.end_ts)[-1].end_ts),
    )

    hard_target_trades = simulate_atpe_exit_policy_on_bars(
        bars_1m=bars,
        variant=variant,
        instrument=instrument,
        point_value=5.0,
        quality_bucket_policy="MEDIUM_HIGH_ONLY",
        exit_policy=ATPE_EXIT_POLICY_HARD_TARGET,
    )
    emitted_decisions.clear()
    checkpoint_trades = simulate_atpe_exit_policy_on_bars(
        bars_1m=bars,
        variant=variant,
        instrument=instrument,
        point_value=5.0,
        quality_bucket_policy="MEDIUM_HIGH_ONLY",
        exit_policy=ATPE_EXIT_POLICY_TARGET_CHECKPOINT,
    )

    assert len(hard_target_trades) == 1
    assert len(checkpoint_trades) == 1
    assert hard_target_trades[0]["exit_reason"] == "atpe_target"
    assert checkpoint_trades[0]["exit_reason"] == "atpe_target_momentum_fade"
    assert checkpoint_trades[0]["realized_pnl"] > hard_target_trades[0]["realized_pnl"]


def test_atpe_target_checkpoint_policy_exits_normally_when_momentum_is_already_degrading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = "MES"
    start = datetime(2026, 3, 24, 3, 1, tzinfo=ZoneInfo("America/New_York"))

    def _bar(index: int, *, open_px: float, high_px: float, low_px: float, close_px: float) -> ResearchBar:
        end_ts = start + timedelta(minutes=index)
        return ResearchBar(
            instrument=instrument,
            timeframe="1m",
            start_ts=end_ts - timedelta(minutes=1),
            end_ts=end_ts,
            open=open_px,
            high=high_px,
            low=low_px,
            close=close_px,
            volume=10,
            session_label="LONDON",
            session_segment="LONDON_OPEN",
        )

        bars = [
            _bar(0, open_px=100.0, high_px=100.2, low_px=99.9, close_px=100.1),
            _bar(1, open_px=100.1, high_px=100.2, low_px=100.0, close_px=100.1),
            _bar(2, open_px=100.1, high_px=100.3, low_px=100.0, close_px=100.2),
            _bar(3, open_px=100.2, high_px=100.4, low_px=100.1, close_px=100.3),
            _bar(4, open_px=100.3, high_px=100.4, low_px=100.2, close_px=100.3),
            _bar(5, open_px=100.3, high_px=101.4, low_px=100.2, close_px=100.7),
        _bar(6, open_px=101.1, high_px=101.2, low_px=100.9, close_px=101.0),
        _bar(7, open_px=101.0, high_px=101.1, low_px=100.8, close_px=100.9),
    ]
    variant = SimpleNamespace(
        variant_id="trend_participation.pullback_continuation.long.conservative",
        family="pullback_continuation",
        side="LONG",
        entry_window_bars_1m=3,
        max_hold_bars_1m=10,
        stop_atr_multiple=0.7,
        target_r_multiple=1.2,
        trigger_reclaim_band_multiple=0.0,
    )
    emitted_decisions: set[str] = set()

    def _fake_generate_signal_decisions(*, feature_rows, variants, higher_priority_signals):
        if not feature_rows:
            return []
        feature = feature_rows[-1]
        decision_id = f"{feature.instrument}:{feature.decision_ts.isoformat()}:{variant.variant_id}"
        if feature.instrument != instrument or emitted_decisions:
            return []
        emitted_decisions.add(decision_id)
        return [
            SimpleNamespace(
                instrument=feature.instrument,
                variant_id=variant.variant_id,
                family=variant.family,
                side="LONG",
                decision_ts=feature.decision_ts,
                session_date=feature.session_date,
                session_segment=feature.session_segment,
                decision_id=decision_id,
                setup_quality_bucket="MEDIUM",
                setup_quality_score=0.8,
                conflict_outcome=ConflictOutcome.NO_CONFLICT,
                block_reason=None,
                setup_signature="mes-atpe-target-test",
                setup_state_signature="mes-atpe-target-state",
                feature_snapshot={"test": True},
                average_range=1.0,
                decision_bar_high=100.4,
                decision_bar_low=99.9,
                decision_bar_open=100.0,
                decision_bar_close=100.2,
            )
        ]

    monkeypatch.setattr(probationary_runtime_module, "generate_signal_decisions", _fake_generate_signal_decisions)
    monkeypatch.setattr(
        probationary_runtime_module,
        "_latest_atpe_feature_state_from_bars",
        lambda *, bars_1m, instrument: SimpleNamespace(
            trend_state="FLAT",
            momentum_persistence="MIXED",
            mtf_agreement_state="MIXED",
            bar_anatomy="UPPER_REJECTION",
            reference_state="MID_RANGE",
            expansion_state="COMPRESSED",
            direction_bias="SHORT_BIAS",
        ),
    )

    checkpoint_trades = simulate_atpe_exit_policy_on_bars(
        bars_1m=bars,
        variant=variant,
        instrument=instrument,
        point_value=5.0,
        quality_bucket_policy="MEDIUM_HIGH_ONLY",
        exit_policy=ATPE_EXIT_POLICY_TARGET_CHECKPOINT,
    )

    assert len(checkpoint_trades) == 1
    assert checkpoint_trades[0]["exit_reason"] == "atpe_target"


def test_probationary_quant_lane_builds_real_strategy_engine_instance(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    quant_spec = next(
        spec
        for spec in _load_probationary_paper_lane_specs(settings)
        if spec.lane_id == "breakout_metals_us_unknown_continuation__GC"
    )
    lane_settings = _build_probationary_paper_lane_settings(settings, quant_spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    logger = ProbationaryLaneStructuredLogger(
        lane_id=quant_spec.lane_id,
        symbol=quant_spec.symbol,
        root_logger=StructuredLogger(settings.probationary_artifacts_path),
        lane_logger=StructuredLogger(lane_settings.probationary_artifacts_path),
    )
    engine = _build_probationary_strategy_engine(
        spec=quant_spec,
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=logger,
        alert_dispatcher=AlertDispatcher(logger),
        runtime_identity={
            "standalone_strategy_id": quant_spec.lane_id,
            "strategy_family": quant_spec.strategy_family,
            "instrument": quant_spec.symbol,
            "lane_id": quant_spec.lane_id,
        },
    )

    assert isinstance(engine, ApprovedQuantStrategyEngine)
    assert lane_settings.symbol == "GC"
    assert "breakout_continuation" in lane_settings.approved_long_entry_sources


def test_probationary_gc_mgc_acceptance_lane_builds_non_authority_strategy_engine(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings_with_gc_mgc_acceptance(tmp_path)
    acceptance_spec = next(
        spec
        for spec in _load_probationary_paper_lane_specs(settings)
        if spec.lane_id == "gc_mgc_london_open_acceptance_continuation_long__GC"
    )
    lane_settings = _build_probationary_paper_lane_settings(settings, acceptance_spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    logger = ProbationaryLaneStructuredLogger(
        lane_id=acceptance_spec.lane_id,
        symbol=acceptance_spec.symbol,
        root_logger=StructuredLogger(settings.probationary_artifacts_path),
        lane_logger=StructuredLogger(lane_settings.probationary_artifacts_path),
    )
    engine = _build_probationary_strategy_engine(
        spec=acceptance_spec,
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=logger,
        alert_dispatcher=AlertDispatcher(logger),
        runtime_identity={
            "standalone_strategy_id": acceptance_spec.lane_id,
            "strategy_family": acceptance_spec.strategy_family,
            "instrument": acceptance_spec.symbol,
            "lane_id": acceptance_spec.lane_id,
        },
    )

    assert isinstance(engine, GcMgcLondonOpenAcceptanceContinuationStrategyEngine)
    assert lane_settings.symbol == "GC"
    assert GC_MGC_LONDON_OPEN_ACCEPTANCE_SOURCE in lane_settings.approved_long_entry_sources


def test_probationary_runtime_blocks_unallowlisted_entry_and_logs_rule_block(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    bar = _build_bar(datetime(2026, 3, 17, 15, 0, tzinfo=ZoneInfo("America/New_York")))
    packet = replace(_blank_signal_packet(bar.bar_id), long_entry=True, long_entry_source="firstBullSnapTurn")

    controlled = engine._apply_runtime_entry_controls(bar, packet)  # noqa: SLF001

    assert controlled.long_entry is False
    assert controlled.long_entry_source is None
    rule_block_lines = (tmp_path / "rule_blocks.jsonl").read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(rule_block_lines[-1])
    assert payload["block_reason"] == "probationary_long_source_not_allowlisted"
    assert payload["source"] == "firstBullSnapTurn"


def test_probationary_runtime_blocks_us_late_1755_carryover_and_logs_rule_block(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    bar = _build_bar(datetime(2026, 3, 17, 16, 55, tzinfo=ZoneInfo("America/New_York")))
    packet = replace(_blank_signal_packet(bar.bar_id), long_entry=True, long_entry_source="usLatePauseResumeLongTurn")

    controlled = engine._apply_runtime_entry_controls(bar, packet)  # noqa: SLF001

    assert controlled.long_entry is False
    assert controlled.long_entry_source is None
    rule_block_lines = (tmp_path / "rule_blocks.jsonl").read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(rule_block_lines[-1])
    assert payload["block_reason"] == "us_late_1755_carryover_exclusion"
    assert payload["source"] == "usLatePauseResumeLongTurn"


def test_live_polling_service_filters_to_new_completed_bars(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    adapter = SchwabMarketDataAdapter(
        settings,
        SchwabMarketDataConfig(
            auth=SchwabAuthConfig(app_key="test", app_secret="test", callback_url="https://localhost"),
            historical_symbol_map={"MGC": "/MGC"},
            quote_symbol_map={"MGC": "/MGC"},
            timeframe_map={"5m": SchwabPriceHistoryFrequency(frequency_type="minute", frequency=5)},
            field_map=SchwabBarFieldMap(
                timestamp_field="datetime",
                open_field="open",
                high_field="high",
                low_field="low",
                close_field="close",
                volume_field="volume",
                is_final_field=None,
                timestamp_semantics=TimestampSemantics.END,
            ),
        ),
    )

    class FakeClient:
        def __init__(self, records):
            self._records = records

        def poll_live_bars(self, external_symbol, external_timeframe, request):
            return self._records

    now = datetime.now(settings.timezone_info)
    latest_completed_end = now.replace(minute=now.minute - (now.minute % 5), second=0, microsecond=0)
    records = [
        {
            "datetime": int((latest_completed_end - timedelta(minutes=5)).timestamp() * 1000),
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 10,
        },
        {
            "datetime": int(latest_completed_end.timestamp() * 1000),
            "open": 101,
            "high": 102,
            "low": 100,
            "close": 101,
            "volume": 11,
        },
        {
            "datetime": int((latest_completed_end + timedelta(minutes=5)).timestamp() * 1000),
            "open": 102,
            "high": 103,
            "low": 101,
            "close": 102,
            "volume": 12,
        },
    ]
    service = LivePollingService(adapter=adapter, client=FakeClient(records), repositories=repositories)

    bars = service.poll_bars(
        SchwabLivePollRequest(internal_symbol="MGC", since=latest_completed_end - timedelta(minutes=5)),
        internal_timeframe="5m",
        default_is_final=True,
    )

    assert [bar.end_ts for bar in bars] == [latest_completed_end]
    saved_row = repositories.bars.get_row(bars[0].bar_id)
    assert saved_row is not None
    assert saved_row["data_source"] == "schwab_live_poll"


def test_sunday_open_first_actionable_bar_is_1805_et(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    adapter = SchwabMarketDataAdapter(
        settings,
        SchwabMarketDataConfig(
            auth=SchwabAuthConfig(app_key="test", app_secret="test", callback_url="https://localhost"),
            historical_symbol_map={"MGC": "/MGC"},
            quote_symbol_map={"MGC": "/MGC"},
            timeframe_map={"5m": SchwabPriceHistoryFrequency(frequency_type="minute", frequency=5)},
            field_map=SchwabBarFieldMap(
                timestamp_field="datetime",
                open_field="open",
                high_field="high",
                low_field="low",
                close_field="close",
                volume_field="volume",
                timestamp_semantics=TimestampSemantics.END,
            ),
        ),
    )
    service = LivePollingService(adapter=adapter)
    ny = ZoneInfo("America/New_York")
    sunday_open = datetime(2026, 3, 22, 18, 0, tzinfo=ny)
    first_completed_end = sunday_open + timedelta(minutes=5)
    first_completed_bar = _build_bar(first_completed_end)

    class FrozenBeforeFirstClose(datetime):
        @classmethod
        def now(cls, tz=None):
            current = sunday_open + timedelta(minutes=4, seconds=59)
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr("mgc_v05l.market_data.live_feed.datetime", FrozenBeforeFirstClose)
    before_first_close = service._filter_completed_bars(  # noqa: SLF001
        [first_completed_bar],
        request=SchwabLivePollRequest(internal_symbol="MGC", since=sunday_open),
        internal_timeframe="5m",
    )

    class FrozenAtFirstClose(datetime):
        @classmethod
        def now(cls, tz=None):
            current = first_completed_end
            return current if tz is None else current.astimezone(tz)

    monkeypatch.setattr("mgc_v05l.market_data.live_feed.datetime", FrozenAtFirstClose)
    at_first_close = service._filter_completed_bars(  # noqa: SLF001
        [first_completed_bar],
        request=SchwabLivePollRequest(internal_symbol="MGC", since=sunday_open),
        internal_timeframe="5m",
    )

    assert before_first_close == []
    assert [bar.end_ts for bar in at_first_close] == [first_completed_end]
    assert _latest_completed_bar_end(sunday_open + timedelta(minutes=4, seconds=59), "5m") == sunday_open
    assert _latest_completed_bar_end(first_completed_end, "5m") == first_completed_end


def test_sunday_open_first_completed_bar_classifies_into_asia_with_dst_offset(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    ny = ZoneInfo("America/New_York")
    classified = classify_sessions(_build_bar(datetime(2026, 3, 22, 18, 5, tzinfo=ny)), settings)

    assert classified.end_ts.isoformat().endswith("-04:00")
    assert classified.session_asia is True
    assert classified.session_allowed is True


def test_probationary_parity_report_writes_artifact(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    end_ts = datetime(2026, 3, 17, 18, 0, tzinfo=ZoneInfo("America/New_York"))
    bar = _build_bar(end_ts)
    repositories.bars.save(bar, data_source="schwab_live_poll")
    repositories.processed_bars.mark_processed(bar)

    override_path = tmp_path / "override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "{settings.database_url}"',
                f'probationary_artifacts_dir: "{tmp_path}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    report_path = generate_probationary_parity_report(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            override_path,
        ],
        start_timestamp=end_ts - timedelta(minutes=1),
        end_timestamp=end_ts + timedelta(minutes=1),
    )

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["captured_live_bar_count"] == 1
    assert payload["replay_bar_count"] == 1
    assert payload["signal_source_counts_match"] is True
    assert payload["order_reason_counts_match"] is True


def test_probationary_inspection_summarizes_current_session(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    end_ts = datetime(2026, 3, 18, 14, 0, tzinfo=ZoneInfo("America/New_York"))
    bar = _build_bar(end_ts)
    repositories.bars.save(bar, data_source="schwab_live_poll")
    repositories.processed_bars.mark_processed(bar)

    (tmp_path / "operator_status.json").write_text(
        json.dumps(
            {
                "health": {"health_status": "HEALTHY"},
                "strategy_status": "READY",
                "position_side": "FLAT",
                "processed_bars": 1,
                "new_bars_last_cycle": 1,
                "last_processed_bar_end_ts": end_ts.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": end_ts.isoformat(),
                "source": "asiaEarlyPauseResumeShortTurn",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "rule_blocks.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": end_ts.isoformat(),
                "source": "usLatePauseResumeLongTurn",
                "block_reason": "us_late_1755_carryover_exclusion",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "alerts.jsonl").write_text(
        json.dumps(
            {
                "logged_at": end_ts.isoformat(),
                "severity": "warning",
                "code": "branch_rule_blocked",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    inspection = inspect_probationary_shadow_session(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            tmp_path / "override.yaml",
        ]
    )

    assert inspection.health_status == "HEALTHY"
    assert inspection.strategy_status == "READY"
    assert inspection.processed_bars_session == 1
    assert inspection.branch_source_counts["asiaEarlyPauseResumeShortTurn"] == 1
    assert inspection.blocked_reason_counts["us_late_1755_carryover_exclusion"] == 1
    assert inspection.alert_counts_by_code["branch_rule_blocked"] == 1


def test_probationary_daily_summary_writes_bundle(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    end_ts = datetime(2026, 3, 18, 15, 0, tzinfo=ZoneInfo("America/New_York"))
    bar = _build_bar(end_ts)
    repositories.bars.save(bar, data_source="schwab_live_poll")
    repositories.processed_bars.mark_processed(bar)

    intent = OrderIntent(
        order_intent_id="order-1",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.SELL_TO_OPEN,
        quantity=1,
        created_at=end_ts,
        reason_code="asiaEarlyPauseResumeShortTurn",
    )
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-order-1")
    (tmp_path / "operator_status.json").write_text(
        json.dumps(
            {
                "health": {"health_status": "HEALTHY"},
                "strategy_status": "READY",
                "position_side": "FLAT",
                "processed_bars": 1,
                "new_bars_last_cycle": 1,
                "last_processed_bar_end_ts": end_ts.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": end_ts.isoformat(),
                "source": "asiaEarlyPauseResumeShortTurn",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "rule_blocks.jsonl").write_text("", encoding="utf-8")
    (tmp_path / "alerts.jsonl").write_text("", encoding="utf-8")

    summary = generate_probationary_daily_summary(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            tmp_path / "override.yaml",
        ]
    )

    payload = json.loads(Path(summary.json_path).read_text(encoding="utf-8"))
    assert payload["processed_bars_session"] == 1
    assert payload["entries_and_exits_by_branch"]["asiaEarlyPauseResumeShortTurn"] == 1
    assert Path(summary.blotter_path).exists()
    assert Path(summary.markdown_path).exists()


def test_probationary_paper_readiness_writes_not_ready_artifact(tmp_path: Path) -> None:
    _build_probationary_settings(tmp_path)
    readiness = build_probationary_paper_readiness(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            tmp_path / "override.yaml",
        ]
    )

    payload = json.loads(Path(readiness.artifact_path).read_text(encoding="utf-8"))
    assert payload["ready_for_paper_soak"] is False
    assert (
        "Paper fill model is still assumption-based: fills occur deterministically at the next completed bar open, not from exchange microstructure."
        in payload["placeholder_or_missing"]
    )


def test_probationary_paper_soak_validation_writes_pass_summary_artifact(tmp_path: Path) -> None:
    _build_probationary_paper_settings(tmp_path)

    validation = run_probationary_paper_soak_validation(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            tmp_path / "paper_override.yaml",
        ]
    )

    payload = json.loads(Path(validation.artifact_path).read_text(encoding="utf-8"))
    scenario_rows = {row["scenario_id"]: row for row in payload["scenarios"]}

    assert Path(validation.artifact_path).exists()
    assert Path(validation.markdown_path).exists()
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["scenario_count"] == 10
    assert payload["summary"]["passed_count"] == 10
    assert payload["summary"]["runtime_phase"] == "READY"
    assert payload["summary"]["strategy_state"] == "READY"
    assert payload["summary"]["position_state"]["side"] == "FLAT"
    assert scenario_rows["clean_entry_exit_cycle"]["status"] == "PASS"
    assert scenario_rows["restart_while_pending_order"]["status"] == "PASS"
    assert scenario_rows["out_of_order_bar_rejected"]["status"] == "PASS"
    assert scenario_rows["stale_missing_bar_handling"]["status"] == "PASS"
    assert scenario_rows["stale_missing_bar_handling"]["summary"]["market_data_health"]["market_data_ok"] is False
    assert scenario_rows["missing_fill_acknowledgement_reconciling"]["summary"]["runtime_phase"] == "RECONCILING"
    assert scenario_rows["missing_fill_acknowledgement_reconciling"]["summary"]["latest_order_timeout_watchdog"]["status"] == "RECONCILING"
    assert scenario_rows["persistence_invariant_fault"]["summary"]["runtime_phase"] == "FAULT"


def test_probationary_live_timing_validation_writes_pass_summary_artifact(tmp_path: Path) -> None:
    _build_probationary_paper_settings(tmp_path)

    validation = _run_probationary_live_timing_validation(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            tmp_path / "paper_override.yaml",
        ]
    )

    payload = json.loads(Path(validation.artifact_path).read_text(encoding="utf-8"))
    scenario_rows = {row["scenario_id"]: row for row in payload["scenarios"]}

    assert Path(validation.artifact_path).exists()
    assert Path(validation.markdown_path).exists()
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["scenario_count"] == 8
    assert payload["summary"]["passed_count"] == 8
    assert payload["contract"]["completed_bar_only"] is True
    assert payload["contract"]["position_transitions_on_fill_only"] is True
    assert payload["contract"]["flat_reset_on_exit_fill_only"] is True
    assert payload["contract"]["broker_truth_decision_order"] == [
        "direct_order_status",
        "open_orders",
        "position_truth",
        "fill_truth",
    ]
    assert scenario_rows["submit_after_completed_bar_close"]["summary"]["submit_attempted_at"] == scenario_rows["submit_after_completed_bar_close"]["summary"]["intent_created_at"]
    assert scenario_rows["ack_prompt_fill_delayed"]["summary"]["pending_stage"] == "AWAITING_FILL"
    assert scenario_rows["fill_missing_position_truth_exists"]["summary"]["pending_stage"] == "RECONCILING"
    assert scenario_rows["rejected_after_submit"]["summary"]["pending_stage"] in {"IDLE", "TERMINAL_NON_FILL"}
    assert scenario_rows["broker_unavailable_at_submit_time"]["summary"]["pending_stage"] == "RECONCILING"
    assert scenario_rows["exit_timing_fill_driven"]["summary"]["pending_stage"] == "FILLED_CONFIRMED"


def test_probationary_paper_soak_extended_writes_restart_and_drift_summary(tmp_path: Path) -> None:
    _build_probationary_paper_settings(tmp_path)

    soak = _run_probationary_paper_soak_extended(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            tmp_path / "paper_override.yaml",
        ]
    )

    payload = json.loads(Path(soak.artifact_path).read_text(encoding="utf-8"))
    checkpoint_rows = {row["checkpoint_id"]: row for row in payload["checkpoint_rows"]}

    assert Path(soak.artifact_path).exists()
    assert Path(soak.markdown_path).exists()
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["bars_processed"] == 24
    assert payload["summary"]["restart_count"] == 5
    assert payload["summary"]["drift_detected"] is False
    assert payload["summary"]["final_runtime_phase"] == "RECONCILING"
    assert payload["summary"]["final_entry_blocker"] == "fill_timeout_escalated"
    assert checkpoint_rows["flat_ready_startup"]["drift_detected"] is False
    assert checkpoint_rows["pending_acknowledged_order"]["duplicate_action_prevention_held"] is True
    assert checkpoint_rows["in_position_restart"]["after"]["position_state"]["side"] == "LONG"
    assert checkpoint_rows["post_exit_fill_restart"]["after"]["position_state"]["side"] == "FLAT"
    assert checkpoint_rows["degraded_watchdog_restart"]["after"]["runtime_phase"] == "RECONCILING"


def test_probationary_paper_soak_unattended_writes_longer_restart_and_drift_summary(tmp_path: Path) -> None:
    _build_probationary_paper_settings(tmp_path)

    soak = _run_probationary_paper_soak_unattended(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            tmp_path / "paper_override.yaml",
        ]
    )

    payload = json.loads(Path(soak.artifact_path).read_text(encoding="utf-8"))
    checkpoint_rows = {row["checkpoint_id"]: row for row in payload["checkpoint_rows"]}

    assert Path(soak.artifact_path).exists()
    assert Path(soak.markdown_path).exists()
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["bars_processed"] == 60
    assert payload["summary"]["runtime_duration_minutes"] == 295
    assert payload["summary"]["restart_count"] == 7
    assert payload["summary"]["drift_detected"] is False
    assert "heartbeat_reconcile_restart" in payload["summary"]["restart_points_hit"]
    assert checkpoint_rows["pending_acknowledged_order"]["duplicate_action_prevention_held"] is True
    assert checkpoint_rows["in_position_restart"]["after"]["position_state"]["side"] == "LONG"
    assert checkpoint_rows["post_exit_fill_restart"]["after"]["position_state"]["side"] == "FLAT"
    assert checkpoint_rows["degraded_watchdog_restart"]["after"]["runtime_phase"] == "RECONCILING"
    assert checkpoint_rows["degraded_watchdog_restart"]["summary_alignment_held"] is True


def test_probationary_paper_soak_unattended_surfaces_injected_drift(tmp_path: Path) -> None:
    _build_probationary_paper_settings(tmp_path)

    soak = _run_probationary_paper_soak_unattended(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            tmp_path / "paper_override.yaml",
        ],
        inject_drift_checkpoint="in_position_restart",
    )

    payload = json.loads(Path(soak.artifact_path).read_text(encoding="utf-8"))
    checkpoint_rows = {row["checkpoint_id"]: row for row in payload["checkpoint_rows"]}

    assert payload["summary"]["result"] == "FAIL"
    assert payload["summary"]["drift_detected"] is True
    assert checkpoint_rows["in_position_restart"]["drift_detected"] is True
    assert "fault_code" in checkpoint_rows["in_position_restart"]["drift_fields"]


def test_probationary_paper_lane_writes_exit_parity_summary_artifact(tmp_path: Path) -> None:
    _build_probationary_paper_settings(tmp_path)
    base_settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            tmp_path / "paper_override.yaml",
        ]
    )
    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=tmp_path / "exit_parity_runtime",
        bars=[
            Bar(
                bar_id="MGC|5m|2026-03-26T22:10:00+00:00",
                symbol="MGC",
                timeframe="5m",
                start_ts=datetime(2026, 3, 26, 18, 5, tzinfo=ZoneInfo("America/New_York")),
                end_ts=datetime(2026, 3, 26, 18, 10, tzinfo=ZoneInfo("America/New_York")),
                open=Decimal("100.9"),
                high=Decimal("101.1"),
                low=Decimal("98.7"),
                close=Decimal("99"),
                volume=100,
                is_final=True,
                session_asia=False,
                session_london=False,
                session_us=False,
                session_allowed=False,
            ),
        ],
    )
    runtime.restore_startup()
    fill = FillEvent(
        order_intent_id="exit-parity-entry",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        order_status=OrderStatus.FILLED,
        fill_timestamp=datetime(2026, 3, 26, 18, 5, tzinfo=ZoneInfo("America/New_York")),
        fill_price=Decimal("100"),
        broker_order_id="paper-exit-parity-entry",
    )
    strategy_engine.apply_fill(
        fill_event=fill,
        signal_bar_id="entry-bar",
        long_entry_family=LongEntryFamily.K,
    )
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        last_swing_low=Decimal("99.5"),
        long_be_armed=True,
        bars_in_trade=6,
    )
    strategy_engine._persist_state(strategy_engine.state, transition_label="seed_exit_parity")  # noqa: SLF001
    setup_bar = Bar(
        bar_id="MGC|5m|2026-03-26T22:05:00+00:00",
        symbol="MGC",
        timeframe="5m",
        start_ts=datetime(2026, 3, 26, 18, 0, tzinfo=ZoneInfo("America/New_York")),
        end_ts=datetime(2026, 3, 26, 18, 5, tzinfo=ZoneInfo("America/New_York")),
        open=Decimal("101.1"),
        high=Decimal("101.5"),
        low=Decimal("100.8"),
        close=Decimal("101.3"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=False,
        session_allowed=False,
    )
    strategy_engine._bar_history = [setup_bar]  # noqa: SLF001
    strategy_engine._feature_history = [strategy_engine._compute_feature_packet(setup_bar)]  # noqa: SLF001
    runtime.poll_and_process()

    payload = json.loads((lane_logger.artifact_dir / "exit_parity_summary_latest.json").read_text(encoding="utf-8"))
    assert payload["current_position_family"] == "K"
    assert payload["latest_exit_decision"]["primary_reason"] == "LONG_STOP"
    assert payload["latest_exit_decision"]["all_true_reasons"] == [
        "LONG_STOP",
        "LONG_SWING_FAIL",
        "LONG_INTEGRITY_FAIL",
        "LONG_TIME_EXIT",
    ]
    assert payload["exit_fill_pending"] is True
    assert payload["exit_fill_confirmed"] is False


def test_probationary_paper_lane_operator_status_reports_post_cycle_execution_and_context_cadence(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    spec = next(spec for spec in _load_probationary_paper_lane_specs(settings) if spec.lane_id == "mgc_us_late_pause_resume_long")
    lane_settings = _build_probationary_paper_lane_settings(settings, spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    structured_logger = StructuredLogger(lane_settings.probationary_artifacts_path)
    strategy_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )

    class FakeLivePollingService:
        def poll_bars(self, *args, **kwargs):
            bars: list[Bar] = []
            for minute in range(46, 54):
                end_ts = datetime(2026, 4, 2, 10, minute, tzinfo=ZoneInfo("America/New_York"))
                bars.append(
                    Bar(
                        bar_id=f"MGC|1m|{end_ts.astimezone(ZoneInfo('UTC')).strftime('%Y-%m-%dT%H:%M:%SZ')}",
                        symbol="MGC",
                        timeframe="1m",
                        start_ts=end_ts - timedelta(minutes=1),
                        end_ts=end_ts,
                        open=Decimal("100"),
                        high=Decimal("101"),
                        low=Decimal("99"),
                        close=Decimal("100"),
                        volume=100,
                        is_final=True,
                        session_asia=False,
                        session_london=False,
                        session_us=True,
                        session_allowed=True,
                    )
                )
            return bars

    lane_runtime = ProbationaryPaperLaneRuntime(
        spec=spec,
        settings=lane_settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        live_polling_service=FakeLivePollingService(),
        structured_logger=ProbationaryLaneStructuredLogger(
            lane_id=spec.lane_id,
            symbol=spec.symbol,
            root_logger=StructuredLogger(tmp_path / "root"),
            lane_logger=structured_logger,
        ),
        alert_dispatcher=AlertDispatcher(structured_logger),
    )

    lane_runtime.poll_and_process()

    payload = json.loads((lane_settings.probationary_artifacts_path / "operator_status.json").read_text(encoding="utf-8"))
    assert payload["execution_timeframe"] == "1m"
    assert payload["context_timeframes"] == ["5m"]
    assert payload["last_execution_bar_evaluated_at"] == "2026-04-02T10:53:00-04:00"
    assert payload["last_completed_context_bars_at"] == {"5m": "2026-04-02T10:50:00-04:00"}


def test_paper_runtime_restores_pending_order_state_and_reconciles_cleanly(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path).model_copy(update={"mode": RuntimeMode.PAPER})
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    bar = _build_bar(datetime(2026, 3, 18, 15, 5, tzinfo=ZoneInfo("America/New_York")))
    intent = OrderIntent(
        order_intent_id=f"{bar.bar_id}|BUY_TO_OPEN",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=bar.end_ts,
        reason_code="usLatePauseResumeLongTurn",
    )
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-order-1")
    engine._state = replace(engine.state, open_broker_order_id="paper-order-1", last_order_intent_id=intent.order_intent_id)
    engine._persist_state(engine.state, transition_label="intent_created")  # noqa: SLF001

    restart_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    execution_engine = restart_engine._execution_engine  # noqa: SLF001

    _restore_paper_runtime_state(
        repositories=repositories,
        strategy_engine=restart_engine,
        execution_engine=execution_engine,
    )
    reconciliation = _reconcile_paper_runtime(
        repositories=repositories,
        strategy_engine=restart_engine,
        execution_engine=execution_engine,
    )

    assert len(execution_engine.pending_executions()) == 1
    assert reconciliation["clean"] is True


def test_restore_startup_flat_restart_records_ready_validation(tmp_path: Path) -> None:
    _settings, spec, lane_settings, _repositories, lane_logger, build_runtime = _build_standard_lane_restart_fixture(tmp_path)
    lane_runtime, _engine, _execution_engine = build_runtime()

    startup_fault = lane_runtime.restore_startup()

    payload = json.loads((lane_logger.artifact_dir / "restore_validation_latest.json").read_text(encoding="utf-8"))
    assert startup_fault is None
    assert payload["restore_result"] == "READY"
    assert payload["duplicate_action_prevention_held"] is True
    assert payload["restored_state_summary"]["position_side"] == "FLAT"
    assert payload["lane_id"] == spec.lane_id
    assert payload["instrument"] == lane_settings.symbol


def test_restore_startup_in_position_restores_state_without_duplicate_action(tmp_path: Path) -> None:
    _settings, _spec, lane_settings, repositories, lane_logger, build_runtime = _build_standard_lane_restart_fixture(tmp_path)
    seed_execution_engine = ExecutionEngine(broker=PaperBroker())
    seed_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=seed_execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=AlertDispatcher(lane_logger),
    )
    bar = _build_bar(datetime(2026, 3, 18, 15, 5, tzinfo=ZoneInfo("America/New_York")))
    intent = OrderIntent(
        order_intent_id=f"{bar.bar_id}|BUY_TO_OPEN",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=bar.end_ts,
        reason_code="usLatePauseResumeLongTurn",
    )
    repositories.order_intents.save(intent, order_status=OrderStatus.FILLED, broker_order_id="paper-entry-1")
    repositories.fills.save(
        FillEvent(
            order_intent_id=intent.order_intent_id,
            intent_type=OrderIntentType.BUY_TO_OPEN,
            order_status=OrderStatus.FILLED,
            fill_timestamp=bar.end_ts,
            fill_price=Decimal("100.0"),
            broker_order_id="paper-entry-1",
        )
    )
    seed_engine._state = replace(  # noqa: SLF001
        seed_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100.0"),
        last_order_intent_id=intent.order_intent_id,
    )
    seed_engine._persist_state(seed_engine.state, transition_label="seed_long")  # noqa: SLF001

    lane_runtime, restart_engine, restart_execution_engine = build_runtime()
    startup_fault = lane_runtime.restore_startup()
    payload = json.loads((lane_logger.artifact_dir / "restore_validation_latest.json").read_text(encoding="utf-8"))

    assert startup_fault is None
    assert restart_engine.state.position_side is PositionSide.LONG
    assert restart_execution_engine.broker.snapshot_state()["position_quantity"] == 1
    assert payload["restore_result"] == "READY"
    assert payload["duplicate_action_prevention_held"] is True
    assert payload["count_snapshot"]["before"]["order_intent_count"] == payload["count_snapshot"]["after"]["order_intent_count"] == 1
    assert payload["count_snapshot"]["before"]["fill_count"] == payload["count_snapshot"]["after"]["fill_count"] == 1


def test_restore_startup_pending_order_does_not_duplicate_submission(tmp_path: Path) -> None:
    _settings, _spec, lane_settings, repositories, lane_logger, build_runtime = _build_standard_lane_restart_fixture(tmp_path)
    seed_execution_engine = ExecutionEngine(broker=PaperBroker())
    seed_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=seed_execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=AlertDispatcher(lane_logger),
    )
    bar = _build_bar(datetime(2026, 3, 18, 15, 5, tzinfo=ZoneInfo("America/New_York")))
    intent = OrderIntent(
        order_intent_id=f"{bar.bar_id}|BUY_TO_OPEN",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=bar.end_ts,
        reason_code="usLatePauseResumeLongTurn",
    )
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-open-1")
    seed_engine._state = replace(  # noqa: SLF001
        seed_engine.state,
        open_broker_order_id="paper-open-1",
        last_order_intent_id=intent.order_intent_id,
    )
    seed_engine._persist_state(seed_engine.state, transition_label="intent_created")  # noqa: SLF001

    lane_runtime, _restart_engine, restart_execution_engine = build_runtime()
    startup_fault = lane_runtime.restore_startup()
    payload = json.loads((lane_logger.artifact_dir / "restore_validation_latest.json").read_text(encoding="utf-8"))

    assert startup_fault is None
    assert len(restart_execution_engine.pending_executions()) == 1
    assert payload["restore_result"] == "READY"
    assert payload["duplicate_action_prevention_held"] is True
    assert payload["count_snapshot"]["before"]["order_intent_count"] == payload["count_snapshot"]["after"]["order_intent_count"] == 1


def test_restore_startup_stale_pending_marker_safely_cleans_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _settings, _spec, lane_settings, repositories, lane_logger, build_runtime = _build_standard_lane_restart_fixture(tmp_path)
    seed_execution_engine = ExecutionEngine(broker=PaperBroker())
    seed_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=seed_execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=AlertDispatcher(lane_logger),
    )
    bar = _build_bar(datetime(2026, 3, 18, 15, 5, tzinfo=ZoneInfo("America/New_York")))
    intent = OrderIntent(
        order_intent_id=f"{bar.bar_id}|BUY_TO_OPEN",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=bar.end_ts,
        reason_code="usLatePauseResumeLongTurn",
    )
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-stale-1")
    seed_engine._state = replace(  # noqa: SLF001
        seed_engine.state,
        open_broker_order_id="paper-stale-1",
        last_order_intent_id=intent.order_intent_id,
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
    )
    seed_engine._persist_state(seed_engine.state, transition_label="intent_created")  # noqa: SLF001

    lane_runtime, restart_engine, restart_execution_engine = build_runtime()

    def _force_flat_broker(*, position, open_order_ids, order_status, last_fill_timestamp=None):
        restart_execution_engine.broker._position = position  # noqa: SLF001
        restart_execution_engine.broker._open_order_ids = []  # noqa: SLF001
        restart_execution_engine.broker._order_status = {}  # noqa: SLF001
        restart_execution_engine.broker._last_fill_timestamp = last_fill_timestamp  # noqa: SLF001

    monkeypatch.setattr(restart_execution_engine.broker, "restore_state", _force_flat_broker)

    startup_fault = lane_runtime.restore_startup()
    payload = json.loads((lane_logger.artifact_dir / "restore_validation_latest.json").read_text(encoding="utf-8"))

    assert startup_fault is None
    assert payload["restore_result"] == "SAFE_CLEANUP_READY"
    assert payload["safe_cleanup_applied"] is True
    assert restart_engine.state.open_broker_order_id is None
    assert restart_engine.state.reconcile_required is False


def test_restore_startup_unresolved_mismatch_escalates_to_reconciling(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _settings, _spec, lane_settings, repositories, lane_logger, build_runtime = _build_standard_lane_restart_fixture(tmp_path)
    seed_execution_engine = ExecutionEngine(broker=PaperBroker())
    seed_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=seed_execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=AlertDispatcher(lane_logger),
    )
    seed_engine._state = replace(  # noqa: SLF001
        seed_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100.0"),
    )
    seed_engine._persist_state(seed_engine.state, transition_label="seed_long")  # noqa: SLF001

    lane_runtime, restart_engine, restart_execution_engine = build_runtime()

    def _force_flat_broker(*, position, open_order_ids, order_status, last_fill_timestamp=None):
        restart_execution_engine.broker._position = probationary_runtime_module.PaperPosition(quantity=0, average_price=None)  # noqa: SLF001
        restart_execution_engine.broker._open_order_ids = []  # noqa: SLF001
        restart_execution_engine.broker._order_status = {}  # noqa: SLF001
        restart_execution_engine.broker._last_fill_timestamp = last_fill_timestamp  # noqa: SLF001

    monkeypatch.setattr(restart_execution_engine.broker, "restore_state", _force_flat_broker)

    startup_fault = lane_runtime.restore_startup()
    payload = json.loads((lane_logger.artifact_dir / "restore_validation_latest.json").read_text(encoding="utf-8"))

    assert startup_fault == "paper_startup_reconciliation_failed"
    assert payload["restore_result"] == "RECONCILING"
    assert restart_engine.state.reconcile_required is True
    assert restart_engine.state.fault_code == "reconciliation_unsafe_ambiguity"


def test_restore_startup_unsafe_ambiguity_escalates_to_fault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _settings, _spec, lane_settings, repositories, lane_logger, build_runtime = _build_standard_lane_restart_fixture(tmp_path)
    seed_execution_engine = ExecutionEngine(broker=PaperBroker())
    seed_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=seed_execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=AlertDispatcher(lane_logger),
    )
    seed_engine._state = replace(  # noqa: SLF001
        seed_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100.0"),
    )
    seed_engine._persist_state(seed_engine.state, transition_label="seed_long")  # noqa: SLF001

    lane_runtime, restart_engine, restart_execution_engine = build_runtime()

    def _force_opposite_side_broker(*, position, open_order_ids, order_status, last_fill_timestamp=None):
        restart_execution_engine.broker._position = probationary_runtime_module.PaperPosition(quantity=-1, average_price=Decimal("99.0"))  # noqa: SLF001
        restart_execution_engine.broker._open_order_ids = []  # noqa: SLF001
        restart_execution_engine.broker._order_status = {}  # noqa: SLF001
        restart_execution_engine.broker._last_fill_timestamp = last_fill_timestamp  # noqa: SLF001

    monkeypatch.setattr(restart_execution_engine.broker, "restore_state", _force_opposite_side_broker)

    startup_fault = lane_runtime.restore_startup()
    payload = json.loads((lane_logger.artifact_dir / "restore_validation_latest.json").read_text(encoding="utf-8"))

    assert startup_fault == "paper_startup_reconciliation_failed"
    assert payload["restore_result"] == "FAULT"
    assert restart_engine.state.fault_code == "reconciliation_unsafe_opposite_side_exposure"


def test_restore_success_resolves_prior_active_restore_alert(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _settings, _spec, lane_settings, repositories, lane_logger, build_runtime = _build_standard_lane_restart_fixture(tmp_path)
    seed_execution_engine = ExecutionEngine(broker=PaperBroker())
    seed_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=seed_execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=AlertDispatcher(lane_logger),
    )
    seed_engine._state = replace(  # noqa: SLF001
        seed_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100.0"),
    )
    seed_engine._persist_state(seed_engine.state, transition_label="seed_long")  # noqa: SLF001

    failing_runtime, _restart_engine, failing_execution_engine = build_runtime()

    def _force_flat_broker(*, position, open_order_ids, order_status, last_fill_timestamp=None):
        failing_execution_engine.broker._position = probationary_runtime_module.PaperPosition(quantity=0, average_price=None)  # noqa: SLF001
        failing_execution_engine.broker._open_order_ids = []  # noqa: SLF001
        failing_execution_engine.broker._order_status = {}  # noqa: SLF001
        failing_execution_engine.broker._last_fill_timestamp = last_fill_timestamp  # noqa: SLF001

    monkeypatch.setattr(failing_execution_engine.broker, "restore_state", _force_flat_broker)
    assert failing_runtime.restore_startup() == "paper_startup_reconciliation_failed"

    repair_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=lane_logger,
        alert_dispatcher=AlertDispatcher(lane_logger),
    )
    repair_engine._state = replace(  # noqa: SLF001
        repair_engine.state,
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        entry_price=None,
        reconcile_required=False,
        fault_code=None,
        open_broker_order_id=None,
    )
    repair_engine._persist_state(repair_engine.state, transition_label="manual_reset_clean")  # noqa: SLF001

    clean_runtime, _clean_engine, _clean_execution_engine = build_runtime()
    assert clean_runtime.restore_startup() is None

    alerts_state = json.loads((lane_logger.artifact_dir / "alerts_state.json").read_text(encoding="utf-8"))
    active_titles = [str(row.get("title") or "") for row in alerts_state.get("active_alerts", [])]
    assert "State Restore Requires Reconciliation" not in active_titles


def test_probationary_operator_flatten_and_halt_submits_paper_exit_intent(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path).model_copy(update={"mode": RuntimeMode.PAPER})
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger)
    execution_engine = ExecutionEngine(broker=PaperBroker())
    engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )
    engine._state = replace(  # noqa: SLF001
        engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        broker_position_qty=1,
        internal_position_qty=1,
        entry_price=Decimal("100.0"),
    )
    engine._persist_state(engine.state, transition_label="seed_long")  # noqa: SLF001

    submit_probationary_operator_control(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            tmp_path / "override.yaml",
        ],
        action="flatten_and_halt",
    )
    result = _apply_probationary_operator_control(
        settings=settings,
        repositories=repositories,
        strategy_engine=engine,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )

    assert result is not None
    assert result["status"] == "flatten_pending"
    assert result["flatten_state"] == "pending_fill"
    assert engine.state.operator_halt is True
    assert len(execution_engine.pending_executions()) == 1
    saved_rows = repositories.order_intents.list_all()
    assert saved_rows[0]["reason_code"] == "operator_flatten_and_halt"


def test_probationary_operator_control_rejects_and_recovers_from_malformed_queue_file(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path).model_copy(update={"mode": RuntimeMode.PAPER})
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger)
    execution_engine = ExecutionEngine(broker=PaperBroker())
    engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )
    control_path = settings.resolved_probationary_operator_control_path
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text('{"action":"resume_entries"}\ntrailing-garbage\n', encoding="utf-8")

    result = _apply_probationary_operator_control(
        settings=settings,
        repositories=repositories,
        strategy_engine=engine,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )

    assert result is not None
    assert result["status"] == "rejected"
    assert "malformed operator control payload" in result["message"].lower()
    assert "JSONDecodeError" in str(result.get("error"))
    assert Path(str(result["invalid_control_path"])).exists()
    repaired_payload = json.loads(control_path.read_text(encoding="utf-8"))
    assert repaired_payload["status"] == "rejected"
    assert repaired_payload["action"] == "unknown"


def test_probationary_paper_lane_specs_admit_pl_and_gc_asia_only(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)

    specs = _load_probationary_paper_lane_specs(settings)
    lane_ids = {spec.lane_id for spec in specs}
    caps = {spec.lane_id: spec.catastrophic_open_loss for spec in specs}

    assert "pl_us_late_pause_resume_long" in lane_ids
    assert "gc_asia_early_normal_breakout_retest_hold_long" in lane_ids
    assert "gc_us_late_pause_resume_long" not in lane_ids
    assert any(spec.symbol == "PL" and spec.session_restriction == "US_LATE" for spec in specs)
    assert any(spec.symbol == "GC" and spec.session_restriction == "ASIA_EARLY" for spec in specs)
    assert caps["pl_us_late_pause_resume_long"] == Decimal("-1000")
    assert caps["gc_asia_early_normal_breakout_retest_hold_long"] == Decimal("-750")
    assert caps["mgc_us_late_pause_resume_long"] == Decimal("-500")


def test_active_probationary_paper_lane_specs_follow_runtime_config_in_force_only(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    runtime_dir = settings.probationary_artifacts_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-10T22:00:00+00:00",
                "lanes": [
                    {
                        "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "GC only",
                        "symbol": "GC",
                        "session_restriction": "ASIA_EARLY",
                        "catastrophic_open_loss": "-600",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    specs = _active_probationary_paper_lane_specs(settings)

    assert [spec.lane_id for spec in specs] == ["gc_asia_early_normal_breakout_retest_hold_long"]


def test_probationary_paper_lane_specs_append_canary_only_when_enabled(tmp_path: Path) -> None:
    without_canary = _build_probationary_paper_settings(tmp_path / "without")
    with_canary = _build_probationary_paper_settings_with_canary(tmp_path / "with")

    without_lane_ids = {spec.lane_id for spec in _load_probationary_paper_lane_specs(without_canary)}
    with_specs = _load_probationary_paper_lane_specs(with_canary)
    with_lane_ids = {spec.lane_id for spec in with_specs}
    canary_spec = next(spec for spec in with_specs if spec.lane_id == "canary_gc_us_early_execution_once")

    assert "canary_gc_us_early_execution_once" not in without_lane_ids
    assert "canary_gc_us_early_execution_once" in with_lane_ids
    assert canary_spec.symbol == "GC"
    assert canary_spec.trade_size == 1
    assert canary_spec.lane_mode == "PAPER_EXECUTION_CANARY"
    assert canary_spec.canary_entry_not_before_et == "10:35:00"
    assert canary_spec.canary_entry_window_end_et == "12:00:00"
    assert canary_spec.canary_exit_not_before_et == "15:45:00"
    assert canary_spec.canary_max_entries_per_session == 2
    assert canary_spec.canary_one_shot_per_session is True
    assert with_canary.probationary_paper_execution_canary_force_fire_once_token == ""


def test_build_probationary_paper_runner_uses_multi_lane_supervisor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paper_override = tmp_path / "paper_override.yaml"
    paper_override.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeLivePollingService:
        def poll_bars(self, *args, **kwargs):
            return []

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime._build_live_polling_service",
        lambda settings, repositories, schwab_config_path: FakeLivePollingService(),
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
        Path("config/schwab.local.json"),
    )

    assert runner.__class__.__name__ == "ProbationaryPaperSupervisor"
    lane_ids = [lane.spec.lane_id for lane in runner._lanes]  # noqa: SLF001
    assert lane_ids[:5] == [
        "mgc_us_late_pause_resume_long",
        "mgc_asia_early_normal_breakout_retest_hold_long",
        "mgc_asia_early_pause_resume_short",
        "pl_us_late_pause_resume_long",
        "gc_asia_early_normal_breakout_retest_hold_long",
    ]
    assert set(lane_ids[5:]) == {
        "breakout_metals_us_unknown_continuation__GC",
        "breakout_metals_us_unknown_continuation__MGC",
        "breakout_metals_us_unknown_continuation__HG",
        "breakout_metals_us_unknown_continuation__PL",
        "failed_move_no_us_reversal_short__6E",
        "failed_move_no_us_reversal_short__6J",
        "failed_move_no_us_reversal_short__CL",
        "failed_move_no_us_reversal_short__ES",
        "failed_move_no_us_reversal_short__QC",
    }


def test_probationary_paper_execution_canary_completes_one_shot_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paper_override = tmp_path / "paper_canary_override.yaml"
    paper_override.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_execution_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeLivePollingService:
        def __init__(self, bars):
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            if not self._bars:
                return []
            return [self._bars.pop(0)]

    canary_bars = [
        _build_symbol_bar("GC", datetime(2026, 3, 20, 10, 35, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 10, 40, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 15, 45, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 15, 50, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
    ]
    empty_service = FakeLivePollingService([])
    service_by_lane: dict[str, FakeLivePollingService] = {}

    def _fake_build_live_polling_service(settings, repositories, schwab_config_path):
        lane_id = settings.probationary_paper_lane_id
        if lane_id not in service_by_lane:
            service_by_lane[lane_id] = FakeLivePollingService(canary_bars if lane_id == "canary_gc_us_early_execution_once" else [])
        return service_by_lane[lane_id]

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime._build_live_polling_service",
        _fake_build_live_polling_service,
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
        Path("config/schwab.local.json"),
    )

    assert runner.__class__.__name__ == "ProbationaryPaperSupervisor"
    canary_lane = next(lane for lane in runner._lanes if lane.spec.lane_id == "canary_gc_us_early_execution_once")  # noqa: SLF001
    non_canary_lanes = [lane for lane in runner._lanes if lane.spec.lane_id != "canary_gc_us_early_execution_once"]  # noqa: SLF001

    runner.run(poll_once=True)
    runner.run(poll_once=True)
    mid_lane_status = json.loads((canary_lane.settings.probationary_artifacts_path / "operator_status.json").read_text(encoding="utf-8"))
    mid_root_status = json.loads((tmp_path / "paper_artifacts" / "operator_status.json").read_text(encoding="utf-8"))
    runner.run(poll_once=True)
    runner.run(poll_once=True)

    with canary_lane.repositories.engine.begin() as connection:
        signal_row = connection.execute(select(signals_table.c.payload_json)).first()
    assert signal_row is not None
    signal_payload = json.loads(signal_row.payload_json)
    assert signal_payload["long_entry_raw"] is True
    assert signal_payload["long_entry"] is True
    assert signal_payload["long_entry_source"] == "paperExecutionCanary"

    canary_intents = canary_lane.repositories.order_intents.list_all()
    canary_fills = canary_lane.repositories.fills.list_all()
    assert [row["reason_code"] for row in canary_intents] == [
        PAPER_EXECUTION_CANARY_ENTRY_REASON,
        PAPER_EXECUTION_CANARY_EXIT_REASON,
    ]
    assert [row["intent_type"] for row in canary_intents] == [
        OrderIntentType.BUY_TO_OPEN.value,
        OrderIntentType.SELL_TO_CLOSE.value,
    ]
    assert len(canary_fills) == 2
    assert canary_fills[0]["intent_type"] == OrderIntentType.BUY_TO_OPEN.value
    assert canary_fills[1]["intent_type"] == OrderIntentType.SELL_TO_CLOSE.value
    assert mid_lane_status["position_side"] == "LONG"
    assert mid_root_status["position_side"] == "MULTI"
    assert canary_lane.strategy_engine.state.position_side is PositionSide.FLAT

    lane_status = json.loads((canary_lane.settings.probationary_artifacts_path / "operator_status.json").read_text(encoding="utf-8"))
    root_status = json.loads((tmp_path / "paper_artifacts" / "operator_status.json").read_text(encoding="utf-8"))
    assert lane_status["lane_mode"] == "PAPER_EXECUTION_CANARY"
    assert root_status["strategy_status"] == "RUNNING_MULTI_LANE"
    assert any(row["lane_id"] == "canary_gc_us_early_execution_once" for row in root_status["lanes"])

    branch_rows = [
        json.loads(line)
        for line in (canary_lane.settings.probationary_artifacts_path / "branch_sources.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert branch_rows[-1]["source"] == "paperExecutionCanary"
    assert branch_rows[-1]["decision"] == "allowed"
    assert (canary_lane.settings.probationary_artifacts_path / "reconciliation_events.jsonl").exists()

    summary = generate_probationary_daily_summary(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
    )
    summary_payload = json.loads(Path(summary.json_path).read_text(encoding="utf-8"))
    blotter_text = Path(summary.blotter_path).read_text(encoding="utf-8")
    assert summary_payload["closed_trade_count"] == 1
    assert "paperExecutionCanaryEntryLateWindow" in blotter_text
    assert "paperExecutionCanaryExitNextBarLateWindow" in blotter_text

    for lane in non_canary_lanes:
        assert lane.repositories.order_intents.list_all() == []
        assert lane.repositories.fills.list_all() == []


def test_probationary_paper_execution_canary_allows_second_same_session_entry_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paper_override = tmp_path / "paper_canary_override.yaml"
    paper_override.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_execution_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeLivePollingService:
        def __init__(self, bars):
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            if not self._bars:
                return []
            return [self._bars.pop(0)]

    canary_bars = [
        _build_symbol_bar("GC", datetime(2026, 3, 20, 11, 20, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
    ]
    service_by_lane: dict[str, FakeLivePollingService] = {}

    def _fake_build_live_polling_service(settings, repositories, schwab_config_path):
        lane_id = settings.probationary_paper_lane_id
        if lane_id not in service_by_lane:
            service_by_lane[lane_id] = FakeLivePollingService(canary_bars if lane_id == "canary_gc_us_early_execution_once" else [])
        return service_by_lane[lane_id]

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime._build_live_polling_service",
        _fake_build_live_polling_service,
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
        Path("config/schwab.local.json"),
    )

    canary_lane = next(lane for lane in runner._lanes if lane.spec.lane_id == "canary_gc_us_early_execution_once")  # noqa: SLF001

    completed_entry_bar = _build_symbol_bar(
        "GC",
        datetime(2026, 3, 20, 10, 40, tzinfo=ZoneInfo("America/New_York")),
        session_asia=False,
        session_us=True,
    )
    _persist_test_intent_fill(
        canary_lane,
        order_intent_id="existing-entry",
        bar=completed_entry_bar,
        intent_type=OrderIntentType.BUY_TO_OPEN,
        created_at=completed_entry_bar.end_ts,
        reason_code=PAPER_EXECUTION_CANARY_ENTRY_REASON,
        broker_order_id="paper-existing-entry",
        fill_price=Decimal("4576.5"),
    )
    completed_exit_bar = _build_symbol_bar(
        "GC",
        datetime(2026, 3, 20, 10, 45, tzinfo=ZoneInfo("America/New_York")),
        session_asia=False,
        session_us=True,
    )
    _persist_test_intent_fill(
        canary_lane,
        order_intent_id="existing-exit",
        bar=completed_exit_bar,
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        created_at=completed_exit_bar.end_ts,
        reason_code=PAPER_EXECUTION_CANARY_EXIT_REASON,
        broker_order_id="paper-existing-exit",
        fill_price=Decimal("4565.5"),
    )

    runner.run(poll_once=True)

    canary_intents = canary_lane.repositories.order_intents.list_all()
    assert [row["reason_code"] for row in canary_intents] == [
        PAPER_EXECUTION_CANARY_ENTRY_REASON,
        PAPER_EXECUTION_CANARY_EXIT_REASON,
        PAPER_EXECUTION_CANARY_ENTRY_REASON,
    ]
    assert [row["intent_type"] for row in canary_intents] == [
        OrderIntentType.BUY_TO_OPEN.value,
        OrderIntentType.SELL_TO_CLOSE.value,
        OrderIntentType.BUY_TO_OPEN.value,
    ]
    assert canary_lane.strategy_engine.state.position_side is PositionSide.FLAT
    assert canary_lane.strategy_engine.state.open_broker_order_id is not None
    assert len(canary_lane.repositories.fills.list_all()) == 2


def test_probationary_paper_execution_canary_force_fires_once_outside_clock_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_override = tmp_path / "paper_canary_override.yaml"
    paper_override.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_execution_canary_enabled: true",
                'probationary_paper_execution_canary_force_fire_once_token: "today_cycle_1"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeLivePollingService:
        def __init__(self, bars):
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            if not self._bars:
                return []
            return [self._bars.pop(0)]

    force_fire_bars = [
        _build_symbol_bar("GC", datetime(2026, 3, 20, 12, 5, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 12, 10, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 12, 15, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 12, 20, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
    ]
    service_by_lane: dict[str, FakeLivePollingService] = {}

    def _fake_build_live_polling_service(settings, repositories, schwab_config_path):
        lane_id = settings.probationary_paper_lane_id
        if lane_id not in service_by_lane:
            service_by_lane[lane_id] = FakeLivePollingService(
                force_fire_bars if lane_id == "canary_gc_us_early_execution_once" else []
            )
        return service_by_lane[lane_id]

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime._build_live_polling_service",
        _fake_build_live_polling_service,
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
        Path("config/schwab.local.json"),
    )

    canary_lane = next(lane for lane in runner._lanes if lane.spec.lane_id == "canary_gc_us_early_execution_once")  # noqa: SLF001

    runner.run(poll_once=True)
    runner.run(poll_once=True)
    mid_lane_status = json.loads((canary_lane.settings.probationary_artifacts_path / "operator_status.json").read_text(encoding="utf-8"))
    runner.run(poll_once=True)
    runner.run(poll_once=True)

    canary_intents = canary_lane.repositories.order_intents.list_all()
    canary_fills = canary_lane.repositories.fills.list_all()
    assert [row["reason_code"] for row in canary_intents] == [
        "paperExecutionCanaryForceFireOnceEntry:today_cycle_1",
        "paperExecutionCanaryForceFireOnceExitNextBar:today_cycle_1",
    ]
    assert [row["intent_type"] for row in canary_intents] == [
        OrderIntentType.BUY_TO_OPEN.value,
        OrderIntentType.SELL_TO_CLOSE.value,
    ]
    assert len(canary_fills) == 2
    assert mid_lane_status["position_side"] == "LONG"
    assert mid_lane_status["canary_force_fire_once_active"] is True
    assert mid_lane_status["canary_force_fire_once_consumed"] is True

    lane_status = json.loads((canary_lane.settings.probationary_artifacts_path / "operator_status.json").read_text(encoding="utf-8"))
    assert lane_status["position_side"] == "FLAT"
    assert lane_status["canary_force_fire_once_active"] is True
    assert lane_status["canary_force_fire_once_consumed"] is True

    with canary_lane.repositories.engine.begin() as connection:
        signal_row = connection.execute(select(signals_table.c.payload_json)).first()
    assert signal_row is not None
    signal_payload = json.loads(signal_row.payload_json)
    assert signal_payload["long_entry_source"] == "paperExecutionCanaryForceFireOnce"

    branch_rows = [
        json.loads(line)
        for line in (canary_lane.settings.probationary_artifacts_path / "branch_sources.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert branch_rows[-1]["source"] == "paperExecutionCanaryForceFireOnce"

    summary = generate_probationary_daily_summary(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
    )
    summary_payload = json.loads(Path(summary.json_path).read_text(encoding="utf-8"))
    with Path(summary.blotter_path).open(encoding="utf-8", newline="") as handle:
        blotter_rows = list(csv.DictReader(handle))
    blotter_text = Path(summary.blotter_path).read_text(encoding="utf-8")
    assert "paperExecutionCanaryForceFireOnceEntry:today_cycle_1" in blotter_text
    assert "paperExecutionCanaryForceFireOnceExitNextBar:today_cycle_1" in blotter_text
    assert summary_payload["realized_net_pnl_scope"] == "ALL_CLOSED_TRADES_FOR_SESSION"
    assert len(summary_payload["closed_trade_digest"]) == 1
    assert summary_payload["closed_trade_digest"][0]["entry_ts"] == blotter_rows[0]["entry_ts"]
    assert summary_payload["closed_trade_digest"][0]["exit_ts"] == blotter_rows[0]["exit_ts"]
    assert summary_payload["closed_trade_digest"][0]["setup_family"] == blotter_rows[0]["setup_family"]
    assert summary_payload["closed_trade_digest"][0]["exit_reason"] == blotter_rows[0]["exit_reason"]
    assert summary_payload["closed_trade_digest"][0]["net_pnl"] == blotter_rows[0]["net_pnl"]
    assert summary_payload["realized_net_pnl"] == blotter_rows[0]["net_pnl"]


def test_probationary_live_shadow_runner_never_submits_and_persists_shadow_intent(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_shadow_test")

    class FakeLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            rows = list(self._bars)
            self._bars = []
            return rows

    class CountingBroker(PaperBroker):
        def __init__(self) -> None:
            super().__init__()
            self.submit_calls = 0

        def submit_order(self, order_intent: OrderIntent) -> str:
            self.submit_calls += 1
            return super().submit_order(order_intent)

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _shadow_broker_truth_snapshot(reconciliation_status="clear")

    broker = CountingBroker()
    execution_engine = ExecutionEngine(broker=broker)
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        shadow_mode_no_submit=True,
    )
    finalized_bar = _build_bar(datetime(2026, 3, 27, 10, 5, tzinfo=ZoneInfo("America/New_York")))
    partial_bar = replace(finalized_bar, bar_id=f"{finalized_bar.bar_id}|partial", is_final=False)
    shadow_intent = OrderIntent(
        order_intent_id=f"{finalized_bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
        bar_id=finalized_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=finalized_bar.end_ts,
        reason_code="shadow_test_long",
    )
    strategy_engine._maybe_create_order_intent = lambda *args, **kwargs: shadow_intent  # type: ignore[method-assign]

    runner = probationary_runtime_module.ProbationaryShadowRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        live_polling_service=FakeLivePollingService([partial_bar, finalized_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    summary = runner.run(poll_once=True)
    live_shadow_summary = json.loads((structured_logger.artifact_dir / "live_shadow_summary_latest.json").read_text(encoding="utf-8"))
    operator_status = json.loads((structured_logger.artifact_dir / "operator_status.json").read_text(encoding="utf-8"))

    assert broker.submit_calls == 0
    assert summary.new_bars == 1
    assert repositories.processed_bars.count() == 1
    assert strategy_engine.state.position_side is PositionSide.FLAT
    assert repositories.order_intents.list_all() == []
    assert repositories.fills.list_all() == []
    assert live_shadow_summary["shadow_submit_suppressed"] is True
    assert live_shadow_summary["latest_shadow_intent"]["intent_type"] == OrderIntentType.BUY_TO_OPEN.value
    assert live_shadow_summary["submit_would_be_allowed_if_shadow_disabled"] is True
    assert operator_status["new_bars_last_cycle"] == 1
    assert operator_status["shadow_mode_no_submit"] is True


def test_probationary_live_shadow_runner_surfaces_broker_truth_blocker_without_position_mutation(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_shadow_test")

    class FakeLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            rows = list(self._bars)
            self._bars = []
            return rows

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _shadow_broker_truth_snapshot(
                reconciliation_status="blocked",
                broker_reachable=True,
                auth_ready=True,
                account_selected=True,
                orders_fresh=True,
                positions_fresh=True,
            )

    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        shadow_mode_no_submit=True,
    )
    finalized_bar = _build_bar(datetime(2026, 3, 27, 10, 10, tzinfo=ZoneInfo("America/New_York")))
    runner = probationary_runtime_module.ProbationaryShadowRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        live_polling_service=FakeLivePollingService([finalized_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    runner.run(poll_once=True)
    live_shadow_summary = json.loads((structured_logger.artifact_dir / "live_shadow_summary_latest.json").read_text(encoding="utf-8"))

    assert live_shadow_summary["current_runtime_phase"] == "RECONCILING"
    assert live_shadow_summary["entries_disabled_blocker"] == "broker_reconciliation_not_clear"
    assert live_shadow_summary["submit_would_be_allowed_if_shadow_disabled"] is False
    assert live_shadow_summary["broker_truth_summary"]["reconciliation_status"] == "blocked"
    assert strategy_engine.state.position_side is PositionSide.FLAT
    assert repositories.fills.list_all() == []


def test_live_strategy_pilot_runner_cannot_enable_accidentally_and_blocks_submit(tmp_path: Path) -> None:
    settings = _build_probationary_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")

    class FakeLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            rows = list(self._bars)
            self._bars = []
            return rows

    class CountingBroker(probationary_runtime_module._LiveTimingValidationBroker):
        def __init__(self) -> None:
            super().__init__()
            self.submit_calls = 0

        def submit_order(self, order_intent: OrderIntent) -> str:
            self.submit_calls += 1
            return super().submit_order(order_intent)

    broker = CountingBroker()
    broker.connect()

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _live_strategy_truth_snapshot_from_broker(broker)

    execution_engine = ExecutionEngine(broker=broker)
    strategy_engine: StrategyEngine | None = None

    def submit_gate(bar: Bar, state, intent: OrderIntent) -> str | None:
        del state
        assert strategy_engine is not None
        return probationary_runtime_module._live_strategy_submit_gate_blocker(
            settings=settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            broker_truth_service=FakeBrokerTruthService(),
            bar=bar,
            intent=intent,
        )

    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        submit_gate_evaluator=submit_gate,
    )
    finalized_bar = _build_bar(datetime(2026, 3, 27, 10, 15, tzinfo=ZoneInfo("America/New_York")))
    forced_intent = OrderIntent(
        order_intent_id=f"{finalized_bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
        bar_id=finalized_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=finalized_bar.end_ts,
        reason_code="live_pilot_test_long",
    )
    strategy_engine._maybe_create_order_intent = lambda *args, **kwargs: forced_intent  # type: ignore[method-assign]

    runner = probationary_runtime_module.ProbationaryLiveStrategyPilotRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=FakeLivePollingService([finalized_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    runner.run(poll_once=True)
    summary = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_summary_latest.json").read_text(encoding="utf-8"))

    assert broker.submit_calls == 0
    assert summary["live_strategy_pilot_enabled"] is False
    assert summary["live_strategy_submit_enabled"] is False
    assert summary["allowed_scope"]["point_value"] == "10"
    assert summary["entries_disabled_blocker"] == "live_strategy_pilot_disabled"
    assert summary["current_strategy_readiness"] is False
    assert repositories.order_intents.list_all() == []
    assert repositories.fills.list_all() == []
    assert strategy_engine.state.position_side is PositionSide.FLAT


def test_live_strategy_pilot_shadow_broker_truth_summary_accepts_auth_healthy_alias(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )
    snapshot = _shadow_broker_truth_snapshot_auth_healthy()

    summary = probationary_runtime_module._shadow_broker_truth_summary(  # noqa: SLF001
        broker_truth_snapshot=snapshot,
        strategy_engine=strategy_engine,
        symbol="MGC",
    )

    assert summary["auth_ready"] is True
    assert summary["classification"] == "SUFFICIENT_BROKER_TRUTH"
    assert summary["blocker"] is None


def test_live_strategy_pilot_shadow_broker_truth_summary_blocks_when_auth_is_missing_or_false(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )
    snapshot = _shadow_broker_truth_snapshot_auth_healthy(auth_ready=False)

    summary = probationary_runtime_module._shadow_broker_truth_summary(  # noqa: SLF001
        broker_truth_snapshot=snapshot,
        strategy_engine=strategy_engine,
        symbol="MGC",
    )

    assert summary["auth_ready"] is False
    assert summary["classification"] == "INSUFFICIENT_TRUTH_RECONCILE"
    assert summary["blocker"] == "auth_not_ready"


def test_live_strategy_pilot_broker_snapshot_state_accepts_auth_healthy_alias(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    broker = object.__new__(LiveStrategyPilotBroker)
    broker._settings = settings  # type: ignore[attr-defined]
    broker._last_snapshot = _shadow_broker_truth_snapshot_auth_healthy()  # type: ignore[attr-defined]

    state = broker.snapshot_state()

    assert state["connected"] is True
    assert state["truth_complete"] is True


def test_live_strategy_pilot_broker_snapshot_state_still_accepts_legacy_auth_key(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    broker = object.__new__(LiveStrategyPilotBroker)
    broker._settings = settings  # type: ignore[attr-defined]
    broker._last_snapshot = _shadow_broker_truth_snapshot()  # type: ignore[attr-defined]

    state = broker.snapshot_state()

    assert state["connected"] is True
    assert state["truth_complete"] is True


def test_live_strategy_signal_observability_summary_matches_persisted_signal_truth(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )

    bar_times = [
        datetime(2026, 3, 27, 10, 0, tzinfo=ZoneInfo("America/New_York")),
        datetime(2026, 3, 27, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        datetime(2026, 3, 27, 10, 10, tzinfo=ZoneInfo("America/New_York")),
        datetime(2026, 3, 27, 10, 15, tzinfo=ZoneInfo("America/New_York")),
        datetime(2026, 3, 27, 10, 20, tzinfo=ZoneInfo("America/New_York")),
    ]
    packets = [
        _blank_signal_packet("placeholder-1"),
        replace(_blank_signal_packet("placeholder-2"), bull_snap_turn_candidate=True),
        replace(_blank_signal_packet("placeholder-3"), asia_reclaim_bar_raw=True, asia_hold_bar_ok=True),
        replace(
            _blank_signal_packet("placeholder-4"),
            midday_pause_resume_long_turn_candidate=True,
            long_entry_raw=True,
            recent_long_setup=True,
        ),
        replace(_blank_signal_packet("placeholder-5"), bear_snap_turn_candidate=True),
    ]

    for bar_time, packet in zip(bar_times, packets, strict=True):
        bar = _build_bar(bar_time)
        repositories.bars.save(bar, created_at=bar.end_ts)
        repositories.signals.save(replace(packet, bar_id=bar.bar_id), created_at=bar.end_ts)
        repositories.processed_bars.mark_processed(bar)

    summary = probationary_runtime_module._build_live_strategy_signal_observability_summary(  # noqa: SLF001
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        observed_at=datetime(2026, 3, 27, 14, 30, tzinfo=ZoneInfo("UTC")),
    )

    assert summary["available"] is True
    assert summary["session_counts"] == {
        "bull_snap_turn_candidate": 1,
        "firstBullSnapTurn": 0,
        "asia_reclaim_bar_raw": 1,
        "asia_hold_bar_ok": 1,
        "asia_acceptance_bar_ok": 0,
        "asiaVWAPLongSignal": 0,
        "bear_snap_turn_candidate": 1,
        "firstBearSnapTurn": 0,
        "longEntryRaw": 1,
        "shortEntryRaw": 0,
        "longEntry": 0,
        "shortEntry": 0,
    }
    assert summary["raw_candidates_seen_vs_final_entries"]["long"] == {
        "raw_candidates_seen": 1,
        "final_entries_produced": 0,
    }
    assert summary["anti_churn"]["recentLongSetup_true_bars"] == 1
    assert summary["anti_churn"]["recentLongSetup_suppressed_bars"] == 1
    assert summary["top_failed_predicates"]["bullSnapLong"][0] == {"predicate": "bull_snap_turn_candidate", "count": 4}
    assert summary["top_failed_predicates"]["asiaVWAPLong"][0] == {"predicate": "asia_reclaim_bar_raw", "count": 4}
    assert summary["top_failed_predicates"]["bearSnapShort"][0] == {"predicate": "bear_snap_turn_candidate", "count": 4}
    assert summary["per_bar_rows"][-1]["barsSinceLongSetup"] == 1
    assert summary["per_bar_rows"][3]["antiChurnLongSuppressed"] is True
    assert "No final entries yet." in summary["why_no_trade_so_far"]


def test_live_strategy_pilot_runner_writes_signal_observability_artifact(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")

    class FakeLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            rows = list(self._bars)
            self._bars = []
            return rows

    broker = probationary_runtime_module._LiveTimingValidationBroker()
    broker.connect()

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _live_strategy_truth_snapshot_from_broker(broker)

    execution_engine = ExecutionEngine(broker=broker)
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )
    finalized_bar = _build_bar(datetime(2026, 3, 27, 10, 30, tzinfo=ZoneInfo("America/New_York")))
    _seed_strategy_warmup(strategy_engine, finalized_bar)
    strategy_engine._evaluate_signals = lambda *args, **kwargs: _blank_signal_packet(finalized_bar.bar_id)  # type: ignore[method-assign]

    runner = probationary_runtime_module.ProbationaryLiveStrategyPilotRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=FakeLivePollingService([finalized_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    runner.run(poll_once=True)
    signal_observability = json.loads(
        (structured_logger.artifact_dir / "live_strategy_signal_observability_latest.json").read_text(encoding="utf-8")
    )
    summary = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_summary_latest.json").read_text(encoding="utf-8"))

    assert signal_observability["available"] is True
    assert signal_observability["signal_packets_session"] == 1
    assert signal_observability["session_counts"]["longEntry"] == 0
    assert signal_observability["per_bar_rows"][0]["why_no_trade"]
    assert signal_observability["per_bar_rows"][0]["bear_snap_location_ok"] is False
    assert summary["signal_observability"]["session_counts"]["bull_snap_turn_candidate"] == 0
    assert summary["signal_observability"]["signal_packets_session"] == 1


def test_live_strategy_pilot_runner_submits_only_after_gates_clear_and_stays_fill_only_until_confirmed(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")

    class FakeLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            rows = list(self._bars)
            self._bars = []
            return rows

    class CountingBroker(probationary_runtime_module._LiveTimingValidationBroker):
        def __init__(self) -> None:
            super().__init__()
            self.submit_calls = 0

        def submit_order(self, order_intent: OrderIntent) -> str:
            self.submit_calls += 1
            return super().submit_order(order_intent)

    broker = CountingBroker()
    broker.connect()

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _live_strategy_truth_snapshot_from_broker(broker)

    execution_engine = ExecutionEngine(broker=broker)
    strategy_engine: StrategyEngine | None = None

    def submit_gate(bar: Bar, state, intent: OrderIntent) -> str | None:
        del state
        assert strategy_engine is not None
        return probationary_runtime_module._live_strategy_submit_gate_blocker(
            settings=settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            broker_truth_service=FakeBrokerTruthService(),
            bar=bar,
            intent=intent,
        )

    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        submit_gate_evaluator=submit_gate,
    )
    finalized_bar = _build_bar(datetime(2026, 3, 27, 10, 20, tzinfo=ZoneInfo("America/New_York")))
    _seed_strategy_warmup(strategy_engine, finalized_bar)
    forced_intent = OrderIntent(
        order_intent_id=f"{finalized_bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
        bar_id=finalized_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=finalized_bar.end_ts,
        reason_code="live_pilot_test_long",
    )
    strategy_engine._maybe_create_order_intent = lambda *args, **kwargs: forced_intent  # type: ignore[method-assign]

    runner = probationary_runtime_module.ProbationaryLiveStrategyPilotRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=FakeLivePollingService([finalized_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    runner.run(poll_once=True)
    summary = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_summary_latest.json").read_text(encoding="utf-8"))
    intent_rows = repositories.order_intents.list_all()

    assert broker.submit_calls == 1
    assert strategy_engine.state.position_side is PositionSide.FLAT
    assert strategy_engine.state.open_broker_order_id is not None
    assert len(intent_rows) == 1
    assert intent_rows[0]["order_status"] == OrderStatus.ACKNOWLEDGED.value
    assert summary["current_strategy_readiness"] is False
    assert summary["pending_stage"] in {"AWAITING_FILL", "AWAITING_ACK"}
    assert summary["latest_live_strategy_intent"]["submit_attempted"] is True
    assert summary["broker_order_id"]


def test_live_strategy_pilot_runner_applies_confirmed_fill_from_broker_truth(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")

    class FakeLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            rows = list(self._bars)
            self._bars = []
            return rows

    class CountingBroker(probationary_runtime_module._LiveTimingValidationBroker):
        def __init__(self) -> None:
            super().__init__()
            self.submit_calls = 0
            self._fill_after_submit = True

        def submit_order(self, order_intent: OrderIntent) -> str:
            self.submit_calls += 1
            broker_order_id = super().submit_order(order_intent)
            if self._fill_after_submit:
                self.restore_live_truth(
                    connected=True,
                    position_quantity=1,
                    average_price=Decimal("100.25"),
                    open_order_ids=[],
                    status_by_order_id={broker_order_id: OrderStatus.FILLED.value},
                    last_fill_timestamp=order_intent.created_at,
                )
                self._forced_order_status[broker_order_id] = OrderStatus.FILLED.value
            return broker_order_id

        def get_order_status(self, broker_order_id: str) -> dict[str, object]:
            payload = super().get_order_status(broker_order_id)
            if payload.get("status") == OrderStatus.FILLED.value:
                payload.update(
                    {
                        "fill_timestamp": "2026-03-27T14:25:00+00:00",
                        "fill_price": "100.25",
                        "raw_payload": {
                            "status": "FILLED",
                            "orderActivityCollection": [
                                {"executionLegs": [{"time": "2026-03-27T14:25:00+00:00", "price": "100.25"}]}
                            ],
                        },
                    }
                )
            return payload

    broker = CountingBroker()
    broker.connect()

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _live_strategy_truth_snapshot_from_broker(broker)

    execution_engine = ExecutionEngine(broker=broker)
    strategy_engine: StrategyEngine | None = None

    def submit_gate(bar: Bar, state, intent: OrderIntent) -> str | None:
        del state
        assert strategy_engine is not None
        return probationary_runtime_module._live_strategy_submit_gate_blocker(
            settings=settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            broker_truth_service=FakeBrokerTruthService(),
            bar=bar,
            intent=intent,
        )

    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        submit_gate_evaluator=submit_gate,
    )
    finalized_bar = _build_bar(datetime(2026, 3, 27, 10, 25, tzinfo=ZoneInfo("America/New_York")))
    _seed_strategy_warmup(strategy_engine, finalized_bar)
    forced_intent = OrderIntent(
        order_intent_id=f"{finalized_bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
        bar_id=finalized_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=finalized_bar.end_ts,
        reason_code="live_pilot_test_long",
    )
    strategy_engine._maybe_create_order_intent = lambda *args, **kwargs: forced_intent  # type: ignore[method-assign]

    runner = probationary_runtime_module.ProbationaryLiveStrategyPilotRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=FakeLivePollingService([finalized_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    runner.run(poll_once=True)
    summary = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_summary_latest.json").read_text(encoding="utf-8"))
    fill_rows = repositories.fills.list_all()
    intent_rows = repositories.order_intents.list_all()

    assert broker.submit_calls == 1
    assert strategy_engine.state.position_side is PositionSide.LONG
    assert strategy_engine.state.internal_position_qty == 1
    assert strategy_engine.state.open_broker_order_id is None
    assert len(fill_rows) == 1
    assert fill_rows[0]["broker_order_id"] == intent_rows[0]["broker_order_id"]
    assert intent_rows[0]["order_status"] == OrderStatus.FILLED.value
    assert summary["broker_fill_at"] == "2026-03-27T14:25:00+00:00"
    assert summary["pending_stage"] == "FILLED_CONFIRMED"


def test_live_strategy_pilot_single_cycle_auto_stops_after_completed_entry_exit_cycle(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path).model_copy(
        update={
            "live_poll_interval_seconds": 1,
            "live_strategy_pilot_single_cycle_mode": True,
        }
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")

    class SequencedLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            if not self._bars:
                return []
            return [self._bars.pop(0)]

    class AutoFillBroker(probationary_runtime_module._LiveTimingValidationBroker):
        def __init__(self) -> None:
            super().__init__()
            self.submit_calls = 0
            self._fill_details: dict[str, dict[str, object]] = {}

        def submit_order(self, order_intent: OrderIntent) -> str:
            self.submit_calls += 1
            broker_order_id = super().submit_order(order_intent)
            if order_intent.intent_type is OrderIntentType.BUY_TO_OPEN:
                fill_price = Decimal("100.25")
                position_quantity = 1
            else:
                fill_price = Decimal("100.75")
                position_quantity = 0
            self.restore_live_truth(
                connected=True,
                position_quantity=position_quantity,
                average_price=fill_price if position_quantity != 0 else None,
                open_order_ids=[],
                status_by_order_id={broker_order_id: OrderStatus.FILLED.value},
                last_fill_timestamp=order_intent.created_at,
            )
            self._forced_order_status[broker_order_id] = OrderStatus.FILLED.value
            self._fill_details[broker_order_id] = {
                "fill_timestamp": order_intent.created_at.isoformat(),
                "fill_price": str(fill_price),
            }
            return broker_order_id

        def get_order_status(self, broker_order_id: str) -> dict[str, object]:
            payload = super().get_order_status(broker_order_id)
            payload.update(self._fill_details.get(broker_order_id, {}))
            return payload

    broker = AutoFillBroker()
    broker.connect()

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _live_strategy_truth_snapshot_from_broker(broker)

    execution_engine = ExecutionEngine(broker=broker)
    strategy_engine: StrategyEngine | None = None

    def submit_gate(bar: Bar, state, intent: OrderIntent) -> str | None:
        del state
        assert strategy_engine is not None
        return probationary_runtime_module._live_strategy_submit_gate_blocker(
            settings=settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            broker_truth_service=FakeBrokerTruthService(),
            bar=bar,
            intent=intent,
        )

    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        submit_gate_evaluator=submit_gate,
    )
    strategy_engine._resolve_long_entry_family = lambda *args, **kwargs: LongEntryFamily.K  # type: ignore[method-assign]
    entry_bar = _build_bar(datetime(2026, 3, 27, 10, 30, tzinfo=ZoneInfo("America/New_York")))
    exit_bar = _build_bar(datetime(2026, 3, 27, 10, 35, tzinfo=ZoneInfo("America/New_York")))
    blocked_second_entry_bar = _build_bar(datetime(2026, 3, 27, 10, 40, tzinfo=ZoneInfo("America/New_York")))
    _seed_strategy_warmup(strategy_engine, entry_bar)
    intent_queue = [
        OrderIntent(
            order_intent_id=f"{entry_bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
            bar_id=entry_bar.bar_id,
            symbol="MGC",
            intent_type=OrderIntentType.BUY_TO_OPEN,
            quantity=1,
            created_at=entry_bar.end_ts,
            reason_code="live_pilot_test_long",
        ),
        OrderIntent(
            order_intent_id=f"{exit_bar.bar_id}|{OrderIntentType.SELL_TO_CLOSE.value}",
            bar_id=exit_bar.bar_id,
            symbol="MGC",
            intent_type=OrderIntentType.SELL_TO_CLOSE,
            quantity=1,
            created_at=exit_bar.end_ts,
            reason_code="LONG_TIME_EXIT",
        ),
        OrderIntent(
            order_intent_id=f"{blocked_second_entry_bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
            bar_id=blocked_second_entry_bar.bar_id,
            symbol="MGC",
            intent_type=OrderIntentType.BUY_TO_OPEN,
            quantity=1,
            created_at=blocked_second_entry_bar.end_ts,
            reason_code="blocked_second_cycle_long",
        ),
    ]
    strategy_engine._maybe_create_order_intent = lambda *args, **kwargs: intent_queue.pop(0) if intent_queue else None  # type: ignore[method-assign]
    strategy_engine._last_exit_decision_summary = {  # noqa: SLF001
        "primary_reason": "LONG_TIME_EXIT",
        "all_true_reasons": ["LONG_TIME_EXIT"],
    }

    runner = probationary_runtime_module.ProbationaryLiveStrategyPilotRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=SequencedLivePollingService([entry_bar, exit_bar, blocked_second_entry_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    result = runner.run(max_cycles=5)
    summary = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_summary_latest.json").read_text(encoding="utf-8"))
    cycle = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_cycle_latest.json").read_text(encoding="utf-8"))

    assert result.stop_reason == "pilot_completed"
    assert broker.submit_calls == 2
    assert strategy_engine.state.position_side is PositionSide.FLAT
    assert strategy_engine.state.open_broker_order_id is None
    assert len(repositories.fills.list_all()) == 2
    assert cycle["pilot_armed"] is False
    assert cycle["rearm_required"] is True
    assert cycle["cycle_status"] == "completed"
    assert cycle["remaining_allowed_live_submits"] == 0
    assert cycle["entry"]["intent_type"] == OrderIntentType.BUY_TO_OPEN.value
    assert cycle["exit"]["intent_type"] == OrderIntentType.SELL_TO_CLOSE.value
    assert cycle["final_result"] == "completed"
    assert cycle["flat_restore_confirmation_time"] is not None
    assert summary["pilot_armed"] is False
    assert summary["cycle_status"] == "completed"
    assert summary["submit_currently_enabled"] is False


def test_live_strategy_pilot_reconciled_cycle_blocks_future_submit_until_rearm(tmp_path: Path) -> None:
    settings = _build_live_strategy_pilot_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_live_strategy_pilot_test")

    class FakeLivePollingService:
        def __init__(self, bars: list[Bar]) -> None:
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            rows = list(self._bars)
            self._bars = []
            return rows

    class CountingBroker(probationary_runtime_module._LiveTimingValidationBroker):
        def __init__(self) -> None:
            super().__init__()
            self.submit_calls = 0

        def submit_order(self, order_intent: OrderIntent) -> str:
            self.submit_calls += 1
            return super().submit_order(order_intent)

    broker = CountingBroker()
    broker.connect()

    class FakeBrokerTruthService:
        def snapshot(self, *, force_refresh: bool = False) -> dict[str, object]:
            return _live_strategy_truth_snapshot_from_broker(broker)

    cycle_payload = probationary_runtime_module._default_live_strategy_pilot_cycle_state(settings=settings)  # noqa: SLF001
    cycle_payload.update(
        {
            "pilot_armed": False,
            "rearm_required": True,
            "submit_enabled": False,
            "cycle_status": "reconciled",
            "remaining_allowed_live_submits": 0,
            "final_result": "reconciled",
            "blocker": "live_strategy_pilot_reconcile_review_required",
            "reconcile_fault_reason": "fill_timeout_escalated",
        }
    )
    probationary_runtime_module._live_strategy_pilot_cycle_path(settings).write_text(  # noqa: SLF001
        json.dumps(cycle_payload, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    execution_engine = ExecutionEngine(broker=broker)
    strategy_engine: StrategyEngine | None = None

    def submit_gate(bar: Bar, state, intent: OrderIntent) -> str | None:
        del state
        assert strategy_engine is not None
        return probationary_runtime_module._live_strategy_submit_gate_blocker(
            settings=settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            broker_truth_service=FakeBrokerTruthService(),
            bar=bar,
            intent=intent,
        )

    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        submit_gate_evaluator=submit_gate,
    )
    finalized_bar = _build_bar(datetime(2026, 3, 27, 10, 45, tzinfo=ZoneInfo("America/New_York")))
    _seed_strategy_warmup(strategy_engine, finalized_bar)
    forced_intent = OrderIntent(
        order_intent_id=f"{finalized_bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
        bar_id=finalized_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=finalized_bar.end_ts,
        reason_code="live_pilot_test_long",
    )
    strategy_engine._maybe_create_order_intent = lambda *args, **kwargs: forced_intent  # type: ignore[method-assign]

    runner = probationary_runtime_module.ProbationaryLiveStrategyPilotRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=FakeLivePollingService([finalized_bar]),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=FakeBrokerTruthService(),
    )

    runner.run(poll_once=True)
    summary = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_summary_latest.json").read_text(encoding="utf-8"))
    cycle = json.loads((structured_logger.artifact_dir / "live_strategy_pilot_cycle_latest.json").read_text(encoding="utf-8"))

    assert broker.submit_calls == 0
    assert summary["entries_disabled_blocker"] == "live_strategy_pilot_reconcile_review_required"
    assert summary["pilot_armed"] is False
    assert summary["submit_currently_enabled"] is False
    assert cycle["cycle_status"] == "reconciled"
    assert cycle["rearm_required"] is True


def test_probationary_paper_daily_summary_uses_runtime_config_in_force_for_canary_lane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_override_with_canary = tmp_path / "paper_canary_enabled.yaml"
    paper_override_with_canary.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_execution_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    paper_override_without_canary = tmp_path / "paper_canary_disabled.yaml"
    paper_override_without_canary.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeLivePollingService:
        def __init__(self, bars):
            self._bars = list(bars)

        def poll_bars(self, *args, **kwargs):
            if not self._bars:
                return []
            return [self._bars.pop(0)]

    canary_bars = [
        _build_symbol_bar("GC", datetime(2026, 3, 20, 10, 35, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 10, 40, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 15, 45, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
        _build_symbol_bar("GC", datetime(2026, 3, 20, 15, 50, tzinfo=ZoneInfo("America/New_York")), session_asia=False, session_us=True),
    ]
    service_by_lane: dict[str, FakeLivePollingService] = {}

    def _fake_build_live_polling_service(settings, repositories, schwab_config_path):
        lane_id = settings.probationary_paper_lane_id
        if lane_id not in service_by_lane:
            service_by_lane[lane_id] = FakeLivePollingService(canary_bars if lane_id == "canary_gc_us_early_execution_once" else [])
        return service_by_lane[lane_id]

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime._build_live_polling_service",
        _fake_build_live_polling_service,
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override_with_canary,
        ],
        Path("config/schwab.local.json"),
    )
    runner.run(poll_once=True)
    runner.run(poll_once=True)
    runner.run(poll_once=True)
    runner.run(poll_once=True)

    summary = generate_probationary_daily_summary(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override_without_canary,
        ]
    )

    summary_payload = json.loads(Path(summary.json_path).read_text(encoding="utf-8"))
    blotter_text = Path(summary.blotter_path).read_text(encoding="utf-8")
    assert summary_payload["closed_trade_count"] == 1
    assert "paperExecutionCanaryEntryLateWindow" in blotter_text
    assert "paperExecutionCanaryExitNextBarLateWindow" in blotter_text


def test_probationary_paper_daily_summary_trade_digest_matches_blotter_and_total_for_multiple_canary_trades(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_override = tmp_path / "paper_canary_enabled.yaml"
    paper_override.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"',
                "probationary_paper_execution_canary_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime._build_live_polling_service",
        lambda settings, repositories, schwab_config_path: SimpleNamespace(poll_bars=lambda *args, **kwargs: []),
    )

    runner = build_probationary_paper_runner(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
        Path("config/schwab.local.json"),
    )
    canary_lane = next(lane for lane in runner._lanes if lane.spec.lane_id == "canary_gc_us_early_execution_once")  # noqa: SLF001

    first_entry_bar = _build_symbol_bar(
        "GC",
        datetime(2026, 3, 20, 10, 40, tzinfo=ZoneInfo("America/New_York")),
        session_asia=False,
        session_us=True,
    )
    first_exit_bar = _build_symbol_bar(
        "GC",
        datetime(2026, 3, 20, 10, 45, tzinfo=ZoneInfo("America/New_York")),
        session_asia=False,
        session_us=True,
    )
    second_entry_bar = _build_symbol_bar(
        "GC",
        datetime(2026, 3, 20, 11, 5, tzinfo=ZoneInfo("America/New_York")),
        session_asia=False,
        session_us=True,
    )
    second_exit_bar = _build_symbol_bar(
        "GC",
        datetime(2026, 3, 20, 11, 15, tzinfo=ZoneInfo("America/New_York")),
        session_asia=False,
        session_us=True,
    )

    _persist_test_intent_fill(
        canary_lane,
        order_intent_id="multi-entry-1",
        bar=first_entry_bar,
        intent_type=OrderIntentType.BUY_TO_OPEN,
        created_at=first_entry_bar.end_ts,
        reason_code="paperExecutionCanaryEntryLateWindow",
        broker_order_id="paper-multi-entry-1",
        fill_price=Decimal("4576.5"),
    )
    _persist_test_intent_fill(
        canary_lane,
        order_intent_id="multi-exit-1",
        bar=first_exit_bar,
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        created_at=first_exit_bar.end_ts,
        reason_code="paperExecutionCanaryExitNextBarLateWindow",
        broker_order_id="paper-multi-exit-1",
        fill_price=Decimal("4565.5"),
    )
    _persist_test_intent_fill(
        canary_lane,
        order_intent_id="multi-entry-2",
        bar=second_entry_bar,
        intent_type=OrderIntentType.BUY_TO_OPEN,
        created_at=second_entry_bar.end_ts,
        reason_code="paperExecutionCanaryEntryLateWindow",
        broker_order_id="paper-multi-entry-2",
        fill_price=Decimal("4566.0"),
    )
    _persist_test_intent_fill(
        canary_lane,
        order_intent_id="multi-exit-2",
        bar=second_exit_bar,
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        created_at=second_exit_bar.end_ts,
        reason_code="LONG_STOP",
        broker_order_id="paper-multi-exit-2",
        fill_price=Decimal("4563.6"),
    )

    summary = generate_probationary_daily_summary(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            paper_override,
        ],
        session_date=date(2026, 3, 20),
    )

    summary_payload = json.loads(Path(summary.json_path).read_text(encoding="utf-8"))
    with Path(summary.blotter_path).open(encoding="utf-8", newline="") as handle:
        blotter_rows = list(csv.DictReader(handle))

    assert summary_payload["realized_net_pnl_scope"] == "ALL_CLOSED_TRADES_FOR_SESSION"
    assert summary_payload["closed_trade_count"] == 2
    assert len(summary_payload["closed_trade_digest"]) == 2
    assert len(blotter_rows) == 2
    assert summary_payload["realized_net_pnl"] == str(
        sum(Decimal(row["net_pnl"]) for row in blotter_rows)
    )
    for digest_row, blotter_row in zip(summary_payload["closed_trade_digest"], blotter_rows):
        assert digest_row["trade_id"] == int(blotter_row["trade_id"])
        assert digest_row["entry_ts"] == blotter_row["entry_ts"]
        assert digest_row["exit_ts"] == blotter_row["exit_ts"]
        assert digest_row["setup_family"] == blotter_row["setup_family"]
        assert digest_row["exit_reason"] == blotter_row["exit_reason"]
        assert digest_row["net_pnl"] == blotter_row["net_pnl"]


def test_pl_lane_only_allows_us_late_entries(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    spec = next(spec for spec in _load_probationary_paper_lane_specs(settings) if spec.lane_id == "pl_us_late_pause_resume_long")
    lane_settings = _build_probationary_paper_lane_settings(settings, spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    structured_logger = StructuredLogger(lane_settings.probationary_artifacts_path)
    engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    bar = Bar(
        bar_id="PL|5m|2026-03-18T15:00:00-04:00",
        symbol="PL",
        timeframe="5m",
        start_ts=datetime(2026, 3, 18, 10, 55, tzinfo=ZoneInfo("America/New_York")),
        end_ts=datetime(2026, 3, 18, 11, 0, tzinfo=ZoneInfo("America/New_York")),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=1,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )
    packet = replace(_blank_signal_packet(bar.bar_id), long_entry=True, long_entry_source="usLatePauseResumeLongTurn")

    controlled = engine._apply_runtime_entry_controls(bar, packet)  # noqa: SLF001

    assert controlled.long_entry is False
    payload = json.loads((lane_settings.probationary_artifacts_path / "rule_blocks.jsonl").read_text(encoding="utf-8").strip())
    assert payload["block_reason"] == "probationary_session_restriction_us_late"


def test_gc_asia_lane_only_allows_asia_early_entries(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    spec = next(
        spec
        for spec in _load_probationary_paper_lane_specs(settings)
        if spec.lane_id == "gc_asia_early_normal_breakout_retest_hold_long"
    )
    lane_settings = _build_probationary_paper_lane_settings(settings, spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    structured_logger = StructuredLogger(lane_settings.probationary_artifacts_path)
    engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    bar = Bar(
        bar_id="GC|5m|2026-03-18T19:00:00-04:00",
        symbol="GC",
        timeframe="5m",
        start_ts=datetime(2026, 3, 18, 14, 55, tzinfo=ZoneInfo("America/New_York")),
        end_ts=datetime(2026, 3, 18, 15, 0, tzinfo=ZoneInfo("America/New_York")),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=1,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )
    packet = replace(
        _blank_signal_packet(bar.bar_id),
        long_entry=True,
        long_entry_source="asiaEarlyNormalBreakoutRetestHoldTurn",
    )

    controlled = engine._apply_runtime_entry_controls(bar, packet)  # noqa: SLF001

    assert controlled.long_entry is False
    payload = json.loads((lane_settings.probationary_artifacts_path / "rule_blocks.jsonl").read_text(encoding="utf-8").strip())
    assert payload["block_reason"] == "probationary_session_restriction_asia_early"


def test_gc_asia_lane_allows_first_three_london_open_entries(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    spec = next(
        spec
        for spec in _load_probationary_paper_lane_specs(settings)
        if spec.lane_id == "gc_asia_early_normal_breakout_retest_hold_long"
    )
    lane_settings = _build_probationary_paper_lane_settings(settings, spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    structured_logger = StructuredLogger(lane_settings.probationary_artifacts_path)
    engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    allowed_bar = Bar(
        bar_id="GC|5m|2026-03-24T07:10:00+00:00",
        symbol="GC",
        timeframe="5m",
        start_ts=datetime(2026, 3, 24, 3, 5, tzinfo=ZoneInfo("America/New_York")),
        end_ts=datetime(2026, 3, 24, 3, 10, tzinfo=ZoneInfo("America/New_York")),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=1,
        is_final=True,
        session_asia=False,
        session_london=True,
        session_us=False,
        session_allowed=True,
    )
    packet = replace(
        _blank_signal_packet(allowed_bar.bar_id),
        long_entry=True,
        long_entry_source="asiaEarlyNormalBreakoutRetestHoldTurn",
    )

    controlled = engine._apply_runtime_entry_controls(allowed_bar, packet)  # noqa: SLF001

    assert controlled.long_entry is True

    blocked_bar = replace(
        allowed_bar,
        bar_id="GC|5m|2026-03-24T07:20:00+00:00",
        start_ts=datetime(2026, 3, 24, 3, 15, tzinfo=ZoneInfo("America/New_York")),
        end_ts=datetime(2026, 3, 24, 3, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    blocked = engine._apply_runtime_entry_controls(blocked_bar, packet)  # noqa: SLF001

    assert blocked.long_entry is False


def test_active_probationary_lane_specs_merge_current_configured_lanes_with_stale_runtime_file(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    runtime_dir = settings.probationary_artifacts_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                        "short_sources": [],
                        "session_restriction": "US_LATE",
                        "point_value": "10",
                        "trade_size": 1,
                        "catastrophic_open_loss": "-500",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    specs = _active_probationary_paper_lane_specs(settings)
    lane_ids = {spec.lane_id for spec in specs}

    assert "mgc_us_late_pause_resume_long" in lane_ids
    assert "breakout_metals_us_unknown_continuation__GC" in lane_ids
    assert "breakout_metals_us_unknown_continuation__MGC" in lane_ids


def _seed_test_lane(
    tmp_path: Path,
    *,
    lane_id: str,
    symbol: str,
    source: str,
    session_restriction: str,
    point_value: Decimal,
    lane_mode: str = "STANDARD",
) -> SimpleNamespace:
    settings = _build_probationary_paper_settings(tmp_path).model_copy(
        update={
            "symbol": symbol,
            "database_url": f"sqlite:///{tmp_path / f'{lane_id}.sqlite3'}",
            "probationary_artifacts_dir": str(tmp_path / lane_id),
            "probationary_paper_lane_id": lane_id,
            "probationary_paper_lane_display_name": lane_id,
            "probationary_paper_lane_session_restriction": session_restriction,
            "enable_us_late_pause_resume_longs": source == "usLatePauseResumeLongTurn",
            "enable_asia_early_normal_breakout_retest_hold_longs": source == "asiaEarlyNormalBreakoutRetestHoldTurn",
            "enable_asia_early_pause_resume_shorts": source == "asiaEarlyPauseResumeShortTurn",
        }
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    execution_engine = ExecutionEngine(broker=PaperBroker())
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    return SimpleNamespace(
        spec=SimpleNamespace(
            lane_id=lane_id,
            display_name=lane_id,
            symbol=symbol,
            session_restriction=session_restriction,
            catastrophic_open_loss=Decimal("-1000"),
            lane_mode=lane_mode,
        ),
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        point_value=point_value,
    )


def _build_symbol_bar(
    symbol: str,
    end_ts: datetime,
    *,
    session_asia: bool,
    session_us: bool,
) -> Bar:
    return Bar(
        bar_id=f"{symbol}|5m|{end_ts.astimezone(ZoneInfo('UTC')).isoformat()}",
        symbol=symbol,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=1,
        is_final=True,
        session_asia=session_asia,
        session_london=False,
        session_us=session_us,
        session_allowed=True,
    )


def _persist_test_intent_fill(
    lane: SimpleNamespace,
    *,
    order_intent_id: str,
    bar: Bar,
    intent_type: OrderIntentType,
    created_at: datetime,
    reason_code: str,
    broker_order_id: str,
    fill_price: Decimal,
) -> None:
    intent = OrderIntent(
        order_intent_id=order_intent_id,
        bar_id=bar.bar_id,
        symbol=lane.spec.symbol,
        intent_type=intent_type,
        quantity=1,
        created_at=created_at,
        reason_code=reason_code,
    )
    lane.repositories.order_intents.save(intent, order_status=OrderStatus.FILLED, broker_order_id=broker_order_id)
    lane.repositories.fills.save(
        FillEvent(
            order_intent_id=order_intent_id,
            intent_type=intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=created_at,
            fill_price=fill_price,
            broker_order_id=broker_order_id,
        )
    )


def test_desk_halt_threshold_blocks_new_entries_without_flattening(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path).model_copy(
        update={"probationary_paper_desk_halt_new_entries_loss": Decimal("-10")}
    )
    lane = _seed_test_lane(
        tmp_path,
        lane_id="pl_lane",
        symbol="PL",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("50"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-03-19")
    metrics = {
        "pl_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-12"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-12"),
            closed_trades=1,
            losing_closed_trades=1,
            intent_count=1,
            fill_count=1,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        )
    }

    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=StructuredLogger(tmp_path),
        alert_dispatcher=AlertDispatcher(StructuredLogger(tmp_path)),
    )

    assert updated_state.desk_halt_new_entries_triggered is True
    assert updated_state.desk_flatten_and_halt_triggered is False
    assert lane.strategy_engine.state.operator_halt is True
    assert lane.execution_engine.pending_executions() == []


def test_desk_flatten_and_halt_threshold_submits_flatten(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path).model_copy(
        update={"probationary_paper_desk_flatten_and_halt_loss": Decimal("-20")}
    )
    lane = _seed_test_lane(
        tmp_path,
        lane_id="pl_lane",
        symbol="PL",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("50"),
    )
    lane.strategy_engine._state = replace(  # noqa: SLF001
        lane.strategy_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        broker_position_qty=1,
        internal_position_qty=1,
        entry_price=Decimal("100"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-03-19")
    metrics = {
        "pl_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-5"),
            unrealized_pnl=Decimal("-20"),
            total_pnl=Decimal("-25"),
            closed_trades=1,
            losing_closed_trades=1,
            intent_count=1,
            fill_count=1,
            open_order_count=0,
            position_side="LONG",
            internal_position_qty=1,
            broker_position_qty=1,
            entry_price=Decimal("100"),
            last_mark=Decimal("99.6"),
            last_processed_bar_end_ts=None,
        )
    }

    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=StructuredLogger(tmp_path),
        alert_dispatcher=AlertDispatcher(StructuredLogger(tmp_path)),
    )

    assert updated_state.desk_flatten_and_halt_triggered is True
    assert lane.strategy_engine.state.operator_halt is True
    assert len(lane.execution_engine.pending_executions()) == 1


def test_lane_catastrophic_cap_flattens_and_halts_only_that_lane(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    risky_lane = _seed_test_lane(
        tmp_path,
        lane_id="gc_lane",
        symbol="GC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("100"),
    )
    safe_lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_lane",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    risky_lane.spec.catastrophic_open_loss = Decimal("-750")  # type: ignore[attr-defined]
    risky_lane.strategy_engine._state = replace(  # noqa: SLF001
        risky_lane.strategy_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        broker_position_qty=1,
        internal_position_qty=1,
        entry_price=Decimal("3000"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-03-19")
    metrics = {
        "gc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("-800"),
            total_pnl=Decimal("-800"),
            closed_trades=0,
            losing_closed_trades=0,
            intent_count=0,
            fill_count=0,
            open_order_count=0,
            position_side="LONG",
            internal_position_qty=1,
            broker_position_qty=1,
            entry_price=Decimal("3000"),
            last_mark=Decimal("2992"),
            last_processed_bar_end_ts=None,
        ),
        "mgc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("0"),
            closed_trades=0,
            losing_closed_trades=0,
            intent_count=0,
            fill_count=0,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        ),
    }

    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[risky_lane, safe_lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=StructuredLogger(tmp_path),
        alert_dispatcher=AlertDispatcher(StructuredLogger(tmp_path)),
    )

    assert updated_state.lane_states["gc_lane"]["risk_state"] == "HALTED_CATASTROPHIC"
    assert risky_lane.strategy_engine.state.operator_halt is True
    assert len(risky_lane.execution_engine.pending_executions()) == 1
    assert safe_lane.execution_engine.pending_executions() == []


def test_lane_two_loser_rule_halts_lane(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_lane",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-03-19")
    metrics = {
        "mgc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-30"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-30"),
            closed_trades=2,
            losing_closed_trades=2,
            intent_count=2,
            fill_count=2,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        )
    }

    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=StructuredLogger(tmp_path),
        alert_dispatcher=AlertDispatcher(StructuredLogger(tmp_path)),
    )

    assert updated_state.lane_states["mgc_lane"]["risk_state"] == "HALTED_DEGRADATION"
    assert updated_state.lane_states["mgc_lane"]["halt_reason"] == "lane_realized_loser_limit_per_session"
    assert lane.strategy_engine.state.operator_halt is True


def test_probationary_paper_observation_mode_disables_loss_halts(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path).model_copy(
        update={
            "probationary_paper_disable_loss_halts": True,
            "probationary_paper_desk_halt_new_entries_loss": Decimal("-10"),
            "probationary_paper_desk_flatten_and_halt_loss": Decimal("-20"),
        }
    )
    lane = _seed_test_lane(
        tmp_path,
        lane_id="gc_lane",
        symbol="GC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("100"),
    )
    lane.spec.catastrophic_open_loss = Decimal("-750")  # type: ignore[attr-defined]
    lane.strategy_engine._state = replace(  # noqa: SLF001
        lane.strategy_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        broker_position_qty=1,
        internal_position_qty=1,
        entry_price=Decimal("3000"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        desk_halt_new_entries_triggered=True,
        desk_flatten_and_halt_triggered=True,
        desk_last_trigger_reason="desk_flatten_and_halt_loss",
        lane_states={
            "gc_lane": {
                "catastrophic_triggered": True,
                "warning_triggered": True,
                "degradation_triggered": True,
                "risk_state": "HALTED_CATASTROPHIC",
                "halt_reason": "lane_catastrophic_open_loss_cap",
                "unblock_action": "Manual inspection required",
            }
        },
    )
    metrics = {
        "gc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-25"),
            unrealized_pnl=Decimal("-800"),
            total_pnl=Decimal("-825"),
            closed_trades=2,
            losing_closed_trades=2,
            intent_count=2,
            fill_count=2,
            open_order_count=0,
            position_side="LONG",
            internal_position_qty=1,
            broker_position_qty=1,
            open_entry_leg_count=1,
            open_add_count=0,
            additional_entry_allowed=False,
            entry_price=Decimal("3000"),
            last_mark=Decimal("2992"),
            last_processed_bar_end_ts=None,
        )
    }

    updated_state, risk_events = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=StructuredLogger(tmp_path / "root"),
        alert_dispatcher=AlertDispatcher(StructuredLogger(tmp_path / "root")),
    )

    assert updated_state.desk_halt_new_entries_triggered is False
    assert updated_state.desk_flatten_and_halt_triggered is False
    assert updated_state.desk_last_trigger_reason is None
    assert updated_state.lane_states["gc_lane"]["risk_state"] == "OK"
    assert updated_state.lane_states["gc_lane"]["halt_reason"] is None
    assert updated_state.lane_states["gc_lane"]["unblock_action"] == "Paper observation mode: loss halts disabled"
    assert risk_events == []
    assert lane.execution_engine.pending_executions() == []


def test_probationary_paper_risk_state_auto_clears_session_scoped_lane_halts_on_session_rollover() -> None:
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        desk_halt_new_entries_triggered=True,
        desk_last_trigger_reason="desk_halt_new_entries_loss",
        lane_states={
            "gc_lane": {
                "degradation_triggered": True,
                "risk_state": "HALTED_DEGRADATION",
                "halt_reason": "lane_realized_loser_limit_per_session",
                "unblock_action": "Next session reset auto-clear",
            }
        },
    )

    rolled = _ensure_probationary_paper_risk_state_session(risk_state, date(2026, 3, 20))

    assert rolled.session_date == "2026-03-20"
    assert rolled.desk_halt_new_entries_triggered is True
    assert rolled.desk_last_trigger_reason == "desk_halt_new_entries_loss"
    assert rolled.lane_states["gc_lane"]["degradation_triggered"] is False
    assert rolled.lane_states["gc_lane"]["risk_state"] == "OK"
    assert rolled.lane_states["gc_lane"]["halt_reason"] is None
    assert rolled.lane_states["gc_lane"]["last_cleared_action"] == "session_reset_auto_clear"
    assert rolled.lane_states["gc_lane"]["session_reset_auto_cleared"] is True


def test_probationary_paper_canary_restore_startup_clears_stale_operator_halt_when_flat(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings_with_canary(tmp_path)
    spec = next(spec for spec in _load_probationary_paper_lane_specs(settings) if spec.lane_id == "canary_gc_us_early_execution_once")
    lane_settings = _build_probationary_paper_lane_settings(settings, spec)
    repositories = RepositorySet(build_engine(lane_settings.database_url))
    structured_logger = StructuredLogger(lane_settings.probationary_artifacts_path)
    seeded_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(broker=PaperBroker()),
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )
    seeded_engine.set_operator_halt(datetime(2026, 3, 20, 11, 30, tzinfo=ZoneInfo("UTC")), True)

    restart_execution_engine = ExecutionEngine(broker=PaperBroker())
    restart_engine = StrategyEngine(
        settings=lane_settings,
        repositories=repositories,
        execution_engine=restart_execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=AlertDispatcher(structured_logger),
    )

    class FakeLivePollingService:
        def poll_bars(self, *args, **kwargs):
            return []

    lane_runtime = ProbationaryPaperLaneRuntime(
        spec=spec,
        settings=lane_settings,
        repositories=repositories,
        strategy_engine=restart_engine,
        execution_engine=restart_execution_engine,
        live_polling_service=FakeLivePollingService(),
        structured_logger=ProbationaryLaneStructuredLogger(
            lane_id=spec.lane_id,
            symbol=spec.symbol,
            root_logger=StructuredLogger(tmp_path / "root"),
            lane_logger=structured_logger,
        ),
        alert_dispatcher=AlertDispatcher(structured_logger),
    )

    startup_fault = lane_runtime.restore_startup()

    assert startup_fault is None
    assert lane_runtime.strategy_engine.state.operator_halt is False
    assert lane_runtime.strategy_engine.state.entries_enabled is True


def test_probationary_paper_canary_ignores_realized_loser_limit_halt(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    canary_lane = _seed_test_lane(
        tmp_path,
        lane_id="canary_gc_lane",
        symbol="GC",
        source="",
        session_restriction="US_EARLY_OBSERVATION",
        point_value=Decimal("100"),
        lane_mode=PAPER_EXECUTION_CANARY_MODE,
    )
    canary_lane.strategy_engine.set_operator_halt(datetime(2026, 3, 20, 11, 30, tzinfo=ZoneInfo("UTC")), True)
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-20",
        lane_states={
            "canary_gc_lane": {
                "degradation_triggered": True,
                "risk_state": "HALTED_DEGRADATION",
                "halt_reason": "lane_realized_loser_limit_per_session",
                "unblock_action": "Next session reset auto-clear",
            }
        },
    )
    metrics = {
        "canary_gc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-20",
            realized_pnl=Decimal("-450"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-450"),
            closed_trades=3,
            losing_closed_trades=2,
            intent_count=6,
            fill_count=6,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        )
    }

    updated_state, risk_events = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[canary_lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=StructuredLogger(tmp_path / "root"),
        alert_dispatcher=AlertDispatcher(StructuredLogger(tmp_path / "root")),
    )

    assert updated_state.lane_states["canary_gc_lane"]["degradation_triggered"] is False
    assert updated_state.lane_states["canary_gc_lane"]["risk_state"] == "OK"
    assert updated_state.lane_states["canary_gc_lane"]["halt_reason"] is None
    assert canary_lane.strategy_engine.state.operator_halt is False
    assert risk_events == []


def test_clear_risk_halts_requires_manual_resume_after_serious_halt(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    lane = _seed_test_lane(
        tmp_path,
        lane_id="gc_lane",
        symbol="GC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("100"),
    )
    lane.strategy_engine.set_operator_halt(datetime(2026, 3, 19, 8, 0, tzinfo=ZoneInfo("UTC")), True)
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        desk_halt_new_entries_triggered=True,
        desk_last_trigger_reason="desk_halt_new_entries_loss",
        lane_states={
            "gc_lane": {
                "catastrophic_triggered": True,
                "risk_state": "HALTED_CATASTROPHIC",
                "halt_reason": "lane_catastrophic_open_loss_cap",
                "unblock_action": "Clear Risk Halts, then Resume Entries",
            }
        },
    )
    control_path = settings.probationary_artifacts_path / "runtime" / "operator_control.json"
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text(
        json.dumps(
            {
                "action": "clear_risk_halts",
                "status": "pending",
                "requested_at": "2026-03-19T08:01:00+00:00",
                "command_id": "clear-risk-1",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    cleared = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
        risk_state=risk_state,
    )

    assert cleared is not None
    assert cleared["status"] == "applied"
    assert cleared["requires_resume_entries"] is True
    assert risk_state.desk_halt_new_entries_triggered is False
    assert risk_state.lane_states["gc_lane"]["catastrophic_triggered"] is False
    assert risk_state.lane_states["gc_lane"]["unblock_action"] == "Resume Entries"
    assert lane.strategy_engine.state.operator_halt is True

    control_path.write_text(
        json.dumps(
            {
                "action": "resume_entries",
                "status": "pending",
                "requested_at": "2026-03-19T08:02:00+00:00",
                "command_id": "resume-1",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    resumed = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
        risk_state=risk_state,
    )

    assert resumed is not None
    assert resumed["status"] == "applied"
    assert lane.strategy_engine.state.operator_halt is False


def test_clear_risk_halts_does_not_restore_same_session_readiness_for_realized_loser_limit(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_lane",
        symbol="MGC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("10"),
    )
    lane.strategy_engine.set_operator_halt(datetime(2026, 3, 19, 8, 0, tzinfo=ZoneInfo("UTC")), True)
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "mgc_lane": {
                "degradation_triggered": True,
                "risk_state": "HALTED_DEGRADATION",
                "halt_reason": "lane_realized_loser_limit_per_session",
                "unblock_action": "Next session reset auto-clear",
                "realized_losing_trades": 2,
            }
        },
    )
    control_path = settings.probationary_artifacts_path / "runtime" / "operator_control.json"
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text(
        json.dumps(
            {
                "action": "clear_risk_halts",
                "status": "pending",
                "requested_at": "2026-03-19T08:01:00+00:00",
                "command_id": "clear-risk-realized-loser-1",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    cleared = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
        risk_state=risk_state,
    )

    assert cleared is not None
    assert cleared["status"] == "applied"
    assert risk_state.lane_states["mgc_lane"]["degradation_triggered"] is False

    control_path.write_text(
        json.dumps(
            {
                "action": "resume_entries",
                "status": "pending",
                "requested_at": "2026-03-19T08:02:00+00:00",
                "command_id": "resume-realized-loser-1",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    resumed = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
        risk_state=risk_state,
    )

    assert resumed is not None
    assert resumed["status"] == "applied"
    assert lane.strategy_engine.state.operator_halt is False

    metrics = {
        "mgc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-30"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-30"),
            closed_trades=2,
            losing_closed_trades=2,
            intent_count=2,
            fill_count=2,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        )
    }

    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
    )

    assert updated_state.lane_states["mgc_lane"]["risk_state"] == "HALTED_DEGRADATION"
    assert updated_state.lane_states["mgc_lane"]["halt_reason"] == "lane_realized_loser_limit_per_session"
    assert lane.strategy_engine.state.operator_halt is True


def test_resume_entries_targets_single_lane_via_shared_identity(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    atp_lane = _seed_test_lane(
        tmp_path / "atp",
        lane_id="atp_companion_v1_asia_us",
        symbol="MGC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("10"),
    )
    atp_lane.spec.shared_strategy_identity = "ATP_COMPANION_V1_ASIA_US"
    other_lane = _seed_test_lane(
        tmp_path / "other",
        lane_id="mgc_us_late_pause_resume_long",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    atp_lane.strategy_engine.set_operator_halt(datetime(2026, 3, 19, 8, 0, tzinfo=ZoneInfo("UTC")), True)
    other_lane.strategy_engine.set_operator_halt(datetime(2026, 3, 19, 8, 0, tzinfo=ZoneInfo("UTC")), True)
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "atp_companion_v1_asia_us": {"risk_state": "OK", "unblock_action": "Resume Entries"},
            "mgc_us_late_pause_resume_long": {"risk_state": "OK", "unblock_action": "Resume Entries"},
        },
    )
    control_path = settings.probationary_artifacts_path / "runtime" / "operator_control.json"
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text(
        json.dumps(
            {
                "action": "resume_entries",
                "status": "pending",
                "requested_at": "2026-03-19T08:02:00+00:00",
                "command_id": "resume-atp-1",
                "lane_id": "atp_companion_v1_asia_us",
                "shared_strategy_identity": "ATP_COMPANION_V1_ASIA_US",
                "control_scope": "lane",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    resumed = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[atp_lane, other_lane],
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
        risk_state=risk_state,
    )

    assert resumed is not None
    assert resumed["status"] == "applied"
    assert resumed["lane_id"] == "atp_companion_v1_asia_us"
    assert resumed["shared_strategy_identity"] == "ATP_COMPANION_V1_ASIA_US"
    assert resumed["message"] == "Entries resumed for lane atp_companion_v1_asia_us."
    assert atp_lane.strategy_engine.state.operator_halt is False
    assert other_lane.strategy_engine.state.operator_halt is True


def test_force_lane_resume_session_override_restores_same_session_readiness_for_eligible_lane(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_lane",
        symbol="MGC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("10"),
    )
    lane.strategy_engine.set_operator_halt(datetime(2026, 3, 19, 8, 0, tzinfo=ZoneInfo("UTC")), True)
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "mgc_lane": {
                "degradation_triggered": True,
                "risk_state": "HALTED_DEGRADATION",
                "halt_reason": "lane_realized_loser_limit_per_session",
                "unblock_action": "Next session reset auto-clear",
                "realized_losing_trades": 2,
            }
        },
    )
    control_path = settings.probationary_artifacts_path / "runtime" / "operator_control.json"
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_path.write_text(
        json.dumps(
            {
                "action": "force_lane_resume_session_override",
                "status": "pending",
                "requested_at": "2026-03-19T08:03:00+00:00",
                "command_id": "force-override-1",
                "lane_id": "mgc_lane",
                "lane_name": "MGC lane",
                "symbol": "MGC",
                "halt_reason": "lane_realized_loser_limit_per_session",
                "local_operator_identity": "local_touch_id_operator",
                "session_override_confirmed": True,
                "session_override_scope": "current_session_only",
                "session_override": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    override_result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
        risk_state=risk_state,
    )

    assert override_result is not None
    assert override_result["status"] == "applied"
    assert override_result["audit_event_type"] == "lane_force_resume_session_override"
    assert override_result["local_operator_identity"] == "local_touch_id_operator"
    assert risk_state.lane_states["mgc_lane"]["session_override_active"] is True
    assert risk_state.lane_states["mgc_lane"]["session_override_reason"] == "lane_realized_loser_limit_per_session"
    assert risk_state.lane_states["mgc_lane"]["risk_state"] == "OK"
    assert lane.strategy_engine.state.operator_halt is False

    metrics = {
        "mgc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-30"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-30"),
            closed_trades=2,
            losing_closed_trades=2,
            intent_count=2,
            fill_count=2,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        )
    }

    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
    )

    assert updated_state.lane_states["mgc_lane"]["session_override_active"] is True
    assert updated_state.lane_states["mgc_lane"]["risk_state"] == "OK"
    assert updated_state.lane_states["mgc_lane"]["halt_reason"] is None
    assert updated_state.lane_states["mgc_lane"]["unblock_action"] == "Session override active for current session"
    assert lane.strategy_engine.state.operator_halt is False

    control_records = (root_logger.artifact_dir / "operator_controls.jsonl").read_text(encoding="utf-8").splitlines()
    assert any('"audit_event_type": "lane_force_resume_session_override"' in line for line in control_records)


def test_realized_loser_session_override_expires_on_next_session_reset() -> None:
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "mgc_lane": {
                "risk_state": "OK",
                "session_override_active": True,
                "session_override_session_date": "2026-03-19",
                "session_override_reason": "lane_realized_loser_limit_per_session",
                "session_override_applied_at": "2026-03-19T08:03:00+00:00",
                "session_override_applied_by": "local_touch_id_operator",
                "session_override_confirmed": True,
            }
        },
    )

    rolled = _ensure_probationary_paper_risk_state_session(risk_state, date(2026, 3, 20))

    assert rolled.session_date == "2026-03-20"
    assert rolled.lane_states["mgc_lane"]["session_override_active"] is False
    assert rolled.lane_states["mgc_lane"]["session_override_expired_reason"] == "session_reset"
    assert "session_override_session_date" not in rolled.lane_states["mgc_lane"]


def test_same_underlying_hold_expiry_is_enforced_in_probationary_runtime(tmp_path: Path) -> None:
    settings = SimpleNamespace(probationary_artifacts_path=tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session")
    state_path = tmp_path / "outputs" / "operator_dashboard" / "same_underlying_conflict_review_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T15:00:00+00:00",
                "records": {
                    "GC": {
                        "instrument": "GC",
                        "hold_new_entries": True,
                        "entry_hold_effective": True,
                        "hold_reason": "Temporary hold.",
                        "hold_set_by": "desk-op",
                        "hold_expires_at": "2026-03-20T12:00:00+00:00",
                        "state_status": "HOLDING",
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    holds = _load_probationary_same_underlying_entry_holds(settings)

    assert holds == {}
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["records"]["GC"]["hold_new_entries"] is False
    assert persisted["records"]["GC"]["hold_expired"] is True
    assert persisted["records"]["GC"]["entry_hold_effective"] is False


def test_two_lanes_same_window_keep_independent_activity_and_lane_halt_state(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    pl_lane = _seed_test_lane(
        tmp_path,
        lane_id="pl_lane",
        symbol="PL",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("50"),
    )
    mgc_lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_lane",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    end_ts = datetime(2026, 3, 19, 15, 30, tzinfo=ZoneInfo("America/New_York"))
    pl_bar = _build_symbol_bar("PL", end_ts, session_asia=False, session_us=True)
    mgc_bar = _build_symbol_bar("MGC", end_ts, session_asia=False, session_us=True)
    packet = replace(
        _blank_signal_packet(pl_bar.bar_id),
        long_entry=True,
        long_entry_source="usLatePauseResumeLongTurn",
    )

    pl_controlled = pl_lane.strategy_engine._apply_runtime_entry_controls(pl_bar, packet)  # noqa: SLF001
    mgc_controlled = mgc_lane.strategy_engine._apply_runtime_entry_controls(  # noqa: SLF001
        mgc_bar,
        replace(packet, bar_id=mgc_bar.bar_id),
    )

    assert pl_controlled.long_entry is True
    assert mgc_controlled.long_entry is True
    pl_branch_rows = [
        json.loads(line)
        for line in (pl_lane.settings.probationary_artifacts_path / "branch_sources.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    mgc_branch_rows = [
        json.loads(line)
        for line in (mgc_lane.settings.probationary_artifacts_path / "branch_sources.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert pl_branch_rows[-1]["decision"] == "allowed"
    assert mgc_branch_rows[-1]["decision"] == "allowed"
    assert pl_branch_rows[-1]["source"] == "usLatePauseResumeLongTurn"
    assert mgc_branch_rows[-1]["source"] == "usLatePauseResumeLongTurn"

    _persist_test_intent_fill(
        pl_lane,
        order_intent_id=f"{pl_bar.bar_id}|BUY_TO_OPEN",
        bar=pl_bar,
        intent_type=OrderIntentType.BUY_TO_OPEN,
        created_at=end_ts,
        reason_code="usLatePauseResumeLongTurn",
        broker_order_id="pl-open-1",
        fill_price=Decimal("100"),
    )
    _persist_test_intent_fill(
        mgc_lane,
        order_intent_id=f"{mgc_bar.bar_id}|BUY_TO_OPEN|1",
        bar=mgc_bar,
        intent_type=OrderIntentType.BUY_TO_OPEN,
        created_at=end_ts,
        reason_code="usLatePauseResumeLongTurn",
        broker_order_id="mgc-open-1",
        fill_price=Decimal("100"),
    )
    _persist_test_intent_fill(
        mgc_lane,
        order_intent_id=f"{mgc_bar.bar_id}|SELL_TO_CLOSE|1",
        bar=mgc_bar,
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        created_at=end_ts + timedelta(minutes=5),
        reason_code="usLatePauseResumeLongTurn",
        broker_order_id="mgc-close-1",
        fill_price=Decimal("99"),
    )
    _persist_test_intent_fill(
        mgc_lane,
        order_intent_id=f"{mgc_bar.bar_id}|BUY_TO_OPEN|2",
        bar=mgc_bar,
        intent_type=OrderIntentType.BUY_TO_OPEN,
        created_at=end_ts + timedelta(minutes=10),
        reason_code="usLatePauseResumeLongTurn",
        broker_order_id="mgc-open-2",
        fill_price=Decimal("100"),
    )
    _persist_test_intent_fill(
        mgc_lane,
        order_intent_id=f"{mgc_bar.bar_id}|SELL_TO_CLOSE|2",
        bar=mgc_bar,
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        created_at=end_ts + timedelta(minutes=15),
        reason_code="usLatePauseResumeLongTurn",
        broker_order_id="mgc-close-2",
        fill_price=Decimal("98"),
    )

    pl_lane.strategy_engine._state = replace(  # noqa: SLF001
        pl_lane.strategy_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        broker_position_qty=1,
        internal_position_qty=1,
        entry_price=Decimal("100"),
        last_order_intent_id=f"{pl_bar.bar_id}|BUY_TO_OPEN",
    )
    mgc_lane.strategy_engine._state = replace(  # noqa: SLF001
        mgc_lane.strategy_engine.state,
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        broker_position_qty=0,
        internal_position_qty=0,
        entry_price=None,
        last_order_intent_id=f"{mgc_bar.bar_id}|SELL_TO_CLOSE|2",
    )

    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-03-19")
    metrics = {
        "pl_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("25"),
            total_pnl=Decimal("25"),
            closed_trades=0,
            losing_closed_trades=0,
            intent_count=1,
            fill_count=1,
            open_order_count=0,
            position_side="LONG",
            internal_position_qty=1,
            broker_position_qty=1,
            entry_price=Decimal("100"),
            last_mark=Decimal("100.5"),
            last_processed_bar_end_ts=end_ts.isoformat(),
        ),
        "mgc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-30"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-30"),
            closed_trades=2,
            losing_closed_trades=2,
            intent_count=4,
            fill_count=4,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=(end_ts + timedelta(minutes=15)).isoformat(),
        ),
    }

    updated_state, risk_events = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[pl_lane, mgc_lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
    )
    _write_probationary_paper_risk_artifacts(
        settings=settings,
        lanes=[pl_lane, mgc_lane],
        lane_metrics=metrics,
        risk_state=updated_state,
        structured_logger=root_logger,
        risk_events=risk_events,
    )
    status_path = _write_probationary_supervisor_operator_status(
        settings=settings,
        lanes=[pl_lane, mgc_lane],
        structured_logger=root_logger,
        risk_state=updated_state,
        latest_operator_control=None,
    )

    assert len(pl_lane.repositories.order_intents.list_all()) == 1
    assert len(pl_lane.repositories.fills.list_all()) == 1
    assert len(mgc_lane.repositories.order_intents.list_all()) == 4
    assert len(mgc_lane.repositories.fills.list_all()) == 4
    assert pl_lane.strategy_engine.state.position_side == PositionSide.LONG
    assert mgc_lane.strategy_engine.state.position_side == PositionSide.FLAT
    assert updated_state.lane_states["pl_lane"]["realized_losing_trades"] == 0
    assert updated_state.lane_states["mgc_lane"]["realized_losing_trades"] == 2
    assert updated_state.lane_states["pl_lane"]["risk_state"] == "OK"
    assert updated_state.lane_states["mgc_lane"]["risk_state"] == "HALTED_DEGRADATION"
    assert updated_state.lane_states["mgc_lane"]["halt_reason"] == "lane_realized_loser_limit_per_session"
    assert pl_lane.strategy_engine.state.operator_halt is False
    assert mgc_lane.strategy_engine.state.operator_halt is True
    assert pl_lane.execution_engine.pending_executions() == []
    assert mgc_lane.execution_engine.pending_executions() == []

    lane_risk_payload = json.loads(
        (settings.probationary_artifacts_path / "runtime" / "paper_lane_risk_snapshot.json").read_text(encoding="utf-8")
    )
    lane_rows = {row["lane_id"]: row for row in lane_risk_payload["lanes"]}
    assert lane_rows["pl_lane"]["risk_state"] == "OK"
    assert lane_rows["pl_lane"]["session_unrealized_pnl"] == "25"
    assert lane_rows["mgc_lane"]["risk_state"] == "HALTED_DEGRADATION"
    assert lane_rows["mgc_lane"]["realized_losing_trades"] == 2

    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    status_lanes = {row["lane_id"]: row for row in status_payload["lanes"]}
    assert status_lanes["pl_lane"]["operator_halt"] is False
    assert status_lanes["pl_lane"]["position_side"] == "LONG"
    assert status_lanes["mgc_lane"]["operator_halt"] is True
    assert status_lanes["mgc_lane"]["halt_reason"] == "lane_realized_loser_limit_per_session"


def test_desk_halt_contaminates_all_lanes_by_design(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path).model_copy(
        update={"probationary_paper_desk_halt_new_entries_loss": Decimal("-10")}
    )
    root_logger = StructuredLogger(tmp_path / "root")
    pl_lane = _seed_test_lane(
        tmp_path,
        lane_id="pl_lane",
        symbol="PL",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("50"),
    )
    mgc_lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_lane",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-03-19")
    metrics = {
        "pl_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-6"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-6"),
            closed_trades=1,
            losing_closed_trades=1,
            intent_count=1,
            fill_count=1,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        ),
        "mgc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("-5"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-5"),
            closed_trades=1,
            losing_closed_trades=1,
            intent_count=1,
            fill_count=1,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        ),
    }

    updated_state, risk_events = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[pl_lane, mgc_lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
    )
    _write_probationary_paper_risk_artifacts(
        settings=settings,
        lanes=[pl_lane, mgc_lane],
        lane_metrics=metrics,
        risk_state=updated_state,
        structured_logger=root_logger,
        risk_events=risk_events,
    )
    status_path = _write_probationary_supervisor_operator_status(
        settings=settings,
        lanes=[pl_lane, mgc_lane],
        structured_logger=root_logger,
        risk_state=updated_state,
        latest_operator_control=None,
    )

    assert updated_state.desk_halt_new_entries_triggered is True
    assert updated_state.desk_last_trigger_reason == "desk_halt_new_entries_loss"
    assert pl_lane.strategy_engine.state.operator_halt is True
    assert mgc_lane.strategy_engine.state.operator_halt is True
    assert pl_lane.execution_engine.pending_executions() == []
    assert mgc_lane.execution_engine.pending_executions() == []

    desk_payload = json.loads(
        (settings.probationary_artifacts_path / "runtime" / "paper_desk_risk_snapshot.json").read_text(encoding="utf-8")
    )
    assert desk_payload["desk_risk_state"] == "HALT_NEW_ENTRIES"
    assert desk_payload["session_total_pnl"] == "-11"
    assert desk_payload["trigger_reason"] == "desk_halt_new_entries_loss"

    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert status_payload["desk_risk_state"] == "HALT_NEW_ENTRIES"
    status_lanes = {row["lane_id"]: row for row in status_payload["lanes"]}
    assert status_lanes["pl_lane"]["operator_halt"] is True
    assert status_lanes["mgc_lane"]["operator_halt"] is True


def test_paper_risk_runtime_state_separates_active_and_historical_lane_states(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    active_lane = _seed_test_lane(
        tmp_path,
        lane_id="active_lane",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "active_lane": {"risk_state": "OK"},
            "historical_lane": {"risk_state": "HALTED_CATASTROPHIC"},
        },
    )
    metrics = {
        "active_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("0"),
            closed_trades=0,
            losing_closed_trades=0,
            intent_count=0,
            fill_count=0,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            open_entry_leg_count=0,
            open_add_count=0,
            additional_entry_allowed=False,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        ),
    }

    _write_probationary_paper_risk_artifacts(
        settings=settings,
        lanes=[active_lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=root_logger,
        risk_events=[],
    )

    payload = json.loads((settings.probationary_artifacts_path / "runtime" / "paper_risk_runtime_state.json").read_text(encoding="utf-8"))
    assert payload["active_lane_ids"] == ["active_lane"]
    assert list(payload["lane_states"].keys()) == ["active_lane"]
    assert list(payload["historical_lane_states"].keys()) == ["historical_lane"]


def test_supervisor_operator_status_keeps_lane_specific_halts_from_poisoning_global_runtime(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    running_lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_lane",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    halted_lane = _seed_test_lane(
        tmp_path,
        lane_id="gc_lane",
        symbol="GC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("100"),
    )
    halted_lane.strategy_engine.set_operator_halt(datetime.now(timezone.utc), True)

    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "gc_lane": {
                "risk_state": "HALTED_CATASTROPHIC",
                "halt_reason": "lane_catastrophic_open_loss_cap",
                "unblock_action": "Manual inspection required",
            }
        },
    )

    status_path = _write_probationary_supervisor_operator_status(
        settings=settings,
        lanes=[running_lane, halted_lane],
        structured_logger=root_logger,
        risk_state=risk_state,
        latest_operator_control=None,
    )

    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["generated_at"]
    assert payload["source_runtime_pid"] > 0
    assert payload["active_lane_ids"] == ["mgc_lane", "gc_lane"]
    assert payload["entries_enabled"] is True
    assert payload["operator_halt"] is False
    assert payload["usable_lane_count"] == 1
    assert payload["halted_lane_count"] == 1


def test_probationary_paper_risk_controls_clear_stale_lane_risk_when_lane_is_rearmed(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    root_logger = StructuredLogger(tmp_path / "root")
    lane = _seed_test_lane(
        tmp_path,
        lane_id="gc_lane",
        symbol="GC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("100"),
    )
    lane.strategy_engine.set_operator_halt(datetime.now(timezone.utc), False)

    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "gc_lane": {
                "catastrophic_triggered": True,
                "risk_state": "HALTED_CATASTROPHIC",
                "halt_reason": None,
                "unblock_action": "Manual inspection required",
                "last_cleared_action": "session_reset_auto_clear",
            }
        },
    )
    metrics = {
        "gc_lane": ProbationaryPaperLaneMetrics(
            session_date="2026-03-19",
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("0"),
            closed_trades=0,
            losing_closed_trades=0,
            intent_count=0,
            fill_count=0,
            open_order_count=0,
            open_entry_leg_count=0,
            open_add_count=0,
            additional_entry_allowed=False,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=datetime(2026, 3, 19, 14, 0, tzinfo=timezone.utc).isoformat(),
        )
    }

    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[lane],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=root_logger,
        alert_dispatcher=AlertDispatcher(root_logger),
    )

    assert updated_state.lane_states["gc_lane"]["catastrophic_triggered"] is False
    assert updated_state.lane_states["gc_lane"]["risk_state"] == "OK"
    assert updated_state.lane_states["gc_lane"]["halt_reason"] is None
    assert updated_state.lane_states["gc_lane"]["unblock_action"] == "No action needed; already eligible"
    assert updated_state.lane_states["gc_lane"]["last_cleared_action"] == "stale_risk_state_reconciled"


def test_supervisor_operator_status_exposes_lane_reason_and_unblock_action(tmp_path: Path) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    lane = _seed_test_lane(
        tmp_path,
        lane_id="gc_lane",
        symbol="GC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("100"),
    )
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-03-19",
        lane_states={
            "gc_lane": {
                "risk_state": "HALTED_CATASTROPHIC",
                "halt_reason": "lane_catastrophic_open_loss_cap",
                "unblock_action": "Manual inspection required",
            }
        },
    )

    path = _write_probationary_supervisor_operator_status(
        settings=settings,
        lanes=[lane],
        structured_logger=StructuredLogger(tmp_path),
        risk_state=risk_state,
        latest_operator_control=None,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["lanes"][0]["halt_reason"] == "lane_catastrophic_open_loss_cap"
    assert payload["lanes"][0]["unblock_action"] == "Manual inspection required"


def test_supervisor_operator_status_exposes_live_lane_eligibility_truth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _build_probationary_paper_settings(tmp_path)
    us_lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_us_late_pause_resume_long",
        symbol="MGC",
        source="usLatePauseResumeLongTurn",
        session_restriction="US_LATE",
        point_value=Decimal("10"),
    )
    asia_lane = _seed_test_lane(
        tmp_path,
        lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
        symbol="MGC",
        source="asiaEarlyNormalBreakoutRetestHoldTurn",
        session_restriction="ASIA_EARLY",
        point_value=Decimal("10"),
    )

    fixed_now = datetime(2026, 3, 22, 18, 5, 30, tzinfo=ZoneInfo("America/New_York"))
    processed_bar = _build_symbol_bar("MGC", datetime(2026, 3, 22, 18, 5, tzinfo=ZoneInfo("America/New_York")), session_asia=True, session_us=False)
    for lane in (us_lane, asia_lane):
        lane.repositories.bars.save(processed_bar, data_source="schwab_live_poll")
        lane.repositories.processed_bars.mark_processed(processed_bar)
        lane.strategy_engine._bar_history = [processed_bar] * lane.settings.warmup_bars_required()  # noqa: SLF001
        lane.strategy_engine._state = replace(  # noqa: SLF001
            lane.strategy_engine.state,
            strategy_status=StrategyStatus.READY,
            position_side=PositionSide.FLAT,
            entries_enabled=True,
            operator_halt=False,
            fault_code=None,
        )

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(probationary_runtime_module, "datetime", FixedDateTime)

    path = _write_probationary_supervisor_operator_status(
        settings=settings,
        lanes=[us_lane, asia_lane],
        structured_logger=StructuredLogger(tmp_path),
        risk_state=ProbationaryPaperRiskRuntimeState(session_date="2026-03-22"),
        latest_operator_control=None,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    lane_rows = {row["lane_id"]: row for row in payload["lanes"]}

    assert payload["current_detected_session"] == "ASIA_EARLY"
    assert lane_rows["mgc_us_late_pause_resume_long"]["current_detected_session"] == "ASIA_EARLY"
    assert lane_rows["mgc_us_late_pause_resume_long"]["eligible_now"] is False
    assert lane_rows["mgc_us_late_pause_resume_long"]["eligibility_reason"] == "wrong_session"
    assert lane_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["current_detected_session"] == "ASIA_EARLY"
    assert lane_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_now"] is True
    assert lane_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligibility_reason"] is None
