from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import mgc_v05l.execution_core.track_b_shadow_monitor as shadow_monitor_module
import mgc_v05l.execution_core.track_b_runtime_candle_capture_cli as runtime_cli
from mgc_v05l.execution_core.track_b_runtime_candle_capture import capture_track_b_runtime_mgc_1m_candles
from mgc_v05l.execution_core.operator_status import OperatorStatusResult, OperatorStatusVerdict
from mgc_v05l.execution_core.track_b_asian_drift_watch_chain import (
    TrackBAsianDriftWatchChainResult,
    TrackBAsianDriftWatchChainVerdict,
)
from mgc_v05l.execution_core.track_b_multi_strategy_runtime_cycle import (
    TrackBMultiStrategyRuntimeCycleResult,
    TrackBMultiStrategyRuntimeCycleVerdict,
)
from mgc_v05l.execution_core.track_b_managed_open_position_maintenance import (
    TrackBManagedOpenPositionMaintenanceResult,
)
from mgc_v05l.execution_core.track_b_runtime_candle_capture import (
    TrackBRuntimeCandleCaptureResult,
    TrackBRuntimeCandleCaptureVerdict,
)
from mgc_v05l.execution_core.track_b_session_strategy_envelope_producer import (
    TrackBSessionStrategyEnvelopeProducerResult,
    TrackBSessionStrategyEnvelopeProducerVerdict,
)
from mgc_v05l.execution_core.track_b_shadow_monitor import (
    TrackBRuntimeDataSource,
    TrackBShadowMonitorConfig,
    TrackBShadowMonitorInstrumentConfig,
    TrackBShadowMonitorStages,
    TrackBShadowMonitorVerdict,
    TrackBStrategyEvaluationMode,
    acquire_monitor_lock,
    default_instruments,
    release_monitor_lock,
    run_track_b_shadow_monitor,
    _run_runtime_candle_capture,
)
from mgc_v05l.execution_core.track_b_snap_turn_envelope_producer import (
    TrackBSnapTurnEnvelopeProducerResult,
    TrackBSnapTurnEnvelopeProducerVerdict,
)


def now() -> datetime:
    return datetime(2026, 5, 5, 12, 0, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def runtime_payload_for_now() -> dict[str, object]:
    candles: list[dict[str, object]] = []
    for index in range(20):
        minute = 41 + index
        candles.append(
            {
                "candle_timestamp": f"2026-05-05T11:{minute:02d}:00+00:00" if minute < 60 else "2026-05-05T12:00:00+00:00",
                "open": str(3400 + index / 10),
                "high": str(3400.2 + index / 10),
                "low": str(3399.8 + index / 10),
                "close": str(3400.1 + index / 10),
                "volume": "1",
            }
        )
    return {
        "source_id": "fresh_runtime_fixture",
        "account_id": "DUM882026",
        "expected_account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "symbol": "MGCM6",
        "local_symbol": "MGCM6",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "timeframe": "1m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "candles": candles,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def completed_5m_payload_for_now(count: int = 8) -> dict[str, object]:
    candles: list[dict[str, object]] = []
    for index in range(count):
        minute = 20 + index * 5
        candles.append(
            {
                "candle_timestamp": f"2026-05-05T11:{minute:02d}:00+00:00" if minute < 60 else "2026-05-05T12:00:00+00:00",
                "open": str(3400 + index / 10),
                "high": str(3400.2 + index / 10),
                "low": str(3399.8 + index / 10),
                "close": str(3400.1 + index / 10),
                "volume": "5",
            }
        )
    return {
        "schema_version": "track_b_databento_live_mgc_completed_5m_candles_v1",
        "generated_at": now().isoformat(),
        "candles": candles,
        "bars_available": len(candles),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def one_minute_candles(start_minute: int, count: int, *, source_tag: str | None = None) -> list[dict[str, object]]:
    candles: list[dict[str, object]] = []
    for index in range(count):
        minute = start_minute + index
        hour = 11 + minute // 60
        actual_minute = minute % 60
        candle: dict[str, object] = {
            "candle_timestamp": f"2026-05-05T{hour:02d}:{actual_minute:02d}:00+00:00",
            "open": str(3400 + index / 10),
            "high": str(3400.2 + index / 10),
            "low": str(3399.8 + index / 10),
            "close": str(3400.1 + index / 10),
            "volume": "1",
        }
        if source_tag:
            candle["source_tag"] = source_tag
        candles.append(candle)
    return candles


def write_live_feed_artifacts(
    cfg: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    candles: list[dict[str, object]],
) -> None:
    symbol = instrument.instrument_family.lower()
    write_json(
        cfg.live_runtime_feed_output_root / f"latest_live_{symbol}_1m_candles.json",
        {
            **runtime_payload_for_now(),
            "contract_key": instrument.contract_key,
            "instrument_family": instrument.instrument_family,
            "local_symbol": instrument.local_symbol,
            "databento_continuous_symbol": instrument.databento_continuous_symbol,
            "candles": candles,
            "candle_history": candles,
            "candle_source_mode": "DATABENTO_LIVE_RUNTIME_FEED",
            "completed_1m_fresh": True,
            "completed_5m_fresh": True,
            "bars_available": len(candles),
        },
    )
    write_json(cfg.live_runtime_feed_output_root / f"latest_live_{symbol}_completed_5m_candles.json", completed_5m_payload_for_now(1))
    live_status = {
        "generated_at": now().isoformat(),
        "contract_key": instrument.contract_key,
        "instrument_family": instrument.instrument_family,
        "local_symbol": instrument.local_symbol,
        "databento_continuous_symbol": instrument.databento_continuous_symbol,
        "dataset": instrument.dataset,
        "live_feed_connected": True,
        "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
        "fresh_for_execution": True,
        "latest_1m_age_seconds": 60,
        "latest_completed_5m_age_seconds": 300,
        "bars_available": len(candles),
    }
    write_json(cfg.live_runtime_feed_output_root / f"latest_databento_live_runtime_feed_{symbol}_heartbeat.json", live_status)
    write_json(cfg.live_runtime_feed_output_root / f"latest_databento_live_runtime_feed_{symbol}_report.json", live_status)


def write_recovery_context_artifact(
    cfg: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    candles: list[dict[str, object]],
) -> None:
    symbol = instrument.instrument_family.lower()
    payload = {
        **runtime_payload_for_now(),
        "contract_key": instrument.contract_key,
        "instrument_family": instrument.instrument_family,
        "local_symbol": instrument.local_symbol,
        "databento_continuous_symbol": instrument.databento_continuous_symbol,
        "candles": candles,
        "candle_history": candles,
        "candle_source_mode": "DATABENTO_HTTP_BACKFILL",
        "bars_available": len(candles),
    }
    write_json(cfg.runtime_candle_capture_output_root / f"latest_runtime_{symbol}_1m_candles.json", payload)
    write_json(
        cfg.runtime_candle_capture_output_root / f"latest_runtime_candle_capture_{symbol}_report.json",
        {
            "data_written": True,
            "fresh_for_execution": False,
            "contract_key": instrument.contract_key,
            "local_symbol": instrument.local_symbol,
            "dataset": instrument.dataset,
        },
    )


class FakeLiveFeedProcess:
    def __init__(self, pid: int = 4242) -> None:
        self.pid = pid
        self.terminated = False
        self.killed = False
        self.returncode = None

    def poll(self):  # type: ignore[no-untyped-def]
        return None if not self.terminated and not self.killed else 0

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def wait(self, timeout=None):  # type: ignore[no-untyped-def]
        self.terminated = True
        self.returncode = 0
        return 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


def config(tmp_path: Path, **overrides: object) -> TrackBShadowMonitorConfig:
    values = {
        "max_cycles": 1,
        "poll_seconds": 0,
        "data_refresh_seconds": 0,
        "output_root": tmp_path / "monitor",
        "runtime_candle_capture_output_root": tmp_path / "runtime",
        "live_runtime_feed_output_root": tmp_path / "live",
        "asian_drift_output_root": tmp_path / "asian",
        "snap_turn_output_root": tmp_path / "snap",
        "session_strategy_output_root": tmp_path / "session",
        "multi_strategy_output_root": tmp_path / "multi",
        "operator_status_output_root": tmp_path / "operator_status",
        "diagnostic_output_root": tmp_path / "diagnostics",
        "lockfile": tmp_path / "monitor" / "track_b_shadow_monitor.lock",
        "pidfile": tmp_path / "monitor" / "track_b_shadow_monitor.pid",
        "backend_health_json": None,
        "current_quote_report_json": None,
        "instruments": (
            TrackBShadowMonitorInstrumentConfig(
                instrument_family="MGC",
                contract_key="MGC-202606",
                local_symbol="MGCM6",
                databento_continuous_symbol="MGC.v.0",
                dataset="GLBX.MDP3",
                enabled_strategies=("ASIAN_DRIFT_V1", "FIRST_BULL_SNAP_TURN_V1"),
                runtime_chain_wired=True,
            ),
        ),
    }
    values.update(overrides)
    return TrackBShadowMonitorConfig(**values)


class FakeStages:
    def __init__(
        self,
        tmp_path: Path,
        *,
        runtime_data_written: bool = True,
        runtime_fresh: bool = True,
        completed_5m: str = "2026-05-05T11:55:00+00:00",
        snap_ok: bool = True,
        session_ok: bool = True,
        runtime_cycle_verdict: str = TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION.value,
        runtime_cycle_overrides: dict[str, object] | None = None,
    ) -> None:
        self.tmp_path = tmp_path
        self.calls: dict[str, int] = {
            "runtime": 0,
            "asian": 0,
            "snap": 0,
            "session": 0,
            "multi": 0,
            "operator": 0,
            "sleep": 0,
        }
        self.runtime_data_written = runtime_data_written
        self.runtime_fresh = runtime_fresh
        self.completed_5m = completed_5m
        self.snap_ok = snap_ok
        self.session_ok = session_ok
        self.runtime_cycle_verdict = runtime_cycle_verdict
        self.runtime_cycle_overrides = runtime_cycle_overrides or {}

    def stages(self) -> TrackBShadowMonitorStages:
        return TrackBShadowMonitorStages(
            runtime_candle_capture=self.runtime,
            asian_drift_watch_chain=self.asian,
            snap_turn_envelopes=self.snap,
            session_strategy_envelopes=self.session,
            multi_strategy_runtime_cycle=self.multi,
            operator_status=self.operator,
            sleep=self.sleep,
            pid_is_alive=lambda _pid: False,
        )

    def runtime(self, _config, _instrument, cycle_index: int, _now: datetime) -> TrackBRuntimeCandleCaptureResult:
        self.calls["runtime"] += 1
        report = {
            "runtime_candle_capture_verdict": (
                TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_EXECUTION_FRESH.value
                if self.runtime_fresh
                else TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_NOT_EXECUTION_FRESH.value
            ),
            "data_written": self.runtime_data_written,
            "fresh_for_execution": self.runtime_fresh,
            "latest_1m_timestamp": "2026-05-05T11:59:00+00:00",
            "latest_completed_5m_timestamp": self.completed_5m,
            "latest_completed_5m_candle_age_seconds": 300,
            "primary_blocker": None if self.runtime_data_written else "provider failed",
            "required_next_action": "continue",
            "report_json_path": str(self.tmp_path / f"runtime-{cycle_index}.json"),
        }
        path = write_json(self.tmp_path / f"runtime-{cycle_index}.json", report)
        event = {"contract_key": "MGC-202606", "candles": []}
        return TrackBRuntimeCandleCaptureResult(
            verdict=TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_EXECUTION_FRESH,
            report_json=path,
            report=report,
            runtime_candles_json=write_json(self.tmp_path / f"runtime-event-{cycle_index}.json", event),
            runtime_candles_event=event,
        )

    def asian(self, _config, _instrument, cycle_index: int, _now: datetime, _runtime) -> TrackBAsianDriftWatchChainResult:
        self.calls["asian"] += 1
        report = {
            "asian_drift_watch_chain_verdict": TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION.value,
            "asian_drift_state_snapshot_path": str(self.tmp_path / f"asian-state-{cycle_index}.json"),
            "primary_blocker": None,
        }
        path = write_json(self.tmp_path / f"asian-{cycle_index}.json", report)
        return TrackBAsianDriftWatchChainResult(
            verdict=TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION,
            report_json=path,
            report=report,
            completed_5m_candles_json=write_json(self.tmp_path / f"completed-5m-{cycle_index}.json", {"candles": []}),
            completed_5m_candles_payload={"candles": []},
            feature_rows_result=None,
            rule_result=None,
        )

    def snap(self, _config, _instrument, cycle_index: int, _now: datetime, _asian) -> TrackBSnapTurnEnvelopeProducerResult:
        self.calls["snap"] += 1
        verdict = (
            TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES
            if self.snap_ok
            else TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_INVALID_INPUT
        )
        report = {
            "snap_turn_envelope_producer_verdict": verdict.value,
            "primary_blocker": None if self.snap_ok else "snap blocked",
        }
        path = write_json(self.tmp_path / f"snap-{cycle_index}.json", report)
        return TrackBSnapTurnEnvelopeProducerResult(
            verdict=verdict,
            report_json=path,
            report=report,
            first_bull_snap_turn_event_json=write_json(self.tmp_path / f"bull-{cycle_index}.json", {}),
            first_bear_snap_turn_event_json=write_json(self.tmp_path / f"bear-{cycle_index}.json", {}),
            first_bull_snap_turn_event={},
            first_bear_snap_turn_event={},
        )

    def session(self, _config, _instrument, cycle_index: int, _now: datetime, _asian) -> TrackBSessionStrategyEnvelopeProducerResult:
        self.calls["session"] += 1
        verdict = (
            TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES
            if self.session_ok
            else TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_INVALID_INPUT
        )
        report = {
            "session_strategy_envelope_producer_verdict": verdict.value,
            "primary_blocker": None if self.session_ok else "session blocked",
        }
        path = write_json(self.tmp_path / f"session-{cycle_index}.json", report)
        return TrackBSessionStrategyEnvelopeProducerResult(
            verdict=verdict,
            report_json=path,
            report=report,
            london_late_pause_resume_short_event_json=write_json(self.tmp_path / f"london-{cycle_index}.json", {}),
            asia_late_flat_pullback_pause_resume_long_event_json=write_json(self.tmp_path / f"asia-late-{cycle_index}.json", {}),
            asia_early_pause_resume_short_event_json=write_json(self.tmp_path / f"pause-{cycle_index}.json", {}),
            asia_early_normal_breakout_retest_hold_long_event_json=write_json(self.tmp_path / f"breakout-{cycle_index}.json", {}),
            us_derivative_bear_turn_event_json=write_json(self.tmp_path / f"us-derivative-bear-{cycle_index}.json", {}),
            mnq_us_derivative_bear_turn_event_json=write_json(self.tmp_path / f"mnq-us-derivative-bear-{cycle_index}.json", {}),
            us_late_pause_resume_long_event_json=write_json(self.tmp_path / f"us-late-long-{cycle_index}.json", {}),
            london_late_pause_resume_short_event={},
            asia_late_flat_pullback_pause_resume_long_event={},
            asia_early_pause_resume_short_event={},
            asia_early_normal_breakout_retest_hold_long_event={},
            us_derivative_bear_turn_event={},
            mnq_us_derivative_bear_turn_event={},
            us_late_pause_resume_long_event={},
        )

    def multi(self, _config, _instrument, cycle_index: int, _now: datetime, _asian, _snap, _session) -> TrackBMultiStrategyRuntimeCycleResult:
        self.calls["multi"] += 1
        report = {
            "multi_strategy_runtime_cycle_verdict": self.runtime_cycle_verdict,
            "evaluated_strategies": [
                {
                    "strategy_id": "ASIAN_DRIFT_V1",
                    "strategy_runtime_verdict": "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION",
                    "decision": "NO_SIGNAL",
                    "signal_emitted": False,
                },
                {
                    "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
                    "strategy_runtime_verdict": "FIRST_BULL_SNAP_TURN_NO_SIGNAL_NO_MUTATION",
                    "decision": "NO_SIGNAL",
                    "signal_emitted": False,
                },
            ],
            "candidate_signals": [],
            "suppressed_signals": [],
            "arbitration_result": {"decision": "NO_TRADE"},
            "chosen_signal": {},
            "chosen_strategy_id": None,
            "decision_journal_summary_path": str(self.tmp_path / "journal-summary.json"),
            "decision_journal_tier_counts": {"TIER_1_NO_SETUP_AGGREGATE": 2},
            "submit_allowed": False,
            "readiness_invoked": False,
            "paper_proof_invoked": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "live_money_readiness": False,
            "required_next_action": "continue",
            "primary_blocker": None,
        }
        report.update(self.runtime_cycle_overrides)
        path = write_json(self.tmp_path / f"multi-{cycle_index}.json", report)
        return TrackBMultiStrategyRuntimeCycleResult(
            verdict=TrackBMultiStrategyRuntimeCycleVerdict(str(report["multi_strategy_runtime_cycle_verdict"])),
            report_json=path,
            report=report,
            strategy_results=(),
            paper_runner_result=None,
        )

    def operator(self, _config, monitor_report_json: Path, _runtime_cycle_report_json: Path | None, _now: datetime) -> OperatorStatusResult:
        self.calls["operator"] += 1
        report = {
            "status_verdict": OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW.value,
            "report_json_path": str(self.tmp_path / "operator.json"),
            "monitor_report_json": str(monitor_report_json),
        }
        path = write_json(self.tmp_path / "operator.json", report)
        return OperatorStatusResult(verdict=OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW, report_json=path, report=report)

    def sleep(self, _seconds: float) -> None:
        self.calls["sleep"] += 1


def test_one_successful_shadow_cycle_writes_multi_instrument_report(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)
    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=fake.stages(), monitor_id="monitor-test", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    assert result.report_json.exists()
    assert result.report["mode"] == "SHADOW"
    assert result.report["instrument_families"] == ["MGC"]
    assert result.report["evaluated_strategy_count"] == 2
    assert result.report["decision_journal_tier_counts"] == {"TIER_1_NO_SETUP_AGGREGATE": 2}
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    assert fake.calls["multi"] == 1
    assert not (tmp_path / "monitor" / "track_b_shadow_monitor.lock").exists()


def test_default_registry_reports_other_instruments_without_ignoring_them(tmp_path: Path) -> None:
    instruments = default_instruments(config(tmp_path))

    families = [item.instrument_family for item in instruments]
    assert families == ["GC", "MGC", "ES", "MES", "NQ", "MNQ"]
    mgc = next(item for item in instruments if item.instrument_family == "MGC")
    assert mgc.runtime_chain_wired is True
    assert "US_DERIVATIVE_BEAR_TURN_V1" in mgc.enabled_strategies
    assert "US_LATE_PAUSE_RESUME_LONG_V1" in mgc.enabled_strategies
    mnq = next(item for item in instruments if item.instrument_family == "MNQ")
    assert mnq.runtime_chain_wired is True
    assert mnq.contract_key == "MNQ-202606"
    assert mnq.local_symbol == "MNQM6"
    assert mnq.enabled_strategies == (
        "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        "MNQ_FIRST_BEAR_SNAP_TURN_V1",
        "MNQ_FIRST_BULL_SNAP_TURN_V1",
    )
    assert next(item for item in instruments if item.instrument_family == "GC").enabled_strategies == ()


def test_provider_blocker_is_primary_even_with_unwired_registry_entries(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path, runtime_data_written=False)
    result = run_track_b_shadow_monitor(
        config=config(tmp_path, instruments=()),
        stages=fake.stages(),
        monitor_id="monitor-default-provider",
        now_func=now,
    )

    assert result.verdict == TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR
    assert result.report["primary_blocker"] == "provider failed"
    assert result.report["instrument_families"] == ["GC", "MGC", "ES", "MES", "NQ", "MNQ"]


def test_provider_failure_blocks_strategy_evaluation_and_continues_as_artifact(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path, runtime_data_written=False)
    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=fake.stages(), monitor_id="monitor-provider", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR
    assert fake.calls["asian"] == 0
    assert fake.calls["multi"] == 0
    assert result.report["instrument_reports"][0]["data_written"] is False
    assert result.report["submit_attempted"] is False


def test_runtime_capture_uses_fresh_runtime_artifact_after_provider_timeout(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    cfg = config(tmp_path, runtime_data_source=TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL)
    capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=runtime_payload_for_now(),
        output_root=cfg.runtime_candle_capture_output_root,
        max_bars=20,
        min_bars=8,
        max_latest_1m_age_seconds=900,
        max_completed_5m_age_seconds=900,
        now=now(),
    )

    def timeout_fetch(**_kwargs):  # type: ignore[no-untyped-def]
        raise TimeoutError("provider timed out")

    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    monkeypatch.setattr(shadow_monitor_module, "_fetch_records", timeout_fetch)

    result = _run_runtime_candle_capture(
        cfg,
        cfg.instruments[0],
        1,
        now(),
    )

    assert result.report["fresh_for_execution"] is True
    assert result.report["monitor_runtime_candle_source"] == "DATABENTO_HTTP_BACKFILL_NOT_LIVE"
    assert result.report["runtime_data_source"] == TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL.value
    assert result.report["provider_fetch_failed_before_fallback"] is True
    assert result.report["provider_fetch_failure_category"] == "PROVIDER_TIMEOUT"
    assert result.report["source_lineage"]["fallback_source_event_path"].endswith("latest_runtime_mgc_1m_candles.json")
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_default_runtime_capture_requires_live_feed_artifact(tmp_path: Path) -> None:
    cfg = config(tmp_path)

    result = _run_runtime_candle_capture(cfg, cfg.instruments[0], 1, now())

    assert result.report["data_written"] is False
    assert result.report["runtime_data_source"] == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value
    assert "Live runtime feed artifact is missing" in str(result.report["primary_blocker"])
    assert "HTTP historical/backfill is not a live runtime source" in str(result.report["primary_blocker"])
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_default_monitor_reports_live_feed_not_ready_when_artifact_missing(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)
    started: list[FakeLiveFeedProcess] = []

    def start_live_feed(_cfg, _instrument, _source_id, _now):  # type: ignore[no-untyped-def]
        process = FakeLiveFeedProcess()
        started.append(process)
        return process

    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
        live_feed_starter=start_live_feed,
    )

    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=stages, monitor_id="monitor-live-missing", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
    assert started and started[0].terminated is True
    assert fake.calls["multi"] == 0
    assert result.report["instrument_reports"][0]["live_feed_managed"] is True
    assert result.report["instrument_reports"][0]["live_feed_pid"] == 4242
    assert result.report["instrument_reports"][0]["runtime_data_source"] == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_default_monitor_evaluates_from_live_feed_artifact(tmp_path: Path) -> None:
    cfg = config(tmp_path, live_runtime_feed_output_root=tmp_path / "live", live_feed_min_bars=8)
    live_payload = runtime_payload_for_now()
    live_payload["candle_source_mode"] = "DATABENTO_LIVE_RUNTIME_FEED"
    live_payload["candles"] = one_minute_candles(20, 40, source_tag="DATABENTO_LIVE_ARTIFACT")
    live_payload["candle_history"] = live_payload["candles"]
    live_payload["bars_available"] = 40
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", live_payload)
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now())
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
            "bars_available": 20,
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "live_runtime_feed_verdict": "TRACK_B_DATABENTO_LIVE_FEED_DATA_WRITTEN_EXECUTION_FRESH",
            "fresh_for_execution": True,
            "latest_record_ts_event": "2026-05-05T11:59:00+00:00",
            "latest_record_ts_recv": "2026-05-05T11:59:01+00:00",
            "latency_ms": 1000,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-live-artifact", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    assert fake.calls["multi"] == 1
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["runtime_data_source"] == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value
    assert instrument_report["monitor_runtime_candle_source"] == "DATABENTO_LIVE_RUNTIME_FEED_ARTIFACT"
    assert instrument_report["live_feed_connected"] is True
    assert instrument_report["live_feed_strategy_ready"] is True
    assert instrument_report["submit_attempted"] is False
    assert instrument_report["live_money_readiness"] is False


def test_monitor_reports_live_feed_warming_up_when_bars_are_insufficient(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        manage_live_feed=False,
        startup_backfill_context_enabled=False,
    )
    live_payload = runtime_payload_for_now()
    live_payload["candles"] = live_payload["candles"][:5]  # type: ignore[index]
    live_payload["candle_history"] = live_payload["candles"]
    live_payload["bars_available"] = 5
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", live_payload)
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now(1))
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
            "bars_available": 5,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-live-warmup", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["live_feed_warmup_1m_count"] == 5
    assert instrument_report["live_feed_warmup_completed_5m_count"] == 1
    assert instrument_report["live_feed_strategy_ready"] is False
    assert fake.calls["multi"] == 0
    assert result.report["submit_attempted"] is False


def test_backfill_seeded_context_allows_evaluation_without_forty_live_minutes(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        max_bars=50,
        manage_live_feed=False,
    )
    context_candles = one_minute_candles(20, 35, source_tag="DATABENTO_HTTP_BACKFILL")
    live_candles = one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT")
    context_payload = {
        **runtime_payload_for_now(),
        "candles": context_candles,
        "candle_history": context_candles,
        "candle_source_mode": "DATABENTO_HTTP_BACKFILL",
        "bars_available": len(context_candles),
    }
    live_payload = {
        **runtime_payload_for_now(),
        "candles": live_candles,
        "candle_history": live_candles,
        "candle_source_mode": "DATABENTO_LIVE_RUNTIME_FEED",
        "fresh_for_execution": False,
        "completed_1m_fresh": True,
        "completed_5m_fresh": True,
        "bars_available": len(live_candles),
    }
    write_json(cfg.runtime_candle_capture_output_root / "latest_runtime_mgc_1m_candles.json", context_payload)
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", live_payload)
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now(1))
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
            "bars_available": len(live_candles),
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-seeded-context", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    assert fake.calls["multi"] == 1
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["feature_context_ready"] is True
    assert instrument_report["feature_context_source"] == "MIXED_BACKFILL_SEEDED_CONTEXT"
    assert instrument_report["live_execution_approved"] is True
    assert instrument_report["paper_evaluation_allowed"] is True
    assert instrument_report["required_bar_count"] == 40
    assert instrument_report["seeded_bar_count"] == 40
    assert instrument_report["live_confirmed_bar_count"] == 5
    assert instrument_report["completed_candles_only"] is True
    assert instrument_report["research_artifact_used"] is False
    assert instrument_report["live_feed_ready"] is True
    assert instrument_report["paper_trade_allowed"] is True
    assert instrument_report["live_feed_warmup_1m_count"] == 5
    diagnostic = json.loads((cfg.diagnostic_output_root / "latest_track_b_startup_readiness_diagnostic.json").read_text())
    assert diagnostic["instruments"]["MGC"]["classification"] == "READY_WITH_BACKFILL_SEEDED_CONTEXT"
    assert diagnostic["instruments"]["MGC"]["required_bar_count"] == 40
    assert diagnostic["instruments"]["MGC"]["seeded_bar_count"] == 40
    assert diagnostic["instruments"]["MGC"]["live_confirmed_bar_count"] == 5
    assert diagnostic["instruments"]["MGC"]["completed_candles_only"] is True
    assert diagnostic["instruments"]["MGC"]["research_artifact_used"] is False
    assert diagnostic["instruments"]["MGC"]["feature_context_ready"] is True
    assert diagnostic["instruments"]["MGC"]["live_feed_ready"] is True
    assert diagnostic["instruments"]["MGC"]["paper_trade_allowed"] is True


def test_backfill_seeded_context_blocks_without_fresh_live_confirmation(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        max_bars=50,
        manage_live_feed=False,
    )
    write_recovery_context_artifact(
        cfg,
        cfg.instruments[0],
        one_minute_candles(20, 40, source_tag="DATABENTO_HTTP_BACKFILL"),
    )
    live_candles = one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT")
    write_json(
        cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json",
        {
            **runtime_payload_for_now(),
            "candles": live_candles,
            "candle_history": live_candles,
            "candle_source_mode": "DATABENTO_LIVE_RUNTIME_FEED",
            "completed_1m_fresh": False,
            "completed_5m_fresh": False,
            "bars_available": len(live_candles),
        },
    )
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now(1))
    stale_status = {
        "generated_at": now().isoformat(),
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "live_feed_connected": True,
        "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
        "fresh_for_execution": False,
        "latest_1m_age_seconds": 901,
        "latest_completed_5m_age_seconds": 901,
        "bars_available": len(live_candles),
    }
    write_json(cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json", stale_status)
    write_json(cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json", stale_status)
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-seeded-live-stale", now_func=now)

    assert fake.calls["multi"] == 0
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["feature_context_ready"] is True
    assert instrument_report["live_feed_ready"] is False
    assert instrument_report["paper_trade_allowed"] is False
    assert instrument_report["paper_evaluation_allowed"] is False
    assert instrument_report["seeded_bar_count"] == 40
    assert instrument_report["live_confirmed_bar_count"] == 5


def test_fewer_than_forty_seed_bars_blocks_feature_context(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        max_bars=50,
        manage_live_feed=False,
    )
    write_recovery_context_artifact(
        cfg,
        cfg.instruments[0],
        one_minute_candles(20, 34, source_tag="DATABENTO_HTTP_BACKFILL"),
    )
    write_live_feed_artifacts(cfg, cfg.instruments[0], one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT"))

    def failed_backfill(*_args: object, **_kwargs: object) -> TrackBRuntimeCandleCaptureResult:
        report_path = tmp_path / "runtime" / "failed-backfill-report.json"
        report = {"data_written": False, "primary_blocker": "bounded backfill unavailable"}
        write_json(report_path, report)
        return TrackBRuntimeCandleCaptureResult(
            verdict=TrackBRuntimeCandleCaptureVerdict.FETCH_FAILED,
            report_json=report_path,
            report=report,
            runtime_candles_json=None,
            runtime_candles_event=None,
        )

    original = shadow_monitor_module._run_http_backfill_runtime_candle_capture
    shadow_monitor_module._run_http_backfill_runtime_candle_capture = failed_backfill  # type: ignore[method-assign]
    try:
        fake = FakeStages(tmp_path)
        stages = TrackBShadowMonitorStages(
            runtime_candle_capture=_run_runtime_candle_capture,
            asian_drift_watch_chain=fake.asian,
            snap_turn_envelopes=fake.snap,
            session_strategy_envelopes=fake.session,
            multi_strategy_runtime_cycle=fake.multi,
            operator_status=fake.operator,
            sleep=fake.sleep,
            pid_is_alive=lambda _pid: False,
        )
        result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-short-seed", now_func=now)
    finally:
        shadow_monitor_module._run_http_backfill_runtime_candle_capture = original  # type: ignore[method-assign]

    assert fake.calls["multi"] == 0
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["feature_context_ready"] is False
    assert instrument_report["live_feed_ready"] is True
    assert instrument_report["paper_trade_allowed"] is False


def test_research_only_startup_context_is_rejected(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        runtime_candle_capture_output_root=tmp_path / "outputs" / "track_b_research" / "snapshots",
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        max_bars=50,
        manage_live_feed=False,
    )
    write_recovery_context_artifact(
        cfg,
        cfg.instruments[0],
        one_minute_candles(20, 40, source_tag="TRACK_B_RESEARCH_SNAPSHOT"),
    )
    write_live_feed_artifacts(cfg, cfg.instruments[0], one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT"))

    def failed_backfill(*_args: object, **_kwargs: object) -> TrackBRuntimeCandleCaptureResult:
        report_path = tmp_path / "runtime" / "failed-backfill-report.json"
        report = {"data_written": False, "primary_blocker": "bounded backfill unavailable"}
        write_json(report_path, report)
        return TrackBRuntimeCandleCaptureResult(
            verdict=TrackBRuntimeCandleCaptureVerdict.FETCH_FAILED,
            report_json=report_path,
            report=report,
            runtime_candles_json=None,
            runtime_candles_event=None,
        )

    original = shadow_monitor_module._run_http_backfill_runtime_candle_capture
    shadow_monitor_module._run_http_backfill_runtime_candle_capture = failed_backfill  # type: ignore[method-assign]
    try:
        fake = FakeStages(tmp_path)
        stages = TrackBShadowMonitorStages(
            runtime_candle_capture=_run_runtime_candle_capture,
            asian_drift_watch_chain=fake.asian,
            snap_turn_envelopes=fake.snap,
            session_strategy_envelopes=fake.session,
            multi_strategy_runtime_cycle=fake.multi,
            operator_status=fake.operator,
            sleep=fake.sleep,
            pid_is_alive=lambda _pid: False,
        )
        result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-research-seed", now_func=now)
    finally:
        shadow_monitor_module._run_http_backfill_runtime_candle_capture = original  # type: ignore[method-assign]

    assert fake.calls["multi"] == 0
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["research_artifact_used"] is True
    assert instrument_report["feature_context_ready"] is False
    assert instrument_report["paper_trade_allowed"] is False
    diagnostic = json.loads((cfg.diagnostic_output_root / "latest_track_b_startup_readiness_diagnostic.json").read_text())
    assert diagnostic["instruments"]["MGC"]["classification"] == "RESEARCH_ONLY_CONTEXT_REJECTED"


def test_repairable_startup_context_gap_is_backfilled_and_allows_evaluation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        max_bars=50,
        manage_live_feed=False,
    )
    recovery_candles = one_minute_candles(20, 34, source_tag="DATABENTO_HTTP_BACKFILL")
    live_candles = one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT")
    repaired_candles = one_minute_candles(20, 40, source_tag="DATABENTO_HTTP_BACKFILL")
    write_json(
        cfg.runtime_candle_capture_output_root / "latest_runtime_mgc_1m_candles.json",
        {
            **runtime_payload_for_now(),
            "candles": recovery_candles,
            "candle_history": recovery_candles,
            "candle_source_mode": "DATABENTO_HTTP_BACKFILL",
            "bars_available": len(recovery_candles),
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json",
        {
            **runtime_payload_for_now(),
            "candles": live_candles,
            "candle_history": live_candles,
            "candle_source_mode": "DATABENTO_LIVE_RUNTIME_FEED",
            "fresh_for_execution": False,
            "completed_1m_fresh": True,
            "completed_5m_fresh": True,
            "bars_available": len(live_candles),
        },
    )
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now(1))
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
            "bars_available": len(live_candles),
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
        },
    )

    def fake_backfill(*_args: object, **_kwargs: object) -> TrackBRuntimeCandleCaptureResult:
        report_path = tmp_path / "runtime" / "backfill-report.json"
        event_path = tmp_path / "runtime" / "backfill-event.json"
        event = {
            **runtime_payload_for_now(),
            "candles": repaired_candles,
            "candle_history": repaired_candles,
            "candle_source_mode": "DATABENTO_HTTP_BACKFILL",
            "bars_available": len(repaired_candles),
        }
        write_json(report_path, {"data_written": True})
        write_json(event_path, event)
        return TrackBRuntimeCandleCaptureResult(
            verdict=TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_NOT_EXECUTION_FRESH,
            report_json=report_path,
            report={"data_written": True},
            runtime_candles_json=event_path,
            runtime_candles_event=event,
        )

    monkeypatch.setattr(shadow_monitor_module, "_run_http_backfill_runtime_candle_capture", fake_backfill)
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-gap-repair", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    assert fake.calls["multi"] == 1
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["feature_context_ready"] is True
    assert instrument_report["live_execution_approved"] is True
    assert instrument_report["paper_evaluation_allowed"] is True
    diagnostic = json.loads((cfg.diagnostic_output_root / "latest_track_b_startup_readiness_diagnostic.json").read_text())
    mgc = diagnostic["instruments"]["MGC"]
    assert mgc["classification"] == "READY_WITH_BACKFILL_SEEDED_CONTEXT"
    assert mgc["latest_decision_bar_source"] == "DATABENTO_LIVE_ARTIFACT"
    assert mgc["gaps"][0]["classification"] == "REPAIRED_BACKFILL_GAP"
    assert mgc["gaps"][0]["repair_succeeded"] is True
    assert mgc["gaps"][0]["repair_request_params"]["requested_symbol"] == "MGCM6"
    assert mgc["gaps"][0]["repair_result_count"] == 40
    assert mgc["gaps"][0]["repair_failure_reason"] is None
    assert mgc["alternate_suffix_result"] == "VALID_REQUIRED_SUFFIX_AVAILABLE"


def test_startup_context_gap_outside_required_window_does_not_block(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        max_bars=90,
        manage_live_feed=False,
    )
    context_candles = one_minute_candles(0, 35, source_tag="DATABENTO_HTTP_BACKFILL")
    live_candles = one_minute_candles(60, 40, source_tag="DATABENTO_LIVE_ARTIFACT")
    write_json(
        cfg.runtime_candle_capture_output_root / "latest_runtime_mgc_1m_candles.json",
        {
            **runtime_payload_for_now(),
            "candles": context_candles,
            "candle_history": context_candles,
            "candle_source_mode": "DATABENTO_HTTP_BACKFILL",
            "bars_available": len(context_candles),
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json",
        {
            **runtime_payload_for_now(),
            "candles": live_candles,
            "candle_history": live_candles,
            "candle_source_mode": "DATABENTO_LIVE_RUNTIME_FEED",
            "fresh_for_execution": False,
            "completed_1m_fresh": True,
            "completed_5m_fresh": True,
            "bars_available": len(live_candles),
        },
    )
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now(1))
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
            "bars_available": len(live_candles),
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-gap-outside-window", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    assert fake.calls["multi"] == 1
    diagnostic = json.loads((cfg.diagnostic_output_root / "latest_track_b_startup_readiness_diagnostic.json").read_text())
    mgc = diagnostic["instruments"]["MGC"]
    assert mgc["context_ready"] is True
    assert mgc["paper_evaluation_allowed"] is True
    assert mgc["alternate_suffix_attempted"] is False
    assert mgc["alternate_suffix_result"] == "NOT_NEEDED"
    gaps = shadow_monitor_module._classify_context_gaps(
        shadow_monitor_module._merge_context_candles(context_candles, live_candles),
        required_1m=40,
    )
    assert gaps[0].classification == "GAP_OUTSIDE_REQUIRED_WINDOW"
    assert gaps[0].within_required_window is False


def test_startup_context_window_expands_to_satisfy_completed_5m_requirement() -> None:
    candles = shadow_monitor_module._merge_context_candles(
        one_minute_candles(20, 41, source_tag="DATABENTO_HTTP_BACKFILL")
    )

    window = shadow_monitor_module._required_context_window(candles, required_1m=40, required_5m=8)

    assert len(window) == 41
    assert shadow_monitor_module._completed_5m_count_from_1m(window) == 8
    assert shadow_monitor_module._classify_context_gaps(candles, required_1m=40, required_5m=8) == []


def test_session_boundary_startup_context_gap_is_allowed() -> None:
    candles = [
        {"candle_timestamp": "2026-05-05T20:59:00+00:00", "source_tag": "DATABENTO_HTTP_BACKFILL"},
        {"candle_timestamp": "2026-05-05T22:00:00+00:00", "source_tag": "DATABENTO_HTTP_BACKFILL"},
    ]

    gaps = shadow_monitor_module._classify_context_gaps(candles, required_1m=2)

    assert gaps[0].classification == "SESSION_BOUNDARY_GAP_ALLOWED"
    assert shadow_monitor_module._blocking_context_gaps(gaps) == []


def test_timestamp_alignment_is_normalized_before_gap_detection() -> None:
    candles = shadow_monitor_module._merge_context_candles(
        [
            {"candle_timestamp": "2026-05-05T11:00:01+00:00", "source_tag": "DATABENTO_HTTP_BACKFILL"},
            {"candle_timestamp": "2026-05-05T11:01:29+00:00", "source_tag": "DATABENTO_HTTP_BACKFILL"},
            {"candle_timestamp": "2026-05-05T11:02:00+00:00", "source_tag": "DATABENTO_LIVE_ARTIFACT"},
        ]
    )

    assert [item["candle_timestamp"] for item in candles] == [
        "2026-05-05T11:00:00+00:00",
        "2026-05-05T11:01:00+00:00",
        "2026-05-05T11:02:00+00:00",
    ]
    assert shadow_monitor_module._classify_context_gaps(candles, required_1m=3) == []


def _mgc_mnq_config(tmp_path: Path) -> tuple[
    TrackBShadowMonitorConfig,
    TrackBShadowMonitorInstrumentConfig,
    TrackBShadowMonitorInstrumentConfig,
]:
    mgc = TrackBShadowMonitorInstrumentConfig(
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        databento_continuous_symbol="MGC.v.0",
        dataset="GLBX.MDP3",
        enabled_strategies=("ASIAN_DRIFT_V1",),
        runtime_chain_wired=True,
    )
    mnq = TrackBShadowMonitorInstrumentConfig(
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        databento_continuous_symbol="MNQ.v.0",
        dataset="GLBX.MDP3",
        enabled_strategies=("MNQ_US_DERIVATIVE_BEAR_TURN_V1",),
        runtime_chain_wired=True,
    )
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        max_bars=50,
        manage_live_feed=False,
        instruments=(mgc, mnq),
    )
    return cfg, mgc, mnq


def _run_multi_instrument_gap_fixture(
    *,
    tmp_path: Path,
    mgc_gap: bool,
    mnq_gap: bool,
) -> tuple[TrackBShadowMonitorConfig, dict[str, Any], dict[str, Any]]:
    cfg, mgc, mnq = _mgc_mnq_config(tmp_path)
    write_live_feed_artifacts(cfg, mgc, one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT"))
    write_live_feed_artifacts(cfg, mnq, one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT"))
    write_recovery_context_artifact(
        cfg,
        mgc,
        one_minute_candles(20, 34 if mgc_gap else 35, source_tag="DATABENTO_HTTP_BACKFILL"),
    )
    write_recovery_context_artifact(
        cfg,
        mnq,
        one_minute_candles(20, 34 if mnq_gap else 35, source_tag="DATABENTO_HTTP_BACKFILL"),
    )

    def failed_backfill(*_args: object, **_kwargs: object) -> TrackBRuntimeCandleCaptureResult:
        report_path = tmp_path / "runtime" / "failed-backfill-report.json"
        report = {"data_written": False, "primary_blocker": "bounded backfill unavailable"}
        write_json(report_path, report)
        return TrackBRuntimeCandleCaptureResult(
            verdict=TrackBRuntimeCandleCaptureVerdict.FETCH_FAILED,
            report_json=report_path,
            report=report,
            runtime_candles_json=None,
            runtime_candles_event=None,
        )

    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )
    original = shadow_monitor_module._run_http_backfill_runtime_candle_capture
    shadow_monitor_module._run_http_backfill_runtime_candle_capture = failed_backfill  # type: ignore[method-assign]
    try:
        result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-multi-gap-scope", now_func=now)
    finally:
        shadow_monitor_module._run_http_backfill_runtime_candle_capture = original  # type: ignore[method-assign]
    reports = {
        str(item.get("instrument_family")): item
        for item in result.report["instrument_reports"]
        if isinstance(item, dict)
    }
    return cfg, reports["MGC"], reports["MNQ"]


def test_mgc_context_gap_does_not_block_clean_mnq(tmp_path: Path) -> None:
    cfg, mgc_report, mnq_report = _run_multi_instrument_gap_fixture(tmp_path=tmp_path, mgc_gap=True, mnq_gap=False)

    assert mgc_report["paper_evaluation_allowed"] is False
    assert "Runtime MGC 1m candle context" in str(mgc_report["primary_blocker"])
    assert mnq_report["feature_context_ready"] is True
    assert mnq_report["live_execution_approved"] is True
    assert mnq_report["paper_evaluation_allowed"] is True
    assert "MGC" not in str(mnq_report.get("primary_blocker") or "")
    diagnostic = json.loads((cfg.diagnostic_output_root / "latest_track_b_startup_readiness_diagnostic.json").read_text())
    assert diagnostic["instruments"]["MGC"]["paper_evaluation_allowed"] is False
    assert diagnostic["instruments"]["MNQ"]["paper_evaluation_allowed"] is True
    assert diagnostic["instruments"]["MGC"]["gap_repair_result_count"] == 0
    assert diagnostic["instruments"]["MGC"]["gap_repair_failure_reason"] == "bounded backfill unavailable"
    assert diagnostic["instruments"]["MGC"]["alternate_suffix_result"] == "NO_VALID_REQUIRED_SUFFIX_WITHOUT_BLOCKING_GAP"


def test_mnq_context_gap_does_not_block_clean_mgc(tmp_path: Path) -> None:
    _cfg, mgc_report, mnq_report = _run_multi_instrument_gap_fixture(tmp_path=tmp_path, mgc_gap=False, mnq_gap=True)

    assert mgc_report["paper_evaluation_allowed"] is True
    assert "MNQ" not in str(mgc_report.get("primary_blocker") or "")
    assert mnq_report["paper_evaluation_allowed"] is False
    assert "Runtime MNQ 1m candle context" in str(mnq_report["primary_blocker"])


def test_both_context_gaps_keep_instrument_specific_blockers(tmp_path: Path) -> None:
    _cfg, mgc_report, mnq_report = _run_multi_instrument_gap_fixture(tmp_path=tmp_path, mgc_gap=True, mnq_gap=True)

    assert mgc_report["paper_evaluation_allowed"] is False
    assert mnq_report["paper_evaluation_allowed"] is False
    assert "Runtime MGC 1m candle context" in str(mgc_report["primary_blocker"])
    assert "Runtime MNQ 1m candle context" in str(mnq_report["primary_blocker"])


def test_global_safety_blocker_is_labeled_global(tmp_path: Path) -> None:
    fake = FakeStages(
        tmp_path,
        runtime_cycle_overrides={
            "submit_attempted": True,
            "paper_proof_invoked": False,
            "paper_proof_classification": None,
            "paper_runner_report_path": None,
        },
    )
    result = run_track_b_shadow_monitor(
        config=config(tmp_path, mode="PAPER", enable_paper_trading=True, paper_on_signal=True, quantity=1),
        stages=fake.stages(),
        monitor_id="monitor-global-safety",
        now_func=now,
    )

    assert result.verdict == TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG
    assert str(result.report["primary_blocker"]).startswith("GLOBAL_SAFETY_BLOCKER:")


def test_backfill_context_without_fresh_live_feed_does_not_evaluate(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=40,
        manage_live_feed=False,
    )
    context_candles = one_minute_candles(20, 40, source_tag="DATABENTO_HTTP_BACKFILL")
    live_candles = one_minute_candles(55, 5, source_tag="DATABENTO_LIVE_ARTIFACT")
    write_json(
        cfg.runtime_candle_capture_output_root / "latest_runtime_mgc_1m_candles.json",
        {**runtime_payload_for_now(), "candles": context_candles, "candle_history": context_candles},
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json",
        {
            **runtime_payload_for_now(),
            "candles": live_candles,
            "candle_history": live_candles,
            "fresh_for_execution": False,
            "completed_1m_fresh": False,
            "completed_5m_fresh": True,
        },
    )
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now(1))
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 999,
            "latest_completed_5m_age_seconds": 300,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 999,
            "latest_completed_5m_age_seconds": 300,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-no-live-approval", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_STALE
    assert fake.calls["multi"] == 0
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["live_execution_approved"] is False


def test_monitor_restarts_stale_live_feed_and_reports_warmup(tmp_path: Path) -> None:
    cfg = config(tmp_path, live_runtime_feed_output_root=tmp_path / "live", live_feed_min_bars=8)
    stale_generated_at = datetime(2026, 5, 5, 11, 0, tzinfo=UTC).isoformat()
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", runtime_payload_for_now())
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now())
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": stale_generated_at,
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
            "bars_available": 20,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
        },
    )
    started: list[FakeLiveFeedProcess] = []

    def start_live_feed(_cfg, _instrument, _source_id, _now):  # type: ignore[no-untyped-def]
        process = FakeLiveFeedProcess(4243)
        started.append(process)
        return process

    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
        live_feed_starter=start_live_feed,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-live-stale", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
    assert started and started[0].terminated is True
    assert result.report["instrument_reports"][0]["live_feed_status"] == "LIVE_FEED_WARMING_UP"
    assert "previous_heartbeat_age_seconds" in str(result.report["instrument_reports"][0]["live_feed_blocker"])
    assert fake.calls["multi"] == 0
    assert result.report["broker_state_mutated"] is False


def test_monitor_blocks_stale_live_feed_when_management_is_disabled(tmp_path: Path) -> None:
    cfg = config(tmp_path, live_runtime_feed_output_root=tmp_path / "live", live_feed_min_bars=8, manage_live_feed=False)
    stale_generated_at = datetime(2026, 5, 5, 11, 0, tzinfo=UTC).isoformat()
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", runtime_payload_for_now())
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now())
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": stale_generated_at,
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
            "bars_available": 20,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-live-stale-no-manage", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_STALE
    assert result.report["instrument_reports"][0]["live_feed_status"] == "LIVE_FEED_STALE"
    assert fake.calls["multi"] == 0


def test_monitor_distinguishes_connected_feed_from_stale_execution_candles(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        live_feed_min_bars=8,
        manage_live_feed=False,
        max_latest_1m_age_seconds=5,
        max_completed_5m_age_seconds=900,
    )
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", runtime_payload_for_now())
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now())
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "bars_available": 20,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": False,
            "latest_1m_age_seconds": 60,
            "latest_completed_5m_age_seconds": 300,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-live-exec-stale", now_func=now)

    instrument_report = result.report["instrument_reports"][0]
    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_STALE
    assert instrument_report["live_feed_connected"] is True
    assert instrument_report["live_feed_execution_fresh"] is False
    assert instrument_report["live_feed_completed_1m_fresh"] is False
    assert "latest 1m candle age" in str(instrument_report["live_feed_execution_freshness_blocker"])
    assert fake.calls["multi"] == 0


def test_monitor_writes_liveness_diagnostic_for_warmup_branch(tmp_path: Path) -> None:
    cfg = config(tmp_path, live_runtime_feed_output_root=tmp_path / "live", live_feed_min_bars=8)
    stale_generated_at = datetime(2026, 5, 5, 11, 0, tzinfo=UTC).isoformat()
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", runtime_payload_for_now())
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now())
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": stale_generated_at,
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
            "bars_available": 20,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
        },
    )

    def start_live_feed(_cfg, _instrument, _source_id, _now):  # type: ignore[no-untyped-def]
        return FakeLiveFeedProcess(4244)

    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
        live_feed_starter=start_live_feed,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-liveness-warmup", now_func=now)

    diagnostic = json.loads((cfg.diagnostic_output_root / "latest_track_b_monitor_liveness_diagnostic.json").read_text())
    heartbeat = json.loads((cfg.output_root / "latest_track_b_shadow_monitor_heartbeat.json").read_text())
    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
    assert diagnostic["last_branch_outcome"] == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value
    assert diagnostic["last_cycle_number"] == 1
    assert diagnostic["live_child_statuses"][0]["instrument_family"] == "MGC"
    assert heartbeat["last_monitor_verdict"] == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value


def test_monitor_liveness_diagnostic_classifies_expected_backoff_sleep(tmp_path: Path) -> None:
    cfg = config(tmp_path, poll_seconds=15, max_backoff_seconds=120)
    lock = shadow_monitor_module.TrackBShadowMonitorLock(
        acquired=True,
        lockfile=cfg.lockfile,
        pidfile=cfg.pidfile,
        owner={"pid": 111, "host": "test-host", "monitor_id": "monitor-sleep"},
    )
    report_path = write_json(
        cfg.output_root / "latest_track_b_shadow_monitor_report.json",
        {
            "completed_at": now().isoformat(),
            "cycle_index": 3,
            "monitor_verdict": TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value,
            "primary_blocker": "latest completed 5m candle age is unavailable",
        },
    )
    write_json(
        cfg.output_root / "latest_track_b_shadow_monitor_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "cycle_index": 3,
            "last_monitor_verdict": TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value,
            "last_report_path": str(report_path),
        },
    )

    path = shadow_monitor_module._write_monitor_liveness_diagnostic(  # noqa: SLF001
        config=cfg,
        now=now(),
        lock=lock,
        instruments=cfg.instruments,
        live_feed_processes={},
        sleep_seconds=60,
        next_wake_at=datetime(2026, 5, 5, 12, 1, tzinfo=UTC),
    )

    diagnostic = json.loads(path.read_text())
    assert diagnostic["suspected_stall_classification"] == "MONITOR_SLEEPING_EXPECTED"
    assert diagnostic["sleep_seconds"] == 60
    assert diagnostic["next_wake_at"] == "2026-05-05T12:01:00+00:00"


def test_monitor_heartbeat_and_report_advance_during_backoff_sleep(tmp_path: Path) -> None:
    cfg = config(tmp_path, poll_seconds=15, max_backoff_seconds=120)
    instrument = cfg.instruments[0]
    write_live_feed_artifacts(cfg, instrument, one_minute_candles(20, 40, source_tag="DATABENTO_LIVE_ARTIFACT"))
    lock = shadow_monitor_module.TrackBShadowMonitorLock(
        acquired=True,
        lockfile=cfg.lockfile,
        pidfile=cfg.pidfile,
        owner={"pid": 333, "host": "test-host", "monitor_id": "monitor-backoff"},
    )
    report_json = cfg.output_root / "cycle-0001" / "track_b_shadow_monitor_report.json"
    initial_time = now()
    report = {
        "completed_at": initial_time.isoformat(),
        "cycle_index": 1,
        "monitor_verdict": TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value,
        "primary_blocker": "Databento Live confirmation window is incomplete.",
        "report_json_path": str(report_json),
        "latest_report_json_path": str(cfg.output_root / "latest_track_b_shadow_monitor_report.json"),
    }
    write_json(report_json, report)
    write_json(cfg.output_root / "latest_track_b_shadow_monitor_report.json", report)

    current_time = initial_time
    sleep_calls: list[float] = []

    def clock() -> datetime:
        return current_time

    def sleep(seconds: float) -> None:
        nonlocal current_time
        sleep_calls.append(seconds)
        current_time = current_time + timedelta(seconds=seconds)

    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=fake.runtime,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=sleep,
        pid_is_alive=lambda _pid: False,
    )

    shadow_monitor_module._sleep_with_monitor_liveness_updates(  # noqa: SLF001
        config=cfg,
        stages=stages,
        monitor_id="monitor-backoff",
        cycle_id="monitor-backoff-cycle-1",
        cycle_index=1,
        instruments=cfg.instruments,
        lock=lock,
        last_verdict=TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value,
        last_report_path=report_json,
        last_report=report,
        live_feed_processes={
            instrument.instrument_family: shadow_monitor_module.TrackBLiveFeedProcessState(
                instrument_family=instrument.instrument_family,
                managed=True,
                owned_by_monitor=True,
                pid=4242,
                status="LIVE_FEED_STARTED",
            )
        },
        sleep_seconds=45,
        next_wake_at=initial_time + timedelta(seconds=45),
        now_func=clock,
    )

    heartbeat = json.loads((cfg.output_root / "latest_track_b_shadow_monitor_heartbeat.json").read_text())
    latest_report = json.loads((cfg.output_root / "latest_track_b_shadow_monitor_report.json").read_text())
    diagnostic = json.loads((cfg.diagnostic_output_root / "latest_track_b_monitor_liveness_diagnostic.json").read_text())
    assert sleep_calls == [15, 15, 15]
    assert heartbeat["generated_at"] == "2026-05-05T12:00:30+00:00"
    assert heartbeat["sleep_seconds"] == 15
    assert heartbeat["next_wake_at"] == "2026-05-05T12:00:45+00:00"
    assert latest_report["completed_at"] == "2026-05-05T12:00:30+00:00"
    assert latest_report["cycle_completed_at"] == "2026-05-05T12:00:00+00:00"
    assert latest_report["monitor_sleeping_expected"] is True
    assert latest_report["sleep_remaining_seconds"] == 15
    assert latest_report["current_blocker"] == "Databento Live confirmation window is incomplete."
    assert diagnostic["generated_at"] == "2026-05-05T12:00:30+00:00"
    assert diagnostic["suspected_stall_classification"] == "MONITOR_SLEEPING_EXPECTED"
    assert diagnostic["live_child_artifacts_advancing"] is True


def test_monitor_liveness_diagnostic_classifies_children_advancing_monitor_stale(tmp_path: Path) -> None:
    cfg = config(tmp_path, live_runtime_feed_output_root=tmp_path / "live", poll_seconds=15)
    instrument = cfg.instruments[0]
    stale_time = datetime(2026, 5, 5, 11, 55, tzinfo=UTC).isoformat()
    write_json(
        cfg.output_root / "latest_track_b_shadow_monitor_report.json",
        {
            "completed_at": stale_time,
            "cycle_index": 5,
            "monitor_verdict": TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value,
            "primary_blocker": "waiting",
        },
    )
    write_json(
        cfg.output_root / "latest_track_b_shadow_monitor_heartbeat.json",
        {
            "generated_at": stale_time,
            "cycle_index": 5,
            "last_monitor_verdict": TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value,
        },
    )
    write_live_feed_artifacts(cfg, instrument, one_minute_candles(20, 40, source_tag="DATABENTO_LIVE_ARTIFACT"))
    lock = shadow_monitor_module.TrackBShadowMonitorLock(
        acquired=True,
        lockfile=cfg.lockfile,
        pidfile=cfg.pidfile,
        owner={"pid": 222, "host": "test-host", "monitor_id": "monitor-stale"},
    )

    path = shadow_monitor_module._write_monitor_liveness_diagnostic(  # noqa: SLF001
        config=cfg,
        now=now(),
        lock=lock,
        instruments=cfg.instruments,
        live_feed_processes={instrument.instrument_family: shadow_monitor_module.TrackBLiveFeedProcessState(
            instrument_family=instrument.instrument_family,
            managed=True,
            owned_by_monitor=True,
            pid=4242,
            status="LIVE_FEED_STARTED",
        )},
    )

    diagnostic = json.loads(path.read_text())
    assert diagnostic["suspected_stall_classification"] == "CHILDREN_ADVANCING_MONITOR_STALE"
    assert diagnostic["live_child_statuses"][0]["live_feed_pid"] == 4242
    assert diagnostic["live_child_artifacts_advancing"] is True


def test_liveness_diagnostic_reports_missing_mnq_completed_5m_reason(tmp_path: Path) -> None:
    mnq = TrackBShadowMonitorInstrumentConfig(
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        databento_continuous_symbol="MNQ.v.0",
        dataset="GLBX.MDP3",
        enabled_strategies=("MNQ_US_DERIVATIVE_BEAR_TURN_V1",),
        runtime_chain_wired=True,
    )
    cfg = config(tmp_path, instruments=(mnq,), live_runtime_feed_output_root=tmp_path / "live")
    write_json(
        cfg.live_runtime_feed_output_root / "latest_live_mnq_1m_candles.json",
        {**runtime_payload_for_now(), "instrument_family": "MNQ", "candles": one_minute_candles(20, 10)},
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_mnq_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "latest_1m_timestamp": "2026-05-05T11:59:00+00:00",
            "latest_1m_age_seconds": 60,
        },
    )
    lock = shadow_monitor_module.TrackBShadowMonitorLock(
        acquired=True,
        lockfile=cfg.lockfile,
        pidfile=cfg.pidfile,
        owner={"pid": 333, "host": "test-host", "monitor_id": "monitor-mnq"},
    )

    path = shadow_monitor_module._write_monitor_liveness_diagnostic(  # noqa: SLF001
        config=cfg,
        now=now(),
        lock=lock,
        instruments=(mnq,),
        live_feed_processes={},
    )

    diagnostic = json.loads(path.read_text())
    mnq_status = diagnostic["live_child_statuses"][0]
    assert mnq_status["instrument_family"] == "MNQ"
    assert mnq_status["completed_5m_exists"] is False
    assert mnq_status["completed_5m_unavailable_reason"] == "artifact missing"


def test_live_artifact_source_does_not_reuse_stale_runtime_cadence_cache(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        data_refresh_seconds=60,
        live_feed_min_bars=8,
        max_latest_1m_age_seconds=900,
        max_completed_5m_age_seconds=900,
    )
    stale_payload = runtime_payload_for_now()
    stale_payload["candles"] = [
        {
            "candle_timestamp": f"2026-05-05T10:{41 + index:02d}:00+00:00"
            if 41 + index < 60
            else "2026-05-05T11:00:00+00:00",
            "open": str(3400 + index / 10),
            "high": str(3400.2 + index / 10),
            "low": str(3399.8 + index / 10),
            "close": str(3400.1 + index / 10),
            "volume": "1",
        }
        for index in range(20)
    ]
    stale_payload["candle_history"] = stale_payload["candles"]
    capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=stale_payload,
        output_root=cfg.runtime_candle_capture_output_root,
        max_bars=20,
        min_bars=8,
        max_latest_1m_age_seconds=5,
        max_completed_5m_age_seconds=5,
        now=now(),
    )
    live_payload = runtime_payload_for_now()
    live_payload["candle_source_mode"] = "DATABENTO_LIVE_RUNTIME_FEED"
    live_payload["candles"] = one_minute_candles(20, 40, source_tag="DATABENTO_LIVE_ARTIFACT")
    live_payload["candle_history"] = live_payload["candles"]
    live_payload["bars_available"] = 40
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", live_payload)
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now())
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
            "bars_available": 20,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-live-no-cache-reuse", now_func=now)

    instrument_report = result.report["instrument_reports"][0]
    assert result.verdict == TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    assert instrument_report["monitor_runtime_candle_source"] == "DATABENTO_LIVE_RUNTIME_FEED_ARTIFACT"
    assert instrument_report["provider_fetch_skipped_for_refresh_cadence"] is False
    assert instrument_report["fresh_for_execution"] is True
    assert fake.calls["multi"] == 1


def test_http_backfill_source_does_not_drive_strategy_evaluation(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)
    fake.runtime = lambda _cfg, _instrument, cycle_index, actual_now: capture_track_b_runtime_mgc_1m_candles(  # type: ignore[method-assign]
        runtime_candle_payload=runtime_payload_for_now(),
        output_root=tmp_path / "runtime",
        max_bars=20,
        min_bars=8,
        max_latest_1m_age_seconds=900,
        max_completed_5m_age_seconds=900,
        source_id=f"http_backfill_{cycle_index}",
        now=actual_now,
    )

    def runtime_with_backfill_marker(cfg, instrument, cycle_index, actual_now):  # type: ignore[no-untyped-def]
        result = fake.runtime(cfg, instrument, cycle_index, actual_now)
        result.report["runtime_data_source"] = TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL.value
        return result

    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=runtime_with_backfill_marker,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=stages, monitor_id="monitor-http-block", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT
    assert fake.calls["multi"] == 0
    assert "backfill/recovery context only" in str(result.report["primary_blocker"])


def test_stale_runtime_context_blocks_strategy_evaluation(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path, runtime_fresh=False)
    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=fake.stages(), monitor_id="monitor-stale", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT
    assert fake.calls["asian"] == 0
    assert fake.calls["multi"] == 0
    assert result.report["instrument_reports"][0]["fresh_for_execution"] is False


def test_completed_bar_only_skips_repeated_completed_bar_after_first_evaluation(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)
    result = run_track_b_shadow_monitor(
        config=config(tmp_path, max_cycles=2),
        stages=fake.stages(),
        monitor_id="monitor-repeat-bar",
        now_func=now,
    )

    assert fake.calls["runtime"] == 2
    assert fake.calls["multi"] == 1
    assert result.verdict == TrackBShadowMonitorVerdict.HEARTBEAT_NO_NEW_COMPLETED_BAR
    assert result.report["instrument_reports"][0]["instrument_verdict"] == (
        TrackBShadowMonitorVerdict.HEARTBEAT_NO_NEW_COMPLETED_BAR.value
    )


def test_completed_bar_only_reuses_fresh_runtime_artifact_inside_refresh_cadence(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)
    runtime_fetch_calls = {"count": 0}

    def runtime_real(
        cfg: TrackBShadowMonitorConfig,
        _instrument: TrackBShadowMonitorInstrumentConfig,
        cycle_index: int,
        actual_now: datetime,
    ) -> TrackBRuntimeCandleCaptureResult:
        runtime_fetch_calls["count"] += 1
        return capture_track_b_runtime_mgc_1m_candles(
            runtime_candle_payload=runtime_payload_for_now(),
            output_root=cfg.runtime_candle_capture_output_root,
            max_bars=20,
            min_bars=8,
            max_latest_1m_age_seconds=900,
            max_completed_5m_age_seconds=900,
            source_id=f"test_runtime_fetch_{cycle_index}",
            now=actual_now,
        )

    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=runtime_real,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )
    base = now()
    calls = {"count": 0}

    def clock() -> datetime:
        calls["count"] += 1
        return base if calls["count"] <= 3 else base.replace(second=15)

    result = run_track_b_shadow_monitor(
        config=config(tmp_path, max_cycles=2, data_refresh_seconds=60, runtime_data_source=TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL),
        stages=stages,
        monitor_id="monitor-refresh-cadence",
        now_func=clock,
    )

    assert runtime_fetch_calls["count"] == 1
    assert fake.calls["multi"] == 1
    assert result.verdict == TrackBShadowMonitorVerdict.HEARTBEAT_NO_NEW_COMPLETED_BAR
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["monitor_runtime_candle_source"] == "FRESH_EXISTING_RUNTIME_ARTIFACT_REFRESH_CADENCE"
    assert instrument_report["provider_fetch_skipped_for_refresh_cadence"] is True
    assert instrument_report["fresh_for_execution"] is True
    assert instrument_report["submit_attempted"] is False
    assert instrument_report["broker_state_mutated"] is False


def test_refresh_cadence_reuse_can_block_stale_cached_runtime_without_provider_fetch(tmp_path: Path) -> None:
    cfg = config(
        tmp_path,
        max_cycles=2,
        data_refresh_seconds=60,
        max_latest_1m_age_seconds=5,
        max_completed_5m_age_seconds=5,
        runtime_data_source=TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL,
    )
    capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=runtime_payload_for_now(),
        output_root=cfg.runtime_candle_capture_output_root,
        max_bars=20,
        min_bars=8,
        max_latest_1m_age_seconds=900,
        max_completed_5m_age_seconds=900,
        now=now(),
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=fake.runtime,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )
    base = datetime(2026, 5, 5, 12, 0, 15, tzinfo=UTC)
    calls = {"count": 0}

    def clock() -> datetime:
        calls["count"] += 1
        return now() if calls["count"] <= 3 else base

    result = run_track_b_shadow_monitor(
        config=cfg,
        stages=stages,
        monitor_id="monitor-refresh-cadence-stale",
        now_func=clock,
    )

    assert result.verdict == TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT
    instrument_report = result.report["instrument_reports"][0]
    assert instrument_report["monitor_runtime_candle_source"] == "EXISTING_RUNTIME_ARTIFACT_REFRESH_CADENCE_NOT_FRESH"
    assert instrument_report["provider_fetch_skipped_for_refresh_cadence"] is True
    assert instrument_report["fresh_for_execution"] is False
    assert fake.calls["multi"] == 1


def test_signal_ready_no_submit_does_not_stop_shadow_monitor(tmp_path: Path) -> None:
    fake = FakeStages(
        tmp_path,
        runtime_cycle_verdict=TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT.value,
        runtime_cycle_overrides={
            "candidate_signals": [{"strategy_id": "FIRST_BULL_SNAP_TURN_V1", "signal_direction": "LONG"}],
            "decision_journal_tier_counts": {"TIER_3_SIGNAL_TRADE_DECISION": 1},
        },
    )
    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=fake.stages(), monitor_id="monitor-signal", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.OK_SIGNAL_READY_NO_SUBMIT
    assert result.report["candidate_signals"] == [{"strategy_id": "FIRST_BULL_SNAP_TURN_V1", "signal_direction": "LONG"}]
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def paper_config(tmp_path: Path, **overrides: object) -> TrackBShadowMonitorConfig:
    values = {
        "mode": "PAPER",
        "enable_paper_trading": True,
        "paper_on_signal": True,
        "quantity": 1,
        "paper_order_pricing_policy": "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT",
        "max_cycles": 2,
        "pause_after_paper_trade": True,
    }
    values.update(overrides)
    return config(tmp_path, **values)


def test_paper_mode_rejects_without_enable_paper_trading(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)

    with pytest.raises(ValueError, match="enable-paper-trading"):
        run_track_b_shadow_monitor(
            config=paper_config(tmp_path, enable_paper_trading=False),
            stages=fake.stages(),
            monitor_id="monitor-paper-missing-enable",
            now_func=now,
        )


def test_paper_mode_rejects_without_paper_on_signal(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)

    with pytest.raises(ValueError, match="paper-on-signal"):
        run_track_b_shadow_monitor(
            config=paper_config(tmp_path, paper_on_signal=False),
            stages=fake.stages(),
            monitor_id="monitor-paper-missing-on-signal",
            now_func=now,
        )


def test_paper_mode_rejects_non_live_runtime_source(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)

    with pytest.raises(ValueError, match="DATABENTO_LIVE_ARTIFACT"):
        run_track_b_shadow_monitor(
            config=paper_config(tmp_path, runtime_data_source=TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL),
            stages=fake.stages(),
            monitor_id="monitor-paper-http-rejected",
            now_func=now,
        )


def test_paper_mode_rejects_manual_prices_without_manual_policy(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)

    with pytest.raises(ValueError, match="Manual PAPER prices are only accepted"):
        run_track_b_shadow_monitor(
            config=paper_config(tmp_path, manual_open_limit_price="4575.0", manual_close_limit_price="4575.3"),
            stages=fake.stages(),
            monitor_id="monitor-paper-manual-price-with-auto-policy",
            now_func=now,
        )


def test_paper_mode_manual_prices_remain_available_for_manual_policy(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)

    result = run_track_b_shadow_monitor(
        config=paper_config(
            tmp_path,
            paper_order_pricing_policy="MANUAL_LIMIT_PRICES",
            manual_open_limit_price="4575.0",
            manual_close_limit_price="4575.3",
        ),
        stages=fake.stages(),
        monitor_id="monitor-paper-manual-policy",
        now_func=now,
    )

    assert result.report["paper_order_pricing_policy"] == "MANUAL_LIMIT_PRICES"
    assert result.report["submit_attempted"] is False


def test_paper_mode_blocks_when_live_feed_is_not_strategy_ready(tmp_path: Path) -> None:
    cfg = paper_config(
        tmp_path,
        live_runtime_feed_output_root=tmp_path / "live",
        manage_live_feed=False,
        live_feed_min_bars=8,
        startup_backfill_context_enabled=False,
    )
    live_payload = runtime_payload_for_now()
    live_payload["candles"] = live_payload["candles"][:5]  # type: ignore[index]
    live_payload["candle_history"] = live_payload["candles"]
    live_payload["bars_available"] = 5
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_1m_candles.json", live_payload)
    write_json(cfg.live_runtime_feed_output_root / "latest_live_mgc_completed_5m_candles.json", completed_5m_payload_for_now(1))
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_heartbeat.json",
        {
            "generated_at": now().isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
            "bars_available": 5,
        },
    )
    write_json(
        cfg.live_runtime_feed_output_root / "latest_databento_live_runtime_feed_report.json",
        {
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "databento_continuous_symbol": "MGC.v.0",
            "dataset": "GLBX.MDP3",
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "fresh_for_execution": True,
        },
    )
    fake = FakeStages(tmp_path)
    stages = TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=fake.asian,
        snap_turn_envelopes=fake.snap,
        session_strategy_envelopes=fake.session,
        multi_strategy_runtime_cycle=fake.multi,
        operator_status=fake.operator,
        sleep=fake.sleep,
        pid_is_alive=lambda _pid: False,
    )

    result = run_track_b_shadow_monitor(config=cfg, stages=stages, monitor_id="monitor-paper-warmup", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
    assert fake.calls["multi"] == 0
    assert result.report["paper_trading_enabled"] is True
    assert result.report["paper_trades_attempted_count"] == 0
    assert result.report["submit_attempted"] is False


def test_paper_mode_records_guarded_lifecycle_and_pauses_after_trade(tmp_path: Path) -> None:
    fake = FakeStages(
        tmp_path,
        runtime_cycle_verdict=TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_PASSED.value,
        runtime_cycle_overrides={
            "chosen_signal": {"strategy_id": "FIRST_BULL_SNAP_TURN_V1", "signal_direction": "LONG"},
            "candidate_signals": [{"strategy_id": "FIRST_BULL_SNAP_TURN_V1", "signal_direction": "LONG"}],
            "paper_runner_report_path": str(tmp_path / "paper-runner.json"),
            "paper_proof_classification": "TRACK_B_PAPER_PROOF_PASSED",
            "final_broker_state_classification": "PROOF_COMPLETE_FLAT",
            "submit_allowed": True,
            "readiness_invoked": True,
            "paper_proof_invoked": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "live_money_readiness": False,
        },
    )

    result = run_track_b_shadow_monitor(config=paper_config(tmp_path), stages=fake.stages(), monitor_id="monitor-paper-proof", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.PAPER_PROOF_PASSED
    assert fake.calls["multi"] == 1
    assert result.report["monitor_mode"] == "PAPER"
    assert result.report["paper_trading_enabled"] is True
    assert result.report["paper_on_signal"] is True
    assert result.report["paper_trades_attempted_count"] == 1
    assert result.report["latest_signal_strategy_id"] == "FIRST_BULL_SNAP_TURN_V1"
    assert result.report["latest_signal_side"] == "LONG"
    assert result.report["latest_paper_lifecycle_report_path"] == str(tmp_path / "paper-runner.json")
    assert result.report["latest_broker_state_classification"] == "TRACK_B_PAPER_PROOF_PASSED"
    assert result.report["submit_attempted"] is True
    assert result.report["broker_state_mutated"] is True
    assert result.report["live_money_readiness"] is False


def test_paper_mode_submit_without_guarded_provenance_is_critical(tmp_path: Path) -> None:
    fake = FakeStages(
        tmp_path,
        runtime_cycle_overrides={
            "submit_attempted": True,
            "broker_state_mutated": True,
            "paper_proof_invoked": True,
            "paper_runner_report_path": None,
            "paper_proof_classification": None,
        },
    )

    result = run_track_b_shadow_monitor(
        config=paper_config(tmp_path),
        stages=fake.stages(),
        monitor_id="monitor-paper-critical",
        now_func=now,
    )

    assert result.verdict == TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG
    assert "without guarded lifecycle provenance" in str(result.report["primary_blocker"])


def test_unexpected_submit_flag_in_shadow_is_critical_and_stops(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path, runtime_cycle_overrides={"submit_attempted": True})
    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=fake.stages(), monitor_id="monitor-critical", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG
    assert "submit_attempted=true" in str(result.report["primary_blocker"])


def test_unexpected_broker_mutation_flag_in_shadow_is_critical(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path, runtime_cycle_overrides={"broker_state_mutated": True})
    result = run_track_b_shadow_monitor(config=config(tmp_path), stages=fake.stages(), monitor_id="monitor-broker", now_func=now)

    assert result.verdict == TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG
    assert "broker_state_mutated=true" in str(result.report["primary_blocker"])


def test_operator_status_update_receives_latest_monitor_report(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)
    result = run_track_b_shadow_monitor(
        config=config(tmp_path, update_operator_status=True),
        stages=fake.stages(),
        monitor_id="monitor-operator",
        now_func=now,
    )

    assert fake.calls["operator"] == 2
    assert result.report["operator_status_verdict"] == OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW.value
    assert result.report["operator_status_path"] == str(tmp_path / "operator.json")


def test_live_lock_refuses_second_monitor_without_force_takeover(tmp_path: Path) -> None:
    lockfile = tmp_path / "monitor.lock"
    pidfile = tmp_path / "monitor.pid"
    write_json(
        lockfile,
        {
            "pid": 12345,
            "monitor_id": "existing",
            "host": "test-host",
            "started_at": now().isoformat(),
            "repo_root": str(tmp_path),
        },
    )
    cfg = config(tmp_path, lockfile=lockfile, pidfile=pidfile)
    lock = acquire_monitor_lock(config=cfg, monitor_id="new", started_at=now(), pid_is_alive=lambda pid: pid == 12345)

    assert lock.acquired is False
    assert "already owns lock" in str(lock.blocker)


def test_stale_lock_is_taken_over_and_released(tmp_path: Path) -> None:
    lockfile = tmp_path / "monitor.lock"
    pidfile = tmp_path / "monitor.pid"
    write_json(lockfile, {"pid": 12345, "monitor_id": "dead", "repo_root": str(tmp_path)})
    cfg = config(tmp_path, lockfile=lockfile, pidfile=pidfile)
    lock = acquire_monitor_lock(config=cfg, monitor_id="new", started_at=now(), pid_is_alive=lambda _pid: False)

    assert lock.acquired is True
    assert lock.stale_lock_takeover is True
    assert json.loads(lockfile.read_text(encoding="utf-8"))["monitor_id"] == "new"
    release_monitor_lock(lock)
    assert not lockfile.exists()
    assert not pidfile.exists()


def test_monitor_source_has_no_private_broker_submit_path() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_shadow_monitor.py").read_text(encoding="utf-8")

    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "paper_proof_cli" not in source


def test_monitor_re_evaluates_managed_open_positions_each_cycle(tmp_path: Path) -> None:
    fake = FakeStages(tmp_path)
    calls: list[str] = []

    def maintenance(_config, maintenance_now):
        calls.append(maintenance_now.isoformat())
        report = {
            "schema_version": "track_b_managed_open_position_maintenance_v1",
            "generated_at": maintenance_now.isoformat(),
            "positions": [
                {
                    "lifecycle_id": "strategy_managed_open",
                    "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "completed_bars_since_entry": 3,
                    "exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                    "close_intent_created": True,
                    "close_submitted": True,
                    "close_filled": True,
                    "final_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
                    "final_position_status": "CLOSED_FLAT",
                    "review_required": False,
                }
            ],
            "close_intent_created_count": 1,
            "close_submitted_count": 1,
            "close_filled_count": 1,
            "review_required_count": 0,
            "broker_state_mutated": True,
            "submit_attempted": True,
            "paper_proof_invoked": False,
            "live_money_readiness": False,
        }
        path = write_json(tmp_path / "diagnostics" / "latest_track_b_managed_open_position_maintenance.json", report)
        return TrackBManagedOpenPositionMaintenanceResult(report_json=path, report=report, lifecycle_results=())

    stages = replace(fake.stages(), managed_open_position_maintenance=maintenance)
    result = run_track_b_shadow_monitor(
        config=config(
            tmp_path,
            mode="PAPER",
            enable_paper_trading=True,
            paper_on_signal=True,
            quantity=1,
            max_cycles=2,
        ),
        stages=stages,
        monitor_id="monitor-managed-maintenance",
        now_func=now,
    )

    assert len(calls) == 2
    assert result.report["managed_open_position_maintenance_close_intent_created_count"] == 1
    assert result.report["managed_open_position_maintenance_close_filled_count"] == 1
    assert result.report["latest_managed_close_intent_status"] == "CLOSE_INTENT_CREATED"
    assert result.report["paper_proof_invoked"] is False
