from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

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
            us_late_pause_resume_long_event_json=write_json(self.tmp_path / f"us-late-long-{cycle_index}.json", {}),
            london_late_pause_resume_short_event={},
            asia_late_flat_pullback_pause_resume_long_event={},
            asia_early_pause_resume_short_event={},
            asia_early_normal_breakout_retest_hold_long_event={},
            us_derivative_bear_turn_event={},
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
    cfg = config(tmp_path, live_runtime_feed_output_root=tmp_path / "live", manage_live_feed=False)
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
        config=config(tmp_path, max_cycles=2, data_refresh_seconds=60),
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
