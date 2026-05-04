from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_readiness_check_runner import (
    StageResult,
    TrackBReadinessCheckRunnerConfig,
    TrackBReadinessCheckRunnerStages,
    TrackBReadinessCheckRunnerVerdict,
    run_track_b_readiness_check,
)
import mgc_v05l.execution_core.track_b_readiness_check_runner as runner_module


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def config(tmp_path: Path) -> TrackBReadinessCheckRunnerConfig:
    return TrackBReadinessCheckRunnerConfig(
        output_root=tmp_path / "runner",
        recovery_output_root=tmp_path / "recovery",
        preflight_output_root=tmp_path / "preflight",
        databento_observer_output_root=tmp_path / "databento_observer",
        current_quote_output_root=tmp_path / "current_quotes",
        readiness_summary_output_root=tmp_path / "readiness",
        operator_status_output_root=tmp_path / "operator_status",
    )


def recovery_stage(tmp_path: Path, **overrides: object):
    def stage(_config: TrackBReadinessCheckRunnerConfig) -> StageResult:
        payload: dict[str, object] = {
            "classification": "RECOVERY_READY_CLEAN",
            "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
            "primary_blocker": None,
            "required_next_action": "Broker state is clean.",
            "submit_allowed": True,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        payload.update(overrides)
        report = write_json(tmp_path / "recovery_report.json", payload)
        payload["report_json_path"] = str(report)
        return StageResult(name="recovery", exit_code=0 if payload["classification"] == "RECOVERY_READY_CLEAN" else 2, report_json=report, payload=payload)

    return stage


def preflight_stage(tmp_path: Path, **overrides: object):
    def stage(_config: TrackBReadinessCheckRunnerConfig) -> StageResult:
        payload: dict[str, object] = {
            "classification": "READY_READ_ONLY",
            "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
            "primary_blocker": None,
            "required_next_action": "Read-only preflight is clean.",
            "submit_allowed": True,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        payload.update(overrides)
        report = write_json(tmp_path / "preflight_report.json", payload)
        payload["report_json_path"] = str(report)
        return StageResult(name="preflight", exit_code=0 if payload["classification"] == "READY_READ_ONLY" else 2, report_json=report, payload=payload)

    return stage


def quote_stage(tmp_path: Path, **overrides: object):
    def stage(config: TrackBReadinessCheckRunnerConfig) -> StageResult:
        quote_payload: dict[str, object] = {
            "schema_version": "track_b_databento_current_quote_v1",
            "classification": "CURRENT_QUOTE_AVAILABLE",
            "quote_status": "CURRENT_QUOTE_AVAILABLE",
            "current_quote_available": True,
            "quote_provider_mode": "REALTIME",
            "realtime_subscription_attempted": True,
            "realtime_quote_received": True,
            "available_end_fallback_used": False,
            "max_current_quote_age_seconds": 300,
            "quote_age_seconds": "120.0",
            "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_WITHIN_TOLERANCE",
            "requested_quote_end": "2026-05-01T21:00:00+00:00",
            "provider_available_end": "2026-05-01T20:58:00+00:00",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        quote_report = write_json(tmp_path / "current_quote_report.json", quote_payload)
        payload: dict[str, object] = {
            "observer_verdict": "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
            "current_quote_available": True,
            "quote_provider_mode": "REALTIME",
            "realtime_subscription_attempted": True,
            "realtime_quote_received": True,
            "wait_succeeded": True,
            "max_current_quote_age_seconds": 300,
            "quote_age_seconds": "120.0",
            "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_WITHIN_TOLERANCE",
            "requested_quote_end": "2026-05-01T21:00:00+00:00",
            "provider_available_end": "2026-05-01T20:58:00+00:00",
            "source_report_path": str(quote_report),
            "primary_blocker": None,
            "required_next_action": "Run explicit downstream no-submit steps if needed.",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        payload.update(overrides)
        observer_report = write_json(config.databento_observer_output_root / "latest_databento_candle_observer_report.json", payload)
        write_json(
            config.databento_observer_output_root / "latest_databento_candle_observer_heartbeat.json",
            {
                "observer_mode": "wait_for_current_quote",
                "current_quote_available": payload.get("current_quote_available"),
                "quote_provider_mode": payload.get("quote_provider_mode"),
                "realtime_subscription_attempted": payload.get("realtime_subscription_attempted"),
                "realtime_quote_received": payload.get("realtime_quote_received"),
                "wait_succeeded": payload.get("wait_succeeded"),
                "max_current_quote_age_seconds": payload.get("max_current_quote_age_seconds"),
                "quote_age_seconds": payload.get("quote_age_seconds"),
                "quote_freshness_verdict": payload.get("quote_freshness_verdict"),
                "last_requested_quote_end": payload.get("requested_quote_end"),
                "last_provider_available_end": payload.get("provider_available_end"),
                "submit_allowed": False,
                "submit_attempted": False,
                "live_money_readiness": False,
            },
        )
        payload["report_json_path"] = str(observer_report)
        return StageResult(
            name="databento_quote",
            exit_code=0 if payload.get("current_quote_available") is True else 2,
            report_json=observer_report,
            payload=payload,
        )

    return stage


def readiness_stage(tmp_path: Path, **overrides: object):
    def stage(_config: TrackBReadinessCheckRunnerConfig, _recovery_json: Path, _preflight_json: Path, _quote_json: Path) -> StageResult:
        payload: dict[str, object] = {
            "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
            "primary_blocker": None,
            "required_next_action": "Inputs are clean; paper proof still requires explicit operator action.",
            "submit_allowed": True,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        payload.update(overrides)
        report = write_json(tmp_path / "readiness_summary_report.json", payload)
        payload["report_json_path"] = str(report)
        return StageResult(
            name="readiness_summary",
            exit_code=0 if payload["final_readiness_verdict"] == "READY_FOR_PAPER_PROOF" else 2,
            report_json=report,
            payload=payload,
        )

    return stage


def operator_status_stage(tmp_path: Path, calls: list[str]):
    def stage(
        _config: TrackBReadinessCheckRunnerConfig,
        _runner_report_json: Path,
        _recovery_json: Path | None,
        _preflight_json: Path | None,
        _quote_json: Path | None,
        _readiness_json: Path | None,
        _databento_observer_json: Path | None,
    ) -> StageResult:
        calls.append("operator_status")
        payload: dict[str, object] = {
            "status_verdict": "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        report = write_json(tmp_path / "operator_status_summary.json", payload)
        payload["report_json_path"] = str(report)
        return StageResult(name="operator_status", exit_code=0, report_json=report, payload=payload)

    return stage


def stages(tmp_path: Path, **overrides: object) -> TrackBReadinessCheckRunnerStages:
    calls: list[str] = overrides.pop("operator_calls", [])  # type: ignore[assignment]
    return TrackBReadinessCheckRunnerStages(
        recovery=overrides.pop("recovery", recovery_stage(tmp_path)),  # type: ignore[arg-type]
        preflight=overrides.pop("preflight", preflight_stage(tmp_path)),  # type: ignore[arg-type]
        databento_quote=overrides.pop("quote", quote_stage(tmp_path)),  # type: ignore[arg-type]
        readiness_summary=overrides.pop("readiness", readiness_stage(tmp_path)),  # type: ignore[arg-type]
        operator_status=operator_status_stage(tmp_path, calls),
    )


def test_all_clean_path_produces_readiness_check_runner_report(tmp_path: Path) -> None:
    operator_calls: list[str] = []
    result = run_track_b_readiness_check(
        config=config(tmp_path),
        stages=stages(tmp_path, operator_calls=operator_calls),
        runner_id="runner-clean",
        now=aware_now(),
    )

    assert result.verdict == TrackBReadinessCheckRunnerVerdict.READY_FOR_PAPER_PROOF_REVIEW
    assert result.report["runner_verdict"] == "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW"
    assert result.report["recovery_verdict"] == "RECOVERY_READY_CLEAN"
    assert result.report["preflight_verdict"] == "READY_READ_ONLY"
    assert result.report["databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["current_quote_available"] is True
    assert result.report["quote_provider_mode"] == "REALTIME"
    assert result.report["realtime_subscription_attempted"] is True
    assert result.report["realtime_quote_received"] is True
    assert result.report["wait_succeeded"] is True
    assert result.report["max_current_quote_age_seconds"] == 300
    assert result.report["quote_age_seconds"] == "120.0"
    assert result.report["quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_ACCEPTED_WITHIN_TOLERANCE"
    assert result.report["requested_quote_end"] == "2026-05-01T21:00:00+00:00"
    assert result.report["provider_available_end"] == "2026-05-01T20:58:00+00:00"
    assert result.report["readiness_verdict"] == "READY_FOR_PAPER_PROOF"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert operator_calls == ["operator_status"]
    latest = tmp_path / "runner" / "latest_track_b_readiness_check_runner_report.json"
    assert latest.exists()
    assert json.loads(latest.read_text(encoding="utf-8"))["track_b_readiness_check_runner_id"] == "runner-clean"


def test_observer_metadata_quote_report_path_produces_readiness_summary(tmp_path: Path) -> None:
    seen_quote_paths: list[Path] = []

    def observer_style_quote_stage(config: TrackBReadinessCheckRunnerConfig) -> StageResult:
        quote_payload: dict[str, object] = {
            "schema_version": "track_b_databento_current_quote_v1",
            "classification": "CURRENT_QUOTE_AVAILABLE",
            "quote_status": "CURRENT_QUOTE_AVAILABLE",
            "current_quote_available": True,
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        quote_report = write_json(tmp_path / "nested_current_quote_report.json", quote_payload)
        payload: dict[str, object] = {
            "observer_verdict": "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
            "current_quote_available": True,
            "wait_succeeded": True,
            "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_AVAILABLE_END_WITHIN_TOLERANCE",
            "quote_candle_source_metadata": {
                "source_schema_version": "track_b_databento_current_quote_v1",
                "source_report_path": str(quote_report),
            },
            "primary_blocker": None,
            "required_next_action": "Run explicit downstream no-submit steps if needed.",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        observer_report = write_json(config.databento_observer_output_root / "latest_databento_candle_observer_report.json", payload)
        payload["report_json_path"] = str(observer_report)
        return StageResult(name="databento_quote", exit_code=0, report_json=observer_report, payload=payload)

    def readiness_asserts_quote_path(
        _config: TrackBReadinessCheckRunnerConfig,
        _recovery_json: Path,
        _preflight_json: Path,
        quote_json: Path,
    ) -> StageResult:
        seen_quote_paths.append(quote_json)
        return readiness_stage(tmp_path)(_config, _recovery_json, _preflight_json, quote_json)

    result = run_track_b_readiness_check(
        config=config(tmp_path),
        stages=stages(tmp_path, quote=observer_style_quote_stage, readiness=readiness_asserts_quote_path),
        runner_id="runner-observer-metadata-quote",
        now=aware_now(),
    )

    assert result.verdict == TrackBReadinessCheckRunnerVerdict.READY_FOR_PAPER_PROOF_REVIEW
    assert seen_quote_paths == [tmp_path / "nested_current_quote_report.json"]
    assert result.report["artifact_paths"]["current_quote_report_json"] == str(tmp_path / "nested_current_quote_report.json")
    assert result.report["artifact_paths"]["readiness_summary_json"] is not None


def test_missing_quote_report_path_still_blocks_explicitly(tmp_path: Path) -> None:
    def readiness_should_not_run(
        _config: TrackBReadinessCheckRunnerConfig,
        _recovery_json: Path,
        _preflight_json: Path,
        _quote_json: Path,
    ) -> StageResult:
        raise AssertionError("readiness summary should not run without a current quote report path")

    result = run_track_b_readiness_check(
        config=config(tmp_path),
        stages=stages(
            tmp_path,
            quote=quote_stage(
                tmp_path,
                source_report_path=None,
                current_quote_report_json=None,
                quote_report_json=None,
            ),
            readiness=readiness_should_not_run,
        ),
        runner_id="runner-missing-quote-path",
        now=aware_now(),
    )

    assert result.verdict == TrackBReadinessCheckRunnerVerdict.BLOCKED_STAGE_ERROR
    assert result.report["primary_blocker"] == "A required report path was not produced by recovery, preflight, or quote stages."
    assert result.report["artifact_paths"]["current_quote_report_json"] is None
    assert result.report["artifact_paths"]["readiness_summary_json"] is None
    assert result.report["readiness_verdict"] == "NOT_PROVIDED"
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_default_databento_stage_requests_realtime_provider_mode(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    captured_args: list[str] = []

    def fake_databento_cli(argv: list[str]) -> int:
        captured_args.extend(argv)
        output_root = Path(argv[argv.index("--output-root") + 1])
        report_payload = {
            "observer_verdict": "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
            "current_quote_available": True,
            "quote_provider_mode": "REALTIME",
            "realtime_subscription_attempted": True,
            "realtime_quote_received": True,
            "source_report_path": str(write_json(tmp_path / "current_quote_report.json", {"schema_version": "track_b_databento_current_quote_v1"})),
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        report = write_json(output_root / "latest_databento_candle_observer_report.json", report_payload)
        print(json.dumps({"report_json": str(report), "current_quote_available": True, "quote_provider_mode": "REALTIME"}))
        return 0

    monkeypatch.setattr(runner_module, "databento_candle_observer_cli_main", fake_databento_cli)

    result = runner_module._run_databento_cli(config(tmp_path))

    assert result.exit_code == 0
    assert captured_args[captured_args.index("--quote-provider-mode") + 1] == "REALTIME"
    assert result.payload["quote_provider_mode"] == "REALTIME"
    assert result.payload["realtime_subscription_attempted"] is True
    assert result.payload["realtime_quote_received"] is True


def test_recovery_blocked_stops_safely(tmp_path: Path) -> None:
    def preflight_should_not_run(_config: TrackBReadinessCheckRunnerConfig) -> StageResult:
        raise AssertionError("preflight should not run after recovery blocks")

    result = run_track_b_readiness_check(
        config=config(tmp_path),
        stages=stages(
            tmp_path,
            recovery=recovery_stage(
                tmp_path,
                classification="RECOVERY_BLOCKED_UNRESOLVED_ORDER",
                final_readiness_verdict="BLOCKED_UNRESOLVED_BROKER_ORDER",
                primary_blocker="Unresolved broker order remains.",
            ),
            preflight=preflight_should_not_run,
        ),
        runner_id="runner-recovery-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBReadinessCheckRunnerVerdict.BLOCKED_RECOVERY
    assert result.report["primary_blocker"] == "Unresolved broker order remains."
    assert result.report["preflight_verdict"] == "NOT_PROVIDED"
    assert result.report["submit_attempted"] is False


def test_preflight_blocked_stops_safely(tmp_path: Path) -> None:
    def quote_should_not_run(_config: TrackBReadinessCheckRunnerConfig) -> StageResult:
        raise AssertionError("quote should not run after preflight blocks")

    result = run_track_b_readiness_check(
        config=config(tmp_path),
        stages=stages(
            tmp_path,
            preflight=preflight_stage(
                tmp_path,
                classification="BLOCKED",
                final_readiness_verdict="BLOCKED_ACCOUNT_MISMATCH",
                primary_blocker="Preflight account mismatch.",
            ),
            quote=quote_should_not_run,
        ),
        runner_id="runner-preflight-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBReadinessCheckRunnerVerdict.BLOCKED_PREFLIGHT
    assert result.report["primary_blocker"] == "Preflight account mismatch."
    assert result.report["databento_observer_verdict"] == "NOT_PROVIDED"


def test_quote_unavailable_after_wait_blocks_safely(tmp_path: Path) -> None:
    result = run_track_b_readiness_check(
        config=config(tmp_path),
        stages=stages(
            tmp_path,
            quote=quote_stage(
                tmp_path,
                observer_verdict="DATABENTO_CANDLE_OBSERVER_BLOCKED_NO_MARKET_DATA",
                current_quote_available=False,
                wait_succeeded=False,
                primary_blocker="Databento available_end is still behind requested current quote window.",
            ),
        ),
        runner_id="runner-quote-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBReadinessCheckRunnerVerdict.BLOCKED_CURRENT_QUOTE
    assert result.report["current_quote_available"] is False
    assert result.report["wait_succeeded"] is False
    assert "available_end" in str(result.report["primary_blocker"])
    assert result.report["readiness_verdict"] == "NOT_PROVIDED"


def test_readiness_summary_blocked_is_reported(tmp_path: Path) -> None:
    result = run_track_b_readiness_check(
        config=config(tmp_path),
        stages=stages(
            tmp_path,
            readiness=readiness_stage(
                tmp_path,
                final_readiness_verdict="BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE",
                primary_blocker="Quote is not currently available.",
                required_next_action="Obtain a usable current quote.",
            ),
        ),
        runner_id="runner-readiness-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBReadinessCheckRunnerVerdict.BLOCKED_READINESS_SUMMARY
    assert result.report["readiness_verdict"] == "BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE"
    assert result.report["primary_blocker"] == "Quote is not currently available."
    assert result.report["paper_proof_cli_called"] is False


def test_runner_source_does_not_call_submit_paths() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_readiness_check_runner.py").read_text(encoding="utf-8")
    assert "paper_proof_cli_main" not in source
    assert ".paper_proof" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
