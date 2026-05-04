"""Bounded no-submit Track B readiness check runner.

This runner composes existing read-only/no-submit boundaries to answer whether
operator evidence is clean enough to consider a separate paper proof step. It
never calls paper_proof_cli and never submits, cancels, or places orders.
"""

from __future__ import annotations

import contextlib
import io
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Sequence

from .databento_candle_observer_cli import main as databento_candle_observer_cli_main
from .models import to_jsonable
from .operator_status import DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT, OperatorStatusInputs, create_operator_status_summary
from .preflight_cli import main as preflight_cli_main
from .readiness_summary_cli import main as readiness_summary_cli_main
from .recovery_status_cli import main as recovery_status_cli_main


DEFAULT_READINESS_CHECK_RUNNER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_readiness_check_runner")


class TrackBReadinessCheckRunnerVerdict(str, Enum):
    READY_FOR_PAPER_PROOF_REVIEW = "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW"
    BLOCKED_RECOVERY = "TRACK_B_READINESS_CHECK_BLOCKED_RECOVERY"
    BLOCKED_PREFLIGHT = "TRACK_B_READINESS_CHECK_BLOCKED_PREFLIGHT"
    BLOCKED_CURRENT_QUOTE = "TRACK_B_READINESS_CHECK_BLOCKED_CURRENT_QUOTE"
    BLOCKED_READINESS_SUMMARY = "TRACK_B_READINESS_CHECK_BLOCKED_READINESS_SUMMARY"
    BLOCKED_STAGE_ERROR = "TRACK_B_READINESS_CHECK_BLOCKED_STAGE_ERROR"


@dataclass(frozen=True)
class TrackBReadinessCheckRunnerConfig:
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17077
    account_id: str = "DUM882026"
    contract_key: str = "MGC-202606"
    broker_order_id: str | None = "1"
    perm_id: str | None = "736787312"
    market_data_mode: str = "DELAYED"
    databento_continuous_symbol: str = "MGC.v.0"
    dataset: str = "GLBX.MDP3"
    allowlisted_local_symbol: str = "MGCM6"
    tick_size: str = "0.1"
    exchange: str = "COMEX"
    currency: str = "USD"
    expected_account_id: str = "DUM882026"
    strategy_id: str = "track_b_example_gold_shadow_v1"
    lane_id: str = "mgc_example_long_lmt_day"
    timeframe: str = "quote_snapshot"
    source_id: str = "track_b_readiness_check_runner"
    proof_timing_status: str = "ACTIVE_SESSION"
    proof_timing_source: str = "track_b_readiness_check_runner"
    proof_timing_detail: str | None = None
    max_wait_cycles: int = 1
    wait_poll_seconds: float = 0.0
    request_timeout_seconds: float = 10.0
    quote_timeout_seconds: float = 3.0
    output_root: Path = DEFAULT_READINESS_CHECK_RUNNER_OUTPUT_ROOT
    recovery_output_root: Path = Path("outputs/track_b_execution_core/recovery_status")
    preflight_output_root: Path = Path("outputs/track_b_execution_core/preflight")
    databento_observer_output_root: Path = Path("outputs/track_b_execution_core/databento_candle_observer")
    current_quote_output_root: Path = Path("outputs/track_b_execution_core/current_quotes")
    readiness_summary_output_root: Path = Path("outputs/track_b_execution_core/readiness_summary")
    operator_status_output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT


@dataclass(frozen=True)
class StageResult:
    name: str
    exit_code: int
    report_json: Path | None
    payload: dict[str, object]
    stdout: str = ""


@dataclass(frozen=True)
class TrackBReadinessCheckRunnerStages:
    recovery: Callable[[TrackBReadinessCheckRunnerConfig], StageResult]
    preflight: Callable[[TrackBReadinessCheckRunnerConfig], StageResult]
    databento_quote: Callable[[TrackBReadinessCheckRunnerConfig], StageResult]
    readiness_summary: Callable[[TrackBReadinessCheckRunnerConfig, Path, Path, Path], StageResult]
    operator_status: Callable[
        [TrackBReadinessCheckRunnerConfig, Path, Path | None, Path | None, Path | None, Path | None, Path | None],
        StageResult,
    ]


@dataclass(frozen=True)
class TrackBReadinessCheckRunnerResult:
    verdict: TrackBReadinessCheckRunnerVerdict
    report_json: Path
    report: dict[str, object]


def default_stages() -> TrackBReadinessCheckRunnerStages:
    return TrackBReadinessCheckRunnerStages(
        recovery=_run_recovery_cli,
        preflight=_run_preflight_cli,
        databento_quote=_run_databento_cli,
        readiness_summary=_run_readiness_summary_cli,
        operator_status=_run_operator_status,
    )


def run_track_b_readiness_check(
    *,
    config: TrackBReadinessCheckRunnerConfig,
    stages: TrackBReadinessCheckRunnerStages | None = None,
    runner_id: str | None = None,
    now: datetime | None = None,
) -> TrackBReadinessCheckRunnerResult:
    actual_stages = stages or default_stages()
    actual_now = now or datetime.now(UTC)
    actual_runner_id = runner_id or f"track_b_readiness_check_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_runner_id / "track_b_readiness_check_runner_report.json"
    stage_results: dict[str, StageResult] = {}
    current_quote_report_json: Path | None = None
    readiness_report_json: Path | None = None
    operator_status_report_json: Path | None = None

    verdict = TrackBReadinessCheckRunnerVerdict.BLOCKED_STAGE_ERROR
    primary_blocker: str | None = None
    required_next_action = "Review the readiness-check runner stage reports."

    try:
        recovery = actual_stages.recovery(config)
        stage_results["recovery"] = recovery
        if recovery.exit_code != 0 or _recovery_verdict(recovery.payload) != "RECOVERY_READY_CLEAN":
            verdict = TrackBReadinessCheckRunnerVerdict.BLOCKED_RECOVERY
            primary_blocker = _primary_blocker(recovery.payload, "Recovery status is not clean.")
            required_next_action = _required_action(recovery.payload, "Resolve recovery blocker before any paper proof review.")
            return _finalize(
                config=config,
                report_json=report_json,
                runner_id=actual_runner_id,
                now=actual_now,
                verdict=verdict,
                primary_blocker=primary_blocker,
                required_next_action=required_next_action,
                stage_results=stage_results,
                current_quote_report_json=current_quote_report_json,
                readiness_report_json=readiness_report_json,
                operator_status_report_json=operator_status_report_json,
                operator_status_stage=actual_stages.operator_status,
            )

        preflight = actual_stages.preflight(config)
        stage_results["preflight"] = preflight
        if preflight.exit_code != 0 or _preflight_verdict(preflight.payload) != "READY_READ_ONLY":
            verdict = TrackBReadinessCheckRunnerVerdict.BLOCKED_PREFLIGHT
            primary_blocker = _primary_blocker(preflight.payload, "Read-only preflight is not clean.")
            required_next_action = _required_action(preflight.payload, "Resolve preflight blocker before any paper proof review.")
            return _finalize(
                config=config,
                report_json=report_json,
                runner_id=actual_runner_id,
                now=actual_now,
                verdict=verdict,
                primary_blocker=primary_blocker,
                required_next_action=required_next_action,
                stage_results=stage_results,
                current_quote_report_json=current_quote_report_json,
                readiness_report_json=readiness_report_json,
                operator_status_report_json=operator_status_report_json,
                operator_status_stage=actual_stages.operator_status,
            )

        quote = actual_stages.databento_quote(config)
        stage_results["databento_quote"] = quote
        current_quote_report_json = _current_quote_report_path(quote)
        if quote.exit_code != 0 or quote.payload.get("current_quote_available") is not True:
            verdict = TrackBReadinessCheckRunnerVerdict.BLOCKED_CURRENT_QUOTE
            primary_blocker = _primary_blocker(quote.payload, "Current Databento quote is not available after bounded wait.")
            required_next_action = _required_action(
                quote.payload,
                "Wait for Databento current quote availability, then rerun the no-submit readiness check.",
            )
            return _finalize(
                config=config,
                report_json=report_json,
                runner_id=actual_runner_id,
                now=actual_now,
                verdict=verdict,
                primary_blocker=primary_blocker,
                required_next_action=required_next_action,
                stage_results=stage_results,
                current_quote_report_json=current_quote_report_json,
                readiness_report_json=readiness_report_json,
                operator_status_report_json=operator_status_report_json,
                operator_status_stage=actual_stages.operator_status,
            )

        if recovery.report_json is None or preflight.report_json is None or current_quote_report_json is None:
            verdict = TrackBReadinessCheckRunnerVerdict.BLOCKED_STAGE_ERROR
            primary_blocker = "A required report path was not produced by recovery, preflight, or quote stages."
            required_next_action = "Review stage outputs before any paper proof review."
            return _finalize(
                config=config,
                report_json=report_json,
                runner_id=actual_runner_id,
                now=actual_now,
                verdict=verdict,
                primary_blocker=primary_blocker,
                required_next_action=required_next_action,
                stage_results=stage_results,
                current_quote_report_json=current_quote_report_json,
                readiness_report_json=readiness_report_json,
                operator_status_report_json=operator_status_report_json,
                operator_status_stage=actual_stages.operator_status,
            )

        readiness = actual_stages.readiness_summary(config, recovery.report_json, preflight.report_json, current_quote_report_json)
        stage_results["readiness_summary"] = readiness
        readiness_report_json = readiness.report_json
        if readiness.exit_code != 0 or readiness.payload.get("final_readiness_verdict") != "READY_FOR_PAPER_PROOF":
            verdict = TrackBReadinessCheckRunnerVerdict.BLOCKED_READINESS_SUMMARY
            primary_blocker = _primary_blocker(readiness.payload, "Readiness summary is blocked.")
            required_next_action = _required_action(readiness.payload, "Resolve readiness summary blocker before any paper proof review.")
            return _finalize(
                config=config,
                report_json=report_json,
                runner_id=actual_runner_id,
                now=actual_now,
                verdict=verdict,
                primary_blocker=primary_blocker,
                required_next_action=required_next_action,
                stage_results=stage_results,
                current_quote_report_json=current_quote_report_json,
                readiness_report_json=readiness_report_json,
                operator_status_report_json=operator_status_report_json,
                operator_status_stage=actual_stages.operator_status,
            )

        verdict = TrackBReadinessCheckRunnerVerdict.READY_FOR_PAPER_PROOF_REVIEW
        primary_blocker = None
        required_next_action = (
            "Read-only recovery, preflight, current quote, and readiness summary are clean. "
            "paper_proof_cli remains a separate explicit operator decision and was not called."
        )
        return _finalize(
            config=config,
            report_json=report_json,
            runner_id=actual_runner_id,
            now=actual_now,
            verdict=verdict,
            primary_blocker=primary_blocker,
            required_next_action=required_next_action,
            stage_results=stage_results,
            current_quote_report_json=current_quote_report_json,
            readiness_report_json=readiness_report_json,
            operator_status_report_json=operator_status_report_json,
            operator_status_stage=actual_stages.operator_status,
        )
    except Exception as exc:  # noqa: BLE001 - runner failures must become reports.
        verdict = TrackBReadinessCheckRunnerVerdict.BLOCKED_STAGE_ERROR
        primary_blocker = f"Readiness check runner stage error: {exc}"
        required_next_action = "Review runner diagnostics and stage outputs before any paper proof review."
        return _finalize(
            config=config,
            report_json=report_json,
            runner_id=actual_runner_id,
            now=actual_now,
            verdict=verdict,
            primary_blocker=primary_blocker,
            required_next_action=required_next_action,
            stage_results=stage_results,
            current_quote_report_json=current_quote_report_json,
            readiness_report_json=readiness_report_json,
            operator_status_report_json=operator_status_report_json,
            operator_status_stage=actual_stages.operator_status,
        )


def _finalize(
    *,
    config: TrackBReadinessCheckRunnerConfig,
    report_json: Path,
    runner_id: str,
    now: datetime,
    verdict: TrackBReadinessCheckRunnerVerdict,
    primary_blocker: str | None,
    required_next_action: str,
    stage_results: dict[str, StageResult],
    current_quote_report_json: Path | None,
    readiness_report_json: Path | None,
    operator_status_report_json: Path | None,
    operator_status_stage: Callable[
        [TrackBReadinessCheckRunnerConfig, Path, Path | None, Path | None, Path | None, Path | None, Path | None],
        StageResult,
    ],
) -> TrackBReadinessCheckRunnerResult:
    report = _build_report(
        config=config,
        report_json=report_json,
        runner_id=runner_id,
        now=now,
        verdict=verdict,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        stage_results=stage_results,
        current_quote_report_json=current_quote_report_json,
        readiness_report_json=readiness_report_json,
        operator_status_report_json=operator_status_report_json,
    )
    _write_report(report_json, report)
    operator_status = operator_status_stage(
        config,
        report_json.parent.parent / "latest_track_b_readiness_check_runner_report.json",
        stage_results.get("recovery").report_json if stage_results.get("recovery") else None,
        stage_results.get("preflight").report_json if stage_results.get("preflight") else None,
        current_quote_report_json,
        readiness_report_json,
        stage_results.get("databento_quote").report_json if stage_results.get("databento_quote") else None,
    )
    stage_results["operator_status"] = operator_status
    operator_status_report_json = operator_status.report_json
    report = _build_report(
        config=config,
        report_json=report_json,
        runner_id=runner_id,
        now=now,
        verdict=verdict,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        stage_results=stage_results,
        current_quote_report_json=current_quote_report_json,
        readiness_report_json=readiness_report_json,
        operator_status_report_json=operator_status_report_json,
    )
    _write_report(report_json, report)
    return TrackBReadinessCheckRunnerResult(verdict=verdict, report_json=report_json, report=report)


def _build_report(
    *,
    config: TrackBReadinessCheckRunnerConfig,
    report_json: Path,
    runner_id: str,
    now: datetime,
    verdict: TrackBReadinessCheckRunnerVerdict,
    primary_blocker: str | None,
    required_next_action: str,
    stage_results: dict[str, StageResult],
    current_quote_report_json: Path | None,
    readiness_report_json: Path | None,
    operator_status_report_json: Path | None,
) -> dict[str, object]:
    recovery = stage_results.get("recovery")
    preflight = stage_results.get("preflight")
    quote = stage_results.get("databento_quote")
    readiness = stage_results.get("readiness_summary")
    operator_status = stage_results.get("operator_status")
    secondary_blockers = _secondary_blockers(stage_results)
    artifact_paths = {
        "recovery_report_json": str(recovery.report_json) if recovery and recovery.report_json else None,
        "preflight_report_json": str(preflight.report_json) if preflight and preflight.report_json else None,
        "databento_observer_report_json": str(quote.report_json) if quote and quote.report_json else None,
        "databento_observer_heartbeat_json": str(Path(config.databento_observer_output_root) / "latest_databento_candle_observer_heartbeat.json"),
        "current_quote_report_json": str(current_quote_report_json) if current_quote_report_json else None,
        "readiness_summary_json": str(readiness_report_json) if readiness_report_json else None,
        "operator_status_json": str(operator_status_report_json) if operator_status_report_json else None,
        "runner_report_json": str(report_json),
        "latest_runner_report_json": str(report_json.parent.parent / "latest_track_b_readiness_check_runner_report.json"),
    }
    return {
        "schema_version": "track_b_readiness_check_runner_v1",
        "generated_at": now.isoformat(),
        "track_b_readiness_check_runner_id": runner_id,
        "runner_verdict": verdict.value,
        "mode": config.mode,
        "account_id": config.account_id,
        "contract_key": config.contract_key,
        "broker_order_id": config.broker_order_id,
        "perm_id": config.perm_id,
        "databento_continuous_symbol": config.databento_continuous_symbol,
        "dataset": config.dataset,
        "proof_timing_status": config.proof_timing_status,
        "recovery_verdict": _value(recovery.payload if recovery else {}, "classification", "final_readiness_verdict"),
        "preflight_verdict": _value(preflight.payload if preflight else {}, "classification", "final_readiness_verdict"),
        "databento_observer_verdict": _value(quote.payload if quote else {}, "observer_verdict", "last_observer_verdict"),
        "current_quote_available": bool(quote and quote.payload.get("current_quote_available") is True),
        "wait_succeeded": bool(quote and quote.payload.get("wait_succeeded") is True),
        "readiness_verdict": _value(readiness.payload if readiness else {}, "final_readiness_verdict"),
        "operator_status_verdict": _value(operator_status.payload if operator_status else {}, "status_verdict"),
        "primary_blocker": primary_blocker,
        "secondary_blockers": secondary_blockers,
        "required_next_action": required_next_action,
        "artifact_paths": artifact_paths,
        "stage_exit_codes": {name: result.exit_code for name, result in stage_results.items()},
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "paper_proof_cli_called": False,
        "place_order_called": False,
        "cancel_called": False,
        "order_plan_created": False,
        "broker_state_mutated": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_readiness_check_runner_report.json"),
    }


def _write_report(report_json: Path, report: dict[str, object]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")


def _run_recovery_cli(config: TrackBReadinessCheckRunnerConfig) -> StageResult:
    args = [
        "--mode", config.mode,
        "--host", config.host,
        "--port", str(config.port),
        "--client-id", str(config.client_id),
        "--account-id", config.account_id,
        "--contract-key", config.contract_key,
        "--output-root", str(config.recovery_output_root),
        "--request-timeout-seconds", str(config.request_timeout_seconds),
    ]
    if config.broker_order_id:
        args.extend(["--broker-order-id", config.broker_order_id])
    if config.perm_id:
        args.extend(["--perm-id", config.perm_id])
    return _call_cli("recovery", recovery_status_cli_main, args)


def _run_preflight_cli(config: TrackBReadinessCheckRunnerConfig) -> StageResult:
    args = [
        "--mode", config.mode,
        "--host", config.host,
        "--port", str(config.port),
        "--client-id", str(config.client_id),
        "--account-id", config.account_id,
        "--contract-key", config.contract_key,
        "--output-root", str(config.preflight_output_root),
        "--market-data-mode", config.market_data_mode,
        "--request-timeout-seconds", str(config.request_timeout_seconds),
        "--quote-timeout-seconds", str(config.quote_timeout_seconds),
    ]
    return _call_cli("preflight", preflight_cli_main, args)


def _run_databento_cli(config: TrackBReadinessCheckRunnerConfig) -> StageResult:
    args = [
        "--live-current-quote",
        "--wait-for-current-quote",
        "--max-wait-cycles", str(config.max_wait_cycles),
        "--wait-poll-seconds", str(config.wait_poll_seconds),
        "--contract-key", config.contract_key,
        "--databento-continuous-symbol", config.databento_continuous_symbol,
        "--dataset", config.dataset,
        "--allowlisted-local-symbol", config.allowlisted_local_symbol,
        "--tick-size", config.tick_size,
        "--exchange", config.exchange,
        "--currency", config.currency,
        "--expected-account-id", config.expected_account_id,
        "--strategy-id", config.strategy_id,
        "--lane-id", config.lane_id,
        "--timeframe", config.timeframe,
        "--source-id", config.source_id,
        "--current-quote-output-root", str(config.current_quote_output_root),
        "--output-root", str(config.databento_observer_output_root),
    ]
    result = _call_cli("databento_quote", databento_candle_observer_cli_main, args)
    observer_report = Path(config.databento_observer_output_root) / "latest_databento_candle_observer_report.json"
    if observer_report.exists():
        report = _read_json(observer_report)
        payload = {**result.payload, **report}
        heartbeat = Path(config.databento_observer_output_root) / "latest_databento_candle_observer_heartbeat.json"
        if heartbeat.exists():
            payload.update(_read_json(heartbeat))
        return StageResult(name=result.name, exit_code=result.exit_code, report_json=observer_report, payload=payload, stdout=result.stdout)
    return result


def _run_readiness_summary_cli(config: TrackBReadinessCheckRunnerConfig, recovery_json: Path, preflight_json: Path, quote_json: Path) -> StageResult:
    args = [
        "--recovery-report-json", str(recovery_json),
        "--preflight-report-json", str(preflight_json),
        "--quote-report-json", str(quote_json),
        "--proof-timing-status", config.proof_timing_status,
        "--proof-timing-source", config.proof_timing_source,
        "--output-root", str(config.readiness_summary_output_root),
    ]
    if config.proof_timing_detail:
        args.extend(["--proof-timing-detail", config.proof_timing_detail])
    return _call_cli("readiness_summary", readiness_summary_cli_main, args)


def _run_operator_status(
    config: TrackBReadinessCheckRunnerConfig,
    runner_report_json: Path,
    recovery_json: Path | None,
    preflight_json: Path | None,
    quote_json: Path | None,
    readiness_json: Path | None,
    databento_observer_json: Path | None,
) -> StageResult:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_readiness_check_runner_report_json=runner_report_json,
            databento_candle_observer_report_json=databento_observer_json,
            databento_candle_observer_heartbeat_json=_existing_path(
                Path(config.databento_observer_output_root) / "latest_databento_candle_observer_heartbeat.json"
            ),
            readiness_summary_json=readiness_json,
            recovery_report_json=recovery_json,
            preflight_report_json=preflight_json,
            quote_report_json=quote_json,
            output_root=config.operator_status_output_root,
        )
    )
    return StageResult(
        name="operator_status",
        exit_code=0,
        report_json=result.report_json,
        payload=result.report,
    )


def _existing_path(path: Path) -> Path | None:
    return path if path.exists() else None


def _call_cli(name: str, main_func: Callable[[Sequence[str]], int], args: Sequence[str]) -> StageResult:
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = int(main_func(args))
    raw_stdout = stdout.getvalue().strip()
    payload: dict[str, object] = {}
    if raw_stdout:
        payload = json.loads(raw_stdout.splitlines()[-1])
    report_json = Path(str(payload["report_json"])) if payload.get("report_json") else None
    if report_json is not None and report_json.exists():
        payload = {**payload, **_read_json(report_json)}
    return StageResult(name=name, exit_code=exit_code, report_json=report_json, payload=payload, stdout=raw_stdout)


def _read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


def _recovery_verdict(payload: dict[str, object]) -> str:
    return str(payload.get("classification") or payload.get("final_readiness_verdict") or "")


def _preflight_verdict(payload: dict[str, object]) -> str:
    return str(payload.get("classification") or payload.get("final_readiness_verdict") or "")


def _current_quote_report_path(result: StageResult) -> Path | None:
    for key in ("source_report_path", "quote_report_json", "current_quote_report_json"):
        value = result.payload.get(key)
        if value:
            return Path(str(value))
    diagnostics_path = result.payload.get("report_json_path")
    if diagnostics_path and str(result.payload.get("schema_version")) == "track_b_databento_current_quote_v1":
        return Path(str(diagnostics_path))
    return None


def _primary_blocker(payload: dict[str, object], fallback: str) -> str:
    return str(payload.get("primary_blocker") or payload.get("provider_error") or payload.get("failure_or_ambiguity") or fallback)


def _required_action(payload: dict[str, object], fallback: str) -> str:
    return str(payload.get("required_next_action") or payload.get("required_action") or fallback)


def _secondary_blockers(stage_results: dict[str, StageResult]) -> list[str]:
    blockers: list[str] = []
    for result in stage_results.values():
        payload = result.payload
        if payload.get("primary_blocker"):
            blockers.append(str(payload["primary_blocker"]))
        blockers.extend(str(item) for item in payload.get("secondary_blockers") or ())
    return _dedupe(blockers)


def _value(payload: dict[str, object], *keys: str) -> object:
    for key in keys:
        if payload.get(key) is not None:
            return payload[key]
    return "NOT_PROVIDED"


def _dedupe(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            unique.append(value)
    return unique
