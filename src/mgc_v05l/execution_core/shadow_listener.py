"""Track B no-submit shadow listener skeleton.

This module is a poll-once file ingestion boundary. It watches an inbox for
signal batch JSON files and runs the existing no-submit shadow replay runner.
It does not execute strategies, connect to market data or broker APIs, or
submit orders.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable
from .shadow_replay_runner import ShadowReplayRunnerResult, ShadowReplayRunnerVerdict, run_shadow_replay


DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/shadow_listener")


class ShadowListenerVerdict(str, Enum):
    COMPLETED = "SHADOW_LISTENER_CYCLE_COMPLETED"
    COMPLETED_WITH_FAILURES = "SHADOW_LISTENER_CYCLE_COMPLETED_WITH_FAILURES"
    NO_FILES = "SHADOW_LISTENER_CYCLE_NO_FILES"
    BLOCKED_INVALID_CONFIG = "SHADOW_LISTENER_BLOCKED_INVALID_CONFIG"
    FILE_FAILED = "SHADOW_LISTENER_FILE_FAILED"
    BLOCKED_SCHEMA_ERROR = "SHADOW_LISTENER_BLOCKED_SCHEMA_ERROR"


class ShadowListenerHealthVerdict(str, Enum):
    OK = "SHADOW_LISTENER_HEALTH_OK"
    NO_FILES = "SHADOW_LISTENER_HEALTH_NO_FILES"
    DEGRADED_FAILURES = "SHADOW_LISTENER_HEALTH_DEGRADED_FAILURES"
    BLOCKED_INVALID_CONFIG = "SHADOW_LISTENER_HEALTH_BLOCKED_INVALID_CONFIG"
    UNKNOWN = "SHADOW_LISTENER_HEALTH_UNKNOWN"


@dataclass(frozen=True)
class ShadowListenerConfig:
    listener_id: str
    mode: str
    inbox_dir: Path
    processing_dir: Path
    processed_dir: Path
    failed_dir: Path
    output_root: Path
    proposal_policy_json: Path
    manifest_json: Path
    registry_json: Path
    readiness_summary_json: Path | None
    poll_once: bool
    file_glob: str
    submit_enabled: bool
    live_money_readiness: bool = False

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any], *, output_root_override: Path | None = None) -> "ShadowListenerConfig":
        return cls(
            listener_id=str(payload.get("listener_id") or f"shadow_listener_{uuid.uuid4().hex}").strip(),
            mode=str(payload.get("mode") or "").strip().upper(),
            inbox_dir=Path(str(payload.get("inbox_dir") or "")),
            processing_dir=Path(str(payload.get("processing_dir") or "")),
            processed_dir=Path(str(payload.get("processed_dir") or "")),
            failed_dir=Path(str(payload.get("failed_dir") or "")),
            output_root=Path(output_root_override or str(payload.get("output_root") or DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT)),
            proposal_policy_json=Path(str(payload.get("proposal_policy_json") or "")),
            manifest_json=Path(str(payload.get("manifest_json") or "")),
            registry_json=Path(str(payload.get("registry_json") or "")),
            readiness_summary_json=_optional_path(payload.get("readiness_summary_json")),
            poll_once=_bool(payload.get("poll_once", True)),
            file_glob=str(payload.get("file_glob") or "*.json").strip(),
            submit_enabled=_bool(payload.get("submit_enabled", False)),
            live_money_readiness=False,
        )


@dataclass(frozen=True)
class ShadowListenerResult:
    verdict: ShadowListenerVerdict
    report_json: Path
    report: dict[str, Any]


def run_shadow_listener_cycle(
    *,
    config_payload: Mapping[str, Any],
    output_root: Path | None = None,
    cycle_id: str | None = None,
    now: datetime | None = None,
) -> ShadowListenerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    fallback_listener_id = str(config_payload.get("listener_id") or f"shadow_listener_{uuid.uuid4().hex}")
    actual_cycle_id = cycle_id or f"shadow_listener_cycle_{uuid.uuid4().hex}"
    fallback_root = Path(output_root or config_payload.get("output_root") or DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT)
    report_json = fallback_root / fallback_listener_id / actual_cycle_id / "shadow_listener_cycle_summary.json"
    try:
        config = ShadowListenerConfig.from_mapping(config_payload, output_root_override=output_root)
        config_blocker = _config_blocker(config)
        if config_blocker is not None:
            blocker, action = config_blocker
            report = _blocked_report(
                report_json=report_json,
                now=actual_now,
                listener_id=config.listener_id,
                cycle_id=actual_cycle_id,
                verdict=ShadowListenerVerdict.BLOCKED_INVALID_CONFIG,
                blocker=blocker,
                action=action,
            )
            return _write(report_json, ShadowListenerVerdict.BLOCKED_INVALID_CONFIG, report)

        cycle_root = config.output_root / config.listener_id / actual_cycle_id
        report_json = cycle_root / "shadow_listener_cycle_summary.json"
        for directory in (config.inbox_dir, config.processing_dir, config.processed_dir, config.failed_dir, cycle_root):
            directory.mkdir(parents=True, exist_ok=True)

        files = sorted(path for path in config.inbox_dir.glob(config.file_glob) if path.is_file())
        if not files:
            report = _cycle_summary(
                report_json=report_json,
                now=actual_now,
                config=config,
                cycle_id=actual_cycle_id,
                verdict=ShadowListenerVerdict.NO_FILES,
                event_reports=[],
                primary_blocker=None,
                required_next_action="No files were available. Keep listener configured or place signal batch JSON into the inbox.",
            )
            return _write(report_json, ShadowListenerVerdict.NO_FILES, report)

        proposal_policy = _read_json(config.proposal_policy_json)
        manifest = _read_json(config.manifest_json)
        registry = _read_json(config.registry_json)
        readiness_summary = _read_json(config.readiness_summary_json)

        event_reports = [
            _process_file(
                source_path=source_path,
                config=config,
                cycle_root=cycle_root,
                cycle_id=actual_cycle_id,
                proposal_policy=proposal_policy,
                manifest=manifest,
                registry=registry,
                readiness_summary=readiness_summary,
                now=actual_now,
                index=index,
            )
            for index, source_path in enumerate(files, start=1)
        ]
        failed = sum(1 for event in event_reports if event["file_succeeded"] is False)
        verdict = ShadowListenerVerdict.COMPLETED if failed == 0 else ShadowListenerVerdict.COMPLETED_WITH_FAILURES
        report = _cycle_summary(
            report_json=report_json,
            now=actual_now,
            config=config,
            cycle_id=actual_cycle_id,
            verdict=verdict,
            event_reports=event_reports,
            primary_blocker=None if failed == 0 else "One or more listener files failed no-submit processing.",
            required_next_action=(
                "Review runner summaries for processed files. Submit gates remain external and required."
                if failed == 0
                else "Review failed listener event reports and source files before retrying."
            ),
        )
        return _write(report_json, verdict, report)
    except (TypeError, ValueError, OSError) as exc:
        report = _blocked_report(
            report_json=report_json,
            now=actual_now,
            listener_id=fallback_listener_id,
            cycle_id=actual_cycle_id,
            verdict=ShadowListenerVerdict.BLOCKED_SCHEMA_ERROR,
            blocker=str(exc),
            action="Fix listener input JSON or referenced file paths before polling again.",
        )
        return _write(report_json, ShadowListenerVerdict.BLOCKED_SCHEMA_ERROR, report)


def _process_file(
    *,
    source_path: Path,
    config: ShadowListenerConfig,
    cycle_root: Path,
    cycle_id: str,
    proposal_policy: Mapping[str, Any],
    manifest: Mapping[str, Any],
    registry: Mapping[str, Any],
    readiness_summary: Mapping[str, Any] | None,
    now: datetime,
    index: int,
) -> dict[str, Any]:
    event_id = f"file_{index:04d}_{source_path.stem}"
    event_json = cycle_root / "events" / event_id / "shadow_listener_event_report.json"
    claimed_path = _unique_target(config.processing_dir / source_path.name)
    source_path.rename(claimed_path)
    runner: ShadowReplayRunnerResult | None = None
    try:
        signal_batch = _read_json(claimed_path)
        runner = run_shadow_replay(
            signal_batch_payload=signal_batch,
            proposal_policy_payload=proposal_policy,
            manifest_payload=manifest,
            registry_payload=registry,
            readiness_summary_payload=readiness_summary,
            readiness_summary_json=config.readiness_summary_json,
            output_root=cycle_root / "shadow_replay_runs",
            run_id=f"{cycle_id}_{event_id}",
            now=now,
        )
        succeeded = runner.verdict in {ShadowReplayRunnerVerdict.COMPLETED_FOR_REVIEW, ShadowReplayRunnerVerdict.COMPLETED_WITH_BLOCKERS}
        final_path = _unique_target((config.processed_dir if succeeded else config.failed_dir) / claimed_path.name)
        claimed_path.rename(final_path)
        report = _event_report(
            event_json=event_json,
            now=now,
            config=config,
            cycle_id=cycle_id,
            event_id=event_id,
            source_path=source_path,
            claimed_path=claimed_path,
            final_path=final_path,
            succeeded=succeeded,
            runner=runner,
            primary_blocker=None if succeeded else str(runner.report.get("primary_blocker") or "Shadow replay runner blocked this file."),
            required_next_action=str(runner.report.get("required_next_action") or "Review shadow replay runner report."),
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        final_path = _unique_target(config.failed_dir / claimed_path.name)
        if claimed_path.exists():
            claimed_path.rename(final_path)
        report = _event_report(
            event_json=event_json,
            now=now,
            config=config,
            cycle_id=cycle_id,
            event_id=event_id,
            source_path=source_path,
            claimed_path=claimed_path,
            final_path=final_path,
            succeeded=False,
            runner=None,
            primary_blocker=str(exc),
            required_next_action="Fix signal batch JSON before retrying listener ingestion.",
        )
    event_json.parent.mkdir(parents=True, exist_ok=True)
    event_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return report


def _event_report(
    *,
    event_json: Path,
    now: datetime,
    config: ShadowListenerConfig,
    cycle_id: str,
    event_id: str,
    source_path: Path,
    claimed_path: Path,
    final_path: Path,
    succeeded: bool,
    runner: ShadowReplayRunnerResult | None,
    primary_blocker: str | None,
    required_next_action: str,
) -> dict[str, Any]:
    runner_report = {} if runner is None else runner.report
    return {
        "schema_version": "track_b_shadow_listener_event_v1",
        "generated_at": now.isoformat(),
        "listener_id": config.listener_id,
        "listener_cycle_id": cycle_id,
        "listener_event_id": event_id,
        "listener_file_verdict": ShadowListenerVerdict.COMPLETED.value if succeeded else ShadowListenerVerdict.FILE_FAILED.value,
        "source_path": str(source_path),
        "claimed_path": str(claimed_path),
        "final_path": str(final_path),
        "file_succeeded": succeeded,
        "runner_verdict": runner_report.get("runner_verdict"),
        "runner_summary_path": None if runner is None else str(runner.report_json),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": list(runner_report.get("secondary_blockers") or ()),
        "required_next_action": required_next_action,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "paper_proof_cli_wired": False,
        "event_report_json": str(event_json),
    }


def _cycle_summary(
    *,
    report_json: Path,
    now: datetime,
    config: ShadowListenerConfig,
    cycle_id: str,
    verdict: ShadowListenerVerdict,
    event_reports: list[Mapping[str, Any]],
    primary_blocker: str | None,
    required_next_action: str,
) -> dict[str, Any]:
    failed_events = [event for event in event_reports if event.get("file_succeeded") is False]
    return {
        "schema_version": "track_b_shadow_listener_cycle_v1",
        "generated_at": now.isoformat(),
        "listener_id": config.listener_id,
        "listener_cycle_id": cycle_id,
        "listener_verdict": verdict.value,
        "mode": config.mode,
        "inbox_dir": str(config.inbox_dir),
        "processing_dir": str(config.processing_dir),
        "processed_dir": str(config.processed_dir),
        "failed_dir": str(config.failed_dir),
        "file_glob": config.file_glob,
        "files_discovered": len(event_reports),
        "files_claimed": len(event_reports),
        "files_processed": len(event_reports),
        "files_succeeded": sum(1 for event in event_reports if event.get("file_succeeded") is True),
        "files_failed": len(failed_events),
        "processed_paths": [str(event.get("final_path")) for event in event_reports if event.get("file_succeeded") is True],
        "failed_paths": [str(event.get("final_path")) for event in failed_events],
        "runner_summary_paths": [str(event.get("runner_summary_path")) for event in event_reports if event.get("runner_summary_path")],
        "event_report_paths": [str(event.get("event_report_json")) for event in event_reports],
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": _summary_secondary_blockers(event_reports),
        "required_next_action": required_next_action,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "strategy_execution_attempted": False,
        "paper_proof_cli_wired": False,
        "poll_once": config.poll_once,
        "health_report_path": str(report_json.parent / "shadow_listener_health.json"),
        "latest_health_report_path": str(report_json.parent.parent / "latest_shadow_listener_health.json"),
        "report_json_path": str(report_json),
    }


def _blocked_report(
    *,
    report_json: Path,
    now: datetime,
    listener_id: str,
    cycle_id: str,
    verdict: ShadowListenerVerdict,
    blocker: str,
    action: str,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_shadow_listener_cycle_v1",
        "generated_at": now.isoformat(),
        "listener_id": listener_id,
        "listener_cycle_id": cycle_id,
        "listener_verdict": verdict.value,
        "inbox_dir": None,
        "processing_dir": None,
        "processed_dir": None,
        "failed_dir": None,
        "files_discovered": 0,
        "files_claimed": 0,
        "files_processed": 0,
        "files_succeeded": 0,
        "files_failed": 0,
        "processed_paths": [],
        "failed_paths": [],
        "runner_summary_paths": [],
        "event_report_paths": [],
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": blocker,
        "secondary_blockers": [],
        "required_next_action": action,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "strategy_execution_attempted": False,
        "paper_proof_cli_wired": False,
        "health_report_path": str(report_json.parent / "shadow_listener_health.json"),
        "latest_health_report_path": str(report_json.parent.parent / "latest_shadow_listener_health.json"),
        "report_json_path": str(report_json),
    }


def _config_blocker(config: ShadowListenerConfig) -> tuple[str, str] | None:
    if config.mode not in {"SHADOW", "PAPER_REVIEW"}:
        return "Shadow listener accepts SHADOW or PAPER_REVIEW no-submit mode only.", "Use mode=SHADOW or mode=PAPER_REVIEW."
    if not config.listener_id:
        return "listener_id is required.", "Provide listener_id in listener config."
    if not config.poll_once:
        return "Only poll_once=true is supported by the current listener skeleton.", "Use poll_once=true; daemon/watch mode is future work."
    if not config.file_glob:
        return "file_glob is required.", "Provide a file glob such as *.json."
    if config.submit_enabled:
        return "Shadow listener is no-submit; submit_enabled must be false.", "Set submit_enabled=false."
    if config.live_money_readiness:
        return "Shadow listener cannot claim live_money_readiness.", "Keep live_money_readiness=false."
    for label, path in (
        ("inbox_dir", config.inbox_dir),
        ("processing_dir", config.processing_dir),
        ("processed_dir", config.processed_dir),
        ("failed_dir", config.failed_dir),
        ("proposal_policy_json", config.proposal_policy_json),
        ("manifest_json", config.manifest_json),
        ("registry_json", config.registry_json),
    ):
        if str(path) in {"", "."}:
            return f"{label} is required.", f"Provide {label} in listener config."
    return None


def _summary_secondary_blockers(event_reports: list[Mapping[str, Any]]) -> list[str]:
    blockers: list[str] = []
    seen: set[str] = set()
    for event in event_reports:
        for blocker in (event.get("primary_blocker"), *tuple(event.get("secondary_blockers") or ())):
            if not blocker:
                continue
            text = str(blocker)
            if text not in seen:
                seen.add(text)
                blockers.append(text)
    return blockers


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


def _unique_target(target: Path) -> Path:
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        return target
    stem = target.stem
    suffix = target.suffix
    for index in range(1, 10_000):
        candidate = target.with_name(f"{stem}_{index:04d}{suffix}")
        if not candidate.exists():
            candidate.parent.mkdir(parents=True, exist_ok=True)
            return candidate
    raise ValueError(f"could not allocate unique target path for {target}")


def _optional_path(value: object) -> Path | None:
    if value in (None, ""):
        return None
    return Path(str(value))


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _write(report_json: Path, verdict: ShadowListenerVerdict, report: dict[str, Any]) -> ShadowListenerResult:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    health_report = _health_report_from_cycle(report=report, now=str(report["generated_at"]), report_json=report_json)
    health_report_path = Path(str(report["health_report_path"]))
    latest_health_report_path = Path(str(report["latest_health_report_path"]))
    health_report_path.parent.mkdir(parents=True, exist_ok=True)
    latest_health_report_path.parent.mkdir(parents=True, exist_ok=True)
    health_payload = json.dumps(to_jsonable(health_report), indent=2, sort_keys=True)
    health_report_path.write_text(health_payload, encoding="utf-8")
    latest_health_report_path.write_text(health_payload, encoding="utf-8")
    return ShadowListenerResult(verdict=verdict, report_json=report_json, report=report)


def _health_report_from_cycle(*, report: Mapping[str, Any], now: str, report_json: Path) -> dict[str, Any]:
    health_verdict = _health_verdict(str(report.get("listener_verdict") or ""))
    failed_paths = list(report.get("failed_paths") or ())
    processed_paths = list(report.get("processed_paths") or ())
    return {
        "schema_version": "track_b_shadow_listener_health_v1",
        "generated_at": now,
        "listener_id": report.get("listener_id"),
        "listener_cycle_id": report.get("listener_cycle_id"),
        "health_verdict": health_verdict.value,
        "last_cycle_verdict": report.get("listener_verdict"),
        "last_cycle_generated_at": report.get("generated_at"),
        "inbox_dir": report.get("inbox_dir"),
        "processing_dir": report.get("processing_dir"),
        "processed_dir": report.get("processed_dir"),
        "failed_dir": report.get("failed_dir"),
        "files_discovered": report.get("files_discovered", 0),
        "files_processed": report.get("files_processed", 0),
        "files_succeeded": report.get("files_succeeded", 0),
        "files_failed": report.get("files_failed", 0),
        "last_success_at": now if processed_paths else None,
        "last_failure_at": now if failed_paths or report.get("primary_blocker") else None,
        "last_primary_blocker": report.get("primary_blocker"),
        "last_required_next_action": report.get("required_next_action"),
        "latest_cycle_summary_path": str(report_json),
        "latest_runner_summary_paths": list(report.get("runner_summary_paths") or ()),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "observer_status_only": True,
        "dashboard_is_not_authority": True,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "paper_proof_cli_wired": False,
        "health_report_path": report.get("health_report_path"),
        "latest_health_report_path": report.get("latest_health_report_path"),
    }


def _health_verdict(listener_verdict: str) -> ShadowListenerHealthVerdict:
    if listener_verdict == ShadowListenerVerdict.COMPLETED.value:
        return ShadowListenerHealthVerdict.OK
    if listener_verdict == ShadowListenerVerdict.NO_FILES.value:
        return ShadowListenerHealthVerdict.NO_FILES
    if listener_verdict in {ShadowListenerVerdict.COMPLETED_WITH_FAILURES.value, ShadowListenerVerdict.FILE_FAILED.value}:
        return ShadowListenerHealthVerdict.DEGRADED_FAILURES
    if listener_verdict == ShadowListenerVerdict.BLOCKED_INVALID_CONFIG.value:
        return ShadowListenerHealthVerdict.BLOCKED_INVALID_CONFIG
    return ShadowListenerHealthVerdict.UNKNOWN
