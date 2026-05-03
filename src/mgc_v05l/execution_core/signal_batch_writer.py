"""Track B no-submit signal batch inbox writer boundary."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .shadow_signal import ShadowSignalValidationConfig, ShadowSignalValidationVerdict, validate_shadow_signal
from .signal_batch import SignalBatch


DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/signal_batch_writer")


class SignalBatchWriterVerdict(str, Enum):
    WROTE_BATCH = "SIGNAL_BATCH_WRITER_WROTE_BATCH"
    BLOCKED_INVALID_BATCH = "SIGNAL_BATCH_WRITER_BLOCKED_INVALID_BATCH"
    BLOCKED_INVALID_SIGNAL = "SIGNAL_BATCH_WRITER_BLOCKED_INVALID_SIGNAL"
    BLOCKED_SCHEMA_ERROR = "SIGNAL_BATCH_WRITER_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class SignalBatchWriterResult:
    verdict: SignalBatchWriterVerdict
    report_json: Path
    report: dict[str, Any]
    batch_json: Path | None


def write_signal_batch_to_inbox(
    *,
    inbox_dir: Path,
    batch_payload: Mapping[str, Any] | None = None,
    signal_payloads: Sequence[Mapping[str, Any]] | None = None,
    batch_id: str | None = None,
    shadow_run_id: str | None = None,
    source_id: str | None = None,
    expected_account_id: str | None = None,
    output_root: Path = DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT,
    writer_id: str | None = None,
    now: datetime | None = None,
) -> SignalBatchWriterResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_writer_id = writer_id or f"signal_batch_writer_{uuid.uuid4().hex}"
    actual_batch_payload = _build_batch_payload(
        batch_payload=batch_payload,
        signal_payloads=signal_payloads,
        batch_id=batch_id,
        shadow_run_id=shadow_run_id,
        source_id=source_id,
        expected_account_id=expected_account_id,
        generated_at=actual_now,
    )
    actual_batch_id = str(actual_batch_payload.get("batch_id") or f"signal_batch_{uuid.uuid4().hex}").strip()
    report_json = Path(output_root) / actual_writer_id / "signal_batch_writer_report.json"

    try:
        batch = SignalBatch.from_mapping(actual_batch_payload)
        batch_verdict, batch_blocker, batch_action = _batch_write_blocker(batch)
        if batch_verdict is not None:
            return _write_report(
                report_json=report_json,
                verdict=batch_verdict,
                now=actual_now,
                writer_id=actual_writer_id,
                batch_payload=actual_batch_payload,
                batch_json=None,
                inbox_dir=inbox_dir,
                primary_blocker=batch_blocker,
                required_next_action=batch_action,
                validation_reports=[],
            )

        validation_reports = _validate_signals(
            batch=batch,
            expected_account_id=expected_account_id or batch.expected_account_id,
            output_root=Path(output_root) / actual_writer_id / "signal_validation",
            now=actual_now,
        )
        invalid_reports = [row for row in validation_reports if row.get("shadow_signal_validation_verdict") != ShadowSignalValidationVerdict.VALID_FOR_REVIEW.value]
        if invalid_reports:
            return _write_report(
                report_json=report_json,
                verdict=SignalBatchWriterVerdict.BLOCKED_INVALID_SIGNAL,
                now=actual_now,
                writer_id=actual_writer_id,
                batch_payload=actual_batch_payload,
                batch_json=None,
                inbox_dir=inbox_dir,
                primary_blocker="One or more signals failed Track B shadow signal validation.",
                required_next_action="Fix invalid signal payloads before writing this batch to the listener inbox.",
                validation_reports=validation_reports,
            )

        final_path = _atomic_write_batch(
            inbox_dir=Path(inbox_dir),
            batch_payload=actual_batch_payload,
            batch_id=actual_batch_id,
            source_id=source_id,
            generated_at=actual_now,
        )
        return _write_report(
            report_json=report_json,
            verdict=SignalBatchWriterVerdict.WROTE_BATCH,
            now=actual_now,
            writer_id=actual_writer_id,
            batch_payload=actual_batch_payload,
            batch_json=final_path,
            inbox_dir=inbox_dir,
            primary_blocker=None,
            required_next_action="Let shadow_listener process the written no-submit signal batch file.",
            validation_reports=validation_reports,
        )
    except (TypeError, ValueError, OSError) as exc:
        return _write_report(
            report_json=report_json,
            verdict=SignalBatchWriterVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            writer_id=actual_writer_id,
            batch_payload=actual_batch_payload,
            batch_json=None,
            inbox_dir=inbox_dir,
            primary_blocker=str(exc),
            required_next_action="Fix signal batch writer input schema before retrying.",
            validation_reports=[],
        )


def _build_batch_payload(
    *,
    batch_payload: Mapping[str, Any] | None,
    signal_payloads: Sequence[Mapping[str, Any]] | None,
    batch_id: str | None,
    shadow_run_id: str | None,
    source_id: str | None,
    expected_account_id: str | None,
    generated_at: datetime,
) -> dict[str, Any]:
    if batch_payload is not None and signal_payloads:
        raise ValueError("Provide either batch_payload or signal_payloads, not both.")
    if batch_payload is not None:
        payload = dict(batch_payload)
        if batch_id:
            payload["batch_id"] = batch_id
        if shadow_run_id:
            payload["shadow_run_id"] = shadow_run_id
        if expected_account_id:
            payload["expected_account_id"] = expected_account_id
    else:
        signals = list(signal_payloads or ())
        payload = {
            "batch_id": batch_id or f"signal_batch_{uuid.uuid4().hex}",
            "shadow_run_id": shadow_run_id,
            "mode": "PAPER",
            "expected_account_id": expected_account_id,
            "signal_items": [{"signal": dict(signal)} for signal in signals],
            "submit_enabled": False,
            "live_money_readiness": False,
        }
    metadata = dict(payload.get("writer_metadata") or {})
    metadata.update(
        {
            "source_id": source_id,
            "writer_generated_at": generated_at.isoformat(),
            "writer_boundary": "signal_batch_writer",
        }
    )
    payload["writer_metadata"] = metadata
    payload.setdefault("submit_enabled", False)
    payload["live_money_readiness"] = False
    return payload


def _batch_write_blocker(batch: SignalBatch) -> tuple[SignalBatchWriterVerdict | None, str | None, str]:
    if batch.mode != "PAPER":
        return SignalBatchWriterVerdict.BLOCKED_INVALID_BATCH, "Signal batch writer accepts PAPER mode only.", "Use PAPER mode for no-submit listener input."
    if batch.submit_enabled:
        return SignalBatchWriterVerdict.BLOCKED_INVALID_BATCH, "Signal batch writer is no-submit; submit_enabled must be false.", "Set submit_enabled=false."
    if not batch.signal_items:
        return SignalBatchWriterVerdict.BLOCKED_INVALID_BATCH, "Signal batch contains no signals.", "Provide one or more signal observations."
    return None, None, "Write signal batch to listener inbox."


def _validate_signals(
    *,
    batch: SignalBatch,
    expected_account_id: str | None,
    output_root: Path,
    now: datetime,
) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for index, signal_payload in enumerate(batch.signal_items, start=1):
        result = validate_shadow_signal(
            payload=signal_payload,
            config=ShadowSignalValidationConfig(expected_account_id=expected_account_id, output_root=output_root),
            run_id=f"signal_{index:04d}",
            now=now,
        )
        reports.append(
            {
                "signal_index": index,
                "signal_id": result.report.get("signal_id"),
                "shadow_signal_validation_verdict": result.verdict.value,
                "primary_blocker": result.report.get("primary_blocker"),
                "validation_report_json": str(result.report_json),
            }
        )
    return reports


def _atomic_write_batch(
    *,
    inbox_dir: Path,
    batch_payload: Mapping[str, Any],
    batch_id: str,
    source_id: str | None,
    generated_at: datetime,
) -> Path:
    inbox_dir.mkdir(parents=True, exist_ok=True)
    safe_source = _safe_name(source_id or "manual")
    safe_batch = _safe_name(batch_id)
    stamp = generated_at.strftime("%Y%m%dT%H%M%S%fZ")
    for _attempt in range(10):
        suffix = uuid.uuid4().hex[:10]
        final_path = inbox_dir / f"{stamp}_{safe_source}_{safe_batch}_{suffix}.json"
        if final_path.exists():
            continue
        tmp_path = inbox_dir / f".{final_path.name}.tmp"
        tmp_path.write_text(json.dumps(to_jsonable(batch_payload), indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(final_path)
        return final_path
    raise FileExistsError("could not allocate a unique signal batch inbox filename.")


def _safe_name(value: str) -> str:
    safe = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value.strip())
    return safe[:80] or "unnamed"


def _write_report(
    *,
    report_json: Path,
    verdict: SignalBatchWriterVerdict,
    now: datetime,
    writer_id: str,
    batch_payload: Mapping[str, Any],
    batch_json: Path | None,
    inbox_dir: Path,
    primary_blocker: str | None,
    required_next_action: str,
    validation_reports: Sequence[Mapping[str, Any]],
) -> SignalBatchWriterResult:
    report = {
        "schema_version": "track_b_signal_batch_writer_v1",
        "generated_at": now.isoformat(),
        "signal_batch_writer_id": writer_id,
        "signal_batch_writer_verdict": verdict.value,
        "batch_file_written": batch_json is not None,
        "batch_id": batch_payload.get("batch_id"),
        "shadow_run_id": batch_payload.get("shadow_run_id") or batch_payload.get("run_id"),
        "source_id": (batch_payload.get("writer_metadata") or {}).get("source_id") if isinstance(batch_payload.get("writer_metadata"), Mapping) else None,
        "inbox_dir": str(inbox_dir),
        "batch_json_path": None if batch_json is None else str(batch_json),
        "total_signals": len(batch_payload.get("signal_items") or batch_payload.get("signals") or ()),
        "signal_validation_reports": list(validation_reports),
        "listener_invoked": False,
        "runner_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "strategy_execution_attempted": False,
        "dynamic_scoring_implemented": False,
        "paper_proof_cli_wired": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": [str(row.get("primary_blocker")) for row in validation_reports if row.get("primary_blocker")],
        "required_next_action": required_next_action,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return SignalBatchWriterResult(verdict=verdict, report_json=report_json, report=report, batch_json=batch_json)
