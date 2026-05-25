"""Track B Asian Drift explicit state snapshot boundary.

This module does not compute Asian Drift from raw candles. It validates and
writes an explicit completed 5m Asia Drift state/feature snapshot so the Track B
strategy rule runner can observe it without importing Track A or research code.
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


DEFAULT_TRACK_B_ASIAN_DRIFT_STATE_OUTPUT_ROOT = Path("outputs/track_b_execution_core/asian_drift_state")


class TrackBAsianDriftStateVerdict(str, Enum):
    WROTE_SNAPSHOT = "TRACK_B_ASIAN_DRIFT_STATE_WROTE_SNAPSHOT"
    BLOCKED_INVALID_STATE = "TRACK_B_ASIAN_DRIFT_STATE_BLOCKED_INVALID_STATE"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_ASIAN_DRIFT_STATE_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBAsianDriftStateResult:
    verdict: TrackBAsianDriftStateVerdict
    report_json: Path
    report: dict[str, Any]
    snapshot_json: Path | None
    snapshot: dict[str, Any] | None


def write_track_b_asian_drift_state_snapshot(
    *,
    state_payload: Mapping[str, Any],
    source_payload_path: Path | None = None,
    expected_account_id: str | None = "DUM882026",
    account_id: str = "DUM882026",
    contract_key: str = "MGC-202606",
    instrument_family: str = "MGC",
    source_id: str = "track_b_asian_drift_state",
    strategy_id: str = "asian_drift_v1",
    lane_id: str = "mgc_example_long_lmt_day",
    output_root: Path = DEFAULT_TRACK_B_ASIAN_DRIFT_STATE_OUTPUT_ROOT,
    builder_id: str | None = None,
    now: datetime | None = None,
) -> TrackBAsianDriftStateResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_builder_id = builder_id or f"track_b_asian_drift_state_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_builder_id / "asian_drift_state_builder_report.json"
    snapshot_json = Path(output_root) / actual_builder_id / "asian_drift_5m_state_snapshot.json"
    try:
        blocker = _validation_blocker(
            payload=state_payload,
            expected_account_id=expected_account_id,
            expected_contract_key=contract_key,
            expected_instrument_family=instrument_family,
        )
        if blocker is not None:
            return _write_report(
                report_json=report_json,
                snapshot_json=snapshot_json,
                verdict=TrackBAsianDriftStateVerdict.BLOCKED_INVALID_STATE,
                now=actual_now,
                builder_id=actual_builder_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                snapshot=None,
                primary_blocker=blocker,
                required_next_action="Provide an explicit completed 5m Asian Drift state/feature snapshot before Track B watch.",
            )
        snapshot = _snapshot(
            payload=state_payload,
            now=actual_now,
            source_id=source_id,
            source_payload_path=source_payload_path,
            account_id=account_id,
            contract_key=contract_key,
            instrument_family=instrument_family,
            strategy_id=strategy_id,
            lane_id=lane_id,
        )
        return _write_report(
            report_json=report_json,
            snapshot_json=snapshot_json,
            verdict=TrackBAsianDriftStateVerdict.WROTE_SNAPSHOT,
            now=actual_now,
            builder_id=actual_builder_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            snapshot=snapshot,
            primary_blocker=None,
            required_next_action="Run track_b_strategy_rule_runner_cli --rule-mode ASIAN_DRIFT_V1 against latest_asian_drift_5m_state_snapshot.json.",
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_report(
            report_json=report_json,
            snapshot_json=snapshot_json,
            verdict=TrackBAsianDriftStateVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            builder_id=actual_builder_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            snapshot=None,
            primary_blocker=str(exc),
            required_next_action="Fix Asian Drift state snapshot input schema before retrying.",
        )


def _validation_blocker(
    *,
    payload: Mapping[str, Any],
    expected_account_id: str | None,
    expected_contract_key: str,
    expected_instrument_family: str,
) -> str | None:
    account_id = _optional_text(payload.get("account_id") or payload.get("expected_account_id"))
    if expected_account_id and account_id and account_id != expected_account_id:
        return f"Asian Drift snapshot account_id {account_id} does not match expected_account_id {expected_account_id}."
    contract_key = _optional_text(payload.get("contract_key") or payload.get("local_execution_contract_key"))
    if contract_key != expected_contract_key:
        return f"Asian Drift snapshot must be scoped to {expected_contract_key}."
    instrument_family = _optional_text(payload.get("instrument_family") or payload.get("symbol"))
    if instrument_family and instrument_family != expected_instrument_family:
        return f"Asian Drift snapshot must be scoped to instrument_family={expected_instrument_family}."
    required = {
        "timeframe": _optional_text(payload.get("timeframe")),
        "asia_drift_state": _optional_text(payload.get("asia_drift_state") or payload.get("state")),
        "asia_drift_regime": _optional_text(payload.get("asia_drift_regime") or payload.get("regime")),
        "hypothetical_entry_ready": _optional_bool(payload.get("hypothetical_entry_ready")),
        "entry_window_open": _optional_bool(payload.get("entry_window_open")),
        "in_scope": _optional_bool(payload.get("in_scope")),
        "feature_version": _optional_text(payload.get("feature_version")),
        "calibration_profile": _optional_text(payload.get("calibration_profile")),
        "close": _optional_text(payload.get("close") or payload.get("last")),
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        return "Asian Drift state snapshot is missing required fields: " + ", ".join(missing)
    if required["timeframe"] != "5m":
        return "Asian Drift state snapshot must use timeframe=5m completed decision bars."
    provider_mode = _optional_text(payload.get("quote_provider_mode"))
    if provider_mode != "REALTIME":
        return "Asian Drift state snapshot requires quote_provider_mode=REALTIME."
    if payload.get("realtime_quote_received") is not True:
        return "Asian Drift state snapshot requires realtime_quote_received=true."
    if payload.get("current_quote_available") is not True:
        return "Asian Drift state snapshot requires current_quote_available=true."
    return None


def _snapshot(
    *,
    payload: Mapping[str, Any],
    now: datetime,
    source_id: str,
    source_payload_path: Path | None,
    account_id: str,
    contract_key: str,
    instrument_family: str,
    strategy_id: str,
    lane_id: str,
) -> dict[str, Any]:
    metadata_value = payload.get("metadata")
    metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
    return {
        **dict(payload),
        "schema_version": "track_b_asian_drift_5m_state_snapshot_v1",
        "source_id": source_id,
        "generated_at": now.isoformat(),
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "account_id": _optional_text(payload.get("account_id")) or account_id,
        "contract_key": contract_key,
        "instrument_family": instrument_family,
        "strategy_id": _optional_text(payload.get("strategy_id")) or strategy_id,
        "signal_family": _optional_text(payload.get("signal_family")) or strategy_id,
        "lane_id": _optional_text(payload.get("lane_id")) or lane_id,
        "timeframe": "5m",
        "asia_drift_state": _optional_text(payload.get("asia_drift_state") or payload.get("state")),
        "asia_drift_regime": _optional_text(payload.get("asia_drift_regime") or payload.get("regime")),
        "hypothetical_entry_ready": _optional_bool(payload.get("hypothetical_entry_ready")),
        "entry_window_open": _optional_bool(payload.get("entry_window_open")),
        "in_scope": _optional_bool(payload.get("in_scope")),
        "session_timeout": _optional_bool(payload.get("session_timeout")) or False,
        "asian_drift_state_ready": True,
        "metadata": {
            **dict(metadata),
            "track_b_asian_drift_state_boundary": "track_b_asian_drift_state",
            "track_b_does_not_infer_asian_drift_from_raw_candles": True,
            "paper_proof_cli_called": False,
            "broker_state_mutated": False,
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _write_report(
    *,
    report_json: Path,
    snapshot_json: Path,
    verdict: TrackBAsianDriftStateVerdict,
    now: datetime,
    builder_id: str,
    source_id: str,
    source_payload_path: Path | None,
    snapshot: dict[str, Any] | None,
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBAsianDriftStateResult:
    output_root = report_json.parent.parent
    latest_report_json = output_root / "latest_asian_drift_state_builder_report.json"
    latest_snapshot_json = output_root / "latest_asian_drift_5m_state_snapshot.json"
    wrote_snapshot = snapshot is not None and verdict == TrackBAsianDriftStateVerdict.WROTE_SNAPSHOT
    watch_verdict = "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    if wrote_snapshot:
        watch_verdict = str(snapshot.get("asian_drift_diagnostic_classification") or "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION")
    report = {
        "schema_version": "track_b_asian_drift_state_builder_report_v1",
        "generated_at": now.isoformat(),
        "asian_drift_state_builder_id": builder_id,
        "asian_drift_state_builder_verdict": verdict.value,
        "asian_drift_watch_verdict": watch_verdict,
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "strategy_id": None if snapshot is None else snapshot.get("strategy_id"),
        "rule_mode": "ASIAN_DRIFT_V1",
        "asian_drift_state_snapshot_path": str(snapshot_json) if wrote_snapshot else None,
        "latest_asian_drift_state_snapshot_path": str(latest_snapshot_json) if wrote_snapshot else None,
        "asian_drift_state_ready": wrote_snapshot,
        "asia_drift_state": None if snapshot is None else snapshot.get("asia_drift_state"),
        "asia_drift_regime": None if snapshot is None else snapshot.get("asia_drift_regime"),
        "hypothetical_entry_ready": None if snapshot is None else snapshot.get("hypothetical_entry_ready"),
        "entry_window_open": None if snapshot is None else snapshot.get("entry_window_open"),
        "in_scope": None if snapshot is None else snapshot.get("in_scope"),
        "asian_drift_diagnostic_classification": None if snapshot is None else snapshot.get("asian_drift_diagnostic_classification"),
        "late_join_classification": None if snapshot is None else snapshot.get("late_join_classification"),
        "late_join_diagnostic": False if snapshot is None else snapshot.get("late_join_diagnostic", False),
        "anchor_required": None if snapshot is None else snapshot.get("anchor_required"),
        "anchor_observed": None if snapshot is None else snapshot.get("anchor_observed"),
        "anchor_window_start": None if snapshot is None else snapshot.get("anchor_window_start"),
        "anchor_window_end": None if snapshot is None else snapshot.get("anchor_window_end"),
        "runtime_context_start": None if snapshot is None else snapshot.get("runtime_context_start"),
        "missing_anchor_reason": None if snapshot is None else snapshot.get("missing_anchor_reason"),
        "drift_observed_after_anchor": None if snapshot is None else snapshot.get("drift_observed_after_anchor"),
        "late_join_policy": None if snapshot is None else snapshot.get("late_join_policy"),
        "hypothetical_late_join_score": None if snapshot is None else snapshot.get("hypothetical_late_join_score"),
        "operator_explanation": None if snapshot is None else snapshot.get("operator_explanation"),
        "no_mutation": True,
        "primary_blocker": primary_blocker,
        "required_next_action": required_next_action,
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(latest_report_json),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    if wrote_snapshot:
        snapshot_payload = json.dumps(to_jsonable(snapshot), indent=2, sort_keys=True)
        snapshot_json.write_text(snapshot_payload, encoding="utf-8")
        latest_snapshot_json.write_text(snapshot_payload, encoding="utf-8")
    return TrackBAsianDriftStateResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        snapshot_json=snapshot_json if wrote_snapshot else None,
        snapshot=snapshot,
    )


def _optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = _optional_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    return None


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
