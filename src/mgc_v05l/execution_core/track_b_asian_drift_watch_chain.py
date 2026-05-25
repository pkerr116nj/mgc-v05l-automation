"""Track B no-submit Asian Drift watch chain.

This wrapper is intentionally bounded and no-submit. It turns accumulated MGC
1m/5m candle artifacts into completed 5m candles, delegates feature rows and
state production to the existing Asian Drift boundaries, and finally runs the
ASIAN_DRIFT_V1 no-submit rule watch.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .track_b_asian_drift_feature_rows import (
    DEFAULT_TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_OUTPUT_ROOT,
    RECOVERY_CONFIRMED,
    TrackBAsianDriftFeatureRowsResult,
    produce_track_b_asian_drift_feature_rows,
)
from .track_b_phase1_runtime_candle_adapter import (
    legacy_p0_runtime_candle_path_blocker,
    normalize_phase1_runtime_candle_payload,
)
from .track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerResult,
    run_track_b_strategy_rule,
)


DEFAULT_TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_OUTPUT_ROOT = DEFAULT_TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_OUTPUT_ROOT


class TrackBAsianDriftWatchChainVerdict(str, Enum):
    NO_SIGNAL_NO_MUTATION = "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    SIGNAL_READY_NO_SUBMIT = "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    NOT_READY_FOR_TONIGHT = "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    STALE_RUNTIME_CONTEXT = "TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_STALE_RUNTIME_CONTEXT_NOT_READY"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBAsianDriftWatchChainResult:
    verdict: TrackBAsianDriftWatchChainVerdict
    report_json: Path
    report: dict[str, Any]
    completed_5m_candles_json: Path | None
    completed_5m_candles_payload: dict[str, Any] | None
    feature_rows_result: TrackBAsianDriftFeatureRowsResult | None
    rule_result: TrackBStrategyRuleRunnerResult | None


def run_track_b_asian_drift_watch_chain(
    *,
    candle_payload: Mapping[str, Any],
    source_payload_path: Path | None = None,
    current_quote_report_payload: Mapping[str, Any] | None = None,
    current_quote_report_json: Path | None = None,
    expected_account_id: str | None = "DUM882026",
    account_id: str = "DUM882026",
    contract_key: str = "MGC-202606",
    instrument_family: str = "MGC",
    local_symbol: str = "MGCM6",
    dataset: str = "GLBX.MDP3",
    source_id: str = "track_b_asian_drift_watch_chain",
    strategy_id: str = "asian_drift_v1",
    lane_id: str = "mgc_example_long_lmt_day",
    calibration_profile: str = RECOVERY_CONFIRMED,
    max_source_bars: int = 250,
    max_completed_5m_age_seconds: int | None = None,
    allow_legacy_runtime_candles: bool = False,
    output_root: Path = DEFAULT_TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_OUTPUT_ROOT,
    feature_rows_output_root: Path = DEFAULT_TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_OUTPUT_ROOT,
    strategy_rule_output_root: Path | None = None,
    strategy_adapter_output_root: Path | None = None,
    candle_producer_output_root: Path | None = None,
    writer_output_root: Path | None = None,
    inbox_dir: Path = Path("examples/track_b_shadow_listener/inbox"),
    chain_id: str | None = None,
    now: datetime | None = None,
) -> TrackBAsianDriftWatchChainResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_chain_id = chain_id or f"track_b_asian_drift_watch_chain_{uuid.uuid4().hex}"
    output_root = Path(output_root)
    report_json = output_root / actual_chain_id / "asian_drift_watch_chain_report.json"
    completed_5m_json = output_root / actual_chain_id / "asian_drift_5m_candles.json"
    latest_report_json = output_root / "latest_asian_drift_watch_chain_report.json"
    latest_completed_5m_json = output_root / "latest_asian_drift_5m_candles.json"
    normalized_payload = normalize_phase1_runtime_candle_payload(
        candle_payload,
        source_path=source_payload_path,
    )
    legacy_blocker = None if allow_legacy_runtime_candles else legacy_p0_runtime_candle_path_blocker(source_payload_path)
    try:
        completed_5m_payload = _completed_5m_payload(
            payload=normalized_payload,
            source_payload_path=source_payload_path,
            current_quote_report_payload=current_quote_report_payload,
            current_quote_report_json=current_quote_report_json,
            account_id=account_id,
            expected_account_id=expected_account_id,
            contract_key=contract_key,
            instrument_family=instrument_family,
            local_symbol=local_symbol,
            dataset=dataset,
            source_id=source_id,
            max_source_bars=max_source_bars,
            generated_at=actual_now,
        )
        _write_json(completed_5m_json, completed_5m_payload)
        _write_json(latest_completed_5m_json, completed_5m_payload)
        freshness = _runtime_candle_freshness(
            completed_5m_payload=completed_5m_payload,
            source_payload=normalized_payload,
            now=actual_now,
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        )
        if legacy_blocker:
            report = _blocked_schema_report(
                now=actual_now,
                chain_id=actual_chain_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                primary_blocker=legacy_blocker,
                report_json=report_json,
                latest_report_json=latest_report_json,
            )
            _write_json(report_json, report)
            _write_json(latest_report_json, report)
            return TrackBAsianDriftWatchChainResult(
                verdict=TrackBAsianDriftWatchChainVerdict.BLOCKED_SCHEMA_ERROR,
                report_json=report_json,
                report=report,
                completed_5m_candles_json=completed_5m_json,
                completed_5m_candles_payload=completed_5m_payload,
                feature_rows_result=None,
                rule_result=None,
            )
        if freshness["runtime_candle_context_stale"] is True:
            report = _stale_runtime_context_report(
                now=actual_now,
                chain_id=actual_chain_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                current_quote_report_json=current_quote_report_json,
                completed_5m_payload=completed_5m_payload,
                completed_5m_json=completed_5m_json,
                latest_completed_5m_json=latest_completed_5m_json,
                freshness=freshness,
                report_json=report_json,
                latest_report_json=latest_report_json,
            )
            _write_json(report_json, report)
            _write_json(latest_report_json, report)
            return TrackBAsianDriftWatchChainResult(
                verdict=TrackBAsianDriftWatchChainVerdict.STALE_RUNTIME_CONTEXT,
                report_json=report_json,
                report=report,
                completed_5m_candles_json=completed_5m_json,
                completed_5m_candles_payload=completed_5m_payload,
                feature_rows_result=None,
                rule_result=None,
            )
        feature_rows = produce_track_b_asian_drift_feature_rows(
            runtime_5m_payload=completed_5m_payload,
            source_payload_path=latest_completed_5m_json,
            current_quote_report_payload=current_quote_report_payload,
            current_quote_report_json=current_quote_report_json,
            expected_account_id=expected_account_id,
            account_id=account_id,
            contract_key=contract_key,
            instrument_family=instrument_family,
            local_symbol=local_symbol,
            dataset=dataset,
            source_id=source_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            calibration_profile=calibration_profile,
            output_root=feature_rows_output_root,
            producer_id=f"{actual_chain_id}_feature_rows",
            invoke_live_state=True,
            now=actual_now,
        )
        rule_result = None
        snapshot_ready = (
            feature_rows.live_state_result is not None
            and feature_rows.live_state_result.snapshot is not None
            and feature_rows.live_state_result.snapshot_json is not None
        )
        if snapshot_ready:
            rule_result = run_track_b_strategy_rule(
                input_event_payload=feature_rows.live_state_result.snapshot,  # type: ignore[union-attr]
                input_event_path=feature_rows.live_state_result.snapshot_json,  # type: ignore[union-attr]
                inbox_dir=inbox_dir,
                expected_account_id=expected_account_id,
                source_id=source_id,
                rule_id=strategy_id,
                rule_mode="ASIAN_DRIFT_V1",
                emit_signal=True,
                output_root=strategy_rule_output_root or output_root,
                strategy_adapter_output_root=strategy_adapter_output_root or output_root / "strategy_adapter",
                candle_producer_output_root=candle_producer_output_root or output_root / "candle_signal_producer",
                writer_output_root=writer_output_root or output_root / "signal_batch_writer",
                runner_id=f"{actual_chain_id}_rule_watch",
                now=actual_now,
            )
        verdict = _chain_verdict(feature_rows, rule_result)
        report = _report(
            verdict=verdict,
            now=actual_now,
            chain_id=actual_chain_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            current_quote_report_json=current_quote_report_json,
            completed_5m_payload=completed_5m_payload,
            completed_5m_json=completed_5m_json,
            latest_completed_5m_json=latest_completed_5m_json,
            feature_rows=feature_rows,
            rule_result=rule_result,
            report_json=report_json,
            latest_report_json=latest_report_json,
            freshness=freshness,
        )
        _write_json(report_json, report)
        _write_json(latest_report_json, report)
        return TrackBAsianDriftWatchChainResult(
            verdict=verdict,
            report_json=report_json,
            report=report,
            completed_5m_candles_json=completed_5m_json,
            completed_5m_candles_payload=completed_5m_payload,
            feature_rows_result=feature_rows,
            rule_result=rule_result,
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        report = _blocked_schema_report(
            now=actual_now,
            chain_id=actual_chain_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            primary_blocker=str(exc),
            report_json=report_json,
            latest_report_json=latest_report_json,
        )
        _write_json(report_json, report)
        _write_json(latest_report_json, report)
        return TrackBAsianDriftWatchChainResult(
            verdict=TrackBAsianDriftWatchChainVerdict.BLOCKED_SCHEMA_ERROR,
            report_json=report_json,
            report=report,
            completed_5m_candles_json=None,
            completed_5m_candles_payload=None,
            feature_rows_result=None,
            rule_result=None,
        )


def _completed_5m_payload(
    *,
    payload: Mapping[str, Any],
    source_payload_path: Path | None,
    current_quote_report_payload: Mapping[str, Any] | None,
    current_quote_report_json: Path | None,
    account_id: str,
    expected_account_id: str | None,
    contract_key: str,
    instrument_family: str,
    local_symbol: str,
    dataset: str,
    source_id: str,
    max_source_bars: int,
    generated_at: datetime,
) -> dict[str, Any]:
    raw = _raw_candles(payload)
    normalized = [_normalize_candle(item) for item in raw]
    normalized = sorted(normalized, key=lambda item: item["candle_timestamp"])[-max_source_bars:]
    source_timeframe = _text(payload.get("timeframe") or (normalized[-1].get("timeframe") if normalized else None))
    if source_timeframe == "5m":
        completed = [item for item in normalized if _completed_value(item) is not False]
    elif source_timeframe == "1m":
        completed = _aggregate_1m_to_5m([item for item in normalized if _completed_value(item) is not False])
    else:
        completed = []
    quote_evidence = _quote_evidence(payload, current_quote_report_payload, current_quote_report_json)
    return {
        "schema_version": "track_b_asian_drift_completed_5m_candles_v1",
        "generated_at": generated_at.isoformat(),
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "source_category": payload.get("source_category"),
        "input_source_category": payload.get("input_source_category"),
        "source_authority": payload.get("source_authority"),
        "source_authority_path": payload.get("source_authority_path"),
        "latest_bar_timestamp": payload.get("latest_bar_timestamp"),
        "freshness_status": payload.get("freshness_status"),
        "phase1_runtime_market_data_authority": payload.get("phase1_runtime_market_data_authority") is True,
        "account_id": account_id,
        "expected_account_id": expected_account_id,
        "contract_key": contract_key,
        "instrument_family": instrument_family,
        "local_symbol": local_symbol,
        "dataset": dataset,
        "timeframe": "5m",
        "source_timeframe": source_timeframe,
        "max_source_bars": max_source_bars,
        "completed_5m_bars_available": len(completed),
        "first_completed_5m_timestamp": None if not completed else completed[0]["candle_timestamp"],
        "last_completed_5m_timestamp": None if not completed else completed[-1]["candle_timestamp"],
        "candles": completed,
        **quote_evidence,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _aggregate_1m_to_5m(candles: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: list[list[Mapping[str, Any]]] = []
    current: list[Mapping[str, Any]] = []
    for candle in candles:
        if current:
            previous_ts = _timestamp(current[-1]["candle_timestamp"])
            current_ts = _timestamp(candle["candle_timestamp"])
            if current_ts - previous_ts != timedelta(minutes=1):
                current = []
        current.append(candle)
        if len(current) == 5:
            groups.append(list(current))
            current = []
    output: list[dict[str, Any]] = []
    for group in groups:
        output.append(
            {
                "candle_timestamp": group[-1]["candle_timestamp"],
                "observed_at": group[-1].get("observed_at") or group[-1]["candle_timestamp"],
                "timeframe": "5m",
                "open": group[0]["open"],
                "high": max(float(item["high"]) for item in group),
                "low": min(float(item["low"]) for item in group),
                "close": group[-1]["close"],
                "volume": sum(_int(item.get("volume")) for item in group),
                "completed": True,
                "source_1m_bar_count": len(group),
                "source_start_timestamp": group[0]["candle_timestamp"],
                "source_end_timestamp": group[-1]["candle_timestamp"],
                "source_tags": sorted(
                    {
                        str(item.get("source_tag"))
                        for item in group
                        if item.get("source_tag") is not None
                    }
                ),
                "latest_decision_bar_source": group[-1].get("source_tag"),
            }
        )
    return output


def _normalize_candle(item: Mapping[str, Any]) -> dict[str, Any]:
    ts = _timestamp(item.get("candle_timestamp") or item.get("timestamp") or item.get("end_ts") or item.get("bar_end_ts"))
    return {
        "candle_timestamp": ts.isoformat(),
        "observed_at": item.get("observed_at") or ts.isoformat(),
        "timeframe": _text(item.get("timeframe")),
        "open": _float_required(item.get("open"), "open"),
        "high": _float_required(item.get("high"), "high"),
        "low": _float_required(item.get("low"), "low"),
        "close": _float_required(item.get("close") or item.get("last"), "close"),
        "volume": _int(item.get("volume")),
        "completed": _completed_value(item),
        "provider_symbol": item.get("provider_symbol"),
        "raw_symbol": item.get("raw_symbol"),
        "source_tag": item.get("source_tag"),
        "source_role": item.get("source_role"),
    }


def _raw_candles(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("candles", "candle_history", "runtime_candles", "bars", "ohlcv"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
    return []


def _chain_verdict(
    feature_rows: TrackBAsianDriftFeatureRowsResult,
    rule_result: TrackBStrategyRuleRunnerResult | None,
) -> TrackBAsianDriftWatchChainVerdict:
    if rule_result is not None and rule_result.report.get("signal_emitted") is True:
        return TrackBAsianDriftWatchChainVerdict.SIGNAL_READY_NO_SUBMIT
    if rule_result is not None:
        return TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION
    if feature_rows.live_state_result is not None and feature_rows.live_state_result.report.get("asian_drift_state_ready") is True:
        return TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION
    return TrackBAsianDriftWatchChainVerdict.NOT_READY_FOR_TONIGHT


def _report(
    *,
    verdict: TrackBAsianDriftWatchChainVerdict,
    now: datetime,
    chain_id: str,
    source_id: str,
    source_payload_path: Path | None,
    current_quote_report_json: Path | None,
    completed_5m_payload: Mapping[str, Any],
    completed_5m_json: Path,
    latest_completed_5m_json: Path,
    feature_rows: TrackBAsianDriftFeatureRowsResult,
    rule_result: TrackBStrategyRuleRunnerResult | None,
    report_json: Path,
    latest_report_json: Path,
    freshness: Mapping[str, Any],
) -> dict[str, Any]:
    live_report = feature_rows.live_state_result.report if feature_rows.live_state_result is not None else {}
    rule_report = rule_result.report if rule_result is not None else {}
    return {
        "schema_version": "track_b_asian_drift_watch_chain_report_v1",
        "generated_at": now.isoformat(),
        "asian_drift_watch_chain_id": chain_id,
        "asian_drift_watch_chain_verdict": verdict.value,
        "asian_drift_watch_verdict": (
            rule_report.get("asian_drift_watch_verdict")
            or live_report.get("asian_drift_watch_verdict")
            or verdict.value
        ),
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "current_quote_report_json": None if current_quote_report_json is None else str(current_quote_report_json),
        "completed_5m_candles_json_path": str(completed_5m_json),
        "latest_completed_5m_candles_json_path": str(latest_completed_5m_json),
        "completed_5m_bars_available": completed_5m_payload.get("completed_5m_bars_available"),
        **dict(freshness),
        "feature_rows_available": feature_rows.report.get("feature_row_count"),
        "feature_rows_verdict": feature_rows.report.get("asian_drift_feature_rows_verdict"),
        "feature_rows_json_path": feature_rows.report.get("latest_feature_rows_json_path"),
        "asian_drift_state_ready": live_report.get("asian_drift_state_ready", False),
        "asian_drift_state_snapshot_path": live_report.get("latest_asian_drift_state_snapshot_path"),
        "live_state_verdict": live_report.get("asian_drift_live_state_verdict"),
        "asian_drift_diagnostic_classification": (
            rule_report.get("asian_drift_diagnostic_classification")
            or live_report.get("asian_drift_diagnostic_classification")
        ),
        "late_join_classification": rule_report.get("late_join_classification") or live_report.get("late_join_classification"),
        "late_join_diagnostic": rule_report.get("late_join_diagnostic", live_report.get("late_join_diagnostic", False)),
        "anchor_required": rule_report.get("anchor_required", live_report.get("anchor_required")),
        "anchor_observed": rule_report.get("anchor_observed", live_report.get("anchor_observed")),
        "anchor_window_start": rule_report.get("anchor_window_start") or live_report.get("anchor_window_start"),
        "anchor_window_end": rule_report.get("anchor_window_end") or live_report.get("anchor_window_end"),
        "runtime_context_start": rule_report.get("runtime_context_start") or live_report.get("runtime_context_start"),
        "missing_anchor_reason": rule_report.get("missing_anchor_reason") or live_report.get("missing_anchor_reason"),
        "drift_observed_after_anchor": rule_report.get(
            "drift_observed_after_anchor",
            live_report.get("drift_observed_after_anchor"),
        ),
        "late_join_policy": rule_report.get("late_join_policy") or live_report.get("late_join_policy"),
        "hypothetical_late_join_score": rule_report.get("hypothetical_late_join_score")
        or live_report.get("hypothetical_late_join_score"),
        "operator_explanation": rule_report.get("operator_explanation") or live_report.get("operator_explanation"),
        "no_mutation": True,
        "rule_evaluated": rule_result is not None,
        "rule_decision": rule_report.get("rule_decision") or rule_report.get("decision"),
        "signal_emitted": rule_report.get("signal_emitted", False),
        "signal_side": rule_report.get("signal_direction"),
        "signal_source": rule_report.get("signal_source"),
        "real_strategy_signal": rule_report.get("real_strategy_signal", False),
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "primary_blocker": None if verdict != TrackBAsianDriftWatchChainVerdict.NOT_READY_FOR_TONIGHT else _primary_blocker(feature_rows, rule_result),
        "required_next_action": _required_next_action(verdict),
        "report_json_path": str(report_json),
        "latest_report_json_path": str(latest_report_json),
    }


def _stale_runtime_context_report(
    *,
    now: datetime,
    chain_id: str,
    source_id: str,
    source_payload_path: Path | None,
    current_quote_report_json: Path | None,
    completed_5m_payload: Mapping[str, Any],
    completed_5m_json: Path,
    latest_completed_5m_json: Path,
    freshness: Mapping[str, Any],
    report_json: Path,
    latest_report_json: Path,
) -> dict[str, Any]:
    blocker = (
        "Track B runtime candle context is stale: latest completed 5m candle age "
        f"{freshness.get('latest_completed_5m_candle_age_seconds')}s exceeds "
        f"max {freshness.get('max_completed_5m_candle_age_seconds')}s."
    )
    return {
        "schema_version": "track_b_asian_drift_watch_chain_report_v1",
        "generated_at": now.isoformat(),
        "asian_drift_watch_chain_id": chain_id,
        "asian_drift_watch_chain_verdict": TrackBAsianDriftWatchChainVerdict.STALE_RUNTIME_CONTEXT.value,
        "asian_drift_watch_verdict": "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT",
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "current_quote_report_json": None if current_quote_report_json is None else str(current_quote_report_json),
        "completed_5m_candles_json_path": str(completed_5m_json),
        "latest_completed_5m_candles_json_path": str(latest_completed_5m_json),
        "completed_5m_bars_available": completed_5m_payload.get("completed_5m_bars_available"),
        **dict(freshness),
        "feature_rows_available": 0,
        "asian_drift_state_ready": False,
        "rule_evaluated": False,
        "rule_decision": None,
        "signal_emitted": False,
        "signal_side": None,
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "primary_blocker": blocker,
        "required_next_action": "Refresh bounded Track B runtime candles before running strategy evaluation.",
        "report_json_path": str(report_json),
        "latest_report_json_path": str(latest_report_json),
    }


def _blocked_schema_report(
    *,
    now: datetime,
    chain_id: str,
    source_id: str,
    source_payload_path: Path | None,
    primary_blocker: str,
    report_json: Path,
    latest_report_json: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_asian_drift_watch_chain_report_v1",
        "generated_at": now.isoformat(),
        "asian_drift_watch_chain_id": chain_id,
        "asian_drift_watch_chain_verdict": TrackBAsianDriftWatchChainVerdict.BLOCKED_SCHEMA_ERROR.value,
        "asian_drift_watch_verdict": "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT",
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "completed_5m_bars_available": 0,
        "feature_rows_available": 0,
        "asian_drift_state_ready": False,
        "rule_evaluated": False,
        "signal_emitted": False,
        "signal_side": None,
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "required_next_action": "Fix Asian Drift watch-chain input schema before retrying.",
        "report_json_path": str(report_json),
        "latest_report_json_path": str(latest_report_json),
    }


def _runtime_candle_freshness(
    *,
    completed_5m_payload: Mapping[str, Any],
    source_payload: Mapping[str, Any],
    now: datetime,
    max_completed_5m_age_seconds: int | None,
) -> dict[str, Any]:
    raw = _raw_candles(source_payload)
    latest_1m_timestamp = _latest_timestamp(raw, timeframe="1m")
    latest_5m_timestamp = _timestamp(completed_5m_payload.get("last_completed_5m_timestamp")) if completed_5m_payload.get("last_completed_5m_timestamp") else None
    latest_1m_age = None if latest_1m_timestamp is None else max(0.0, (now - latest_1m_timestamp).total_seconds())
    latest_5m_age = None if latest_5m_timestamp is None else max(0.0, (now - latest_5m_timestamp).total_seconds())
    stale = (
        max_completed_5m_age_seconds is not None
        and (latest_5m_age is None or latest_5m_age > max_completed_5m_age_seconds)
    )
    return {
        "runtime_candle_source_path": completed_5m_payload.get("source_payload_path"),
        "runtime_candle_source_timeframe": completed_5m_payload.get("source_timeframe"),
        "runtime_candle_provider_mode": completed_5m_payload.get("quote_provider_mode"),
        "latest_1m_candle_timestamp": None if latest_1m_timestamp is None else latest_1m_timestamp.isoformat(),
        "latest_1m_candle_age_seconds": None if latest_1m_age is None else round(latest_1m_age, 3),
        "latest_1m_candle_age_minutes": None if latest_1m_age is None else round(latest_1m_age / 60.0, 3),
        "latest_completed_5m_candle_timestamp": None if latest_5m_timestamp is None else latest_5m_timestamp.isoformat(),
        "latest_completed_5m_candle_age_seconds": None if latest_5m_age is None else round(latest_5m_age, 3),
        "latest_completed_5m_candle_age_minutes": None if latest_5m_age is None else round(latest_5m_age / 60.0, 3),
        "max_completed_5m_candle_age_seconds": max_completed_5m_age_seconds,
        "runtime_candle_context_stale": stale,
        "runtime_candle_context_fresh": not stale if max_completed_5m_age_seconds is not None else None,
        "data_available_end": source_payload.get("data_available_end") or source_payload.get("provider_available_end"),
    }


def _latest_timestamp(raw_candles: Sequence[Mapping[str, Any]], *, timeframe: str) -> datetime | None:
    values: list[datetime] = []
    for item in raw_candles:
        item_timeframe = _text(item.get("timeframe"))
        if item_timeframe and item_timeframe != timeframe:
            continue
        try:
            values.append(_timestamp(item.get("candle_timestamp") or item.get("timestamp") or item.get("end_ts") or item.get("bar_end_ts")))
        except (TypeError, ValueError):
            continue
    return max(values) if values else None


def _primary_blocker(feature_rows: TrackBAsianDriftFeatureRowsResult, rule_result: TrackBStrategyRuleRunnerResult | None) -> object | None:
    if rule_result is not None:
        return rule_result.report.get("primary_blocker")
    if feature_rows.live_state_result is not None:
        return feature_rows.live_state_result.report.get("primary_blocker")
    return feature_rows.report.get("primary_blocker")


def _required_next_action(verdict: TrackBAsianDriftWatchChainVerdict) -> str:
    if verdict == TrackBAsianDriftWatchChainVerdict.SIGNAL_READY_NO_SUBMIT:
        return "Review the real ASIAN_DRIFT_V1 signal; PAPER execution remains a separate explicit guarded decision."
    if verdict == TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION:
        return "Continue bounded Asian Drift watch; no broker mutation was attempted."
    return "Collect completed 5m Asian Drift context and realtime quote evidence before watch."


def _quote_evidence(
    payload: Mapping[str, Any],
    quote_report: Mapping[str, Any] | None,
    quote_report_json: Path | None,
) -> dict[str, Any]:
    quote = quote_report or {}
    return {
        "quote_provider_mode": _text(payload.get("quote_provider_mode") or quote.get("quote_provider_mode")),
        "realtime_quote_received": _bool(payload.get("realtime_quote_received"))
        if _bool(payload.get("realtime_quote_received")) is not None
        else _bool(quote.get("realtime_quote_received")),
        "current_quote_available": _bool(payload.get("current_quote_available"))
        if _bool(payload.get("current_quote_available")) is not None
        else _bool(quote.get("current_quote_available")),
        "quote_freshness_verdict": payload.get("quote_freshness_verdict") or quote.get("quote_freshness_verdict"),
        "current_quote_report_json": None if quote_report_json is None else str(quote_report_json),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True), encoding="utf-8")


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Asian Drift watch-chain candle timestamp must be timezone-aware.")
    return dt


def _completed_value(row: Mapping[str, Any]) -> bool | None:
    if "completed" in row:
        return _bool(row.get("completed"))
    if "is_complete" in row:
        return _bool(row.get("is_complete"))
    return None


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = _text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    return None


def _int(value: object) -> int:
    try:
        if value is None:
            return 0
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _float_required(value: object, field_name: str) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Asian Drift watch-chain candle field {field_name} must be numeric.") from exc
