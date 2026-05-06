"""Bounded Databento Live freshness diagnostics for Track B.

This diagnostic is read-only. It explains whether transport, raw messages,
completed 1m artifacts, and completed 5m artifacts are fresh enough for
execution-live Track B decisions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_LIVE_FEED_FRESHNESS_DIAGNOSTIC_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_TRACK_B_LIVE_FEED_FRESHNESS_DIAGNOSTIC_JSON = (
    DEFAULT_TRACK_B_LIVE_FEED_FRESHNESS_DIAGNOSTIC_OUTPUT_ROOT
    / "latest_track_b_live_feed_freshness_diagnostic.json"
)
DEFAULT_LATEST_1M_THRESHOLD_SECONDS = 120
DEFAULT_COMPLETED_5M_THRESHOLD_SECONDS = 420
MAX_JSON_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class TrackBLiveFeedFreshnessDiagnosticResult:
    report_json: Path
    report: dict[str, Any]


def build_track_b_live_feed_freshness_diagnostic(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_TRACK_B_LIVE_FEED_FRESHNESS_DIAGNOSTIC_OUTPUT_ROOT,
    now: datetime | None = None,
    write: bool = True,
) -> TrackBLiveFeedFreshnessDiagnosticResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    feed_root = root / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed"
    monitor_root = root / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    monitor_path = monitor_root / "latest_track_b_shadow_monitor_report.json"
    heartbeat_path = monitor_root / "latest_track_b_shadow_monitor_heartbeat.json"
    monitor = _load_json(monitor_path)
    heartbeat = _load_json(heartbeat_path)
    instruments = _instrument_rows(monitor)
    if not instruments:
        instruments = [
            {"instrument_family": "MGC", "contract_key": "MGC-202606", "local_symbol": "MGCM6", "databento_continuous_symbol": "MGC.v.0", "dataset": "GLBX.MDP3"},
            {"instrument_family": "MNQ", "contract_key": "MNQ-202606", "local_symbol": "MNQM6", "databento_continuous_symbol": "MNQ.v.0", "dataset": "GLBX.MDP3"},
        ]

    instrument_reports = [_instrument_freshness(row, feed_root=feed_root, now=actual_now) for row in instruments]
    stale = [item for item in instrument_reports if item.get("freshness_verdict") == "EXECUTION_STALE"]
    report = {
        "schema_version": "track_b_live_feed_freshness_diagnostic_v1",
        "generated_at": actual_now.isoformat(),
        "monitor_running": heartbeat.get("monitor_running"),
        "monitor_mode": monitor.get("mode") or monitor.get("monitor_mode"),
        "runtime_source": monitor.get("runtime_decision_source") or monitor.get("runtime_data_source"),
        "latest_monitor_verdict": monitor.get("monitor_verdict"),
        "heartbeat_age_seconds": _age_seconds(heartbeat.get("generated_at"), actual_now),
        "instrument_count": len(instrument_reports),
        "instrument_reports": instrument_reports,
        "stale_instruments": [item.get("instrument_family") for item in stale],
        "fresh_instruments": [
            item.get("instrument_family")
            for item in instrument_reports
            if item.get("freshness_verdict") == "EXECUTION_FRESH"
        ],
        "diagnosis_classification": "STALE_LIVE_FEED" if stale else "LIVE_FEED_EXECUTION_FRESH",
        "primary_blocker": _primary_blocker(stale),
        "bounded_policy": "Reads latest monitor, heartbeat, and per-instrument Live hot artifacts only.",
        "http_backfill_can_satisfy_execution_freshness": False,
        "full_ledger_scanned": False,
        "source_artifact_paths": {
            "latest_monitor_report": str(monitor_path),
            "latest_monitor_heartbeat": str(heartbeat_path),
            "databento_live_runtime_feed_root": str(feed_root),
        },
    }
    report_json = Path(output_root) / "latest_track_b_live_feed_freshness_diagnostic.json"
    if write:
        _write_json(report_json, report)
    return TrackBLiveFeedFreshnessDiagnosticResult(report_json=report_json, report=report)


def _instrument_freshness(row: Mapping[str, Any], *, feed_root: Path, now: datetime) -> dict[str, Any]:
    family = str(row.get("instrument_family") or "MGC").upper()
    enabled = row.get("enabled_strategies") if isinstance(row.get("enabled_strategies"), list) else []
    if row.get("runtime_chain_wired") is not True or not enabled:
        return {
            "instrument_family": family,
            "feed_process_pid": row.get("live_feed_pid"),
            "subscribed_symbol": row.get("databento_continuous_symbol"),
            "local_symbol": row.get("local_symbol"),
            "contract_key": row.get("contract_key"),
            "dataset": row.get("dataset"),
            "transport_connected": None,
            "live_feed_connected": None,
            "raw_messages_fresh": None,
            "completed_1m_fresh": None,
            "completed_5m_fresh": None,
            "execution_fresh": None,
            "freshness_verdict": "NO_STRATEGIES_CONFIGURED",
            "reason_for_stale_verdict": None,
            "primary_blocker": row.get("primary_blocker") or f"{family} has no enabled Track B strategies configured.",
        }
    symbol = family.lower()
    one_m_path = _existing_path(
        row.get("live_feed_event_path"),
        feed_root / f"latest_live_{symbol}_1m_candles.json",
        feed_root / "latest_live_mgc_1m_candles.json" if family == "MGC" else None,
    )
    five_m_path = _existing_path(
        row.get("completed_5m_path"),
        row.get("live_feed_completed_5m_path"),
        feed_root / f"latest_live_{symbol}_completed_5m_candles.json",
        feed_root / "latest_live_mgc_completed_5m_candles.json" if family == "MGC" else None,
    )
    report_path = _existing_path(
        row.get("live_feed_report_path"),
        feed_root / f"latest_databento_live_runtime_feed_{symbol}_report.json",
        feed_root / "latest_databento_live_runtime_feed_report.json" if family == "MGC" else None,
    )
    heartbeat_path = _existing_path(
        row.get("live_feed_heartbeat_path"),
        feed_root / f"latest_databento_live_runtime_feed_{symbol}_heartbeat.json",
        feed_root / "latest_databento_live_runtime_feed_heartbeat.json" if family == "MGC" else None,
    )
    one_m = _load_json(one_m_path)
    five_m = _load_json(five_m_path)
    feed_report = _load_json(report_path)
    feed_heartbeat = _load_json(heartbeat_path)
    latest_1m = _latest_1m_timestamp(one_m)
    latest_5m = _latest_5m_timestamp(five_m) or _parse_time_optional(feed_heartbeat.get("latest_completed_5m_timestamp"))
    latest_raw = _first_time(
        feed_heartbeat.get("latest_record_ts_recv"),
        feed_heartbeat.get("latest_record_ts_event"),
        feed_report.get("latest_record_ts_recv"),
        feed_report.get("latest_record_ts_event"),
        latest_1m.isoformat() if latest_1m else None,
    )
    max_1m = _int_or_default(
        row.get("max_latest_1m_age_seconds"),
        feed_report.get("max_latest_1m_age_seconds"),
        one_m.get("max_latest_1m_age_seconds"),
        DEFAULT_LATEST_1M_THRESHOLD_SECONDS,
    )
    max_5m = _int_or_default(
        row.get("max_completed_5m_age_seconds"),
        feed_report.get("max_completed_5m_age_seconds"),
        one_m.get("max_completed_5m_age_seconds"),
        DEFAULT_COMPLETED_5M_THRESHOLD_SECONDS,
    )
    latest_1m_age = None if latest_1m is None else max(0.0, (now - latest_1m).total_seconds())
    latest_5m_age = None if latest_5m is None else max(0.0, (now - latest_5m).total_seconds())
    raw_age = None if latest_raw is None else max(0.0, (now - latest_raw).total_seconds())
    connected = _bool_or_none(feed_heartbeat.get("live_feed_connected"))
    if connected is None:
        connected = _bool_or_none(feed_report.get("live_feed_connected"))
    raw_fresh = None if raw_age is None else raw_age <= max_1m
    one_m_fresh = latest_1m_age is not None and latest_1m_age <= max_1m
    five_m_fresh = latest_5m_age is not None and latest_5m_age <= max_5m
    execution_fresh = connected is True and one_m_fresh and five_m_fresh
    blocker = _freshness_blocker(
        connected=connected,
        one_m_age=latest_1m_age,
        five_m_age=latest_5m_age,
        max_1m=max_1m,
        max_5m=max_5m,
        one_m_path=one_m_path,
        five_m_path=five_m_path,
    )
    writer_1m_path = feed_report.get("latest_live_1m_candles_path") or str(one_m_path)
    writer_5m_path = feed_report.get("latest_live_completed_5m_candles_path") or str(five_m_path)
    return {
        "instrument_family": family,
        "feed_process_pid": row.get("live_feed_pid"),
        "subscribed_symbol": feed_report.get("databento_continuous_symbol")
        or row.get("databento_continuous_symbol"),
        "local_symbol": feed_report.get("local_symbol") or row.get("local_symbol"),
        "contract_key": feed_report.get("contract_key") or row.get("contract_key"),
        "dataset": feed_report.get("dataset") or row.get("dataset"),
        "transport_connected": connected,
        "live_feed_connected": connected,
        "subscription_status": feed_heartbeat.get("subscription_status") or feed_report.get("subscription_status"),
        "raw_messages_fresh": raw_fresh,
        "last_raw_message_time": None if latest_raw is None else latest_raw.isoformat(),
        "last_quote_or_tick_time": None if latest_raw is None else latest_raw.isoformat(),
        "last_completed_1m_candle_timestamp": None if latest_1m is None else latest_1m.isoformat(),
        "last_completed_1m_candle_artifact_write_time": _mtime_iso(one_m_path),
        "latest_1m_candle_age_seconds": None if latest_1m_age is None else round(latest_1m_age, 3),
        "last_completed_5m_candle_timestamp": None if latest_5m is None else latest_5m.isoformat(),
        "last_completed_5m_artifact_write_time": _mtime_iso(five_m_path),
        "latest_completed_5m_candle_age_seconds": None if latest_5m_age is None else round(latest_5m_age, 3),
        "latest_raw_message_age_seconds": None if raw_age is None else round(raw_age, 3),
        "completed_1m_fresh": one_m_fresh,
        "completed_5m_fresh": five_m_fresh,
        "execution_fresh": execution_fresh,
        "freshness_threshold": {
            "max_latest_1m_age_seconds": max_1m,
            "max_completed_5m_age_seconds": max_5m,
        },
        "freshness_verdict": "EXECUTION_FRESH" if execution_fresh else "EXECUTION_STALE",
        "reason_for_stale_verdict": None if execution_fresh else blocker,
        "artifact_path_read_by_monitor": str(one_m_path),
        "artifact_path_written_by_feed": str(writer_1m_path),
        "completed_5m_artifact_path_read_by_monitor": str(five_m_path),
        "completed_5m_artifact_path_written_by_feed": str(writer_5m_path),
        "read_write_path_mismatch": str(one_m_path) != str(writer_1m_path),
        "report_path": str(report_path),
        "heartbeat_path": str(heartbeat_path),
    }


def _instrument_rows(monitor: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = monitor.get("instrument_reports")
    return [dict(item) for item in rows if isinstance(item, Mapping)] if isinstance(rows, list) else []


def _existing_path(*values: object) -> Path:
    candidates = [Path(str(value)) for value in values if value not in (None, "", {})]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0] if candidates else Path("__missing__")


def _latest_1m_timestamp(payload: Mapping[str, Any]) -> datetime | None:
    return _first_time(payload.get("last_candle_timestamp"), payload.get("candle_timestamp"))


def _latest_5m_timestamp(payload: Mapping[str, Any]) -> datetime | None:
    candles = payload.get("candles")
    if isinstance(candles, list) and candles:
        latest = candles[-1]
        if isinstance(latest, Mapping):
            parsed = _parse_time_optional(latest.get("candle_timestamp"))
            return None if parsed is None else parsed + timedelta(minutes=5)
    return _first_time(payload.get("latest_completed_5m_timestamp"), payload.get("candle_timestamp"))


def _freshness_blocker(
    *,
    connected: bool | None,
    one_m_age: float | None,
    five_m_age: float | None,
    max_1m: int,
    max_5m: int,
    one_m_path: Path,
    five_m_path: Path,
) -> str:
    parts: list[str] = []
    if connected is not True:
        parts.append("transport is not connected")
    if not one_m_path.exists():
        parts.append(f"1m artifact missing at {one_m_path}")
    elif one_m_age is None:
        parts.append("latest 1m candle timestamp is unavailable")
    elif one_m_age > max_1m:
        parts.append(f"latest 1m candle age {round(one_m_age, 3)}s exceeds max {max_1m}s")
    if not five_m_path.exists():
        parts.append(f"5m artifact missing at {five_m_path}")
    elif five_m_age is None:
        parts.append("latest completed 5m candle timestamp is unavailable")
    elif five_m_age > max_5m:
        parts.append(f"latest completed 5m candle age {round(five_m_age, 3)}s exceeds max {max_5m}s")
    return "; ".join(parts) or "execution freshness is unavailable"


def _primary_blocker(stale: list[Mapping[str, Any]]) -> str | None:
    if not stale:
        return None
    first = stale[0]
    return f"{first.get('instrument_family')}: {first.get('reason_for_stale_verdict')}"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists() or path.stat().st_size > MAX_JSON_BYTES:
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _mtime_iso(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()
    except OSError:
        return None


def _age_seconds(value: object, now: datetime) -> float | None:
    parsed = _parse_time_optional(value)
    return None if parsed is None else round(max(0.0, (now - parsed).total_seconds()), 3)


def _first_time(*values: object) -> datetime | None:
    for value in values:
        parsed = _parse_time_optional(value)
        if parsed is not None:
            return parsed
    return None


def _parse_time_optional(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _int_or_default(*values: object) -> int:
    default = int(values[-1])
    for value in values[:-1]:
        try:
            return int(str(value))
        except (TypeError, ValueError):
            continue
    return default


def _bool_or_none(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None
