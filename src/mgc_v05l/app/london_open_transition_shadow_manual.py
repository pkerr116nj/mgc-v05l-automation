"""Manual read-only runner for the London-open transition shadow observer."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from mgc_v05l.app.london_open_transition_shadow_observer import (
    DEFAULT_SHADOW_ROOT,
    REQUIRED_PROVENANCE,
    evaluate_london_open_transition_shadow_candidate,
    write_shadow_artifacts,
)


DEFAULT_INPUT_ARTIFACT = Path(
    "outputs/track_b_execution_core/phase1_runtime_market_data/GC/5m/latest_runtime_candles.json"
)
NY_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")
NON_ROUTING_FLAG_KEYS = (
    "non_authoritative",
    "diagnostic_only",
    "paper_shadow_only",
    "observer_only",
    "live_money_eligible",
    "order_intent_created",
    "route_attempted",
    "broker_state_mutated",
    "broker_mutation",
    "lifecycle_mutated",
    "lifecycle_mutation",
    "route_capable",
    "submit_capable",
)
STRUCTURAL_FIELD_NAMES = (
    "breakout_breaks_prior_1_high",
    "signal_retests_and_holds_breakout_level",
    "breakout_bar_expansion_is_normal",
    "breakout_bar_slope_is_flat",
    "range_expansion_ratio",
    "candidate_family",
)


def run_manual_shadow_check(
    *,
    input_artifact_path: Path = DEFAULT_INPUT_ARTIFACT,
    output_root: Path = DEFAULT_SHADOW_ROOT,
    write_shadow: bool = False,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    """Evaluate the observer from a persisted Phase-1 artifact, optionally writing shadow output."""

    evaluated_at = _coerce_datetime(now) or _utc_now()
    artifact_path = Path(input_artifact_path)
    source_doc, load_error = _load_json_object(artifact_path)
    observer_artifact = _observer_artifact_from_phase1_doc(
        source_doc, input_artifact_path=artifact_path, now=evaluated_at
    )
    decision = evaluate_london_open_transition_shadow_candidate(observer_artifact, now=evaluated_at)
    output_paths = None
    if write_shadow:
        output_paths = write_shadow_artifacts(decision, output_root=output_root)
    return _preflight_report(
        input_artifact_path=artifact_path,
        source_doc=source_doc,
        observer_artifact=observer_artifact,
        decision=decision,
        load_error=load_error,
        evaluated_at=evaluated_at,
        write_shadow=write_shadow,
        output_paths=output_paths,
    )


def _load_json_object(path: Path) -> tuple[dict[str, Any], str | None]:
    if not path.exists():
        return {}, f"missing input artifact: {path}"
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, f"invalid JSON input artifact: {exc}"
    if not isinstance(loaded, dict):
        return {}, "input artifact is not a JSON object"
    return loaded, None


def _observer_artifact_from_phase1_doc(
    doc: Mapping[str, Any], *, input_artifact_path: Path, now: datetime
) -> dict[str, Any]:
    if not doc:
        return {}
    latest_bar = _latest_completed_bar(doc)
    bar_ts = _bar_timestamp(latest_bar) or _field(doc, "last_completed_bar_ts", "timestamp", "bar_end_ts")
    generated_at = _field(doc, "generated_at", "artifact_generated_at")
    freshness = _freshness_status(doc, now=now)
    readiness = _readiness_value(doc)
    normalized: dict[str, Any] = {
        "phase1_artifact_present": True,
        "required_market_data_provenance": _field(doc, "required_market_data_provenance", "source", "provenance"),
        "generated_at": generated_at,
        "timestamp": bar_ts,
        "bar_id": _bar_id(doc, latest_bar, bar_ts),
        "instrument": str(
            _field(doc, "instrument", "symbol", default=_symbol_from_path(input_artifact_path)) or ""
        ).upper(),
        "session": _field(doc, "observed_session_label", "session"),
        "canonical_session_label": _field(doc, "canonical_session_label"),
        "direction": _field(doc, "direction"),
        "range_regime": _field(doc, "range_regime"),
        "atr_regime": _field(doc, "atr_regime"),
        "freshness_state": freshness["state"],
        "readiness_state": readiness,
        "startup_catchup_flag": bool(_field(doc, "startup_catchup", "startup_catchup_flag", default=False)),
        "source_feature_values": dict(_field(doc, "source_feature_values", default={}) or {}),
    }
    if latest_bar:
        normalized["last_completed_bar"] = dict(latest_bar)
        normalized["would_have_shadow_entry_price"] = _field(latest_bar, "open")
    for name in STRUCTURAL_FIELD_NAMES:
        value = _field(doc, name)
        if value is not None:
            normalized[name] = value
    return normalized


def _preflight_report(
    *,
    input_artifact_path: Path,
    source_doc: Mapping[str, Any],
    observer_artifact: Mapping[str, Any],
    decision: Mapping[str, Any],
    load_error: str | None,
    evaluated_at: datetime,
    write_shadow: bool,
    output_paths: Mapping[str, str] | None,
) -> dict[str, Any]:
    freshness = _freshness_status(source_doc, now=evaluated_at) if source_doc else {"state": "MISSING", "ok": False}
    provenance = str(_field(source_doc, "required_market_data_provenance", "source", "provenance", default="") or "")
    readiness = _readiness_value(source_doc) if source_doc else None
    report = {
        "mode": "manual_shadow_run" if write_shadow else "preflight_check",
        "observer_only": True,
        "input_artifact_path": str(input_artifact_path),
        "input_artifact_loaded": load_error is None,
        "input_artifact_error": load_error,
        "evaluated_at": evaluated_at.isoformat(),
        "freshness_status": freshness,
        "provenance_status": {
            "value": provenance or None,
            "required": REQUIRED_PROVENANCE,
            "ok": provenance == REQUIRED_PROVENANCE,
        },
        "readiness_status": {
            "value": readiness,
            "ok": bool(readiness and "READY" in str(readiness).upper()),
        },
        "observed_bar_timestamp": observer_artifact.get("timestamp"),
        "candidate": {
            "accepted": bool(decision.get("accepted")),
            "decision": decision.get("decision"),
            "reject_reason": decision.get("reject_reason"),
        },
        "non_routing_flags": {key: decision.get(key) for key in NON_ROUTING_FLAG_KEYS},
        "wrote_shadow_artifacts": bool(write_shadow),
        "artifact_paths": dict(output_paths or {}),
    }
    return report


def _latest_completed_bar(doc: Mapping[str, Any]) -> dict[str, Any]:
    rows = _field(doc, "bars", "candles", default=[])
    if not isinstance(rows, list):
        return {}
    completed = [row for row in rows if isinstance(row, dict) and row.get("completed", True) is True]
    if not completed:
        return {}
    return dict(completed[-1])


def _bar_timestamp(bar: Mapping[str, Any]) -> Any:
    return _field(bar, "bar_end", "bar_end_ts", "timestamp", "candle_timestamp")


def _bar_id(doc: Mapping[str, Any], bar: Mapping[str, Any], bar_ts: Any) -> str:
    existing = _field(doc, "bar_id")
    if existing:
        return str(existing)
    symbol = str(_field(doc, "instrument", "symbol", default="UNKNOWN") or "UNKNOWN").upper()
    suffix = bar_ts or "missing_bar_ts"
    return f"{symbol}-{suffix}"


def _freshness_status(doc: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    generated_at = _coerce_datetime(_field(doc, "generated_at", "artifact_generated_at"))
    freshness_seconds = _float_or_none(_field(doc, "freshness_seconds"))
    if generated_at is None:
        return {"state": "MISSING", "ok": False, "age_seconds": None, "max_age_seconds": freshness_seconds}
    age_seconds = max(0.0, (now - generated_at).total_seconds())
    max_age = freshness_seconds if freshness_seconds is not None else 600.0
    ok = age_seconds <= max_age
    return {
        "state": "FRESH" if ok else "STALE",
        "ok": ok,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age,
        "generated_at": generated_at.isoformat(),
    }


def _readiness_value(doc: Mapping[str, Any]) -> str | None:
    value = _field(doc, "readiness_state", "canonical_readiness", "realtime_feed_block_reason")
    if value is not None:
        return str(value)
    if _field(doc, "runtime_candles_ready", "realtime_feed_confirmed") is True:
        return "READY"
    return None


def _field(mapping: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    source = mapping.get("source_feature_values")
    if isinstance(source, Mapping):
        for name in names:
            if name in source:
                return source[name]
    return default


def _symbol_from_path(path: Path) -> str | None:
    parts = [part.upper() for part in path.parts]
    for symbol in ("GC", "MGC"):
        if symbol in parts:
            return symbol
    return None


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        resolved = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            resolved = datetime.fromisoformat(text)
        except ValueError:
            return None
    if resolved.tzinfo is None:
        return resolved.replace(tzinfo=NY_TZ)
    return resolved


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> datetime:
    return datetime.now(tz=UTC_TZ)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="london-open-transition-shadow-manual")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Read and report only; do not write shadow artifacts.")
    mode.add_argument("--write-shadow", action="store_true", help="Write latest/history shadow artifacts after evaluation.")
    parser.add_argument(
        "--artifact", type=Path, default=DEFAULT_INPUT_ARTIFACT, help="Phase-1 runtime candle JSON artifact."
    )
    parser.add_argument(
        "--output-root", type=Path, default=DEFAULT_SHADOW_ROOT, help="Shadow-only output directory."
    )
    parser.add_argument("--now", default=None, help="Optional ISO timestamp for deterministic checks.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run_manual_shadow_check(
        input_artifact_path=args.artifact,
        output_root=args.output_root,
        write_shadow=bool(args.write_shadow),
        now=args.now,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
