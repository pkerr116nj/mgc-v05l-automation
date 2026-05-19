"""Diagnostic-only paper-shadow observer for the London post-04:00 transition pocket."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


CANDIDATE_ID = "gc_mgc_london_open_post_04_transition_shadow_v1"
DEFAULT_SHADOW_ROOT = Path("outputs/reports/track_b_london_open_transition_shadow")
NY_TZ = ZoneInfo("America/New_York")
REQUIRED_PROVENANCE = "DATABENTO_REALTIME_PHASE1"
MAX_ARTIFACT_AGE = timedelta(minutes=10)
REQUIRED_STRUCTURAL_FIELDS = (
    "breakout_breaks_prior_1_high",
    "signal_retests_and_holds_breakout_level",
    "breakout_bar_expansion_is_normal",
    "breakout_bar_slope_is_flat",
    "range_expansion_ratio",
    "candidate_family",
)


@dataclass(frozen=True)
class LondonOpenTransitionShadowObserver:
    """Intentless observer that emits shadow diagnostics and never routes."""

    output_root: Path = DEFAULT_SHADOW_ROOT
    now: datetime | None = None

    def evaluate_and_emit(self, artifact: Mapping[str, Any]) -> dict[str, Any]:
        decision = evaluate_london_open_transition_shadow_candidate(artifact, now=self.now)
        write_shadow_artifacts(decision, output_root=self.output_root)
        return decision


def evaluate_london_open_transition_shadow_candidate(
    artifact: Mapping[str, Any], *, now: datetime | None = None
) -> dict[str, Any]:
    """Evaluate one completed-bar artifact without creating intents or touching broker paths."""

    evaluated_at = _coerce_datetime(now) or _utc_now()
    base = _base_payload(artifact=artifact, evaluated_at=evaluated_at)
    reject_reason = _reject_reason(artifact, evaluated_at=evaluated_at)
    accepted = reject_reason is None
    return {
        **base,
        "decision": "ACCEPTED_SHADOW_CANDIDATE" if accepted else "REJECTED_SHADOW_CANDIDATE",
        "accepted": accepted,
        "reject_reason": reject_reason,
        "non_authoritative": True,
        "diagnostic_only": True,
        "paper_shadow_only": True,
        "observer_only": True,
        "live_money_eligible": False,
        "order_intent_created": False,
        "route_attempted": False,
        "broker_state_mutated": False,
        "broker_mutation": False,
        "lifecycle_mutated": False,
        "lifecycle_mutation": False,
        "route_capable": False,
        "submit_capable": False,
        "canonical_session_labels_changed": False,
        "global_session_policy_widened": False,
        "would_have_shadow_entry_price": _field(artifact, "next_bar_open", "would_have_shadow_entry_price"),
    }


def write_shadow_artifacts(decision: Mapping[str, Any], *, output_root: Path = DEFAULT_SHADOW_ROOT) -> dict[str, str]:
    """Write latest-state and append-only diagnostic artifacts."""

    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    latest_path = output_root / "latest_state.json"
    history_path = output_root / "shadow_diagnostic_history.jsonl"
    latest_path.write_text(json.dumps(decision, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(decision, sort_keys=True, default=_json_ready) + "\n")
    return {"latest_state": str(latest_path), "history": str(history_path)}


def _reject_reason(artifact: Mapping[str, Any], *, evaluated_at: datetime) -> str | None:
    if not artifact:
        return "missing_phase1_artifact"
    if _field(artifact, "phase1_artifact_present") is not True:
        return "missing_phase1_artifact"
    provenance = str(_field(artifact, "required_market_data_provenance", "provenance", "source_provenance") or "")
    if provenance != REQUIRED_PROVENANCE:
        return "wrong_provenance"
    generated_at = _coerce_datetime(_field(artifact, "generated_at", "artifact_generated_at", "bar_completed_at"))
    if generated_at is None:
        return "missing_artifact_timestamp"
    if evaluated_at - generated_at > MAX_ARTIFACT_AGE:
        return "stale_artifact"
    if str(_field(artifact, "session", "observed_session_label") or "") != "LONDON_OPEN":
        return "wrong_observed_session"
    timestamp = _coerce_datetime(_field(artifact, "timestamp", "bar_end_ts", "completed_bar_end_ts"))
    if timestamp is None:
        return "missing_bar_timestamp"
    minutes = _ny_minutes(timestamp)
    if minutes == 4 * 60:
        return "exact_04_00_boundary"
    if not (4 * 60 < minutes <= 4 * 60 + 10):
        return "outside_post_04_10m_window"
    if str(_field(artifact, "instrument", "symbol") or "").upper() not in {"GC", "MGC"}:
        return "unsupported_instrument"
    if _direction(artifact) != "long":
        return "wrong_direction"
    range_regime = _range_regime(artifact)
    if range_regime == "range_compressed":
        return "compressed_range"
    if range_regime not in {"range_normal", "range_expanded"}:
        return "missing_or_unknown_range_regime"
    missing = [field for field in REQUIRED_STRUCTURAL_FIELDS if _field(artifact, field) is None]
    if missing:
        return "missing_structural_predicate_fields"
    if not _structural_predicates_pass(artifact):
        return "structural_predicate_reject"
    freshness_value = _field(artifact, "freshness_state", "runtime_freshness_state")
    if freshness_value is None:
        return "missing_freshness_state"
    freshness = str(freshness_value or "")
    if freshness.upper() not in {"FRESH", "READY", "READY_SUBMIT_CAPABLE"}:
        return "runtime_freshness_not_fresh"
    readiness_value = _field(artifact, "readiness_state", "canonical_readiness")
    if readiness_value is None:
        return "missing_readiness_state"
    readiness = str(readiness_value or "")
    if "READY" not in readiness.upper():
        return "readiness_not_converged"
    if bool(_field(artifact, "startup_catchup", "startup_catchup_flag", default=False)):
        return "startup_catchup_diagnostic_only"
    return None


def _base_payload(*, artifact: Mapping[str, Any], evaluated_at: datetime) -> dict[str, Any]:
    timestamp = _coerce_datetime(_field(artifact, "timestamp", "bar_end_ts", "completed_bar_end_ts"))
    timestamp_ny = timestamp.astimezone(NY_TZ) if timestamp else None
    return {
        "candidate_id": CANDIDATE_ID,
        "schema_version": "london_open_transition_shadow_observer_v1",
        "evaluated_at": evaluated_at.isoformat(),
        "bar_id": str(_field(artifact, "bar_id", default="")),
        "timestamp_utc": timestamp.isoformat() if timestamp else None,
        "timestamp_ny": timestamp_ny.isoformat() if timestamp_ny else None,
        "instrument": str(_field(artifact, "instrument", "symbol") or "").upper(),
        "observed_session_label": _field(artifact, "session", "observed_session_label"),
        "canonical_session_label": _field(artifact, "canonical_session_label", "session"),
        "boundary_offset_minutes_after_04_00": (_ny_minutes(timestamp) - 4 * 60) if timestamp else None,
        "range_regime": _range_regime(artifact),
        "atr_regime": _field(artifact, "atr_regime"),
        "direction": _direction(artifact),
        "source_family": _field(artifact, "candidate_family", "source_family"),
        "structural_predicate_snapshot": {field: _field(artifact, field) for field in REQUIRED_STRUCTURAL_FIELDS},
        "freshness_state": _field(artifact, "freshness_state", "runtime_freshness_state"),
        "readiness_state": _field(artifact, "readiness_state", "canonical_readiness"),
        "startup_catchup_flag": bool(_field(artifact, "startup_catchup", "startup_catchup_flag", default=False)),
        "required_market_data_provenance": _field(
            artifact, "required_market_data_provenance", "provenance", "source_provenance"
        ),
    }


def _structural_predicates_pass(artifact: Mapping[str, Any]) -> bool:
    return (
        _field(artifact, "breakout_breaks_prior_1_high") is True
        and _field(artifact, "signal_retests_and_holds_breakout_level") is True
        and _field(artifact, "breakout_bar_expansion_is_normal") is True
        and _field(artifact, "breakout_bar_slope_is_flat") is True
    )


def _range_regime(artifact: Mapping[str, Any]) -> str | None:
    explicit = _field(artifact, "range_regime")
    if explicit is not None:
        return str(explicit)
    ratio = _float_or_none(_field(artifact, "range_expansion_ratio"))
    if ratio is None:
        return None
    if ratio < 0.85:
        return "range_compressed"
    if ratio <= 1.25:
        return "range_normal"
    return "range_expanded"


def _direction(artifact: Mapping[str, Any]) -> str:
    explicit = str(_field(artifact, "direction", default="") or "").lower()
    if explicit in {"long", "short"}:
        return explicit
    family = str(_field(artifact, "candidate_family", "source_family", default="") or "").lower()
    if "short" in family or "bear" in family:
        return "short"
    return "long"


def _field(artifact: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in artifact:
            return artifact[name]
    source = artifact.get("source_feature_values")
    if isinstance(source, Mapping):
        for name in names:
            if name in source:
                return source[name]
    return default


def _ny_minutes(timestamp: datetime) -> int:
    local = timestamp.astimezone(NY_TZ)
    return local.hour * 60 + local.minute


def _coerce_datetime(value: Any) -> datetime | None:
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
    return datetime.now(tz=ZoneInfo("UTC"))


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value
