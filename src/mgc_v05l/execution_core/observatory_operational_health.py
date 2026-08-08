"""Operational LED health producer for The Observatory.

The producer reads existing prepared status artifacts and normalizes only
producer-authored statuses into conventional LED states. It does not compute
readiness, Safe-State, broker coherence, reconciliation, or trading authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "observatory_operational_health_v1"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "observatory"
    / "operational_health"
    / "latest_operational_health.json"
)


@dataclass(frozen=True)
class HealthIndicatorSpec:
    indicator_id: str
    label: str
    artifact_path: Path | None
    status_keys: tuple[str, ...]
    not_ready_status: str | None = None


INDICATORS: tuple[HealthIndicatorSpec, ...] = (
    HealthIndicatorSpec(
        "market_data",
        "Market Data",
        Path("outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json"),
        ("final_classification", "classification", "status"),
    ),
    HealthIndicatorSpec("regime_context", "Regime / Context", None, (), not_ready_status="NOT_READY"),
    HealthIndicatorSpec(
        "magic_runtime",
        "Magic Runtime",
        Path("outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json"),
        ("classification", "runtime_status", "status"),
    ),
    HealthIndicatorSpec(
        "broker_tws",
        "Broker / TWS",
        Path("outputs/operator_dashboard/runtime/latest_broker_session_authority.json"),
        ("classification", "status"),
    ),
    HealthIndicatorSpec(
        "trade_evidence",
        "Trade Evidence",
        Path("outputs/track_b_execution_core/managed_exit_service/latest_managed_exit_service_status.json"),
        ("classification", "status"),
    ),
    HealthIndicatorSpec(
        "exposure_reconciliation",
        "Exposure / Reconciliation",
        Path("outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"),
        ("classification", "status"),
    ),
    HealthIndicatorSpec(
        "crr_research",
        "CRR / Research",
        Path("outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_record_validation_report.json"),
        ("status", "classification"),
    ),
    HealthIndicatorSpec(
        "prospective_validation",
        "Prospective Validation",
        Path("outputs/track_b_execution_core/research_analytics/prospective_nq_cohort_monitor/validation_report.json"),
        ("status", "classification"),
    ),
)


def build_operational_health(*, repo_root: Path = REPO_ROOT, generated_at: datetime | None = None) -> dict[str, Any]:
    now = _coerce_datetime(generated_at) or datetime.now(UTC)
    indicators = [_build_indicator(repo_root=repo_root, spec=spec) for spec in INDICATORS]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "producer": "observatory_operational_health",
        "status": "VALID_WITH_WARNINGS"
        if any(row["led_state"] in {"AMBER", "GRAY", "RED"} for row in indicators)
        else "READY",
        "source_contract": "PREPARED_PRODUCER_STATUS_ARTIFACTS",
        "indicators": indicators,
        "guardrails": {
            "display_only": True,
            "trading_input": False,
            "broker_authority": False,
            "runtime_authority": False,
            "strategy_input": False,
            "readiness_computation": False,
            "safe_state_computation": False,
            "reconciliation_computation": False,
        },
    }
    payload["deterministic_fingerprint"] = _fingerprint(payload)
    return payload


def write_operational_health(snapshot: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _build_indicator(*, repo_root: Path, spec: HealthIndicatorSpec) -> dict[str, Any]:
    if spec.artifact_path is None:
        status = spec.not_ready_status or "UNKNOWN"
        led = _map_led(spec.indicator_id, status)
        return {
            "indicator_id": spec.indicator_id,
            "label": spec.label,
            "producer_status": status,
            "source_artifact": None,
            "schema_version": None,
            "generated_at": None,
            "freshness": {},
            "led_state": led["led_state"],
            "mapping_type": led["mapping_type"],
            "mapping_justification": led["mapping_justification"],
            "source_error": None,
        }
    path = _resolve(repo_root, spec.artifact_path)
    display_path = _display_path(repo_root, path)
    if not path.exists():
        led = _map_led(spec.indicator_id, "UNKNOWN")
        return {
            "indicator_id": spec.indicator_id,
            "label": spec.label,
            "producer_status": "MISSING",
            "source_artifact": display_path,
            "schema_version": None,
            "generated_at": None,
            "freshness": {},
            "led_state": "GRAY",
            "mapping_type": "UNSUPPORTED",
            "mapping_justification": "Source artifact is missing; health is not inferred from absence.",
            "source_error": "source_artifact_missing",
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "indicator_id": spec.indicator_id,
            "label": spec.label,
            "producer_status": "INVALID",
            "source_artifact": display_path,
            "schema_version": None,
            "generated_at": None,
            "freshness": {},
            "led_state": "RED",
            "mapping_type": "DIRECT_NORMALIZATION",
            "mapping_justification": "Artifact is present but invalid JSON.",
            "source_error": str(exc),
        }
    status = _first_text(data, spec.status_keys) or "UNKNOWN"
    led = _map_led(spec.indicator_id, status)
    return {
        "indicator_id": spec.indicator_id,
        "label": spec.label,
        "producer_status": status,
        "source_artifact": display_path,
        "schema_version": data.get("schema_version"),
        "generated_at": data.get("generated_at") or data.get("authority_source_timestamp"),
        "freshness": _freshness_fields(data),
        "led_state": led["led_state"],
        "mapping_type": led["mapping_type"],
        "mapping_justification": led["mapping_justification"],
        "source_error": None,
    }


def _map_led(indicator_id: str, status: str) -> dict[str, str]:
    normalized = str(status or "UNKNOWN").upper()
    if normalized in {"UNKNOWN", "NOT_READY", "MISSING"}:
        return {
            "led_state": "GRAY",
            "mapping_type": "PASS_THROUGH" if normalized == "NOT_READY" else "UNSUPPORTED",
            "mapping_justification": "Producer does not provide an authoritative current health state.",
        }
    if normalized == "PHASE1_DATABENTO_LIVE_LISTENER_RUNNING":
        return {
            "led_state": "GREEN",
            "mapping_type": "DIRECT_NORMALIZATION",
            "mapping_justification": "Producer explicitly reports the Phase-1 live candle listener is running.",
        }
    if normalized in {"NO_ELIGIBLE_EXITS", "TRACK_B_MANAGED_EXIT_SERVICE_POST_BROKER_MUTATION_REFRESH_RUNNING", "APPLY_SUCCEEDED"}:
        return {
            "led_state": "GREEN",
            "mapping_type": "DIRECT_NORMALIZATION",
            "mapping_justification": "Managed Exit service status is explicitly non-failed for this service view.",
        }
    if normalized in {"VALID_WITH_WARNINGS", "STALE_RUNTIME_TRUTH", "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE", "WAITING_FOR_BROKER_TRUTH_SETTLEMENT", "OPEN_MANAGED_EXIT_DUE"}:
        return {
            "led_state": "AMBER",
            "mapping_type": "DIRECT_NORMALIZATION",
            "mapping_justification": "Producer explicitly reports warning, stale, unreliable, or settling evidence.",
        }
    if any(token in normalized for token in ("FAILED", "DISCONNECTED", "INVALID", "BLOCKED", "HARD_HOLD")):
        return {
            "led_state": "RED",
            "mapping_type": "DIRECT_NORMALIZATION",
            "mapping_justification": "Producer status contains an explicit failed, disconnected, invalid, or blocked condition.",
        }
    if any(token in normalized for token in ("WARNING", "DEGRADED", "STALE", "UNRELIABLE", "MISMATCH", "SETTLEMENT")):
        return {
            "led_state": "AMBER",
            "mapping_type": "DIRECT_NORMALIZATION",
            "mapping_justification": "Producer status names a degraded/warning/stale/unreliable state.",
        }
    return {
        "led_state": "GRAY",
        "mapping_type": "UNSUPPORTED",
        "mapping_justification": "No direct LED mapping is defined for this producer status.",
    }


def _first_text(data: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _freshness_fields(data: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "current_lag_seconds": data.get("current_lag_seconds"),
        "latest_durable_completed_bar_ts": data.get("latest_durable_completed_bar_ts"),
        "fresh_until": data.get("fresh_until"),
        "expires_at": data.get("expires_at")
        or (data.get("broker_position_lease") or {}).get("expires_at")
        or (data.get("broker_open_order_lease") or {}).get("expires_at"),
        "producer_fresh": data.get("fresh")
        or (data.get("broker_position_lease") or {}).get("fresh")
        or (data.get("broker_open_order_lease") or {}).get("fresh"),
    }


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _display_path(repo_root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _coerce_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _fingerprint(payload: Mapping[str, Any]) -> str:
    stable = {key: value for key, value in payload.items() if key != "deterministic_fingerprint"}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Observatory operational LED health from prepared status artifacts.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(list(argv) if argv is not None else None)
    snapshot = build_operational_health(repo_root=args.repo_root)
    output_path = _resolve(args.repo_root, args.output_json)
    write_operational_health(snapshot, output_path)
    print(json.dumps({"status": snapshot["status"], "output_json": str(output_path), "indicator_count": len(snapshot["indicators"])}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
