"""Read-only Track B PAPER managed-position registry authority.

Managed Position Registry authority lives in execution_core; dashboard
artifacts are projections and must not be used as runtime, readiness, or
routing authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    is_registry_eligible,
    normalize_lifecycle_state,
    requires_operator_action,
)
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata


NO_MANAGED_POSITIONS = "NO_MANAGED_POSITIONS"
OPEN_MANAGED_MATCHED = "OPEN_MANAGED_MATCHED"
OPEN_MANAGED_EXIT_DUE = "OPEN_MANAGED_EXIT_DUE"
OPEN_MANAGED_CLOSE_WORKING = "OPEN_MANAGED_CLOSE_WORKING"
BROKER_BACKED_ADOPTION_REQUIRED = "BROKER_BACKED_ADOPTION_REQUIRED"
MANAGED_POSITION_METADATA_INCOMPLETE = "MANAGED_POSITION_METADATA_INCOMPLETE"
LIFECYCLE_WITHOUT_BROKER = "LIFECYCLE_WITHOUT_BROKER"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
STALE_MANAGED_POSITION_EVIDENCE = "STALE_MANAGED_POSITION_EVIDENCE"

DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_MANAGED_POSITION_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "managed_position_events.jsonl"
)
DEFAULT_DASHBOARD_MANAGED_POSITION_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_managed_positions.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_LIVE_POSITION_STATUS_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_trade_ledger"
    / "latest_track_b_live_position_status.json"
)
DEFAULT_LIFECYCLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)
DEFAULT_MANIFEST_ROOT = Path("outputs") / "track_b_execution_core" / "position_management_manifests"


@dataclass(frozen=True)
class TrackBManagedPositionRegistryConfig:
    repo_root: Path
    output_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    event_log_path: Path = DEFAULT_MANAGED_POSITION_EVENTS
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_MANAGED_POSITION_PROJECTION
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    live_position_status_path: Path = DEFAULT_LIVE_POSITION_STATUS_ARTIFACT
    lifecycle_root: Path = DEFAULT_LIFECYCLE_ROOT
    manifest_root: Path = DEFAULT_MANIFEST_ROOT
    artifact_max_age_seconds: float = 180.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_managed_position_registry(
    *,
    config: TrackBManagedPositionRegistryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    managed_order_registry = _read_json(config.resolve(config.managed_order_registry_path))
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    lifecycle_reports = _load_lifecycle_reports(config.resolve(config.lifecycle_root))
    manifests = _load_manifests(config.resolve(config.manifest_root))

    broker_positions = _list(reconciliation.get("track_b_broker_positions"))
    lifecycle_positions = [
        item for item in _list(reconciliation.get("track_b_lifecycle_positions")) if _lifecycle_position_registry_eligible(item)
    ]
    unresolved_ownership = _list(reconciliation.get("unresolved_submit_intent_ownership_records"))
    review_positions = _review_required_positions(
        reconciliation=reconciliation,
        live_position_status=live_position_status,
        lifecycle_reports=lifecycle_reports,
        position_truth=position_truth,
    )
    open_order_states = _list(open_order_truth.get("order_states"))
    managed_order_states = _list(managed_order_registry.get("managed_orders"))
    source_stale = _source_stale(
        now=actual_now,
        config=config,
        position_truth=position_truth,
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
        reconciliation=reconciliation,
    )
    managed_positions = _managed_positions(
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        review_positions=review_positions,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        lifecycle_reports=lifecycle_reports,
        manifests=manifests,
        source_stale=source_stale,
    )
    classification = _overall_classification(
        managed_positions=managed_positions,
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        review_positions=review_positions,
        source_stale=source_stale,
    )
    payload = {
        "schema_version": "track_b_managed_position_registry_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "classification": classification,
        "managed_positions": managed_positions,
        "broker_positions": broker_positions,
        "lifecycle_open_positions": lifecycle_positions,
        "review_required_positions": review_positions,
        "unresolved_submit_ownership": unresolved_ownership,
        "source_freshness": source_stale,
        "position_truth": _authority_summary(position_truth, config.resolve(config.position_truth_path)),
        "open_order_truth": _authority_summary(open_order_truth, config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _authority_summary(
            managed_order_registry,
            config.resolve(config.managed_order_registry_path),
        ),
        "reconciliation": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "review_required_count": reconciliation.get("review_required_count"),
            "unresolved_submit_intent_ownership_count": reconciliation.get(
                "unresolved_submit_intent_ownership_count"
            ),
            "generated_at": reconciliation.get("generated_at"),
            "artifact_path": str(config.resolve(config.reconciliation_path)),
        },
        "summary": {
            "classification": classification,
            "managed_position_count": len(managed_positions),
            "attention_required_count": sum(1 for item in managed_positions if item.get("attention_required") is True),
            "exit_due_count": sum(1 for item in managed_positions if item.get("exit_due") is True),
            "close_working_count": sum(1 for item in managed_positions if item.get("close_order_state")),
            "suspicious_managed_order_count": sum(
                1
                for item in managed_positions
                if str(_mapping(item.get("managed_order_state")).get("classification") or "") == "CLOSE_ORDER_SUSPICIOUS"
            ),
            "duplicate_close_risk_count": sum(
                1
                for item in managed_positions
                if str(_mapping(item.get("managed_order_state")).get("classification") or "")
                == "DUPLICATE_CLOSE_ORDER_BLOCKED"
            ),
            "broker_position_count": len(broker_positions),
            "lifecycle_position_count": len(lifecycle_positions),
            "review_required_count": len(review_positions),
        },
        "event_state": _event_state(classification=classification, managed_positions=managed_positions),
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
            "manifest_root": str(config.resolve(config.manifest_root)),
        },
    }
    return payload


def write_track_b_managed_position_registry(
    *,
    config: TrackBManagedPositionRegistryConfig,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[Path, list[dict[str, Any]]]:
    output_path = config.resolve(config.output_path)
    event_log_path = config.resolve(config.event_log_path)
    previous = _read_json(output_path)
    events = build_managed_position_events(previous=previous, current=payload, now=now)
    _write_json_atomic(output_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_managed_position_projection(authority_payload=payload, authority_path=output_path),
        )
    if events:
        event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with event_log_path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
    return output_path, events


def build_dashboard_managed_position_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_managed_position_dashboard_projection_v1",
        **build_projection_metadata(source_authority_path=authority_path),
    }


def build_managed_position_events(
    *,
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    previous_state = _mapping((previous or {}).get("event_state"))
    current_state = _mapping(current.get("event_state"))
    if previous_state == current_state:
        return []
    actual_now = _ensure_utc(now or datetime.now(UTC))
    return [
        {
            "schema_version": "track_b_managed_position_event_v1",
            "event_type": "MANAGED_POSITION_REGISTRY_CHANGED",
            "generated_at": actual_now.isoformat(),
            "previous_classification": previous_state.get("classification"),
            "classification": current_state.get("classification"),
            "previous_signature": previous_state.get("signature"),
            "signature": current_state.get("signature"),
            "read_only": True,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
    ]


def _managed_positions(
    *,
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    review_positions: list[dict[str, Any]],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    source_stale: Mapping[str, Any],
) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    keys = sorted(
        {
            _position_key(item)
            for item in [*broker_positions, *lifecycle_positions, *review_positions]
            if _position_key(item)
        }
    )
    for key in keys:
        broker = _first_match(broker_positions, key)
        lifecycle = _first_match(lifecycle_positions, key)
        review = _first_match(review_positions, key)
        lifecycle_id = str((lifecycle or review or {}).get("lifecycle_id") or "")
        lifecycle_report = _lifecycle_report(lifecycle_id, lifecycle_reports)
        manifest = _manifest_for_position(
            lifecycle=lifecycle or review or {},
            lifecycle_report=lifecycle_report,
            manifests=manifests,
        )
        close_order_state = _close_order_state(key=key, open_order_states=open_order_states)
        managed_order_state = _managed_order_state(key=key, managed_order_states=managed_order_states)
        effective_close_order_state = close_order_state or managed_order_state
        classification = _position_classification(
            broker=broker,
            lifecycle=lifecycle,
            review=review,
            lifecycle_report=lifecycle_report,
            manifest=manifest,
            close_order_state=effective_close_order_state,
            source_stale=source_stale,
        )
        exit_due = _exit_due(lifecycle=lifecycle, lifecycle_report=lifecycle_report, classification=classification)
        position = {
            "classification": OPEN_MANAGED_EXIT_DUE if exit_due and classification == OPEN_MANAGED_MATCHED else classification,
            "symbol": _symbol(broker or lifecycle or review),
            "contract_key": (lifecycle or review or {}).get("contract_key") or _contract_key_from_broker(broker or {}),
            "local_symbol": (lifecycle or review or broker or {}).get("local_symbol"),
            "con_id": (lifecycle or review or broker or {}).get("con_id"),
            "side": (lifecycle or review or {}).get("side") or _side_from_broker(broker or {}),
            "quantity": (lifecycle or review or broker or {}).get("quantity"),
            "lane_id": (lifecycle or review or manifest or {}).get("lane_id"),
            "strategy_id": (lifecycle or review or lifecycle_report or manifest or {}).get("strategy_id"),
            "lifecycle_id": lifecycle_id or None,
            "manifest_id": (manifest or {}).get("entry_intent_id"),
            "manifest_path": _manifest_path(manifest),
            "entry_time": (lifecycle or review or lifecycle_report or {}).get("entry_timestamp")
            or _mapping(lifecycle_report.get("entry_fill")).get("filled_at"),
            "entry_price": (lifecycle or review or {}).get("avg_entry_price")
            or _mapping(lifecycle_report.get("entry_fill")).get("price"),
            "managed_exit_policy_id": _managed_exit_policy_id(lifecycle, review, lifecycle_report, manifest),
            "bars_since_entry": _bars_since_entry(lifecycle, lifecycle_report),
            "exit_due": bool(exit_due),
            "exit_due_state": _exit_due_state(exit_due),
            "close_order_state": effective_close_order_state,
            "managed_order_state": managed_order_state,
            "reconciliation_status": _reconciliation_status(broker=broker, lifecycle=lifecycle, review=review),
            "attention_required": classification
            in {
                BROKER_BACKED_ADOPTION_REQUIRED,
                MANAGED_POSITION_METADATA_INCOMPLETE,
                LIFECYCLE_WITHOUT_BROKER,
                REVIEW_REQUIRED,
                STALE_MANAGED_POSITION_EVIDENCE,
            },
            "recommended_operator_action": _recommended_action(
                classification=OPEN_MANAGED_EXIT_DUE if exit_due and classification == OPEN_MANAGED_MATCHED else classification
            ),
            "broker_position": broker,
            "lifecycle_position": lifecycle,
            "review_required_position": review,
        }
        positions.append(position)
    return positions


def _position_classification(
    *,
    broker: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
    review: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    manifest: Mapping[str, Any] | None,
    close_order_state: Mapping[str, Any] | None,
    source_stale: Mapping[str, Any],
) -> str:
    if source_stale.get("stale") is True:
        return STALE_MANAGED_POSITION_EVIDENCE
    if review or _truthy(lifecycle_report.get("review_required")) or _lifecycle_requires_operator_action(lifecycle, lifecycle_report):
        return REVIEW_REQUIRED
    if broker and not lifecycle:
        return BROKER_BACKED_ADOPTION_REQUIRED
    if lifecycle and not _managed_exit_policy_id(lifecycle, None, lifecycle_report, manifest):
        return MANAGED_POSITION_METADATA_INCOMPLETE
    if lifecycle and not broker:
        return LIFECYCLE_WITHOUT_BROKER
    if close_order_state:
        return OPEN_MANAGED_CLOSE_WORKING
    if lifecycle and broker:
        return OPEN_MANAGED_MATCHED
    return NO_MANAGED_POSITIONS


def _overall_classification(
    *,
    managed_positions: list[dict[str, Any]],
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    review_positions: list[dict[str, Any]],
    source_stale: Mapping[str, Any],
) -> str:
    if source_stale.get("stale") is True:
        return STALE_MANAGED_POSITION_EVIDENCE
    if not broker_positions and not lifecycle_positions and not review_positions and not managed_positions:
        return NO_MANAGED_POSITIONS
    priority = [
        REVIEW_REQUIRED,
        BROKER_BACKED_ADOPTION_REQUIRED,
        MANAGED_POSITION_METADATA_INCOMPLETE,
        LIFECYCLE_WITHOUT_BROKER,
        OPEN_MANAGED_CLOSE_WORKING,
        OPEN_MANAGED_EXIT_DUE,
        OPEN_MANAGED_MATCHED,
    ]
    classifications = [str(item.get("classification") or "") for item in managed_positions]
    for item in priority:
        if item in classifications:
            return item
    return classifications[0] if classifications else NO_MANAGED_POSITIONS


def _review_required_positions(
    *,
    reconciliation: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
    position_truth: Mapping[str, Any],
) -> list[dict[str, Any]]:
    values = _list(reconciliation.get("review_required_positions")) or _list(
        live_position_status.get("review_required_positions")
    )
    if values:
        return values
    if not _active_lifecycle_report_evidence(
        reconciliation=reconciliation,
        live_position_status=live_position_status,
        position_truth=position_truth,
    ):
        return []
    return [
        report
        for report in lifecycle_reports
        if report.get("review_required") is True or "REVIEW" in str(report.get("paper_lifecycle_classification") or "")
    ]


def _active_lifecycle_report_evidence(
    *,
    reconciliation: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    position_truth: Mapping[str, Any],
) -> bool:
    if _list(reconciliation.get("track_b_broker_positions")):
        return True
    if _list(reconciliation.get("track_b_lifecycle_positions")):
        return True
    if _list(reconciliation.get("unresolved_submit_intent_ownership_records")):
        return True
    if _int_or_none(reconciliation.get("review_required_count")):
        return True
    if _int_or_none(reconciliation.get("unresolved_submit_intent_ownership_count")):
        return True
    if _int_or_none(live_position_status.get("open_position_count")):
        return True
    if _list(live_position_status.get("positions")) or _list(live_position_status.get("open_positions")):
        return True
    position_summary = _mapping(position_truth.get("summary"))
    position_classification = str(position_truth.get("classification") or position_summary.get("overall_classification") or "")
    if position_classification in {"CLEAN_FLAT_READY", "FLAT_CLEAN"}:
        return False
    return position_summary.get("broker_exposure_present") is True


def _exit_due(*, lifecycle: Mapping[str, Any] | None, lifecycle_report: Mapping[str, Any], classification: str) -> bool:
    if classification != OPEN_MANAGED_MATCHED:
        return False
    policy = str(_managed_exit_policy_id(lifecycle, None, lifecycle_report, None) or "")
    if policy != "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1":
        return False
    bars = _bars_since_entry(lifecycle, lifecycle_report)
    required = _int_or_none((lifecycle or {}).get("required_completed_5m_bars")) or _int_or_none(
        lifecycle_report.get("managed_exit_policy_max_completed_5m_bars")
    ) or 3
    return bars is not None and bars >= required


def _close_order_state(*, key: str, open_order_states: list[dict[str, Any]]) -> dict[str, Any] | None:
    for state in open_order_states:
        if state.get("is_close_order") is True and _position_key(state.get("order") or state) == key:
            return dict(state)
    return None


def _managed_order_state(*, key: str, managed_order_states: list[dict[str, Any]]) -> dict[str, Any] | None:
    for state in managed_order_states:
        if state.get("is_close_order") is True and _position_key(state) == key:
            return dict(state)
    return None


def _source_stale(
    *,
    now: datetime,
    config: TrackBManagedPositionRegistryConfig,
    position_truth: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    sources = {
        "position_truth": position_truth.get("generated_at"),
        "open_order_truth": open_order_truth.get("generated_at"),
        "reconciliation": reconciliation.get("generated_at"),
    }
    if managed_order_registry:
        sources["managed_order_registry"] = managed_order_registry.get("generated_at")
    ages = {name: _age_seconds(value, now) for name, value in sources.items()}
    stale_sources = [
        name
        for name, age in ages.items()
        if age is None or age > float(config.artifact_max_age_seconds)
    ]
    return {
        "stale": bool(stale_sources),
        "stale_sources": stale_sources,
        "ages_seconds": ages,
        "ttl_seconds": float(config.artifact_max_age_seconds),
    }


def _authority_summary(payload: Mapping[str, Any], path: Path) -> dict[str, Any]:
    return {
        "classification": payload.get("classification")
        or _mapping(payload.get("summary")).get("overall_classification"),
        "generated_at": payload.get("generated_at"),
        "artifact_path": str(path),
        "projection_only": payload.get("projection_only") is True,
    }


def _event_state(*, classification: str, managed_positions: list[dict[str, Any]]) -> dict[str, Any]:
    signature_items = [
        "|".join(
            str(value or "")
            for value in (
                item.get("classification"),
                item.get("local_symbol"),
                item.get("side"),
                item.get("quantity"),
                item.get("lifecycle_id"),
                item.get("exit_due"),
            )
        )
        for item in managed_positions
    ]
    return {
        "classification": classification,
        "signature": ";".join(sorted(signature_items)) or classification,
    }


def _recommended_action(*, classification: str) -> str:
    return {
        NO_MANAGED_POSITIONS: "No managed PAPER positions require action.",
        OPEN_MANAGED_MATCHED: "Observe; position is broker/lifecycle matched.",
        OPEN_MANAGED_EXIT_DUE: "Observe runtime-managed exit path; do not manually interfere unless safety degrades.",
        OPEN_MANAGED_CLOSE_WORKING: "Monitor existing close order; do not submit a duplicate close.",
        BROKER_BACKED_ADOPTION_REQUIRED: "Run scoped broker-backed adoption before any close remediation.",
        MANAGED_POSITION_METADATA_INCOMPLETE: "Repair manifest/lifecycle management metadata before exit handling.",
        LIFECYCLE_WITHOUT_BROKER: "Review lifecycle artifact against broker-flat truth; local cleanup may be needed.",
        REVIEW_REQUIRED: "Review lifecycle diagnostics; do not restart trading until resolved.",
        STALE_MANAGED_POSITION_EVIDENCE: "Refresh authority artifacts before acting.",
    }.get(classification, "Review managed position state.")


def _reconciliation_status(
    *,
    broker: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
    review: Mapping[str, Any] | None,
) -> str:
    if review:
        return REVIEW_REQUIRED
    if broker and lifecycle:
        return OPEN_MANAGED_MATCHED
    if broker and not lifecycle:
        return BROKER_BACKED_ADOPTION_REQUIRED
    if lifecycle and not broker:
        return LIFECYCLE_WITHOUT_BROKER
    return NO_MANAGED_POSITIONS


def _lifecycle_position_registry_eligible(position: Mapping[str, Any]) -> bool:
    state = _lifecycle_state_from_mapping(position)
    if not state:
        return True
    return is_registry_eligible(state)


def _lifecycle_requires_operator_action(
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
) -> bool:
    state = _lifecycle_state_from_mapping(lifecycle or lifecycle_report)
    return bool(state and requires_operator_action(state))


def _lifecycle_state_from_mapping(payload: Mapping[str, Any]) -> str:
    return normalize_lifecycle_state(
        payload.get("final_position_status")
        or payload.get("lifecycle_status")
        or payload.get("paper_lifecycle_classification")
        or payload.get("strategy_managed_lifecycle_classification")
        or payload.get("classification")
    )


def _load_lifecycle_reports(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    reports: list[dict[str, Any]] = []
    for path in root.glob("*/track_b_strategy_managed_paper_lifecycle_report.json"):
        payload = _read_json(path)
        if payload:
            payload.setdefault("report_json_path", str(path))
            reports.append(payload)
    return reports


def _load_manifests(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    manifests: list[dict[str, Any]] = []
    for path in root.glob("*.json"):
        payload = _read_json(path)
        if payload:
            payload.setdefault("manifest_path", str(path))
            manifests.append(payload)
    return manifests


def _lifecycle_report(lifecycle_id: str, lifecycle_reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not lifecycle_id:
        return {}
    matches = [item for item in lifecycle_reports if str(item.get("lifecycle_id") or "") == lifecycle_id]
    return matches[-1] if matches else {}


def _manifest_for_position(
    *,
    lifecycle: Mapping[str, Any],
    lifecycle_report: Mapping[str, Any],
    manifests: list[dict[str, Any]],
) -> dict[str, Any] | None:
    explicit = str(
        lifecycle.get("position_management_manifest_path")
        or lifecycle_report.get("position_management_manifest_path")
        or ""
    )
    if explicit:
        for manifest in manifests:
            if str(manifest.get("manifest_path") or "").endswith(Path(explicit).name):
                return manifest
    lifecycle_id = str(lifecycle.get("lifecycle_id") or lifecycle_report.get("lifecycle_id") or "")
    for manifest in manifests:
        if lifecycle_id and str(manifest.get("lifecycle_id") or "") == lifecycle_id:
            return manifest
    return None


def _manifest_path(manifest: Mapping[str, Any] | None) -> str | None:
    if not manifest:
        return None
    return str(manifest.get("manifest_path") or "") or None


def _managed_exit_policy_id(
    lifecycle: Mapping[str, Any] | None,
    review: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    manifest: Mapping[str, Any] | None,
) -> str:
    return str(
        (lifecycle or {}).get("managed_exit_policy_id")
        or (review or {}).get("managed_exit_policy_id")
        or lifecycle_report.get("managed_exit_policy_id")
        or (manifest or {}).get("managed_exit_policy_id")
        or ""
    )


def _bars_since_entry(lifecycle: Mapping[str, Any] | None, lifecycle_report: Mapping[str, Any]) -> int | None:
    for value in (
        (lifecycle or {}).get("bars_since_fill"),
        (lifecycle or {}).get("completed_bars_since_entry"),
        lifecycle_report.get("bars_since_fill"),
        lifecycle_report.get("open_position_age_completed_5m_bars"),
    ):
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _exit_due_state(exit_due: bool) -> str:
    return "EXIT_DUE" if exit_due else "NOT_DUE_OR_UNKNOWN"


def _position_key(row: Mapping[str, Any]) -> str:
    local_symbol = str(row.get("local_symbol") or "").upper()
    if local_symbol:
        return local_symbol
    contract_key = str(row.get("contract_key") or row.get("position_key") or "").upper()
    return contract_key


def _first_match(rows: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
    matches = [row for row in rows if _position_key(row) == key]
    return matches[-1] if matches else None


def _symbol(row: Mapping[str, Any] | None) -> str | None:
    if not row:
        return None
    return str(row.get("symbol") or row.get("track_b_root") or row.get("instrument_family") or "").upper() or None


def _contract_key_from_broker(row: Mapping[str, Any]) -> str | None:
    symbol = _symbol(row)
    expiry = str(row.get("expiry") or "").strip()
    return f"{symbol}-{expiry[:6]}" if symbol and expiry else None


def _side_from_broker(row: Mapping[str, Any]) -> str | None:
    quantity = _decimal(row.get("quantity"))
    if quantity is None or quantity == 0:
        return None
    return "LONG" if quantity > 0 else "SHORT"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, _to_jsonable(dict(payload)))


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_time(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _age_seconds(value: object, now: datetime) -> float | None:
    parsed = _parse_time(value)
    return None if parsed is None else max(0.0, (now - parsed).total_seconds())


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: object) -> bool:
    return value is True or str(value).strip().lower() == "true"


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    return value
