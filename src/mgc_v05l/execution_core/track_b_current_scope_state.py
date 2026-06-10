"""Canonical current-scope Track B PAPER state snapshot.

This module owns the post-mutation answer to "what is true now?" for Track B
PAPER exposure/order risk. Legacy projections remain inputs and diagnostics.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .track_b_atomic_io import write_json_atomic


REPO_ROOT = Path(__file__).resolve().parents[3]

DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "current_scope_state"
    / "latest_current_scope_state.json"
)

BROKER_TRUTH_LEASE_PATH = Path("outputs/operator_dashboard/runtime/latest_broker_truth_lease.json")
BROKER_RECONCILIATION_PATH = Path(
    "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
)
OPEN_ORDER_TRUTH_PATH = Path("outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json")
MANAGED_POSITIONS_PATH = Path("outputs/track_b_execution_core/managed_positions/latest_managed_positions.json")
MANAGED_ORDERS_PATH = Path("outputs/track_b_execution_core/managed_orders/latest_managed_orders.json")
BROKER_SESSION_AUTHORITY_PATH = Path("outputs/operator_dashboard/runtime/latest_broker_session_authority.json")
CANONICAL_READINESS_PATH = Path("outputs/operator_dashboard/runtime/latest_canonical_readiness.json")
CONTROL_PLANE_PATH = Path("outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json")
POST_MUTATION_REFRESH_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "post_broker_mutation_refresh"
    / "latest_post_broker_mutation_refresh.json"
)

PAPER_ACCOUNT = "DUM882026"
FRESHNESS_TTL_SECONDS = 300.0
BROKER_TTL_SECONDS = 900.0

CURRENT_SCOPE_CLASSIFICATIONS = {
    "FLAT",
    "OPEN_MANAGED",
    "OPEN_MANAGED_EXIT_DUE",
    "OPEN_UNMANAGED",
    "OPEN_ORDERS",
    "UNKNOWN_ORDER_RISK",
    "STALE_OBSERVABILITY_BROKER_SAFE",
    "BLOCKED_SAFETY",
    "UNKNOWN_REFRESH_REQUIRED",
}


@dataclass(frozen=True)
class CurrentScopeSource:
    name: str
    path: Path
    payload: dict[str, Any]
    state: str
    age_seconds: float | None

    @property
    def fresh(self) -> bool:
        return self.state == "FRESH"

    @property
    def available(self) -> bool:
        return self.state in {"FRESH", "STALE"}


def build_current_scope_state(
    *,
    repo_root: Path = REPO_ROOT,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    now: datetime | None = None,
    write: bool = False,
) -> dict[str, Any]:
    repo_root = Path(repo_root)
    actual_now = _ensure_utc(now or datetime.now(UTC))
    sources = _load_sources(repo_root=repo_root, now=actual_now)
    payload = _build_payload(repo_root=repo_root, output_path=output_path, now=actual_now, sources=sources)
    if write:
        write_json_atomic(_resolve(repo_root, output_path), payload)
    return payload


def write_current_scope_state(
    *,
    repo_root: Path = REPO_ROOT,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    now: datetime | None = None,
) -> dict[str, Any]:
    return build_current_scope_state(repo_root=repo_root, output_path=output_path, now=now, write=True)


def _build_payload(
    *,
    repo_root: Path,
    output_path: Path,
    now: datetime,
    sources: Mapping[str, CurrentScopeSource],
) -> dict[str, Any]:
    broker = sources["broker_truth_lease"]
    reconciliation = sources["reconciliation"]
    open_order_truth = sources["open_order_truth"]
    managed_positions = sources["managed_positions"]
    managed_orders = sources["managed_orders"]
    bsa = sources["broker_session_authority"]
    canonical = sources["canonical_readiness"]
    control_plane = sources["control_plane"]
    refresh = sources["post_mutation_refresh"]

    safety_blockers = _safety_blockers(sources)
    if safety_blockers:
        classification = "BLOCKED_SAFETY"
    elif not _broker_truth_current(broker):
        classification = "UNKNOWN_REFRESH_REQUIRED"
    elif _unknown_order_risk(broker.payload, reconciliation.payload, open_order_truth):
        classification = "UNKNOWN_ORDER_RISK"
    else:
        classification = _exposure_classification(
            broker=broker,
            reconciliation=reconciliation,
            open_order_truth=open_order_truth,
            managed_positions=managed_positions,
            managed_orders=managed_orders,
        )
    diagnostics = _diagnostics(sources, classification=classification)

    broker_positions = _track_b_positions(broker.payload, reconciliation.payload)
    broker_open_orders = _track_b_open_orders(broker.payload, reconciliation.payload)
    latest_executions = _latest_executions(broker.payload, reconciliation.payload)
    submit_allowed = canonical.payload.get("submit_allowed") is True
    canonical_classification = canonical.payload.get("canonical_readiness") or canonical.payload.get("classification")
    state = {
        "schema_version": "track_b_current_scope_state_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "account_id": _account_id(broker.payload, reconciliation.payload),
        "execution_domain": "TRACK_B_PAPER",
        "mode": "PAPER",
        "read_only": True,
        "broker_mutation_allowed": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "global_cancel_allowed": False,
        "broad_flatten_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broker_positions": broker_positions,
        "broker_position_count": len(broker_positions),
        "broker_open_orders": broker_open_orders,
        "broker_open_order_count": len(broker_open_orders),
        "unknown_broker_open_order_count": _max_count(
            broker.payload,
            reconciliation.payload,
            key="unknown_broker_open_order_count",
        ),
        "latest_relevant_executions": latest_executions,
        "bsa": {
            "classification": bsa.payload.get("classification"),
            "allowed_uses": _mapping(bsa.payload.get("allowed_uses")),
            "freshness_state": bsa.state,
            "age_seconds": bsa.age_seconds,
        },
        "reconciliation": {
            "classification": reconciliation.payload.get("classification"),
            "broker_reconciled": reconciliation.payload.get("broker_reconciled") is True,
            "freshness_state": reconciliation.state,
            "age_seconds": reconciliation.age_seconds,
        },
        "managed_positions": {
            "classification": managed_positions.payload.get("classification"),
            "count": len(_list(managed_positions.payload.get("managed_positions"))),
            "freshness_state": managed_positions.state,
            "age_seconds": managed_positions.age_seconds,
        },
        "managed_orders": {
            "classification": managed_orders.payload.get("classification"),
            "count": len(_list(managed_orders.payload.get("managed_orders"))),
            "freshness_state": managed_orders.state,
            "age_seconds": managed_orders.age_seconds,
        },
        "canonical_readiness": {
            "classification": canonical_classification,
            "submit_allowed": submit_allowed,
            "freshness_state": canonical.state,
            "age_seconds": canonical.age_seconds,
        },
        "current_price_availability": _current_price_availability(canonical.payload, broker.payload),
        "control_plane": {
            "classification": control_plane.payload.get("classification"),
            "freshness_state": control_plane.state,
            "age_seconds": control_plane.age_seconds,
            "diagnostic_only": True,
        },
        "post_mutation_refresh": {
            "classification": refresh.payload.get("classification"),
            "freshness_state": refresh.state,
            "age_seconds": refresh.age_seconds,
            "first_failing_step": refresh.payload.get("first_failing_step"),
            "diagnostic_only": True,
        },
        "diagnostics": diagnostics,
        "safety_blockers": safety_blockers,
        "source_states": {
            name: {"state": source.state, "age_seconds": source.age_seconds}
            for name, source in sources.items()
        },
        "artifact_paths": {
            name: str(source.path)
            for name, source in sources.items()
        }
        | {"current_scope_state": str(_resolve(repo_root, output_path))},
    }
    state["broker_state_for_ods"] = _ods_broker_state(classification)
    state["first_blocker_for_ods"] = _ods_first_blocker(state)
    state["next_safe_action_for_ods"] = _ods_next_safe_action(state)
    return state


def _load_sources(*, repo_root: Path, now: datetime) -> dict[str, CurrentScopeSource]:
    return {
        "broker_truth_lease": _read_source(
            name="broker_truth_lease",
            path=_resolve(repo_root, BROKER_TRUTH_LEASE_PATH),
            now=now,
            ttl_seconds=BROKER_TTL_SECONDS,
        ),
        "reconciliation": _read_source(
            name="reconciliation",
            path=_resolve(repo_root, BROKER_RECONCILIATION_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
        "open_order_truth": _read_source(
            name="open_order_truth",
            path=_resolve(repo_root, OPEN_ORDER_TRUTH_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
        "managed_positions": _read_source(
            name="managed_positions",
            path=_resolve(repo_root, MANAGED_POSITIONS_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
        "managed_orders": _read_source(
            name="managed_orders",
            path=_resolve(repo_root, MANAGED_ORDERS_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
        "broker_session_authority": _read_source(
            name="broker_session_authority",
            path=_resolve(repo_root, BROKER_SESSION_AUTHORITY_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
        "canonical_readiness": _read_source(
            name="canonical_readiness",
            path=_resolve(repo_root, CANONICAL_READINESS_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
        "control_plane": _read_source(
            name="control_plane",
            path=_resolve(repo_root, CONTROL_PLANE_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
        "post_mutation_refresh": _read_source(
            name="post_mutation_refresh",
            path=_resolve(repo_root, POST_MUTATION_REFRESH_PATH),
            now=now,
            ttl_seconds=FRESHNESS_TTL_SECONDS,
        ),
    }


def _read_source(*, name: str, path: Path, now: datetime, ttl_seconds: float) -> CurrentScopeSource:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return CurrentScopeSource(name=name, path=path, payload={}, state="MISSING", age_seconds=None)
    except json.JSONDecodeError:
        return CurrentScopeSource(name=name, path=path, payload={}, state="INVALID_SCHEMA", age_seconds=None)
    if not isinstance(raw, Mapping):
        return CurrentScopeSource(name=name, path=path, payload={}, state="INVALID_SCHEMA", age_seconds=None)
    payload = dict(raw)
    age = _age_seconds(payload, now)
    threshold = _freshness_threshold(payload, default=ttl_seconds)
    state = "FRESH" if age is not None and age <= threshold else "STALE"
    return CurrentScopeSource(name=name, path=path, payload=payload, state=state, age_seconds=age)


def _broker_truth_current(source: CurrentScopeSource) -> bool:
    if not source.fresh:
        return False
    payload = source.payload
    position_lease = _mapping(payload.get("broker_position_lease"))
    order_lease = _mapping(payload.get("broker_open_order_lease"))
    positions_known = (
        payload.get("positions_complete") is True
        or position_lease.get("complete") is True
        or "track_b_broker_position_count" in payload
    )
    orders_known = (
        payload.get("open_orders_complete") is True
        or order_lease.get("complete") is True
        or "track_b_broker_open_order_count" in payload
    )
    return positions_known and orders_known


def _unknown_order_risk(
    broker: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    open_order_truth: CurrentScopeSource,
) -> bool:
    if _max_count(broker, reconciliation, key="unknown_broker_open_order_count") > 0:
        return True
    open_order_class = str(open_order_truth.payload.get("classification") or "").upper()
    return open_order_truth.fresh and ("UNKNOWN" in open_order_class or "REVIEW" in open_order_class)


def _exposure_classification(
    *,
    broker: CurrentScopeSource,
    reconciliation: CurrentScopeSource,
    open_order_truth: CurrentScopeSource,
    managed_positions: CurrentScopeSource,
    managed_orders: CurrentScopeSource,
) -> str:
    broker_payload = broker.payload
    reconciliation_payload = reconciliation.payload
    broker_positions = _track_b_positions(broker_payload, reconciliation_payload)
    broker_open_orders = _track_b_open_orders(broker_payload, reconciliation_payload)
    if broker_open_orders:
        return "OPEN_ORDERS"
    if open_order_truth.fresh:
        open_order_class = str(open_order_truth.payload.get("classification") or "")
        if open_order_class and open_order_class != "NO_OPEN_ORDERS":
            return "OPEN_ORDERS" if "ORDER" in open_order_class else "UNKNOWN_ORDER_RISK"
    if not broker_positions:
        if _broker_flat_current_enough(
            broker=broker,
            open_order_truth=open_order_truth,
        ):
            return "FLAT"
        if _derived_flat_enough(
            reconciliation=reconciliation,
            managed_positions=managed_positions,
            managed_orders=managed_orders,
        ):
            return "FLAT"
        return "STALE_OBSERVABILITY_BROKER_SAFE"
    if not reconciliation.fresh:
        return "UNKNOWN_REFRESH_REQUIRED"
    managed_class = str(managed_positions.payload.get("classification") or "")
    owner_class = str(_mapping(reconciliation.payload.get("current_exposure_owner_resolution")).get("classification") or "")
    if managed_class == "OPEN_MANAGED_EXIT_DUE":
        return "OPEN_MANAGED_EXIT_DUE"
    if managed_class in {"OPEN_MANAGED_MATCHED", "OPEN_MANAGED_CLOSE_WORKING"}:
        return "OPEN_MANAGED"
    if owner_class == "OWNED_MANAGED_EXPOSURE":
        return "OPEN_MANAGED"
    return "OPEN_UNMANAGED"


def _broker_flat_current_enough(
    *,
    broker: CurrentScopeSource,
    open_order_truth: CurrentScopeSource,
) -> bool:
    if not _broker_truth_current(broker):
        return False
    payload = broker.payload
    if _count(payload, "unknown_broker_open_order_count") != 0:
        return False
    open_orders = _track_b_open_orders(payload, {})
    if open_orders:
        return False
    if open_order_truth.fresh:
        open_order_class = str(open_order_truth.payload.get("classification") or "")
        if open_order_class and open_order_class != "NO_OPEN_ORDERS":
            return False
    return not _track_b_positions(payload, {})


def _derived_flat_enough(
    *,
    reconciliation: CurrentScopeSource,
    managed_positions: CurrentScopeSource,
    managed_orders: CurrentScopeSource,
) -> bool:
    if not reconciliation.fresh:
        return False
    if str(reconciliation.payload.get("classification") or "") not in {
        "TRACK_B_PAPER_BROKER_RECONCILED",
        "BROKER_LIFECYCLE_RECONCILED",
        "",
    }:
        return False
    if _count(reconciliation.payload, "track_b_broker_position_count") != 0:
        return False
    if _count(reconciliation.payload, "track_b_broker_open_order_count") != 0:
        return False
    if _count(reconciliation.payload, "unknown_broker_open_order_count") != 0:
        return False
    managed_position_class = str(managed_positions.payload.get("classification") or "")
    managed_order_class = str(managed_orders.payload.get("classification") or "")
    managed_positions_flat_or_diagnostic = (
        managed_position_class in {"", "NO_MANAGED_POSITIONS"}
        or all(_diagnostic_only(row) for row in _list(managed_positions.payload.get("managed_positions")))
    )
    managed_orders_flat_or_stale = managed_order_class in {"", "NO_MANAGED_ORDERS"} or not managed_orders.fresh
    return managed_positions_flat_or_diagnostic and managed_orders_flat_or_stale


def _track_b_positions(broker: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    if "positions" in broker:
        rows = [
            row
            for row in _list(broker.get("positions"))
            if _is_track_b_future(row) and (_number(_mapping(row).get("quantity")) or 0.0) != 0.0
        ]
    else:
        rows = _list(broker.get("track_b_broker_positions")) or _list(reconciliation.get("track_b_broker_positions"))
    return [dict(_mapping(row)) for row in rows if (_number(_mapping(row).get("quantity") or _mapping(row).get("signed_qty")) or 0.0) != 0.0]


def _track_b_open_orders(broker: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    if "open_orders" in broker:
        rows = [row for row in _list(broker.get("open_orders")) if _is_track_b_future(row)]
    else:
        rows = _list(broker.get("track_b_broker_open_orders")) or _list(reconciliation.get("track_b_broker_open_orders"))
    return [dict(_mapping(row)) for row in rows]


def _latest_executions(broker: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _list(broker.get("latest_relevant_executions")) or _list(broker.get("executions")) or _list(
        reconciliation.get("latest_relevant_executions")
    )
    return [dict(_mapping(row)) for row in rows[:20]]


def _diagnostics(sources: Mapping[str, CurrentScopeSource], *, classification: str) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for name, source in sources.items():
        if source.state not in {"FRESH", "MISSING"}:
            diagnostics.append(
                {
                    "code": f"{name}_{source.state.lower()}",
                    "source": name,
                    "detail": f"{name} artifact is {source.state}.",
                    "age_seconds": source.age_seconds,
                    "diagnostic_only": name
                    in {"open_order_truth", "managed_positions", "managed_orders", "control_plane", "post_mutation_refresh"}
                    or (classification == "FLAT" and name in {"reconciliation", "canonical_readiness"}),
                }
            )
    refresh = sources["post_mutation_refresh"].payload
    if refresh.get("classification") == "POST_BROKER_MUTATION_REFRESH_DEGRADED":
        failing = _mapping(refresh.get("first_failing_step"))
        diagnostics.append(
            {
                "code": "post_mutation_refresh_degraded",
                "source": "post_mutation_refresh",
                "detail": f"{failing.get('name') or 'refresh'} did not complete.",
                "first_failing_step": failing,
                "diagnostic_only": True,
            }
        )
    if classification == "FLAT":
        reconciliation_class = str(sources["reconciliation"].payload.get("classification") or "")
        if reconciliation_class and reconciliation_class not in {"TRACK_B_PAPER_BROKER_RECONCILED"}:
            diagnostics.append(
                {
                    "code": "reconciliation_contradicted_by_current_broker_flat",
                    "source": "reconciliation",
                    "detail": f"Reconciliation is {reconciliation_class}, but current broker truth is flat with no orders.",
                    "diagnostic_only": True,
                }
            )
        managed_position_class = str(sources["managed_positions"].payload.get("classification") or "")
        if managed_position_class and managed_position_class not in {"NO_MANAGED_POSITIONS"}:
            diagnostics.append(
                {
                    "code": "managed_positions_contradicted_by_current_broker_flat",
                    "source": "managed_positions",
                    "detail": f"Managed positions are {managed_position_class}, but current broker truth is flat with no orders.",
                    "diagnostic_only": True,
                }
            )
        managed_order_class = str(sources["managed_orders"].payload.get("classification") or "")
        if managed_order_class and managed_order_class not in {"NO_MANAGED_ORDERS"}:
            diagnostics.append(
                {
                    "code": "managed_orders_contradicted_by_current_broker_flat",
                    "source": "managed_orders",
                    "detail": f"Managed orders are {managed_order_class}, but current broker truth is flat with no orders.",
                    "diagnostic_only": True,
                }
            )
    return diagnostics


def _safety_blockers(sources: Mapping[str, CurrentScopeSource]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for name, source in sources.items():
        payload = source.payload
        if payload.get("live_money_eligible") is True:
            blockers.append({"code": "live_money_eligible_true", "source": name})
        if payload.get("paper_proof_invoked") is True:
            blockers.append({"code": "paper_proof_invoked_true", "source": name})
        account = _account_value(payload)
        if account and str(account) != PAPER_ACCOUNT:
            blockers.append({"code": "wrong_account", "source": name, "account_id": str(account)})
    return blockers


def _current_price_availability(canonical: Mapping[str, Any], broker: Mapping[str, Any]) -> dict[str, Any]:
    price = _mapping(canonical.get("price_availability") or canonical.get("current_price_availability"))
    if price:
        return dict(price)
    instruments = _list(broker.get("allowed_instruments"))
    return {
        "classification": "PRICE_AVAILABILITY_DIAGNOSTIC_UNKNOWN",
        "required_instruments": instruments,
        "diagnostic_only": True,
    }


def _ods_broker_state(classification: str) -> str:
    if classification == "FLAT":
        return "FLAT"
    if classification in {"OPEN_MANAGED", "OPEN_MANAGED_EXIT_DUE"}:
        return "EXPOSED_MANAGED"
    if classification == "OPEN_ORDERS":
        return "OPEN_ORDERS"
    if classification in {"OPEN_UNMANAGED", "UNKNOWN_ORDER_RISK", "BLOCKED_SAFETY", "UNKNOWN_REFRESH_REQUIRED"}:
        return "EXPOSED_AMBIGUOUS" if classification == "OPEN_UNMANAGED" else "UNKNOWN"
    if classification == "STALE_OBSERVABILITY_BROKER_SAFE":
        return "FLAT"
    return "UNKNOWN"


def _ods_first_blocker(state: Mapping[str, Any]) -> dict[str, Any] | None:
    classification = str(state.get("classification") or "")
    if classification == "FLAT" and _mapping(state.get("canonical_readiness")).get("submit_allowed") is True:
        return None
    if classification == "STALE_OBSERVABILITY_BROKER_SAFE":
        return {
            "code": "observability_stale_broker_safe",
            "detail": "Broker truth is current and safe; one or more derived projections are stale.",
            "source": "current_scope_state",
        }
    if classification == "UNKNOWN_REFRESH_REQUIRED":
        return {
            "code": "broker_truth_refresh_required",
            "detail": "Current broker position/open-order truth is not available.",
            "source": "current_scope_state",
        }
    if classification == "UNKNOWN_ORDER_RISK":
        return {
            "code": "unknown_order_risk",
            "detail": "Unknown or conflicting open order risk is present.",
            "source": "current_scope_state",
        }
    if classification == "BLOCKED_SAFETY":
        blockers = _list(state.get("safety_blockers"))
        first = _mapping(blockers[0]) if blockers else {}
        return {
            "code": str(first.get("code") or "current_scope_safety_blocked"),
            "detail": "Current-scope safety blocker is present.",
            "source": str(first.get("source") or "current_scope_state"),
        }
    if classification == "OPEN_UNMANAGED":
        return {
            "code": "broker_exposure_unmanaged",
            "detail": "Broker exposure exists without current managed attribution.",
            "source": "current_scope_state",
        }
    return None


def _ods_next_safe_action(state: Mapping[str, Any]) -> str:
    classification = str(state.get("classification") or "")
    first_blocker = _mapping(state.get("first_blocker_for_ods"))
    if classification == "FLAT" and first_blocker == {}:
        return "NO_ACTION"
    if classification in {"OPEN_MANAGED", "OPEN_MANAGED_EXIT_DUE", "OPEN_ORDERS"}:
        return "WAIT"
    if classification in {"STALE_OBSERVABILITY_BROKER_SAFE", "UNKNOWN_REFRESH_REQUIRED"}:
        return "REFRESH_AUTHORITY"
    return "OPERATOR_REVIEW_REQUIRED"


def _account_id(broker: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> str | None:
    return _account_value(broker) or _account_value(reconciliation)


def _account_value(payload: Mapping[str, Any]) -> str | None:
    for key in ("account_id", "account", "selected_account_id"):
        if payload.get(key):
            return str(payload.get(key))
    return None


def _is_track_b_future(row: Any) -> bool:
    payload = _mapping(row)
    symbol = str(payload.get("symbol") or payload.get("instrument") or "").upper()
    local = str(payload.get("local_symbol") or payload.get("localSymbol") or "").upper()
    security_type = str(payload.get("security_type") or payload.get("secType") or "").upper()
    return (symbol in {"MES", "MNQ", "MGC", "ES", "NQ", "GC"} or local.startswith(("MES", "MNQ", "MGC", "ES", "NQ", "GC"))) and (
        security_type in {"", "FUT", "FUTURE"}
    )


def _diagnostic_only(row: Any) -> bool:
    payload = _mapping(row)
    if payload.get("diagnostic_only") is True or payload.get("historical_only") is True:
        return True
    scope = str(payload.get("current_hot_path_scope") or payload.get("scope") or "").upper()
    return "HISTORICAL" in scope or "DIAGNOSTIC" in scope or "FULL_AUDIT_ONLY" in scope


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _max_count(*payloads: Mapping[str, Any], key: str) -> int:
    return max((_count(payload, key) for payload in payloads), default=0)


def _count(payload: Mapping[str, Any], key: str) -> int:
    try:
        return int(payload.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _age_seconds(payload: Mapping[str, Any], now: datetime) -> float | None:
    timestamp = _parse_dt(
        payload.get("generated_at")
        or payload.get("authority_source_timestamp")
        or payload.get("broker_truth_generated_at")
        or payload.get("observed_at")
    )
    if timestamp is None:
        return None
    return max((now - timestamp).total_seconds(), 0.0)


def _freshness_threshold(payload: Mapping[str, Any], *, default: float) -> float:
    for key in ("freshness_threshold_seconds", "max_age_seconds", "ttl_seconds"):
        value = _number(payload.get(key))
        if value and value > 0:
            return max(value, 30.0)
    return default


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Track B canonical current-scope state.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    payload = build_current_scope_state(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output_path),
        write=not args.no_write,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            "CURRENT_SCOPE_STATE "
            f"classification={payload.get('classification')} "
            f"broker_positions={payload.get('broker_position_count')} "
            f"broker_orders={payload.get('broker_open_order_count')} "
            f"submit_allowed={_mapping(payload.get('canonical_readiness')).get('submit_allowed')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
