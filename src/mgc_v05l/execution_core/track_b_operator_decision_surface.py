"""Compact read-only Track B Operator Decision Surface v1."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[3]

DEFAULT_OUTPUT_PATH = (
    REPO_ROOT
    / "outputs"
    / "track_b_execution_core"
    / "operator_decision_surface"
    / "latest_operator_decision_surface.json"
)

RUNTIME_TRUTH_PATH = "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json"
BROKER_TRUTH_LEASE_PATH = "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
BROKER_RECONCILIATION_PATH = (
    "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
)
CANONICAL_READINESS_PATH = "outputs/operator_dashboard/runtime/latest_canonical_readiness.json"
CONTROL_PLANE_PATH = "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
BROKER_AUTHORITY_OWNERSHIP_PATH = "outputs/operator_dashboard/runtime/latest_broker_authority_ownership.json"
BROKER_SESSION_AUTHORITY_PATH = "outputs/operator_dashboard/runtime/latest_broker_session_authority.json"
OPEN_ORDER_TRUTH_PATH = "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json"
MANAGED_POSITIONS_PATH = "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
MANAGED_ORDERS_PATH = "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json"
OPERATOR_READINESS_REFRESHER_STATUS_PATH = (
    "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json"
)
PAPER_SESSION_PATH = "outputs/probationary_pattern_engine/paper_session"

FRESHNESS_TTL_SECONDS = 300.0
RUNTIME_TTL_SECONDS = 240.0

BROKER_STATE_VALUES = {"FLAT", "EXPOSED_MANAGED", "EXPOSED_AMBIGUOUS", "OPEN_ORDERS", "UNKNOWN"}
NEXT_SAFE_ACTION_VALUES = {
    "NO_ACTION",
    "WAIT",
    "REFRESH_AUTHORITY",
    "RELOAD_AUTHORITY_REFRESHER_SERVICE",
    "CONTROLLED_RUNTIME_RESTART",
    "OPERATOR_REVIEW_REQUIRED",
}


@dataclass(frozen=True)
class SourceArtifact:
    path: Path
    payload: dict[str, Any]
    source_state: str
    age_seconds: float | None

    @property
    def available(self) -> bool:
        return self.source_state == "FRESH"


def build_operator_decision_surface(
    *,
    repo_root: Path = REPO_ROOT,
    now: datetime | None = None,
) -> dict[str, Any]:
    repo_root = Path(repo_root)
    now = (now or _utc_now()).astimezone(timezone.utc)
    sources = _load_sources(repo_root=repo_root, now=now)
    ods = {
        "schema_version": "track_b_operator_decision_surface_v1",
        "generated_at": now.isoformat(),
        "read_only": True,
        "broker_mutation_allowed": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "broad_flatten_allowed": False,
        "global_cancel_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "artifact_paths": {name: str(source.path) for name, source in sources.items()},
        "source_states": {
            name: {"state": source.source_state, "age_seconds": source.age_seconds}
            for name, source in sources.items()
        },
        "tie_break_rules": [
            "BROKER_BACKED_HOT_AUTHORITY_BEATS_PROJECTIONS",
            "CANONICAL_READINESS_OWNS_SUBMIT_ALLOWED",
            "CONTROL_PLANE_OWNS_OPERATIONAL_SAFETY",
            "FRESH_CURRENT_SCOPE_TRUTH_BEATS_HISTORICAL_DEBRIS",
            "NEWER_DIAGNOSTICS_DO_NOT_OVERRIDE_HEALTHY_HOT_AUTHORITY",
        ],
    }
    runtime_live = _runtime_live(sources["runtime_truth"], sources["canonical_readiness"])
    broker_state = _broker_state(
        sources["broker_truth_lease"],
        sources["broker_reconciliation"],
        sources["open_order_truth"],
        sources["managed_positions"],
        sources["managed_orders"],
    )
    refresh_failure = _refresh_failure(sources["operator_readiness_refresher"])
    refresh_failure_superseded = _refresh_failure_superseded_by_current_authority(
        refresh_failure=refresh_failure,
        sources=sources,
        broker_state=broker_state,
    )
    submit_allowed = _submit_allowed(
        sources["canonical_readiness"],
        sources["operator_readiness_refresher"],
        refresh_failure=refresh_failure,
        refresh_failure_superseded=refresh_failure_superseded,
    )
    authority_health = _authority_health(sources["broker_authority_ownership"], sources["broker_session_authority"])
    control_plane_state = _control_plane_state(sources["control_plane"])
    latest_accepted_signal = _latest_accepted_signal(repo_root=repo_root, now=now)
    first_blocker = _first_blocker(
        runtime_live=runtime_live,
        broker_state=broker_state,
        submit_allowed=submit_allowed,
        authority_health=authority_health,
        control_plane_state=control_plane_state,
        sources=sources,
    )
    ods.update(
        {
            "runtime_live": runtime_live,
            "broker_state": broker_state,
            "submit_allowed": submit_allowed,
            "first_blocker": first_blocker,
            "latest_accepted_signal": latest_accepted_signal,
            "next_safe_action": _next_safe_action(
                first_blocker=first_blocker,
                runtime_live=runtime_live,
                submit_allowed=submit_allowed,
                authority_health=authority_health,
                control_plane_state=control_plane_state,
            ),
            "authority_health": authority_health,
            "control_plane_state": control_plane_state,
            "diagnostic_warnings": _diagnostic_warnings(
                refresh_failure=refresh_failure,
                refresh_failure_superseded=refresh_failure_superseded,
            ),
        }
    )
    return ods


def write_operator_decision_surface(
    *,
    repo_root: Path = REPO_ROOT,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = build_operator_decision_surface(repo_root=repo_root, now=now)
    _write_json_atomic(Path(output_path), payload)
    return payload


def _load_sources(*, repo_root: Path, now: datetime) -> dict[str, SourceArtifact]:
    return {
        "runtime_truth": _read_source(repo_root / RUNTIME_TRUTH_PATH, now=now, ttl_seconds=RUNTIME_TTL_SECONDS),
        "broker_truth_lease": _read_source(repo_root / BROKER_TRUTH_LEASE_PATH, now=now),
        "broker_reconciliation": _read_source(
            repo_root / BROKER_RECONCILIATION_PATH,
            now=now,
            honor_payload_threshold=False,
        ),
        "canonical_readiness": _read_source(repo_root / CANONICAL_READINESS_PATH, now=now),
        "control_plane": _read_source(repo_root / CONTROL_PLANE_PATH, now=now),
        "broker_authority_ownership": _read_source(repo_root / BROKER_AUTHORITY_OWNERSHIP_PATH, now=now),
        "broker_session_authority": _read_source(repo_root / BROKER_SESSION_AUTHORITY_PATH, now=now),
        "open_order_truth": _read_source(repo_root / OPEN_ORDER_TRUTH_PATH, now=now),
        "managed_positions": _read_source(repo_root / MANAGED_POSITIONS_PATH, now=now),
        "managed_orders": _read_source(repo_root / MANAGED_ORDERS_PATH, now=now),
        "operator_readiness_refresher": _read_source(
            repo_root / OPERATOR_READINESS_REFRESHER_STATUS_PATH,
            now=now,
            ttl_seconds=RUNTIME_TTL_SECONDS,
        ),
    }


def _read_source(
    path: Path,
    *,
    now: datetime,
    ttl_seconds: float = FRESHNESS_TTL_SECONDS,
    honor_payload_threshold: bool = True,
) -> SourceArtifact:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return SourceArtifact(path=path, payload={}, source_state="MISSING", age_seconds=None)
    except json.JSONDecodeError:
        return SourceArtifact(path=path, payload={}, source_state="INVALID_SCHEMA", age_seconds=None)
    if not isinstance(raw, Mapping):
        return SourceArtifact(path=path, payload={}, source_state="INVALID_SCHEMA", age_seconds=None)
    payload = dict(raw)
    if _artifact_reports_ambiguity(payload):
        return SourceArtifact(path=path, payload=payload, source_state="AMBIGUOUS", age_seconds=_age_seconds(payload, now))
    age = _age_seconds(payload, now)
    threshold = _freshness_threshold(payload, default=ttl_seconds) if honor_payload_threshold else ttl_seconds
    if age is None or age > threshold:
        return SourceArtifact(path=path, payload=payload, source_state="STALE", age_seconds=age)
    return SourceArtifact(path=path, payload=payload, source_state="FRESH", age_seconds=age)


def _runtime_live(source: SourceArtifact, canonical_source: SourceArtifact | None = None) -> dict[str, Any]:
    if not source.available:
        return {
            "state": "UNKNOWN",
            "pid": None,
            "commit": None,
            "profile": None,
            "lane_count": None,
            "heartbeat_age_seconds": source.age_seconds,
        }
    payload = source.payload
    runtime = _mapping(payload.get("runtime"))
    pid_metadata = _mapping(payload.get("pid_metadata"))
    canonical = _mapping(payload.get("canonical_readiness"))
    canonical_runtime = _mapping((canonical_source.payload if canonical_source and canonical_source.available else {}).get("runtime"))
    pid_alive = _boolish(runtime.get("pid_alive"))
    heartbeat_state = str(runtime.get("heartbeat_state") or runtime.get("freshness_state") or "")
    state = "LIVE" if pid_alive else "DOWN"
    if "STALE" in heartbeat_state.upper() and pid_alive:
        state = "STALE"
    return {
        "state": state,
        "pid": runtime.get("pid") or pid_metadata.get("pid") or payload.get("source_pid"),
        "commit": runtime.get("current_head") or runtime.get("loaded_commit") or pid_metadata.get("source_runtime_git_head"),
        "profile": runtime.get("profile")
        or pid_metadata.get("profile")
        or canonical_runtime.get("profile")
        or _profile_from_pid_metadata(pid_metadata),
        "lane_count": runtime.get("lane_count") or pid_metadata.get("lane_count"),
        "heartbeat_age_seconds": _number(runtime.get("runtime_truth_age_seconds") or source.age_seconds),
        "canonical_readiness": canonical.get("classification"),
    }


def _broker_state(
    lease_source: SourceArtifact,
    reconciliation_source: SourceArtifact,
    open_order_truth_source: SourceArtifact | None = None,
    managed_positions_source: SourceArtifact | None = None,
    managed_orders_source: SourceArtifact | None = None,
) -> str:
    if not lease_source.available or not reconciliation_source.available:
        return "UNKNOWN"
    if open_order_truth_source is not None and not open_order_truth_source.available:
        return "UNKNOWN"
    if managed_positions_source is not None and not managed_positions_source.available:
        return "UNKNOWN"
    if managed_orders_source is not None and not managed_orders_source.available:
        return "UNKNOWN"
    lease = lease_source.payload
    reconciliation = reconciliation_source.payload
    open_order_truth = open_order_truth_source.payload if open_order_truth_source and open_order_truth_source.available else {}
    managed_positions = managed_positions_source.payload if managed_positions_source and managed_positions_source.available else {}
    managed_orders = managed_orders_source.payload if managed_orders_source and managed_orders_source.available else {}
    if _count(lease, "unknown_broker_open_order_count") > 0 or _count(reconciliation, "unknown_broker_open_order_count") > 0:
        return "OPEN_ORDERS"
    if _count(lease, "track_b_broker_open_order_count") > 0 or _count(reconciliation, "track_b_broker_open_order_count") > 0:
        return "OPEN_ORDERS"
    open_order_classification = str(open_order_truth.get("classification") or "")
    if open_order_classification and open_order_classification != "NO_OPEN_ORDERS":
        return "OPEN_ORDERS" if "ORDER" in open_order_classification else "UNKNOWN"
    broker_positions = max(
        _count(lease, "track_b_broker_position_count"),
        _count(reconciliation, "track_b_broker_position_count"),
        _nonzero_position_count(lease.get("positions")),
        _nonzero_position_count(reconciliation.get("track_b_broker_positions")),
    )
    reconciliation_classification = str(reconciliation.get("classification") or "")
    broker_reconciled = (
        lease.get("broker_reconciled") is True or reconciliation.get("broker_reconciled") is True
    ) and reconciliation_classification in {"", "TRACK_B_PAPER_BROKER_RECONCILED"}
    managed_classification = str(managed_positions.get("classification") or "")
    managed_order_classification = str(managed_orders.get("classification") or "")
    if broker_positions == 0:
        if (
            broker_reconciled
            and managed_classification in {"", "NO_MANAGED_POSITIONS"}
            and managed_order_classification in {"", "NO_MANAGED_ORDERS"}
        ):
            return "FLAT"
        return "UNKNOWN"
    if broker_reconciled and managed_classification in {"OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE"}:
        return "EXPOSED_MANAGED"
    owner = _mapping(reconciliation.get("current_exposure_owner_resolution"))
    owner_classification = str(owner.get("classification") or "")
    if broker_reconciled and owner_classification in {"OWNED_MANAGED_EXPOSURE", "NO_OPEN_EXPOSURE"}:
        return "EXPOSED_MANAGED"
    return "EXPOSED_AMBIGUOUS"


def _submit_allowed(
    source: SourceArtifact,
    refresher_source: SourceArtifact | None = None,
    *,
    refresh_failure: Mapping[str, Any] | None = None,
    refresh_failure_superseded: bool = False,
) -> dict[str, Any]:
    if not source.available:
        return {"submit_allowed": False, "canonical_readiness": "UNKNOWN"}
    payload = source.payload
    if refresh_failure is None:
        refresh_failure = _refresh_failure(refresher_source)
    if refresh_failure is not None and not refresh_failure_superseded:
        return {
            "submit_allowed": False,
            "canonical_readiness": payload.get("canonical_readiness") or payload.get("state") or "UNKNOWN",
            "degraded": True,
            "degraded_reason": refresh_failure["code"],
        }
    return {
        "submit_allowed": payload.get("submit_allowed") is True,
        "canonical_readiness": payload.get("canonical_readiness") or payload.get("state") or "UNKNOWN",
    }


def _authority_health(ownership_source: SourceArtifact, bsa_source: SourceArtifact) -> dict[str, Any]:
    ownership = ownership_source.payload if ownership_source.available else {}
    bsa = bsa_source.payload if bsa_source.available else {}
    return {
        "classification": ownership.get("classification")
        or bsa.get("classification")
        or ("UNKNOWN" if not ownership_source.available else None),
        "lease_bsa_aligned": ownership.get("lease_bsa_generation_aligned") is True,
        "writer": ownership.get("authority_writer") or bsa.get("authority_writer"),
        "reload_needed": ownership.get("running_writer_needs_reload") is True,
        "next_safe_action": ownership.get("next_safe_action"),
        "bsa_classification": bsa.get("classification"),
        "bsa_connection_mode": bsa.get("connection_mode"),
        "bsa_allowed_uses": _mapping(bsa.get("allowed_uses")),
        "non_owner_hot_write_attempt_count": _count(ownership, "non_owner_hot_write_attempt_count"),
        "duplicate_hot_writer_detected": ownership.get("duplicate_hot_writer_detected") is True,
    }


def _control_plane_state(source: SourceArtifact) -> dict[str, Any]:
    if not source.available:
        return {"classification": "UNKNOWN", "safe_to_start_runtime": False, "blockers": []}
    payload = source.payload
    return {
        "classification": payload.get("classification") or "UNKNOWN",
        "safe_to_start_runtime": payload.get("safe_to_start_runtime") is True,
        "blockers": _list(payload.get("blockers")),
    }


def _first_blocker(
    *,
    runtime_live: Mapping[str, Any],
    broker_state: str,
    submit_allowed: Mapping[str, Any],
    authority_health: Mapping[str, Any],
    control_plane_state: Mapping[str, Any],
    sources: Mapping[str, SourceArtifact],
) -> dict[str, Any] | None:
    safety = _safety_blocker(sources)
    if safety is not None:
        return safety
    refresh_failure = _refresh_failure(sources.get("operator_readiness_refresher"))
    if refresh_failure is not None and not _refresh_failure_superseded_by_current_authority(
        refresh_failure=refresh_failure,
        sources=sources,
        broker_state=broker_state,
    ):
        return refresh_failure
    if broker_state in {"OPEN_ORDERS", "EXPOSED_AMBIGUOUS", "UNKNOWN"}:
        readiness_blocker = _matching_blocker(
            sources["canonical_readiness"].payload,
            ("open_order", "reconciliation", "ambiguous", "unknown_order", "broker"),
        )
        if readiness_blocker is not None:
            return readiness_blocker
        return {"code": f"broker_state_{broker_state.lower()}", "detail": f"Broker state is {broker_state}.", "source": "broker_state"}
    if _authority_unhealthy(authority_health):
        return {
            "code": "broker_authority_ownership_unhealthy",
            "detail": str(authority_health.get("classification") or "Broker authority ownership is not healthy."),
            "source": "broker_authority_ownership",
        }
    bsa_blocker = _bsa_blocker(sources["broker_session_authority"].payload)
    if bsa_blocker is not None:
        return bsa_blocker
    control_blocker = _control_plane_blocker(control_plane_state)
    if control_blocker is not None:
        return control_blocker
    if str(runtime_live.get("state")) != "LIVE":
        return {
            "code": "runtime_not_live",
            "detail": f"Runtime state is {runtime_live.get('state')}.",
            "source": "runtime_truth",
        }
    if submit_allowed.get("submit_allowed") is not True:
        readiness_blocker = _first_readiness_blocker(sources["canonical_readiness"].payload)
        if readiness_blocker is not None:
            return readiness_blocker
        return {
            "code": "canonical_readiness_not_submit_allowed",
            "detail": f"Canonical readiness is {submit_allowed.get('canonical_readiness')}.",
            "source": "canonical_readiness",
        }
    return None


def _refresh_failure_superseded_by_current_authority(
    *,
    refresh_failure: Mapping[str, Any] | None,
    sources: Mapping[str, SourceArtifact],
    broker_state: str,
) -> bool:
    if refresh_failure is None:
        return False
    refresher = sources.get("operator_readiness_refresher")
    if refresher is None or not refresher.available:
        return False
    current_names = (
        "broker_truth_lease",
        "broker_reconciliation",
        "open_order_truth",
        "managed_positions",
        "managed_orders",
    )
    current_sources = [sources.get(name) for name in current_names]
    if any(source is None or not source.available for source in current_sources):
        return False
    refresher_ts = _source_generated_at(refresher)
    if refresher_ts is None:
        return False
    if any((_source_generated_at(source) is None or _source_generated_at(source) <= refresher_ts) for source in current_sources if source):
        return False
    return broker_state in {"FLAT", "EXPOSED_MANAGED"}


def _diagnostic_warnings(
    *,
    refresh_failure: Mapping[str, Any] | None,
    refresh_failure_superseded: bool,
) -> list[dict[str, Any]]:
    if refresh_failure is None or not refresh_failure_superseded:
        return []
    return [
        {
            "code": "operator_readiness_refresh_failure_superseded",
            "detail": "Earlier operator readiness refresh failure was superseded by newer clean current authority.",
            "source": refresh_failure.get("source") or "operator_readiness_refresher",
            "superseded_blocker": dict(refresh_failure),
        }
    ]


def _refresh_failure(source: SourceArtifact | None) -> dict[str, Any] | None:
    if source is None:
        return None
    if not source.available:
        if source.source_state == "MISSING":
            return None
        return {
            "code": "operator_readiness_refresher_not_fresh",
            "detail": f"Operator readiness refresher status is {source.source_state}.",
            "source": "operator_readiness_refresher",
        }
    payload = source.payload
    classification = str(payload.get("classification") or "")
    if classification != "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED":
        return None
    failures = _list(payload.get("dependency_refresh_failures"))
    if failures:
        first = _mapping(failures[0])
        code = str(first.get("code") or f"{first.get('step') or 'dependency'}_refresh_failed")
        return {
            "code": code,
            "detail": f"{first.get('step') or 'dependency'} refresh failed with returncode={first.get('returncode')}.",
            "source": "operator_readiness_refresher",
        }
    return {
        "code": "operator_readiness_refresh_failed",
        "detail": "Operator readiness refresh failed before completing the authority chain.",
        "source": "operator_readiness_refresher",
    }


def _next_safe_action(
    *,
    first_blocker: Mapping[str, Any] | None,
    runtime_live: Mapping[str, Any],
    submit_allowed: Mapping[str, Any],
    authority_health: Mapping[str, Any],
    control_plane_state: Mapping[str, Any],
) -> str:
    ownership_action = str(authority_health.get("next_safe_action") or "")
    if ownership_action in NEXT_SAFE_ACTION_VALUES and ownership_action != "NO_ACTION":
        return ownership_action
    if authority_health.get("reload_needed") is True:
        return "RELOAD_AUTHORITY_REFRESHER_SERVICE"
    if first_blocker is None and submit_allowed.get("submit_allowed") is True:
        return "NO_ACTION"
    code = str((first_blocker or {}).get("code") or "").lower()
    if any(token in code for token in ("live_money", "paper_proof", "wrong_account", "ambiguous", "unknown")):
        return "OPERATOR_REVIEW_REQUIRED"
    if any(
        token in code
        for token in (
            "stale",
            "refresh",
            "shared_truth",
            "reconciliation",
            "open_order",
            "authority",
            "bsa",
            "broker_session",
            "connection",
        )
    ):
        return "REFRESH_AUTHORITY"
    if runtime_live.get("state") != "LIVE" and control_plane_state.get("safe_to_start_runtime") is True:
        return "CONTROLLED_RUNTIME_RESTART"
    if code:
        return "WAIT"
    return "OPERATOR_REVIEW_REQUIRED"


def _latest_accepted_signal(*, repo_root: Path, now: datetime) -> dict[str, Any] | None:
    paper_session = repo_root / PAPER_SESSION_PATH
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    for path in _signal_candidate_paths(paper_session):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        signal = _extract_signal(dict(payload))
        if signal is None:
            continue
        ts = _parse_dt(signal.get("signal_ts") or signal.get("bar_ts") or payload.get("generated_at"))
        if ts is None:
            ts = now
        candidates.append((ts, signal))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _signal_candidate_paths(paper_session: Path) -> Iterable[Path]:
    if not paper_session.exists():
        return []
    paths: list[Path] = []
    for pattern in (
        "lanes/*/accepted_strategy_intent_latest.json",
        "lanes/*/strategy_intent_latest.json",
        "lanes/*/blocked_strategy_intent_latest.json",
        "lanes/*/operator_status.json",
        "operator_status.json",
    ):
        paths.extend(sorted(paper_session.glob(pattern)))
    return paths


def _extract_signal(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    text = " ".join(
        str(payload.get(key) or "")
        for key in ("classification", "stage", "status", "bridge_classification", "blocker_classification")
    ).upper()
    if "ACCEPTED" not in text and payload.get("accepted") is not True:
        return None
    lane_id = payload.get("lane_id") or payload.get("strategy_lane_id")
    if not lane_id and isinstance(payload.get("lanes"), list):
        return None
    side = payload.get("side") or payload.get("intent_side") or payload.get("action") or payload.get("intent_type")
    return {
        "lane_id": lane_id,
        "side": _normalize_side(side),
        "instrument": payload.get("instrument") or payload.get("symbol") or payload.get("local_symbol"),
        "bar_ts": payload.get("bar_ts") or payload.get("decision_bar_timestamp") or payload.get("bar_id"),
        "signal_ts": payload.get("signal_timestamp") or payload.get("signal_ts") or payload.get("created_at"),
        "stage": payload.get("stage") or payload.get("bridge_classification") or payload.get("classification"),
    }


def _safety_blocker(sources: Mapping[str, SourceArtifact]) -> dict[str, Any] | None:
    for source_name, source in sources.items():
        payload = source.payload
        if payload.get("live_money_eligible") is True:
            return {"code": "live_money_eligible_true", "detail": "A source reports live_money_eligible=true.", "source": source_name}
        if payload.get("paper_proof_invoked") is True:
            return {"code": "paper_proof_invoked_true", "detail": "A source reports paper_proof_invoked=true.", "source": source_name}
        account = payload.get("account") or payload.get("account_id")
        if account and str(account) != "DUM882026":
            return {"code": "wrong_account", "detail": f"Source account is {account}.", "source": source_name}
    return None


def _authority_unhealthy(authority_health: Mapping[str, Any]) -> bool:
    classification = str(authority_health.get("classification") or "")
    if authority_health.get("reload_needed") is True or authority_health.get("duplicate_hot_writer_detected") is True:
        return True
    if authority_health.get("lease_bsa_aligned") is False:
        return True
    return bool(classification and classification != "BROKER_AUTHORITY_PUBLISHER_HEALTHY")


def _bsa_blocker(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    allowed = _mapping(payload.get("allowed_uses"))
    if allowed.get("new_entry") is True:
        return None
    blockers = _list(payload.get("authority_blockers") or payload.get("blockers"))
    if blockers:
        first = _mapping(blockers[0])
        return {
            "code": str(first.get("code") or "broker_session_new_entry_not_allowed"),
            "detail": str(first.get("detail") or payload.get("classification") or "Broker Session Authority blocks new entries."),
            "source": "broker_session_authority",
        }
    classification = payload.get("classification")
    if classification:
        return {
            "code": "broker_session_new_entry_not_allowed",
            "detail": str(classification),
            "source": "broker_session_authority",
        }
    return None


def _control_plane_blocker(control_plane_state: Mapping[str, Any]) -> dict[str, Any] | None:
    classification = str(control_plane_state.get("classification") or "")
    blockers = _list(control_plane_state.get("blockers"))
    if classification in {"CONTROL_PLANE_SNAPSHOT_READY", "UNKNOWN"} and not blockers:
        return None
    if blockers:
        first = _mapping(blockers[0])
        return {
            "code": str(first.get("code") or "control_plane_blocked"),
            "detail": str(first.get("detail") or classification),
            "source": "control_plane",
        }
    if "BLOCKED" in classification or "HARD_HOLD" in classification:
        return {"code": "control_plane_blocked", "detail": classification, "source": "control_plane"}
    return None


def _first_readiness_blocker(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    blockers = _list(payload.get("readiness_blockers") or payload.get("blockers"))
    if not blockers:
        return None
    first = _mapping(blockers[0])
    return {
        "code": str(first.get("code") or "canonical_readiness_blocked"),
        "detail": str(first.get("detail") or first),
        "source": str(first.get("source") or "canonical_readiness"),
    }


def _matching_blocker(payload: Mapping[str, Any], tokens: Sequence[str]) -> dict[str, Any] | None:
    for blocker in _list(payload.get("readiness_blockers") or payload.get("blockers")):
        row = _mapping(blocker)
        code = str(row.get("code") or "").lower()
        detail = str(row.get("detail") or "").lower()
        if any(token in code or token in detail for token in tokens):
            return {
                "code": str(row.get("code") or "canonical_readiness_blocked"),
                "detail": str(row.get("detail") or row),
                "source": str(row.get("source") or "canonical_readiness"),
            }
    return None


def _artifact_reports_ambiguity(payload: Mapping[str, Any]) -> bool:
    classification = str(payload.get("classification") or payload.get("canonical_readiness") or "")
    if "AMBIGUOUS" in classification:
        return True
    if payload.get("ambiguous") is True:
        return True
    return False


def _freshness_threshold(payload: Mapping[str, Any], *, default: float) -> float:
    for key in ("freshness_threshold_seconds", "max_age_seconds", "ttl_seconds"):
        value = _number(payload.get(key))
        if value is not None and value > 0:
            return max(value, 30.0)
    return default


def _age_seconds(payload: Mapping[str, Any], now: datetime) -> float | None:
    generated = payload.get("generated_at") or payload.get("observed_at") or payload.get("authority_source_timestamp")
    timestamp = _parse_dt(generated)
    if timestamp is None:
        return None
    return max((now - timestamp.astimezone(timezone.utc)).total_seconds(), 0.0)


def _source_generated_at(source: SourceArtifact) -> datetime | None:
    return _parse_dt(
        source.payload.get("generated_at")
        or source.payload.get("observed_at")
        or source.payload.get("authority_source_timestamp")
    )


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _profile_from_pid_metadata(payload: Mapping[str, Any]) -> str | None:
    profile = payload.get("profile")
    if profile:
        return str(profile)
    config_path = str(payload.get("paper_config_in_force_path") or "")
    if "mnq_mes_full_session_active_evidence" in config_path:
        return "mnq_mes_full_session_active_evidence"
    return None


def _normalize_side(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if "BUY" in text.upper():
        return "BUY"
    if "SELL" in text.upper():
        return "SELL"
    return text


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _count(payload: Mapping[str, Any], key: str) -> int:
    try:
        return int(payload.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _nonzero_position_count(value: Any) -> int:
    count = 0
    for row in _list(value):
        position = _mapping(row)
        quantity = _number(position.get("quantity") or position.get("position") or position.get("signed_qty"))
        if quantity is not None and quantity != 0:
            count += 1
    return count


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"true", "1", "yes"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the Track B Operator Decision Surface v1.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root)
    if args.no_write:
        payload = build_operator_decision_surface(repo_root=repo_root)
    else:
        payload = write_operator_decision_surface(repo_root=repo_root, output_path=Path(args.output_path))
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        first_blocker = payload.get("first_blocker") or {}
        print(
            "ODS "
            f"runtime={payload.get('runtime_live', {}).get('state')} "
            f"broker={payload.get('broker_state')} "
            f"submit_allowed={payload.get('submit_allowed', {}).get('submit_allowed')} "
            f"first_blocker={first_blocker.get('code')} "
            f"next_safe_action={payload.get('next_safe_action')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
