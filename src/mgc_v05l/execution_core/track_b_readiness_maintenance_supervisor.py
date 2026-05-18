"""Shared Track B PAPER readiness-maintenance supervisor decision engine.

This module is deliberately pure: it classifies watchdog/readiness artifacts and
recommends permitted maintenance actions. It does not execute repairs, restart
services, mutate lifecycle, or touch broker/order APIs.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SUPERVISOR_STATES = {
    "OBSERVING",
    "DEGRADED",
    "REPAIRING_RECOMMENDED",
    "RECOVERED",
    "BLOCKED",
    "OPERATOR_REQUIRED",
}

SUPERVISOR_ACTIONS = {
    "NO_ACTION",
    "RETRY",
    "ROTATE_CLIENT_ID",
    "RESTART_SIDECAR",
    "REFRESH_BROKER_TRUTH",
    "REFRESH_RECONCILIATION",
    "RECONNECT_MARKET_DATA",
    "RESTART_OBSERVATION_RUNTIME",
    "QUARANTINE_LANE",
    "ARCHIVE_ARTIFACTS",
    "ALERT_OPERATOR",
    "BLOCK_SUBMIT",
    "REQUIRE_OPERATOR_APPROVAL",
}

ACTION_SCOPE = {
    "NO_ACTION": "read_only",
    "RETRY": "read_only",
    "ROTATE_CLIENT_ID": "read_only",
    "RESTART_SIDECAR": "sidecar_only",
    "REFRESH_BROKER_TRUTH": "read_only",
    "REFRESH_RECONCILIATION": "read_only",
    "RECONNECT_MARKET_DATA": "sidecar_only",
    "RESTART_OBSERVATION_RUNTIME": "observation_runtime_only",
    "QUARANTINE_LANE": "read_only",
    "ARCHIVE_ARTIFACTS": "read_only",
    "ALERT_OPERATOR": "operator_only",
    "BLOCK_SUBMIT": "read_only",
    "REQUIRE_OPERATOR_APPROVAL": "operator_only",
}

DEFAULT_DECISION_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json"
)


def classify_readiness_maintenance(inputs: Mapping[str, Any]) -> dict[str, Any]:
    """Classify maintenance state from normalized Track B readiness inputs."""

    generated_at = str(inputs.get("generated_at") or datetime.now(timezone.utc).isoformat())
    canonical = _mapping(inputs.get("canonical_readiness"))
    root_guard = _mapping(inputs.get("root_guard") or inputs.get("root_guard_summary"))
    ibkr = _mapping(inputs.get("ibkr_connectivity"))
    broker_truth = _mapping(inputs.get("broker_truth"))
    market_data = _mapping(inputs.get("market_data"))
    runtime = _mapping(inputs.get("runtime"))
    lane_quarantine = _mapping(inputs.get("lane_quarantine"))
    reconciliation = _mapping(inputs.get("reconciliation") or inputs.get("phase1_reconciliation"))
    artifact_pressure = _mapping(inputs.get("artifact_pressure"))

    decisions = _DecisionBuilder(generated_at=generated_at, canonical=canonical)

    if _any_live_money_eligible(inputs, canonical, broker_truth, reconciliation, runtime, lane_quarantine):
        decisions.set_state("BLOCKED")
        decisions.add_action("BLOCK_SUBMIT", "live_money_eligible=true is forbidden for Track B PAPER maintenance.")
        decisions.add_action("ALERT_OPERATOR", "Operator review required because a PAPER artifact advertised live-money eligibility.")
        decisions.block("live_money_eligible_true", "live_money_eligible must remain false for Track B PAPER.")
        return decisions.result(inputs=inputs)

    if root_guard.get("root_match") is False or _canonical_state(canonical) == "NOT_READY_WRONG_ROOT":
        decisions.set_state("BLOCKED")
        decisions.add_action("ALERT_OPERATOR", "Wrong-root runtime/process ambiguity requires operator action.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked while active root does not match expected Dev root.")
        decisions.block("wrong_root_process", "Wrong-root process detected by root guard or canonical readiness.")
        return decisions.result(inputs=inputs)

    _classify_ibkr_connectivity(ibkr, decisions)
    _classify_broker_truth(broker_truth, decisions)
    _classify_market_data(market_data, runtime, canonical, decisions)
    _classify_lane_quarantine(lane_quarantine, runtime, decisions)
    _classify_reconciliation(reconciliation, broker_truth, decisions)
    _classify_artifact_pressure(artifact_pressure, decisions)
    _classify_canonical(canonical, decisions)

    if not decisions.actions:
        if _canonical_state(canonical) == "READY_SUBMIT_CAPABLE":
            decisions.set_state("RECOVERED")
            decisions.add_action("NO_ACTION", "Canonical readiness is submit-capable and no maintenance repair is recommended.")
        else:
            decisions.set_state("OBSERVING")
            decisions.add_action("NO_ACTION", "No shared maintenance action is recommended.")

    return decisions.result(inputs=inputs)


def write_maintenance_supervisor_decision(*, output_path: Path, decision: Mapping[str, Any]) -> None:
    """Write the supervisor decision artifact only; does not execute actions."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _classify_ibkr_connectivity(ibkr: Mapping[str, Any], decisions: "_DecisionBuilder") -> None:
    classification = str(ibkr.get("classification") or "").strip().upper()
    if not classification:
        return
    if classification == "IBKR_CONNECTED_READ_ONLY":
        decisions.warn("ibkr_connected_read_only", "IBKR connectivity watchdog reports read-only callback health.")
        return
    if classification == "TWS_NOT_LISTENING":
        decisions.set_state("OPERATOR_REQUIRED")
        decisions.add_action("ALERT_OPERATOR", "TWS is not listening; the app may not restart TWS automatically.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked while TWS is unavailable.")
        decisions.block("tws_not_listening", "IBKR connectivity watchdog classified TWS as not listening.")
        return
    if classification == "MANAGED_ACCOUNTS_TIMEOUT":
        decisions.set_state("DEGRADED")
        decisions.add_action("RETRY", "Retry read-only IBKR connectivity/broker-truth check after cooldown.")
        decisions.add_action("ROTATE_CLIENT_ID", "Rotate diagnostic/refresher client id inside the approved range.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked until managedAccounts callback recovers.")
        decisions.block("managed_accounts_timeout", "IBKR managedAccounts callback timed out.")
        decisions.retry(retry_count=1, cooldown_seconds=60)
        return
    if classification == "CLIENT_ID_COLLISION_SUSPECTED":
        decisions.set_state("DEGRADED")
        decisions.add_action("ROTATE_CLIENT_ID", "Rotate diagnostic/refresher client id after suspected collision.")
        decisions.add_action("RETRY", "Retry read-only IBKR connectivity after rotating client id.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked while client-id collision is suspected.")
        decisions.block("client_id_collision_suspected", "IBKR watchdog suspects a client-id collision.")
        decisions.retry(retry_count=1, cooldown_seconds=60)
        return
    if classification in {"IBKR_HANDSHAKE_TIMEOUT", "POSITIONS_TIMEOUT", "OPEN_ORDERS_TIMEOUT", "IBKR_CONNECTED_BUT_INCOMPLETE"}:
        decisions.set_state("DEGRADED")
        decisions.add_action("RETRY", f"Retry read-only IBKR check after {classification}.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked until complete read-only broker callbacks recover.")
        decisions.block(classification.lower(), f"IBKR connectivity watchdog reported {classification}.")
        decisions.retry(retry_count=1, cooldown_seconds=60)
        return
    if classification == "API_MODAL_OR_BLOCKED_SUSPECTED":
        decisions.set_state("OPERATOR_REQUIRED")
        decisions.add_action("ALERT_OPERATOR", "TWS API modal/block condition requires operator inspection.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked while TWS API may be blocked.")
        decisions.block("api_modal_or_blocked_suspected", "IBKR watchdog suspects a modal/API-block condition.")


def _classify_broker_truth(broker_truth: Mapping[str, Any], decisions: "_DecisionBuilder") -> None:
    if not broker_truth:
        return
    fresh = _bool(broker_truth.get("fresh"))
    positions_complete = _bool(broker_truth.get("positions_complete"))
    open_orders_complete = _bool(broker_truth.get("open_orders_complete"))
    preserved = bool(broker_truth.get("last_successful_broker_truth") or broker_truth.get("using_last_successful_broker_truth"))
    classification = str(broker_truth.get("classification") or broker_truth.get("source_classification") or "").upper()
    if fresh and positions_complete and open_orders_complete:
        if "PRESERVED" in classification or preserved:
            decisions.warn("broker_truth_last_good_preserved", "Broker truth is using preserved last-good evidence.")
        return
    decisions.set_state("DEGRADED")
    decisions.add_action("REFRESH_BROKER_TRUTH", "Refresh complete read-only broker truth.")
    decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked while broker truth is stale or incomplete.")
    if preserved:
        decisions.warn("broker_truth_last_good_preserved", "Last-good broker truth exists but is stale or insufficient for submit.")
    decisions.block("broker_truth_stale_or_incomplete", "Fresh complete broker truth is not available.")
    decisions.retry(retry_count=1, cooldown_seconds=60)


def _classify_market_data(
    market_data: Mapping[str, Any],
    runtime: Mapping[str, Any],
    canonical: Mapping[str, Any],
    decisions: "_DecisionBuilder",
) -> None:
    if not market_data:
        return
    classification = str(market_data.get("classification") or market_data.get("status") or "").upper()
    socket_closed = _bool(market_data.get("socket_closed")) or "SOCKET_CLOSED" in classification or "DATABENTO_SOCKET_CLOSE" in classification
    fresh = _bool(market_data.get("fresh")) or _bool(market_data.get("market_data_ok"))
    if not socket_closed and fresh:
        return
    if socket_closed or not fresh:
        decisions.set_state("DEGRADED")
        decisions.add_action("RECONNECT_MARKET_DATA", "Reconnect or retry the market-data producer.")
        observe_only_ok = _bool(runtime.get("observation_only_safe")) or _canonical_state(canonical) == "READY_OBSERVATION_ONLY"
        if observe_only_ok:
            decisions.warn("market_data_degraded_observation_only", "Market data is degraded; observation-only mode may remain up if policy allows.")
        else:
            decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked while market-data freshness is degraded.")
            decisions.block("market_data_degraded", "Market-data freshness or socket health is degraded.")
        decisions.retry(retry_count=1, cooldown_seconds=60)


def _classify_lane_quarantine(
    lane_quarantine: Mapping[str, Any],
    runtime: Mapping[str, Any],
    decisions: "_DecisionBuilder",
) -> None:
    if not lane_quarantine:
        return
    lane_scoped = _bool(lane_quarantine.get("lane_scoped_startup_reconciliation_failure")) or str(
        lane_quarantine.get("reason") or ""
    ).lower() == "paper_startup_reconciliation_failed"
    quarantine_count = int(lane_quarantine.get("quarantine_count") or len(lane_quarantine.get("quarantined_lane_ids") or []))
    other_eligible = int(
        lane_quarantine.get("healthy_lane_count")
        or lane_quarantine.get("eligible_lane_count")
        or runtime.get("eligible_lane_count")
        or 0
    )
    if not lane_scoped and quarantine_count <= 0:
        return
    decisions.set_state("DEGRADED")
    decisions.add_action("QUARANTINE_LANE", "Quarantine lane-scoped startup reconciliation failure.")
    if lane_scoped and other_eligible > 0:
        decisions.warn("lane_quarantine_recommended", "Lane-scoped failure can be quarantined without killing the whole runtime.")
    else:
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked when no other eligible lanes are available.")
        decisions.block("lane_quarantine_no_eligible_lanes", "Lane quarantine leaves no eligible lanes.")


def _classify_reconciliation(
    reconciliation: Mapping[str, Any],
    broker_truth: Mapping[str, Any],
    decisions: "_DecisionBuilder",
) -> None:
    if not reconciliation:
        return
    lifecycle_count = int(reconciliation.get("lifecycle_open_position_count") or 0)
    review_count = int(reconciliation.get("review_required_count") or 0)
    dry_run_ready = _bool(reconciliation.get("guarded_cleanup_dry_run_ready")) or _bool(
        reconciliation.get("manual_close_cleanup_dry_run_ready")
    )
    broker_flat = _bool(reconciliation.get("broker_flat")) or int(reconciliation.get("track_b_broker_position_count") or 0) == 0
    if lifecycle_count > 0 or review_count > 0:
        if dry_run_ready and broker_flat:
            decisions.set_state("REPAIRING_RECOMMENDED")
            decisions.add_action("REQUIRE_OPERATOR_APPROVAL", "Guarded lifecycle cleanup/adoption apply requires explicit operator approval.")
            decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked until guarded lifecycle cleanup clears reconciliation.")
            decisions.block("guarded_lifecycle_cleanup_required", "Stale lifecycle/reconciliation repair is available only through guarded tools.")
        else:
            decisions.set_state("OPERATOR_REQUIRED")
            decisions.add_action("ALERT_OPERATOR", "Lifecycle/reconciliation mismatch requires operator review.")
            decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked while reconciliation requires review.")
            decisions.block("reconciliation_review_required", "Reconciliation has lifecycle/open review blockers.")
        return
    fresh = _bool(reconciliation.get("fresh"))
    clean = (
        str(reconciliation.get("classification") or "") == "TRACK_B_PAPER_BROKER_RECONCILED"
        and _bool(reconciliation.get("broker_reconciled"))
    )
    broker_fresh = _bool(broker_truth.get("fresh"))
    if broker_fresh and not (fresh and clean):
        decisions.set_state("REPAIRING_RECOMMENDED")
        decisions.add_action("REFRESH_RECONCILIATION", "Refresh Phase-1 reconciliation from current broker truth.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked until reconciliation is fresh and clean.")
        decisions.block("reconciliation_stale_or_unclean", "Phase-1 reconciliation is stale or not clean.")


def _classify_artifact_pressure(artifact_pressure: Mapping[str, Any], decisions: "_DecisionBuilder") -> None:
    if not artifact_pressure:
        return
    hot_path_bloat = _bool(artifact_pressure.get("hot_path_bloat")) or _bool(artifact_pressure.get("archive_recommended"))
    unresolved = _bool(artifact_pressure.get("unresolved_authoritative_evidence"))
    if not hot_path_bloat:
        return
    if unresolved:
        decisions.set_state("OPERATOR_REQUIRED")
        decisions.add_action("ALERT_OPERATOR", "Artifact pressure includes unresolved authoritative evidence.")
        decisions.warn("artifact_archive_blocked", "Artifact archive requires operator review because unresolved evidence is present.")
        return
    decisions.set_state("REPAIRING_RECOMMENDED")
    decisions.add_action("ARCHIVE_ARTIFACTS", "Archive cold artifacts off hot paths while preserving audit evidence.")
    decisions.warn("artifact_archive_recommended", "Artifact pressure/hot-path bloat can be reduced by archival maintenance.")


def _classify_canonical(canonical: Mapping[str, Any], decisions: "_DecisionBuilder") -> None:
    state = _canonical_state(canonical)
    if not state:
        return
    if state.startswith("NOT_READY") and "BLOCK_SUBMIT" not in decisions.actions:
        decisions.add_action("BLOCK_SUBMIT", f"Canonical readiness is {state}; submit must remain blocked.")
    if state == "NOT_READY_RECONCILIATION" and not decisions.actions:
        decisions.set_state("REPAIRING_RECOMMENDED")
        decisions.add_action("REFRESH_RECONCILIATION", "Canonical readiness is blocked on reconciliation.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked until reconciliation is clean.")
    elif state == "NOT_READY_DEPENDENCY" and not decisions.actions:
        decisions.set_state("DEGRADED")
        decisions.add_action("RETRY", "Canonical readiness is blocked on a dependency; retry relevant read-only checks.")
        decisions.add_action("BLOCK_SUBMIT", "Submit must remain blocked until dependency recovers.")


class _DecisionBuilder:
    def __init__(self, *, generated_at: str, canonical: Mapping[str, Any]) -> None:
        self.generated_at = generated_at
        self.canonical = canonical
        self.state = "OBSERVING"
        self.actions: list[str] = []
        self.reasons: list[str] = []
        self.blockers: list[dict[str, Any]] = []
        self.warnings: list[dict[str, Any]] = []
        self.retry_count = 0
        self.cooldown_seconds = 0

    def set_state(self, state: str) -> None:
        if state not in SUPERVISOR_STATES:
            return
        priority = {
            "OBSERVING": 0,
            "RECOVERED": 0,
            "DEGRADED": 1,
            "REPAIRING_RECOMMENDED": 2,
            "OPERATOR_REQUIRED": 3,
            "BLOCKED": 4,
        }
        if priority[state] >= priority.get(self.state, 0):
            self.state = state

    def add_action(self, action: str, reason: str) -> None:
        if action not in SUPERVISOR_ACTIONS:
            return
        if action not in self.actions:
            self.actions.append(action)
        if reason and reason not in self.reasons:
            self.reasons.append(reason)

    def block(self, code: str, detail: str) -> None:
        if not any(row.get("code") == code for row in self.blockers):
            self.blockers.append({"code": code, "detail": detail})

    def warn(self, code: str, detail: str) -> None:
        if not any(row.get("code") == code for row in self.warnings):
            self.warnings.append({"code": code, "detail": detail})

    def retry(self, *, retry_count: int, cooldown_seconds: int) -> None:
        self.retry_count = max(self.retry_count, int(retry_count))
        self.cooldown_seconds = max(self.cooldown_seconds, int(cooldown_seconds))

    def result(self, *, inputs: Mapping[str, Any]) -> dict[str, Any]:
        actions = self.actions or ["NO_ACTION"]
        action_scopes = sorted({ACTION_SCOPE[action] for action in actions})
        operator_action_required = bool(
            self.state in {"OPERATOR_REQUIRED", "BLOCKED"}
            or "ALERT_OPERATOR" in actions
            or "REQUIRE_OPERATOR_APPROVAL" in actions
        )
        submit_block_required = bool("BLOCK_SUBMIT" in actions or _canonical_state(self.canonical).startswith("NOT_READY"))
        cooldown_until = None
        if self.cooldown_seconds > 0:
            cooldown_until = f"+{self.cooldown_seconds}s"
        return {
            "schema_version": "track_b_readiness_maintenance_supervisor_v1",
            "generated_at": self.generated_at,
            "paper_only": True,
            "supervisor_state": self.state,
            "recommended_actions": actions,
            "action_scope": action_scopes,
            "reasons": list(self.reasons),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "retry_count": self.retry_count,
            "cooldown_until": cooldown_until,
            "operator_action_required": operator_action_required,
            "submit_block_required": submit_block_required,
            "submit_block_reason": "; ".join(row["detail"] for row in self.blockers) if submit_block_required else None,
            "canonical_readiness": _canonical_state(self.canonical),
            "live_money_eligible": False,
            "authority": {
                "paper_only": True,
                "submit_authority": False,
                "live_money_eligible": False,
                "broker_mutation_allowed": False,
                "order_api_allowed": False,
                "lifecycle_mutation_allowed": "guarded_lifecycle_tool_only_with_operator_approval",
                "runtime_restart_allowed": False,
                "sidecar_restart_allowed": False,
                "dashboard_owner": False,
                "canonical_readiness_authority": True,
            },
            "input_summary": _input_summary(inputs),
        }


def _input_summary(inputs: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "canonical_readiness": _canonical_state(_mapping(inputs.get("canonical_readiness"))),
        "ibkr_connectivity": _classification(inputs.get("ibkr_connectivity")),
        "broker_truth": _classification(inputs.get("broker_truth")),
        "market_data": _classification(inputs.get("market_data")),
        "runtime": _classification(inputs.get("runtime")),
        "lane_quarantine": _classification(inputs.get("lane_quarantine")),
        "reconciliation": _classification(inputs.get("reconciliation") or inputs.get("phase1_reconciliation")),
        "artifact_pressure": _classification(inputs.get("artifact_pressure")),
    }


def _classification(value: Any) -> str | None:
    payload = _mapping(value)
    raw = payload.get("classification") or payload.get("state") or payload.get("status")
    return str(raw) if raw is not None else None


def _canonical_state(canonical: Mapping[str, Any]) -> str:
    return str(canonical.get("canonical_readiness") or canonical.get("state") or "").strip()


def _any_live_money_eligible(*values: Any) -> bool:
    for value in values:
        if _bool(value):
            return True
        payload = _mapping(value)
        if _bool(payload.get("live_money_eligible")):
            return True
        if _bool(payload.get("strategy_registry_live_money_eligible")):
            return True
    return False


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _bool(value: Any) -> bool:
    return value is True
