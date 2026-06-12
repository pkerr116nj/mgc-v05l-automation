"""Track B PAPER position-management manifest contract.

This module is a durable metadata sidecar. It never submits, cancels, closes,
or queries broker state; it only records and resolves the management metadata
required for broker-backed PAPER positions to remain managed after fill/adoption.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_lifecycle_state_transition import (
    BLOCKED_NO_BROKER_EFFECT,
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
    OPEN_MANAGED,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    broker_backed_fill_evidence_blockers,
    classify_managed_position_transition,
    is_pre_submit_no_broker_effect,
)


DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT = Path(
    "outputs/track_b_execution_core/position_management_manifests"
)
DEFAULT_TRACK_B_RUNTIME_CONFIG_IN_FORCE_JSON = Path(
    "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
)
DEFAULT_TRACK_B_PROBATIONARY_LANE_CONFIG_YAML = Path(
    "config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml"
)
POSITION_MANAGEMENT_MANIFEST_SCHEMA_VERSION = "track_b_position_management_manifest_v1"
PAPER_EXECUTION_TEST_MULE_MANAGED_EXIT_POLICY_ID = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
PAPER_ACTIVE_EVIDENCE_MANAGED_EXIT_POLICY_ID = "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
PAPER_EXECUTION_TEST_MULE_LANE_IDS = {
    "track_b_paper_execution_test_mule_v1__mgc",
    "track_b_paper_execution_test_mule_v1__mnq",
}


@dataclass(frozen=True)
class TrackBPositionManagementManifestResult:
    manifest_path: Path
    manifest: dict[str, Any]


@dataclass(frozen=True)
class TrackBManagementMetadataResolution:
    managed_exit_policy_id: str | None
    source: str | None
    manifest_path: Path | None
    classification: str
    blockers: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return self.classification == "POSITION_MANAGEMENT_METADATA_COMPLETE"


def manifest_path_for_intent(
    entry_intent_id: str,
    *,
    output_root: Path = DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
) -> Path:
    intent = str(entry_intent_id or "").strip()
    digest = hashlib.sha256(intent.encode("utf-8")).hexdigest()[:12]
    safe_intent = "".join(char if char.isalnum() or char in "._|-+" else "_" for char in intent)[:120]
    name = f"{safe_intent or 'unknown_intent'}__{digest}.json"
    return Path(output_root) / name


def create_or_update_position_management_manifest(
    *,
    entry_intent_id: str,
    lane_id: str | None,
    strategy_id: str | None,
    instrument_family: str | None,
    contract_key: str | None,
    local_symbol: str | None,
    con_id: int | str | None,
    side: str | None,
    quantity: int | str | None,
    managed_exit_policy_id: str | None,
    lifecycle_status: str,
    runtime_instance_id: str | None = None,
    restart_generation: int | str | None = None,
    source_commit: str | None = None,
    config_fingerprint: str | None = None,
    policy_config_refs: Mapping[str, Any] | None = None,
    broker_ownership_identity: Mapping[str, Any] | None = None,
    lifecycle_id: str | None = None,
    output_root: Path = DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    existing_manifest: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> TrackBPositionManagementManifestResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    path = manifest_path_for_intent(entry_intent_id, output_root=output_root)
    existing = dict(existing_manifest or _read_json(path) or {})
    created_at = str(existing.get("created_at") or actual_now.isoformat())
    requested_lifecycle_status = str(lifecycle_status)
    broker_identity = dict(broker_ownership_identity or existing.get("broker_ownership_identity") or {})
    transition = classify_managed_position_transition(
        {
            **broker_identity,
            "requested_lifecycle_status": requested_lifecycle_status,
            "entry_intent_id": entry_intent_id,
            "lane_id": _first_text(lane_id, existing.get("lane_id")),
            "strategy_id": _first_text(strategy_id, existing.get("strategy_id")),
            "contract_key": _first_text(
                contract_key,
                existing.get("contract", {}).get("contract_key") if isinstance(existing.get("contract"), Mapping) else None,
            ),
            "local_symbol": _first_text(
                local_symbol,
                existing.get("contract", {}).get("local_symbol") if isinstance(existing.get("contract"), Mapping) else None,
            ),
            "con_id": _first_text(
                con_id,
                existing.get("contract", {}).get("con_id") if isinstance(existing.get("contract"), Mapping) else None,
            ),
            "side": _first_text(side, existing.get("side")),
            "quantity": _first_text(quantity, existing.get("quantity")),
            "managed_exit_policy_id": _first_text(managed_exit_policy_id, existing.get("managed_exit_policy_id")),
            "lifecycle_id": _first_text(lifecycle_id, existing.get("lifecycle_id")),
        }
    )
    requested_lifecycle_status = transition.classification
    lifecycle_status_blockers = list(transition.blockers)

    manifest = {
        "schema_version": POSITION_MANAGEMENT_MANIFEST_SCHEMA_VERSION,
        "runtime_instance_id": _first_text(runtime_instance_id, existing.get("runtime_instance_id"), os.environ.get("MGC_TRACK_B_RUNTIME_INSTANCE_ID")),
        "restart_generation": _first_text(
            restart_generation,
            existing.get("restart_generation"),
            os.environ.get("MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION"),
            os.environ.get("MGC_TRACK_B_RESTART_GENERATION"),
        ),
        "source_commit": _first_text(source_commit, existing.get("source_commit"), _git_head_from_env()),
        "config_fingerprint": _first_text(config_fingerprint, existing.get("config_fingerprint"), os.environ.get("MGC_TRACK_B_PAPER_CONFIG_FINGERPRINT")),
        "lane_id": _first_text(lane_id, existing.get("lane_id")),
        "strategy_id": _first_text(strategy_id, existing.get("strategy_id")),
        "entry_intent_id": str(entry_intent_id),
        "contract": {
            "instrument_family": _first_text(instrument_family, existing.get("contract", {}).get("instrument_family") if isinstance(existing.get("contract"), Mapping) else None),
            "contract_key": _first_text(contract_key, existing.get("contract", {}).get("contract_key") if isinstance(existing.get("contract"), Mapping) else None),
            "local_symbol": _first_text(local_symbol, existing.get("contract", {}).get("local_symbol") if isinstance(existing.get("contract"), Mapping) else None),
            "con_id": _first_text(con_id, existing.get("contract", {}).get("con_id") if isinstance(existing.get("contract"), Mapping) else None),
        },
        "side": _first_text(side, existing.get("side")),
        "quantity": _first_text(quantity, existing.get("quantity")),
        "managed_exit_policy_id": _first_text(managed_exit_policy_id, existing.get("managed_exit_policy_id")),
        "policy_config_refs": dict(policy_config_refs or existing.get("policy_config_refs") or {}),
        "broker_ownership_identity": broker_identity,
        "lifecycle_id": _first_text(lifecycle_id, existing.get("lifecycle_id")),
        "lifecycle_status": requested_lifecycle_status,
        "lifecycle_status_blockers": lifecycle_status_blockers,
        "created_at": created_at,
        "updated_at": actual_now.isoformat(),
        "paper_only": True,
        "live_money_eligible": False,
        "broker_mutation_authority": False,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return TrackBPositionManagementManifestResult(manifest_path=path, manifest=manifest)


def create_manifest_from_order_intent(
    *,
    order_intent: Any,
    runtime_identity: Mapping[str, Any],
    output_root: Path = DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    now: datetime | None = None,
) -> TrackBPositionManagementManifestResult | None:
    if getattr(order_intent, "is_entry", False) is not True:
        return None
    side = "LONG" if str(getattr(order_intent, "intent_type", "")).endswith("BUY_TO_OPEN") else "SHORT"
    lane_id = _first_text(runtime_identity.get("lane_id"))
    strategy_id = _first_text(runtime_identity.get("standalone_strategy_id"), runtime_identity.get("strategy_id"))
    instrument_family = _first_text(runtime_identity.get("instrument"), getattr(order_intent, "symbol", None))
    contract_key = _first_text(runtime_identity.get("contract_key"))
    local_symbol = _first_text(runtime_identity.get("local_symbol"))
    con_id = runtime_identity.get("con_id")
    managed_exit_policy_id = _first_text(runtime_identity.get("managed_exit_policy_id"))
    metadata_resolution: TrackBManagementMetadataResolution | None = None
    if not managed_exit_policy_id:
        metadata_resolution = resolve_management_metadata(
            source={
                "entry_intent_id": str(getattr(order_intent, "order_intent_id")),
                "order_intent_id": str(getattr(order_intent, "order_intent_id")),
                "lane_id": lane_id,
                "strategy_id": strategy_id,
                "instrument": instrument_family,
                "contract_key": contract_key,
                "local_symbol": local_symbol,
                "con_id": con_id,
                "side": side,
                "quantity": getattr(order_intent, "quantity", None),
            },
            output_root=output_root,
        )
        managed_exit_policy_id = metadata_resolution.managed_exit_policy_id
    policy_config_refs = {
        "source": "runtime_identity" if _first_text(runtime_identity.get("managed_exit_policy_id")) else "management_metadata_resolution",
        "strategy_family": runtime_identity.get("strategy_family"),
        "config_source": runtime_identity.get("config_source"),
    }
    if metadata_resolution is not None:
        policy_config_refs.update(
            {
                "metadata_resolution_source": metadata_resolution.source,
                "metadata_resolution_classification": metadata_resolution.classification,
                "metadata_resolution_blockers": list(metadata_resolution.blockers),
            }
        )
    return create_or_update_position_management_manifest(
        entry_intent_id=str(getattr(order_intent, "order_intent_id")),
        lane_id=lane_id,
        strategy_id=strategy_id,
        instrument_family=instrument_family,
        contract_key=contract_key,
        local_symbol=local_symbol,
        con_id=con_id,
        side=side,
        quantity=getattr(order_intent, "quantity", None),
        managed_exit_policy_id=managed_exit_policy_id,
        lifecycle_status="INTENT_CREATED",
        policy_config_refs=policy_config_refs,
        output_root=output_root,
        now=now,
    )


def update_manifest_from_filled_bridge_result(
    *,
    filled_bridge_result: Mapping[str, Any],
    output_root: Path = DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    now: datetime | None = None,
) -> TrackBPositionManagementManifestResult | None:
    intent_id = str(filled_bridge_result.get("order_intent_id") or "").strip()
    if not intent_id:
        return None
    existing_path = _manifest_path_from_payload(filled_bridge_result, output_root=output_root)
    existing = _read_json(existing_path) if existing_path else None
    contract = filled_bridge_result.get("contract") if isinstance(filled_bridge_result.get("contract"), Mapping) else {}
    identity = {
        "broker_order_id": filled_bridge_result.get("broker_order_id"),
        "perm_id": filled_bridge_result.get("perm_id"),
        "client_id": filled_bridge_result.get("client_id"),
        "exec_id": filled_bridge_result.get("exec_id") or filled_bridge_result.get("execution_id"),
        "account_id": filled_bridge_result.get("account_id"),
        "fill_price": filled_bridge_result.get("fill_price") or filled_bridge_result.get("entry_fill_price"),
        "fill_timestamp": filled_bridge_result.get("fill_timestamp") or filled_bridge_result.get("entry_timestamp"),
    }
    resolution = resolve_management_metadata(
        source=filled_bridge_result,
        output_root=output_root,
    )
    transition = classify_managed_position_transition(
        {
            **dict(filled_bridge_result),
            **identity,
            "requested_lifecycle_status": OPEN_MANAGED,
            "entry_intent_id": _first_text(filled_bridge_result.get("entry_intent_id"), filled_bridge_result.get("order_intent_id")),
            "side": _side_from_filled_bridge_result(filled_bridge_result),
            "lifecycle_id": _first_text(
                filled_bridge_result.get("lifecycle_id"),
                f"bridge_fill_{intent_id}" if intent_id else None,
            ),
            "managed_exit_policy_id": resolution.managed_exit_policy_id,
        }
    )
    lifecycle_status = transition.classification
    return create_or_update_position_management_manifest(
        entry_intent_id=intent_id,
        lane_id=_first_text(filled_bridge_result.get("lane_id"), existing.get("lane_id") if existing else None),
        strategy_id=_first_text(filled_bridge_result.get("strategy_id"), existing.get("strategy_id") if existing else None),
        instrument_family=_first_text(filled_bridge_result.get("instrument"), filled_bridge_result.get("symbol")),
        contract_key=_first_text(filled_bridge_result.get("contract_key"), existing.get("contract", {}).get("contract_key") if existing else None),
        local_symbol=_first_text(filled_bridge_result.get("local_symbol"), contract.get("local_symbol")),
        con_id=_first_text(filled_bridge_result.get("con_id"), contract.get("qualified_contract_identifier")),
        side=_side_from_filled_bridge_result(filled_bridge_result),
        quantity=filled_bridge_result.get("quantity"),
        managed_exit_policy_id=resolution.managed_exit_policy_id,
        lifecycle_status=lifecycle_status,
        policy_config_refs={"metadata_source": resolution.source},
        broker_ownership_identity=identity,
        lifecycle_id=_first_text(filled_bridge_result.get("lifecycle_id"), f"bridge_fill_{intent_id}" if intent_id else None),
        output_root=output_root,
        existing_manifest=existing,
        now=now,
    )


def resolve_management_metadata(
    *,
    source: Mapping[str, Any],
    output_root: Path = DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    lane_registry_paths: Iterable[Path] | None = None,
) -> TrackBManagementMetadataResolution:
    direct_policy = _first_text(source.get("managed_exit_policy_id"))
    if direct_policy:
        return TrackBManagementMetadataResolution(direct_policy, "payload", None, "POSITION_MANAGEMENT_METADATA_COMPLETE")
    manifest_path = _manifest_path_from_payload(source, output_root=output_root)
    manifest = _read_json(manifest_path) if manifest_path else None
    manifest_policy = _first_text(manifest.get("managed_exit_policy_id") if manifest else None)
    if manifest_policy:
        return TrackBManagementMetadataResolution(manifest_policy, "manifest", manifest_path, "POSITION_MANAGEMENT_METADATA_COMPLETE")
    lane_policy = _policy_from_lane_registry(source=source, lane_registry_paths=lane_registry_paths)
    if lane_policy:
        return TrackBManagementMetadataResolution(lane_policy, "lane_registry", manifest_path, "POSITION_MANAGEMENT_METADATA_COMPLETE")
    blockers = tuple(_missing_required_management_fields(source, manifest=manifest))
    return TrackBManagementMetadataResolution(
        None,
        None,
        manifest_path,
        OPEN_MANAGED_METADATA_INCOMPLETE,
        blockers=blockers or ("managed_exit_policy_id",),
    )


def lifecycle_metadata_complete(source: Mapping[str, Any]) -> TrackBManagementMetadataResolution:
    policy = _first_text(source.get("managed_exit_policy_id"))
    blockers = tuple(_missing_required_management_fields(source, manifest=None))
    if policy and not blockers:
        return TrackBManagementMetadataResolution(policy, "lifecycle", None, "POSITION_MANAGEMENT_METADATA_COMPLETE")
    return TrackBManagementMetadataResolution(policy, "lifecycle" if policy else None, None, OPEN_MANAGED_METADATA_INCOMPLETE, blockers=blockers)


def broker_backed_fill_evidence_complete(source: Mapping[str, Any]) -> TrackBManagementMetadataResolution:
    blockers = tuple(broker_backed_fill_evidence_blockers(source))
    if blockers:
        return TrackBManagementMetadataResolution(
            None,
            "broker_fill_identity",
            None,
            BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
            blockers=blockers,
        )
    return TrackBManagementMetadataResolution(None, "broker_fill_identity", None, "BROKER_BACKED_FILL_EVIDENCE_COMPLETE")


def _manifest_path_from_payload(source: Mapping[str, Any], *, output_root: Path) -> Path | None:
    explicit = _first_text(source.get("position_management_manifest_path"))
    if explicit:
        return Path(explicit)
    intent_id = _first_text(source.get("entry_intent_id"), source.get("order_intent_id"))
    if not intent_id:
        lifecycle_id = _first_text(source.get("lifecycle_id"))
        if lifecycle_id and lifecycle_id.startswith("bridge_fill_"):
            intent_id = lifecycle_id.removeprefix("bridge_fill_")
    return manifest_path_for_intent(intent_id, output_root=output_root) if intent_id else None


def _policy_from_lane_registry(
    *,
    source: Mapping[str, Any],
    lane_registry_paths: Iterable[Path] | None,
) -> str | None:
    lane_id = _first_text(source.get("lane_id"))
    strategy_id = _first_text(source.get("strategy_id"))
    paths = list(lane_registry_paths or ())
    paths.extend([DEFAULT_TRACK_B_RUNTIME_CONFIG_IN_FORCE_JSON, DEFAULT_TRACK_B_PROBATIONARY_LANE_CONFIG_YAML])
    for path in paths:
        for lane in _lane_rows_from_path(Path(path)):
            if _lane_matches_source(lane, lane_id=lane_id, strategy_id=strategy_id):
                policy = _first_text(lane.get("managed_exit_policy_id"), _active_evidence_policy_from_lane(lane))
                if policy:
                    return policy
    if lane_id in PAPER_EXECUTION_TEST_MULE_LANE_IDS or strategy_id in PAPER_EXECUTION_TEST_MULE_LANE_IDS:
        return PAPER_EXECUTION_TEST_MULE_MANAGED_EXIT_POLICY_ID
    return None


def _lane_matches_source(lane: Mapping[str, Any], *, lane_id: str | None, strategy_id: str | None) -> bool:
    if lane_id and str(lane.get("lane_id") or "") == lane_id:
        return True
    if not strategy_id:
        return False
    return strategy_id in _strategy_ids_from_lane(lane)


def _strategy_ids_from_lane(lane: Mapping[str, Any]) -> set[str]:
    values: set[str] = set()
    for key in (
        "standalone_strategy_id",
        "strategy_id",
        "strategy_identity_root",
        "rule_id",
        "rule_mode",
    ):
        text = _first_text(lane.get(key))
        if text:
            values.add(text)
    overlay = lane.get("runtime_overlay_params")
    if isinstance(overlay, Mapping):
        for key in ("strategy_id", "rule_id", "rule_mode", "entry_source"):
            text = _first_text(overlay.get(key))
            if text:
                values.add(text)
    for key in ("long_sources", "short_sources"):
        raw_values = lane.get(key)
        if isinstance(raw_values, Iterable) and not isinstance(raw_values, (str, bytes)):
            for raw in raw_values:
                text = _first_text(raw)
                if text:
                    values.add(text)
    return values


def _active_evidence_policy_from_lane(lane: Mapping[str, Any]) -> str | None:
    strategy_ids = _strategy_ids_from_lane(lane)
    lane_id = str(lane.get("lane_id") or "")
    family = str(lane.get("strategy_family") or lane.get("source_family") or "")
    mode = str(lane.get("lane_mode") or "")
    if (
        "paper_active_evidence" in family
        or "ACTIVE_EVIDENCE" in mode
        or "_active_participation_" in lane_id
        or any(value.startswith("PAPER_ACTIVE_EVIDENCE_") and "PARTICIPATION" in value for value in strategy_ids)
    ):
        return PAPER_ACTIVE_EVIDENCE_MANAGED_EXIT_POLICY_ID
    return None


def _lane_rows_from_path(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _lane_rows_from_yaml_text(path.read_text(encoding="utf-8"))
    if isinstance(payload, Mapping):
        lanes = payload.get("lanes")
        if isinstance(lanes, list):
            return [row for row in lanes if isinstance(row, Mapping)]
        lanes_json = payload.get("probationary_paper_lanes_json")
        if isinstance(lanes_json, str):
            try:
                parsed = json.loads(lanes_json)
            except json.JSONDecodeError:
                return []
            return [row for row in parsed if isinstance(row, Mapping)]
    return []


def _lane_rows_from_yaml_text(text: str) -> list[Mapping[str, Any]]:
    for line in text.splitlines():
        if not line.startswith("probationary_paper_lanes_json:"):
            continue
        raw = line.split(":", 1)[1].strip()
        try:
            lanes_json = ast.literal_eval(raw)
            parsed = json.loads(lanes_json)
        except (ValueError, SyntaxError, json.JSONDecodeError):
            return []
        return [row for row in parsed if isinstance(row, Mapping)]
    return []


def _missing_required_management_fields(source: Mapping[str, Any], *, manifest: Mapping[str, Any] | None) -> list[str]:
    contract = manifest.get("contract") if isinstance(manifest, Mapping) and isinstance(manifest.get("contract"), Mapping) else {}
    required = {
        "lane_id": _first_text(source.get("lane_id"), manifest.get("lane_id") if manifest else None),
        "strategy_id": _first_text(source.get("strategy_id"), manifest.get("strategy_id") if manifest else None),
        "entry_intent_id": _first_text(source.get("entry_intent_id"), source.get("order_intent_id"), manifest.get("entry_intent_id") if manifest else None),
        "side": _first_text(source.get("side"), source.get("action"), manifest.get("side") if manifest else None),
        "quantity": _first_text(source.get("quantity"), manifest.get("quantity") if manifest else None),
        "managed_exit_policy_id": _first_text(source.get("managed_exit_policy_id"), manifest.get("managed_exit_policy_id") if manifest else None),
        "contract": _first_text(source.get("contract_key"), source.get("local_symbol"), source.get("con_id"), contract.get("contract_key"), contract.get("local_symbol"), contract.get("con_id")),
    }
    return [key for key, value in required.items() if not value]


def _is_pre_submit_no_broker_effect(source: Mapping[str, Any], identity: Mapping[str, Any]) -> bool:
    return is_pre_submit_no_broker_effect({**dict(source), **dict(identity)})


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _git_head_from_env() -> str | None:
    return _first_text(os.environ.get("MGC_TRACK_B_EXPECTED_SOURCE_COMMIT"), os.environ.get("MGC_TRACK_B_SOURCE_COMMIT"))


def _side_from_filled_bridge_result(payload: Mapping[str, Any]) -> str | None:
    side = _first_text(payload.get("side"))
    if side in {"LONG", "SHORT"}:
        return side
    action = _first_text(payload.get("action"), payload.get("intent_type")) or ""
    if "BUY_TO_OPEN" in action or action == "BUY":
        return "LONG"
    if "SELL_TO_OPEN" in action or action == "SELL":
        return "SHORT"
    return None
