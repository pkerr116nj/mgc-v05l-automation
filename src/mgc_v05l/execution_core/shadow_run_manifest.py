"""Track B-native no-submit shadow run manifest boundary."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import TrackBModelError, require_aware_datetime, require_id, to_jsonable


DEFAULT_SHADOW_RUN_MANIFEST_OUTPUT_ROOT = Path("outputs/track_b_execution_core/shadow_run_manifests")
DEFAULT_REQUIRED_ARTIFACTS = (
    "intent_validation",
    "lane_validation",
    "order_plan",
    "shadow_evaluation",
)


class ShadowRunInputSourceType(str, Enum):
    MANUAL_INTENT_JSON = "MANUAL_INTENT_JSON"
    REPLAY_FILE = "REPLAY_FILE"
    SHADOW_SIGNAL_FILE = "SHADOW_SIGNAL_FILE"


class ShadowRunManifestVerdict(str, Enum):
    VALID = "SHADOW_RUN_MANIFEST_VALID"
    BLOCKED_LIVE_MODE = "SHADOW_RUN_MANIFEST_BLOCKED_LIVE_MODE"
    BLOCKED_SUBMIT_ENABLED = "SHADOW_RUN_MANIFEST_BLOCKED_SUBMIT_ENABLED"
    BLOCKED_NO_CONTRACTS = "SHADOW_RUN_MANIFEST_BLOCKED_NO_CONTRACTS"
    BLOCKED_MISSING_ACCOUNT = "SHADOW_RUN_MANIFEST_BLOCKED_MISSING_ACCOUNT"
    BLOCKED_MISSING_OUTPUT_ROOT = "SHADOW_RUN_MANIFEST_BLOCKED_MISSING_OUTPUT_ROOT"
    BLOCKED_SCHEMA_ERROR = "SHADOW_RUN_MANIFEST_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class ShadowRunManifest:
    run_id: str
    run_mode: str
    expected_account_id: str
    allowed_local_execution_contract_keys: tuple[str, ...]
    registry_version: str | None
    registry_path: str | None
    registry_name: str | None
    input_source_type: str
    output_root: str
    required_artifacts: tuple[str, ...]
    submit_enabled: bool = False
    live_money_readiness: bool = False
    generated_at: datetime | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ShadowRunManifest":
        generated_at = _parse_timestamp(payload.get("generated_at") or payload.get("created_at"))
        return cls(
            run_id=str(payload.get("run_id") or "").strip(),
            run_mode=str(payload.get("run_mode") or payload.get("mode") or "").strip().upper(),
            expected_account_id=str(payload.get("expected_account_id") or "").strip(),
            allowed_local_execution_contract_keys=_string_tuple(payload.get("allowed_local_execution_contract_keys")),
            registry_version=_optional_str(payload.get("registry_version")),
            registry_path=_optional_str(payload.get("registry_path")),
            registry_name=_optional_str(payload.get("registry_name")),
            input_source_type=_input_source_type(payload.get("input_source_type")),
            output_root=str(payload.get("output_root") or "").strip(),
            required_artifacts=_required_artifacts(payload.get("required_artifacts")),
            submit_enabled=_bool(payload.get("submit_enabled", False)),
            live_money_readiness=False,
            generated_at=generated_at,
        )


@dataclass(frozen=True)
class ShadowRunManifestResult:
    verdict: ShadowRunManifestVerdict
    report_json: Path
    report: dict[str, Any]
    manifest: ShadowRunManifest | None


def validate_shadow_run_manifest(
    *,
    payload: Mapping[str, Any],
    output_root: Path = DEFAULT_SHADOW_RUN_MANIFEST_OUTPUT_ROOT,
    run_id: str | None = None,
    now: datetime | None = None,
) -> ShadowRunManifestResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or f"shadow_run_manifest_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_run_id / "shadow_run_manifest_report.json"
    try:
        manifest = ShadowRunManifest.from_mapping(payload)
        verdict, blocker, action = _classify_manifest(manifest)
    except (TrackBModelError, ValueError, TypeError) as exc:
        manifest = None
        verdict = ShadowRunManifestVerdict.BLOCKED_SCHEMA_ERROR
        blocker = str(exc)
        action = "Fix shadow run manifest schema before Track B shadow evaluation."

    allowed = verdict == ShadowRunManifestVerdict.VALID
    report = {
        "schema_version": "track_b_shadow_run_manifest_v1",
        "generated_at": actual_now.isoformat(),
        "manifest_validation_verdict": verdict.value,
        "manifest_allowed_for_shadow_run": allowed,
        "submit_enabled": False if manifest is None else manifest.submit_enabled,
        "submit_allowed": False,
        "submit_attempted": False,
        "primary_blocker": blocker,
        "secondary_blockers": [],
        "required_next_action": action,
        "run_id": None if manifest is None else manifest.run_id,
        "run_mode": None if manifest is None else manifest.run_mode,
        "expected_account_id": None if manifest is None else manifest.expected_account_id,
        "allowed_local_execution_contract_keys": [] if manifest is None else list(manifest.allowed_local_execution_contract_keys),
        "registry_version": None if manifest is None else manifest.registry_version,
        "registry_path": None if manifest is None else manifest.registry_path,
        "registry_name": None if manifest is None else manifest.registry_name,
        "input_source_type": None if manifest is None else manifest.input_source_type,
        "output_root": None if manifest is None else manifest.output_root,
        "required_artifacts": list(DEFAULT_REQUIRED_ARTIFACTS if manifest is None else manifest.required_artifacts),
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "strategy_execution_attempted": False,
        "paper_proof_cli_wired": False,
        "manifest_authorizes_lanes": False,
        "lane_authorization_required": True,
        "registry_governs_lane_authorization": True,
        "manifest_governs_run_context_only": True,
        "local_execution_contract_keys_are_execution_authority": True,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return ShadowRunManifestResult(verdict=verdict, report_json=report_json, report=report, manifest=manifest)


def _classify_manifest(manifest: ShadowRunManifest) -> tuple[ShadowRunManifestVerdict, str | None, str]:
    if not manifest.expected_account_id:
        return (
            ShadowRunManifestVerdict.BLOCKED_MISSING_ACCOUNT,
            "expected_account_id is required for Track B shadow runs.",
            "Set the explicit paper account before shadow evaluation.",
        )
    if str(manifest.run_mode).upper() != "PAPER":
        return (
            ShadowRunManifestVerdict.BLOCKED_LIVE_MODE,
            "Shadow run manifest accepts PAPER mode only.",
            "Use PAPER mode. Live-money shadow run manifests are not supported.",
        )
    if manifest.submit_enabled:
        return (
            ShadowRunManifestVerdict.BLOCKED_SUBMIT_ENABLED,
            "Shadow run manifest is no-submit; submit_enabled must be false.",
            "Set submit_enabled=false. Submit remains isolated to gated paper_proof_cli.",
        )
    if not manifest.allowed_local_execution_contract_keys:
        return (
            ShadowRunManifestVerdict.BLOCKED_NO_CONTRACTS,
            "At least one explicit local execution contract key is required.",
            "Add allowlisted Track B local execution contract keys to the manifest.",
        )
    if not manifest.output_root:
        return (
            ShadowRunManifestVerdict.BLOCKED_MISSING_OUTPUT_ROOT,
            "output_root is required for reproducible Track B shadow run artifacts.",
            "Set an explicit output_root for the shadow run.",
        )
    return (
        ShadowRunManifestVerdict.VALID,
        None,
        "Manifest is valid for no-submit Track B shadow run review. Lane authorization and downstream gates remain separate.",
    )


def _input_source_type(value: object) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        raise TrackBModelError("input_source_type is required.")
    try:
        return ShadowRunInputSourceType(raw).value
    except ValueError as exc:
        allowed = ", ".join(item.value for item in ShadowRunInputSourceType)
        raise TrackBModelError(f"input_source_type must be one of: {allowed}.") from exc


def _required_artifacts(value: object) -> tuple[str, ...]:
    if value is None:
        return DEFAULT_REQUIRED_ARTIFACTS
    if isinstance(value, Mapping):
        artifacts = tuple(str(key).strip() for key, enabled in value.items() if enabled and str(key).strip())
    else:
        artifacts = _string_tuple(value)
    if not artifacts:
        return DEFAULT_REQUIRED_ARTIFACTS
    return tuple(dict.fromkeys(artifacts))


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        candidates: Sequence[object] = (value,)
    elif isinstance(value, Sequence):
        candidates = value
    else:
        raise TrackBModelError("expected a list of strings.")
    return tuple(require_id(str(item), "list item") for item in candidates if str(item or "").strip())


def _optional_str(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _parse_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        require_aware_datetime(value, "generated_at")
        return value
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    require_aware_datetime(parsed, "generated_at")
    return parsed


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)
