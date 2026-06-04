"""Read-only Track B registry/truth diagnostics.

This module surfaces canonical truth, trade-registry reconstruction, shadow
registry state, and the latest lifecycle stress preflight result as diagnostics
only. It is not submit authority and is not wired into runtime gates.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    TRUTH_CONFLICT_REVIEW_REQUIRED,
    TrackBTruthSnapshot,
    TrackBTruthSnapshotConfig,
    TrackBTruthSource,
    build_track_b_truth_snapshot,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState
from mgc_v05l.execution_core.track_b_lifecycle_stress_preflight import DEFAULT_PREFLIGHT_SUMMARY_PATH
from mgc_v05l.execution_core.track_b_live_trade_registry import load_live_trade_registry_records
from mgc_v05l.execution_core.track_b_terminal_registry_truth import filter_terminal_superseded_current_rows
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import (
    TradeRegistryReconstructionConfig,
    TradeRegistryReconstructionReport,
    reconstruct_trade_registry_from_artifacts,
)
from mgc_v05l.execution_core.track_b_trade_registry_shadow_report import (
    TradeRegistryShadowReport,
    TradeRegistryShadowReportConfig,
    build_trade_registry_shadow_report,
)


SCHEMA_VERSION = "track_b_registry_truth_diagnostics_v1"
DEFAULT_DIAGNOSTICS_REPORT_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_track_b_registry_truth_diagnostics.json"
)

TRACK_B_DIAGNOSTICS_CLEAN = "TRACK_B_DIAGNOSTICS_CLEAN"
TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED = "TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED"
TRACK_B_DIAGNOSTICS_STALE = "TRACK_B_DIAGNOSTICS_STALE"
TRACK_B_DIAGNOSTICS_CONFLICT = "TRACK_B_DIAGNOSTICS_CONFLICT"
TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE = "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE"
TRACK_B_DIAGNOSTICS_STALE_AUTHORITY = "TRACK_B_DIAGNOSTICS_STALE_AUTHORITY"
TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED = "TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED"
TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE = "TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE"

TRACK_B_FUTURES_SYMBOLS = {"MGC", "GC", "MNQ", "MES"}


class TrackBDiagnosticsMode(str, Enum):
    CURRENT_HOT_PATH = "CURRENT_HOT_PATH"
    HISTORICAL_RECONSTRUCTION = "HISTORICAL_RECONSTRUCTION"
    FULL_ARTIFACT_AUDIT = "FULL_ARTIFACT_AUDIT"


@dataclass(frozen=True)
class TrackBRegistryTruthDiagnosticsConfig:
    repo_root: Path
    mode: TrackBDiagnosticsMode = TrackBDiagnosticsMode.CURRENT_HOT_PATH
    output_path: Path = DEFAULT_DIAGNOSTICS_REPORT_PATH
    truth_config: TrackBTruthSnapshotConfig | None = None
    reconstruction_config: TradeRegistryReconstructionConfig | None = None
    shadow_config: TradeRegistryShadowReportConfig | None = None
    preflight_summary_path: Path = DEFAULT_PREFLIGHT_SUMMARY_PATH
    max_preflight_age_seconds: float = 86_400.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path

    def effective_truth_config(self) -> TrackBTruthSnapshotConfig:
        return self.truth_config or TrackBTruthSnapshotConfig(repo_root=self.repo_root)

    def effective_reconstruction_config(self) -> TradeRegistryReconstructionConfig:
        return self.reconstruction_config or TradeRegistryReconstructionConfig(repo_root=self.repo_root)

    def effective_shadow_config(self) -> TradeRegistryShadowReportConfig:
        if self.shadow_config is not None:
            return self.shadow_config
        return TradeRegistryShadowReportConfig(
            repo_root=self.repo_root,
            truth_config=self.effective_truth_config(),
            reconstruction_config=self.effective_reconstruction_config(),
        )


@dataclass(frozen=True)
class TrackBRegistryTruthDiagnosticsReport:
    schema_version: str
    generated_at: datetime
    mode: str
    classification: str
    read_only: bool
    diagnostic_only: bool
    broker_mutation_allowed: bool
    runtime_restart_allowed: bool
    production_gate_wiring_allowed: bool
    runtime_ready: bool
    runtime_alive: bool
    recovery_active: bool
    broker_lifecycle_reconciled: bool
    broker_open_order_count: int
    broker_position_count: int
    track_b_managed_futures_position_count: int
    unrelated_broker_position_count: int
    unknown_scope_position_count: int
    broker_positions_by_scope: Mapping[str, Any]
    lifecycle_open_position_count: int
    registry_trade_state_counts: Mapping[str, int]
    current_scope_trade_states: tuple[Mapping[str, Any], ...]
    review_required_trade_ids: tuple[str, ...]
    historical_review_required_trade_ids: tuple[str, ...]
    truth_conflicts: tuple[Mapping[str, Any], ...]
    registry_reconciliation_disagreements: tuple[str, ...]
    terminal_superseded_current_rows: tuple[Mapping[str, Any], ...]
    latest_preflight_hard_failure_count: int
    latest_preflight_stale: bool
    latest_preflight_source_path: str
    reason_codes: tuple[str, ...]
    truth_snapshot: Mapping[str, Any]
    reconstruction_summary: Mapping[str, Any]
    shadow_summary: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "mode": self.mode,
            "classification": self.classification,
            "read_only": self.read_only,
            "diagnostic_only": self.diagnostic_only,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "runtime_restart_allowed": self.runtime_restart_allowed,
            "production_gate_wiring_allowed": self.production_gate_wiring_allowed,
            "runtime_ready": self.runtime_ready,
            "runtime_alive": self.runtime_alive,
            "recovery_active": self.recovery_active,
            "broker_lifecycle_reconciled": self.broker_lifecycle_reconciled,
            "broker_open_order_count": self.broker_open_order_count,
            "broker_position_count": self.broker_position_count,
            "track_b_managed_futures_position_count": self.track_b_managed_futures_position_count,
            "unrelated_broker_position_count": self.unrelated_broker_position_count,
            "unknown_scope_position_count": self.unknown_scope_position_count,
            "broker_positions_by_scope": dict(self.broker_positions_by_scope),
            "lifecycle_open_position_count": self.lifecycle_open_position_count,
            "registry_trade_state_counts": dict(self.registry_trade_state_counts),
            "current_scope_trade_states": list(self.current_scope_trade_states),
            "review_required_trade_ids": list(self.review_required_trade_ids),
            "historical_review_required_trade_ids": list(self.historical_review_required_trade_ids),
            "truth_conflicts": list(self.truth_conflicts),
            "registry_reconciliation_disagreements": list(self.registry_reconciliation_disagreements),
            "terminal_superseded_current_rows": list(self.terminal_superseded_current_rows),
            "latest_preflight_hard_failure_count": self.latest_preflight_hard_failure_count,
            "latest_preflight_stale": self.latest_preflight_stale,
            "latest_preflight_source_path": self.latest_preflight_source_path,
            "reason_codes": list(self.reason_codes),
            "truth_snapshot": dict(self.truth_snapshot),
            "reconstruction_summary": dict(self.reconstruction_summary),
            "shadow_summary": dict(self.shadow_summary),
        }


def build_track_b_registry_truth_diagnostics(
    *,
    config: TrackBRegistryTruthDiagnosticsConfig,
    now: datetime | None = None,
) -> TrackBRegistryTruthDiagnosticsReport:
    generated_at = _ensure_utc(now or datetime.now(UTC))
    truth_config = config.effective_truth_config()
    reconstruction_config = config.effective_reconstruction_config()
    shadow_config = config.effective_shadow_config()
    truth = build_track_b_truth_snapshot(config=truth_config, now=generated_at)
    reconstruction = reconstruct_trade_registry_from_artifacts(config=reconstruction_config, now=generated_at)
    shadow = build_trade_registry_shadow_report(config=shadow_config, now=generated_at)
    scoped_positions = _broker_positions_by_scope(truth)
    preflight = _preflight_state(
        path=config.resolve(config.preflight_summary_path),
        now=generated_at,
        max_age_seconds=config.max_preflight_age_seconds,
    )
    current_rows = _current_scope_shadow_rows(truth=truth, shadow=shadow, scoped_positions=scoped_positions)
    terminal_records = load_live_trade_registry_records(repo_root=config.repo_root)
    current_rows, terminal_superseded_rows = filter_terminal_superseded_current_rows(
        rows=current_rows,
        records=terminal_records,
        broker_positions=tuple(truth.broker_truth.positions),
        broker_open_orders=tuple(truth.broker_truth.open_orders),
    )
    current_rows, owner_suppressed_rows = _filter_reconciliation_authorized_owner_rows(
        truth=truth,
        rows=current_rows,
    )
    terminal_superseded_rows = (*terminal_superseded_rows, *owner_suppressed_rows)
    reason_codes = _reason_codes(
        mode=config.mode,
        truth=truth,
        shadow=shadow,
        current_rows=current_rows,
        scoped_positions=scoped_positions,
        preflight=preflight,
    )
    classification = _classification(
        mode=config.mode,
        truth=truth,
        shadow=shadow,
        current_rows=current_rows,
        scoped_positions=scoped_positions,
        preflight=preflight,
        terminal_superseded_rows=terminal_superseded_rows,
    )
    return TrackBRegistryTruthDiagnosticsReport(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        mode=config.mode.value,
        classification=classification,
        read_only=True,
        diagnostic_only=True,
        broker_mutation_allowed=False,
        runtime_restart_allowed=False,
        production_gate_wiring_allowed=False,
        runtime_ready=truth.runtime.submit_capable,
        runtime_alive=truth.runtime.runtime_alive,
        recovery_active=truth.recovery.active,
        broker_lifecycle_reconciled=truth.reconciliation.reconciled,
        broker_open_order_count=truth.broker_truth.open_order_count,
        broker_position_count=truth.broker_truth.broker_position_count,
        track_b_managed_futures_position_count=len(scoped_positions["track_b_managed_futures_positions"]),
        unrelated_broker_position_count=len(scoped_positions["unrelated_broker_positions"]),
        unknown_scope_position_count=len(scoped_positions["unknown_scope_positions"]),
        broker_positions_by_scope=scoped_positions,
        lifecycle_open_position_count=truth.lifecycle.open_position_count,
        registry_trade_state_counts=_registry_state_counts_for_mode(
            mode=config.mode,
            reconstruction=reconstruction,
            current_rows=current_rows,
        ),
        current_scope_trade_states=tuple(_trade_state_surface(row) for row in current_rows),
        review_required_trade_ids=tuple(
            row.trade_id
            for row in _review_required_rows_for_mode(config.mode, shadow, current_rows)
        ),
        historical_review_required_trade_ids=tuple(
            record.trade_id for record in reconstruction.records if record.current_state == TradeCurrentState.REVIEW_REQUIRED
        ),
        truth_conflicts=tuple(_conflict_summary(conflict) for conflict in truth.conflicts),
        registry_reconciliation_disagreements=tuple(
            row.trade_id
            for row in (current_rows if config.mode == TrackBDiagnosticsMode.CURRENT_HOT_PATH else shadow.rows)
            if not row.registry_agrees_with_reconciliation
        ),
        latest_preflight_hard_failure_count=int(preflight["hard_failure_count"]),
        latest_preflight_stale=bool(preflight["stale"]),
        latest_preflight_source_path=str(preflight["source_path"]),
        reason_codes=reason_codes,
        truth_snapshot=_truth_surface(truth),
        reconstruction_summary=_reconstruction_summary(reconstruction),
        shadow_summary=shadow.to_dict()["summary"],
        terminal_superseded_current_rows=tuple(terminal_superseded_rows),
    )


def write_track_b_registry_truth_diagnostics(
    *,
    config: TrackBRegistryTruthDiagnosticsConfig,
    report: TrackBRegistryTruthDiagnosticsReport,
) -> Path:
    path = config.resolve(config.output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _classification(
    *,
    mode: TrackBDiagnosticsMode,
    truth: TrackBTruthSnapshot,
    shadow: TradeRegistryShadowReport,
    current_rows: tuple[Any, ...],
    scoped_positions: Mapping[str, Any],
    preflight: Mapping[str, Any],
    terminal_superseded_rows: tuple[Mapping[str, Any], ...] = (),
) -> str:
    if _truth_has_stale_authority(truth) or preflight["stale"]:
        return TRACK_B_DIAGNOSTICS_STALE_AUTHORITY
    if mode == TrackBDiagnosticsMode.CURRENT_HOT_PATH:
        if _current_scope_conflict(
            truth=truth,
            rows=current_rows,
            scoped_positions=scoped_positions,
            terminal_superseded_rows=terminal_superseded_rows,
        ):
            return TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE
        if any(row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value for row in current_rows):
            return TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED
        if int(preflight["hard_failure_count"]) > 0:
            return TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED
        return TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE
    if mode == TrackBDiagnosticsMode.HISTORICAL_RECONSTRUCTION:
        if any(row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value for row in shadow.rows):
            return TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED
        return TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE
    if truth.classification == TRUTH_CONFLICT_REVIEW_REQUIRED or _full_artifact_conflict(shadow):
        return TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE
    if any(row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value for row in shadow.rows):
        return TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED
    if int(preflight["hard_failure_count"]) > 0:
        return TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED
    return TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE


def _reason_codes(
    *,
    mode: TrackBDiagnosticsMode,
    truth: TrackBTruthSnapshot,
    shadow: TradeRegistryShadowReport,
    current_rows: tuple[Any, ...],
    scoped_positions: Mapping[str, Any],
    preflight: Mapping[str, Any],
) -> tuple[str, ...]:
    reasons: list[str] = list(truth.reason_codes)
    if _truth_has_stale_authority(truth):
        reasons.append("TRUTH_AUTHORITY_STALE")
    if preflight["stale"]:
        reasons.append("LIFECYCLE_STRESS_PREFLIGHT_STALE")
    if int(preflight["hard_failure_count"]) > 0:
        reasons.append("LIFECYCLE_STRESS_PREFLIGHT_HARD_FAILURE")
    if scoped_positions["unknown_scope_positions"]:
        reasons.append("UNKNOWN_SCOPE_BROKER_POSITION")
    if any(row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value for row in current_rows):
        reasons.append("CURRENT_SCOPE_REGISTRY_REVIEW_REQUIRED_TRADE")
    if any(row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value for row in shadow.rows):
        reasons.append("HISTORICAL_REGISTRY_REVIEW_REQUIRED_TRADE")
    if mode == TrackBDiagnosticsMode.CURRENT_HOT_PATH:
        if any(not row.registry_agrees_with_reconciliation for row in current_rows):
            reasons.append("CURRENT_SCOPE_REGISTRY_RECONCILIATION_DISAGREEMENT")
    elif any(not row.registry_agrees_with_reconciliation for row in shadow.rows):
        reasons.append("REGISTRY_RECONCILIATION_DISAGREEMENT")
    return tuple(dict.fromkeys(reasons))


def _truth_has_stale_authority(truth: TrackBTruthSnapshot) -> bool:
    sources: tuple[TrackBTruthSource, ...] = (
        truth.runtime.source,
        truth.broker_truth.source,
        truth.lifecycle.source,
        truth.reconciliation.source,
        truth.safe_state.source,
        truth.control_plane.source,
    )
    return any(not source.fresh for source in sources)


def _broker_positions_by_scope(truth: TrackBTruthSnapshot) -> Mapping[str, Any]:
    lifecycle_keys = (
        {_position_key(row) for row in truth.lifecycle.open_positions}
        if truth.lifecycle.fresh and truth.reconciliation.fresh
        else set()
    )
    track_b: list[Mapping[str, Any]] = []
    unrelated: list[Mapping[str, Any]] = []
    unknown: list[Mapping[str, Any]] = []
    for position in truth.broker_truth.positions:
        row = dict(position)
        scope = str(row.get("track_b_scope") or row.get("scope") or "").strip().upper()
        key = _position_key(row)
        symbol = _symbol(row)
        if scope in {"TRACK_B", "TRACK_B_MANAGED", "MANAGED"} or any(_keys_match(key, lifecycle_key) for lifecycle_key in lifecycle_keys):
            track_b.append(row)
        elif scope in {"UNRELATED", "OUT_OF_SCOPE", "NON_TRACK_B"} or symbol not in TRACK_B_FUTURES_SYMBOLS:
            unrelated.append(row)
        else:
            unknown.append(row)
    return {
        "track_b_managed_futures_positions": track_b,
        "unrelated_broker_positions": unrelated,
        "unknown_scope_positions": unknown,
    }


def _current_scope_shadow_rows(
    *,
    truth: TrackBTruthSnapshot,
    shadow: TradeRegistryShadowReport,
    scoped_positions: Mapping[str, Any],
) -> tuple[Any, ...]:
    current_keys = {
        _position_key(row)
        for bucket in (
            scoped_positions["track_b_managed_futures_positions"],
            scoped_positions["unknown_scope_positions"],
            truth.broker_truth.open_orders,
            truth.lifecycle.open_positions if truth.lifecycle.fresh and truth.reconciliation.fresh else (),
        )
        for row in bucket
    }
    current_trade_ids = _current_trade_ids_from_reconciliation_source(truth)
    if current_trade_ids:
        current_id_rows = tuple(row for row in shadow.rows if getattr(row, "trade_id", None) in current_trade_ids)
        if current_id_rows:
            return current_id_rows
    if not current_keys:
        return ()
    return tuple(row for row in shadow.rows if any(_keys_match(_row_key(row), current_key) for current_key in current_keys))


def _current_trade_ids_from_reconciliation_source(truth: TrackBTruthSnapshot) -> set[str]:
    path_text = str(truth.reconciliation.source.artifact_path or "")
    if not path_text:
        return set()
    path = Path(path_text)
    if not path.is_absolute():
        path = Path.cwd() / path
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    registry = payload.get("registry_reconciliation")
    if not isinstance(registry, Mapping):
        return set()
    broker_count = _int_from_preferred_mapping(
        primary=registry,
        primary_key="broker_position_count",
        fallback=payload,
        fallback_key="track_b_broker_position_count",
    )
    lifecycle_count = _int_from_preferred_mapping(
        primary=registry,
        primary_key="lifecycle_position_count",
        fallback=payload,
        fallback_key="lifecycle_open_position_count",
    )
    order_count = _int_from_preferred_mapping(
        primary=registry,
        primary_key="broker_open_order_count",
        fallback=payload,
        fallback_key="track_b_broker_open_order_count",
    )
    blocking = registry.get("blocking") is True
    current_ids: set[str] = set()
    if blocking:
        current_ids.update(str(trade_id) for trade_id in registry.get("review_required_trade_ids") or [] if str(trade_id or "").strip())
    if broker_count or lifecycle_count or order_count:
        current_ids.update(str(trade_id) for trade_id in registry.get("mapped_trade_ids") or [] if str(trade_id or "").strip())
    return current_ids


def _filter_reconciliation_authorized_owner_rows(
    *,
    truth: TrackBTruthSnapshot,
    rows: tuple[Any, ...],
) -> tuple[tuple[Any, ...], tuple[Mapping[str, Any], ...]]:
    owner_trade_ids = _reconciliation_authorized_owner_trade_ids(truth)
    if not owner_trade_ids:
        return rows, ()
    kept: list[Any] = []
    suppressed: list[Mapping[str, Any]] = []
    for row in rows:
        trade_id = str(getattr(row, "trade_id", "") or "").strip()
        if (
            trade_id in owner_trade_ids
            and getattr(row, "current_derived_state", "") == TradeCurrentState.REVIEW_REQUIRED.value
            and getattr(row, "registry_agrees_with_reconciliation", True) is False
        ):
            suppressed.append(
                {
                    "classification": "CANONICAL_OWNER_RECONCILIATION_SUPPRESSED_REVIEW_ROW",
                    "full_audit_only": True,
                    "reason_codes": [
                        "RECONCILIATION_MATCHED_CURRENT_OWNER",
                        "NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED",
                    ],
                    "trade_id": trade_id,
                    "lifecycle_id": getattr(row, "lifecycle_id", None),
                    "current_derived_state": getattr(row, "current_derived_state", None),
                    "registry_agrees_with_reconciliation": getattr(row, "registry_agrees_with_reconciliation", None),
                }
            )
            continue
        kept.append(row)
    return tuple(kept), tuple(suppressed)


def _reconciliation_authorized_owner_trade_ids(truth: TrackBTruthSnapshot) -> set[str]:
    payload = _reconciliation_payload(truth)
    if not payload:
        return set()
    registry = payload.get("registry_reconciliation")
    if not isinstance(registry, Mapping):
        return set()
    if str(registry.get("classification") or "") != "REGISTRY_RECONCILIATION_MATCHED":
        return set()
    if registry.get("blocking") is True:
        return set()
    current_review_count = _int_from_preferred_mapping(
        primary=payload,
        primary_key="current_scope_review_required_count",
        fallback=payload,
        fallback_key="review_required_count",
    )
    if current_review_count != 0:
        return set()
    mapped_trade_ids = {str(item).strip() for item in registry.get("mapped_trade_ids") or [] if str(item or "").strip()}
    owner_resolution = payload.get("current_exposure_owner_resolution")
    if not isinstance(owner_resolution, Mapping):
        return set()
    owner_trade_ids: set[str] = set()
    for exposure in owner_resolution.get("owned_exposures") or []:
        if not isinstance(exposure, Mapping):
            continue
        reason_codes = {str(code or "") for code in exposure.get("reason_codes") or []}
        if "NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED" not in reason_codes:
            continue
        trade_id = str(exposure.get("trade_id") or "").strip()
        if trade_id and (not mapped_trade_ids or trade_id in mapped_trade_ids):
            owner_trade_ids.add(trade_id)
    return owner_trade_ids


def _reconciliation_payload(truth: TrackBTruthSnapshot) -> Mapping[str, Any]:
    path_text = str(truth.reconciliation.source.artifact_path or "")
    if not path_text:
        return {}
    path = Path(path_text)
    if not path.is_absolute():
        path = Path.cwd() / path
    return _read_json(path)


def _int_from_preferred_mapping(
    *,
    primary: Mapping[str, Any],
    primary_key: str,
    fallback: Mapping[str, Any],
    fallback_key: str,
) -> int:
    if primary_key in primary:
        return int(primary.get(primary_key) or 0)
    return int(fallback.get(fallback_key) or 0)


def _review_required_rows_for_mode(
    mode: TrackBDiagnosticsMode,
    shadow: TradeRegistryShadowReport,
    current_rows: tuple[Any, ...],
) -> tuple[Any, ...]:
    rows = current_rows if mode == TrackBDiagnosticsMode.CURRENT_HOT_PATH else shadow.rows
    return tuple(row for row in rows if row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value)


def _trade_state_surface(row: Any) -> Mapping[str, Any]:
    payload = row.to_dict()
    return {
        "trade_id": payload.get("trade_id"),
        "lifecycle_id": payload.get("lifecycle_id"),
        "lane_id": payload.get("lane_id"),
        "strategy_id": payload.get("thesis_strategy_id"),
        "symbol": payload.get("symbol"),
        "con_id": payload.get("con_id"),
        "local_symbol": payload.get("local_symbol"),
        "current_derived_state": payload.get("current_derived_state"),
        "broker_backed_entry": payload.get("broker_backed_entry"),
        "broker_backed_exit": payload.get("broker_backed_exit"),
        "registry_agrees_with_reconciliation": payload.get("registry_agrees_with_reconciliation"),
        "ambiguous_reconstruction": payload.get("ambiguous_reconstruction"),
        "reason_codes": payload.get("agreement_reason_codes") or payload.get("truth_reason_codes") or [],
    }


def _current_scope_conflict(
    *,
    truth: TrackBTruthSnapshot,
    rows: tuple[Any, ...],
    scoped_positions: Mapping[str, Any],
    terminal_superseded_rows: tuple[Mapping[str, Any], ...] = (),
) -> bool:
    if scoped_positions["unknown_scope_positions"]:
        return True
    effective_lifecycle_open_count = max(
        0,
        int(truth.lifecycle.open_position_count) - len(terminal_superseded_rows),
    )
    has_current_exposure_or_order = bool(
        scoped_positions["track_b_managed_futures_positions"]
        or scoped_positions["unknown_scope_positions"]
        or truth.broker_truth.open_order_count
        or effective_lifecycle_open_count
        or rows
    )
    if truth.classification == TRUTH_CONFLICT_REVIEW_REQUIRED and (
        has_current_exposure_or_order
    ):
        return True
    return any(
        not row.registry_agrees_with_reconciliation
        and row.current_derived_state != TradeCurrentState.REVIEW_REQUIRED.value
        for row in rows
    )


def _full_artifact_conflict(shadow: TradeRegistryShadowReport) -> bool:
    return any(
        not row.registry_agrees_with_reconciliation
        and row.current_derived_state != TradeCurrentState.REVIEW_REQUIRED.value
        for row in shadow.rows
    )


def _position_key(row: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    return (
        _text(row.get("con_id") or row.get("conId") or row.get("contract_id")),
        _text(row.get("local_symbol") or row.get("localSymbol")),
        _symbol(row),
    )


def _row_key(row: Any) -> tuple[str | None, str | None, str | None]:
    return (_text(getattr(row, "con_id", None)), _text(getattr(row, "local_symbol", None)), _text(getattr(row, "symbol", None)))


def _keys_match(
    left: tuple[str | None, str | None, str | None],
    right: tuple[str | None, str | None, str | None],
) -> bool:
    left_con, left_local, left_symbol = left
    right_con, right_local, right_symbol = right
    if left_con and right_con:
        return left_con == right_con
    if left_local and right_local:
        return left_local == right_local
    if left_symbol and right_symbol:
        return left_symbol == right_symbol
    return False


def _symbol(row: Mapping[str, Any]) -> str | None:
    text = _text(row.get("symbol") or row.get("root_symbol") or row.get("underlying"))
    return text.upper() if text else None


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _preflight_state(*, path: Path, now: datetime, max_age_seconds: float) -> Mapping[str, Any]:
    payload = _read_json(path)
    generated_at = _parse_datetime(payload.get("generated_at"))
    age = (now - generated_at).total_seconds() if generated_at is not None else float("inf")
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    hard_failure_count = int(summary.get("hard_failure_stage_count") or 0)
    hard_failure_count += int(summary.get("unexpected_invariant_failures") or 0)
    hard_failure_count += int(summary.get("reducer_crashes") or 0)
    hard_failure_count += int(summary.get("trade_id_collisions") or 0)
    hard_failure_count += int(summary.get("silent_ambiguity_merges") or 0)
    hard_failure_count += int(summary.get("impossible_states") or 0)
    hard_failure_count += int(summary.get("bad_lifecycles_without_reason_codes") or 0)
    hard_failure_count += int(summary.get("broker_backed_evidence_violations") or 0)
    stale = not payload or generated_at is None or age > max_age_seconds
    return {
        "source_path": str(path),
        "generated_at": generated_at,
        "age_seconds": age,
        "stale": stale,
        "hard_failure_count": hard_failure_count,
    }


def _registry_state_counts(reconstruction: TradeRegistryReconstructionReport) -> Mapping[str, int]:
    counts: dict[str, int] = {}
    for record in reconstruction.records:
        counts[record.current_state.value] = counts.get(record.current_state.value, 0) + 1
    return counts


def _registry_state_counts_for_mode(
    *,
    mode: TrackBDiagnosticsMode,
    reconstruction: TradeRegistryReconstructionReport,
    current_rows: tuple[Any, ...],
) -> Mapping[str, int]:
    if mode != TrackBDiagnosticsMode.CURRENT_HOT_PATH:
        return _registry_state_counts(reconstruction)
    counts: dict[str, int] = {}
    for row in current_rows:
        state = str(getattr(row, "current_derived_state", "") or "")
        if state:
            counts[state] = counts.get(state, 0) + 1
    return counts


def _conflict_summary(conflict: Any) -> Mapping[str, Any]:
    return {
        "classification": conflict.classification,
        "question": conflict.question,
        "reason_codes": list(conflict.reason_codes),
        "review_required": conflict.review_required,
    }


def _truth_surface(truth: TrackBTruthSnapshot) -> Mapping[str, Any]:
    return {
        "snapshot_id": truth.snapshot_id,
        "classification": truth.classification,
        "runtime_ready": truth.runtime.submit_capable,
        "runtime_alive": truth.runtime.runtime_alive,
        "recovery_active": truth.recovery.active,
        "broker_truth_fresh": truth.broker_truth.fresh,
        "broker_open_order_count": truth.broker_truth.open_order_count,
        "broker_position_count": truth.broker_truth.broker_position_count,
        "lifecycle_fresh": truth.lifecycle.fresh,
        "lifecycle_open_position_count": truth.lifecycle.open_position_count,
        "reconciliation_fresh": truth.reconciliation.fresh,
        "reconciled": truth.reconciliation.reconciled,
        "safe_state_submit_allowed": truth.safe_state.submit_allowed,
        "control_plane_fresh": truth.control_plane.fresh,
    }


def _reconstruction_summary(reconstruction: TradeRegistryReconstructionReport) -> Mapping[str, Any]:
    payload = reconstruction.to_dict()
    return {
        "record_count": payload["summary"]["record_count"],
        "review_required_count": payload["summary"]["review_required_count"],
        "closed_flat_count": payload["summary"]["closed_flat_count"],
        "cancelled_count": payload["summary"]["cancelled_count"],
        "missing_link_count": len(reconstruction.missing_links),
        "ambiguity_reason_codes": list(reconstruction.ambiguity_reason_codes),
    }


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
