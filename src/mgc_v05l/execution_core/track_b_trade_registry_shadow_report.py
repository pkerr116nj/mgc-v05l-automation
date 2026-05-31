"""Read-only Track B trade registry shadow report.

The shadow report compares reconstructed trade registry state against the
canonical truth snapshot and current broker/lifecycle reconciliation. It is a
diagnostic artifact only and is not wired into submit, runtime, reconciliation,
or broker authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    TrackBTruthSnapshot,
    TrackBTruthSnapshotConfig,
    build_track_b_truth_snapshot,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeRegistryRecord,
)
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import (
    DEFAULT_RECONSTRUCTION_REPORT,
    TradeRegistryReconstructionConfig,
    TradeRegistryReconstructionReport,
    reconstruct_trade_registry_from_artifacts,
)


SCHEMA_VERSION = "track_b_trade_registry_shadow_report_v1"
DEFAULT_SHADOW_REPORT_PATH = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "latest_shadow_report.json"
)


@dataclass(frozen=True)
class TradeRegistryShadowReportConfig:
    repo_root: Path
    output_path: Path = DEFAULT_SHADOW_REPORT_PATH
    reconstruction_config: TradeRegistryReconstructionConfig | None = None
    truth_config: TrackBTruthSnapshotConfig | None = None

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path

    def effective_reconstruction_config(self) -> TradeRegistryReconstructionConfig:
        if self.reconstruction_config is not None:
            return self.reconstruction_config
        return TradeRegistryReconstructionConfig(
            repo_root=self.repo_root,
            output_path=DEFAULT_RECONSTRUCTION_REPORT,
        )

    def effective_truth_config(self) -> TrackBTruthSnapshotConfig:
        if self.truth_config is not None:
            return self.truth_config
        return TrackBTruthSnapshotConfig(repo_root=self.repo_root)


@dataclass(frozen=True)
class TradeRegistryShadowRow:
    trade_id: str
    lifecycle_id: str | None
    lane_id: str | None
    thesis_strategy_id: str | None
    symbol: str | None
    con_id: int | None
    local_symbol: str | None
    event_chain_summary: tuple[Mapping[str, Any], ...]
    current_derived_state: str
    broker_backed_entry: bool
    broker_backed_exit: bool
    reconciliation_status: str
    reconciliation_reconciled: bool
    truth_classification: str
    truth_conflicts: tuple[Mapping[str, Any], ...]
    truth_reason_codes: tuple[str, ...]
    missing_links: tuple[Mapping[str, Any], ...]
    ambiguous_reconstruction: bool
    registry_agrees_with_reconciliation: bool
    agreement_reason_codes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "lifecycle_id": self.lifecycle_id,
            "lane_id": self.lane_id,
            "thesis_strategy_id": self.thesis_strategy_id,
            "symbol": self.symbol,
            "con_id": self.con_id,
            "local_symbol": self.local_symbol,
            "event_chain_summary": list(self.event_chain_summary),
            "current_derived_state": self.current_derived_state,
            "broker_backed_entry": self.broker_backed_entry,
            "broker_backed_exit": self.broker_backed_exit,
            "reconciliation_status": self.reconciliation_status,
            "reconciliation_reconciled": self.reconciliation_reconciled,
            "truth_classification": self.truth_classification,
            "truth_conflicts": list(self.truth_conflicts),
            "truth_reason_codes": list(self.truth_reason_codes),
            "missing_links": list(self.missing_links),
            "ambiguous_reconstruction": self.ambiguous_reconstruction,
            "registry_agrees_with_reconciliation": self.registry_agrees_with_reconciliation,
            "agreement_reason_codes": list(self.agreement_reason_codes),
        }


@dataclass(frozen=True)
class TradeRegistryShadowReport:
    schema_version: str
    generated_at: datetime
    read_only: bool
    diagnostic_only: bool
    broker_mutation_allowed: bool
    runtime_restart_allowed: bool
    reconstruction_schema_version: str
    canonical_truth_snapshot_id: str
    canonical_truth_classification: str
    reconciliation_status: str
    reconciliation_reconciled: bool
    rows: tuple[TradeRegistryShadowRow, ...]
    source_paths: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "read_only": self.read_only,
            "diagnostic_only": self.diagnostic_only,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "runtime_restart_allowed": self.runtime_restart_allowed,
            "reconstruction_schema_version": self.reconstruction_schema_version,
            "canonical_truth_snapshot_id": self.canonical_truth_snapshot_id,
            "canonical_truth_classification": self.canonical_truth_classification,
            "reconciliation_status": self.reconciliation_status,
            "reconciliation_reconciled": self.reconciliation_reconciled,
            "rows": [row.to_dict() for row in self.rows],
            "source_paths": dict(self.source_paths),
            "summary": {
                "trade_count": len(self.rows),
                "review_required_count": sum(
                    1 for row in self.rows if row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value
                ),
                "ambiguous_reconstruction_count": sum(1 for row in self.rows if row.ambiguous_reconstruction),
                "registry_reconciliation_disagreement_count": sum(
                    1 for row in self.rows if not row.registry_agrees_with_reconciliation
                ),
            },
        }


def build_trade_registry_shadow_report(
    *,
    config: TradeRegistryShadowReportConfig,
    now: datetime | None = None,
) -> TradeRegistryShadowReport:
    generated_at = _ensure_utc(now or datetime.now(UTC))
    reconstruction_config = config.effective_reconstruction_config()
    truth_config = config.effective_truth_config()
    reconstruction = reconstruct_trade_registry_from_artifacts(config=reconstruction_config, now=generated_at)
    truth = build_track_b_truth_snapshot(config=truth_config, now=generated_at)
    reconciliation_payload = _read_json(truth_config.resolve(truth_config.reconciliation_path))
    reconciliation_status = str(
        reconciliation_payload.get("classification")
        or reconciliation_payload.get("reconciliation_status")
        or truth.reconciliation.classification
    )
    reconciliation_reconciled = bool(truth.reconciliation.reconciled)

    rows = tuple(
        _shadow_row(
            record=record,
            reconstruction=reconstruction,
            truth=truth,
            reconciliation_status=reconciliation_status,
            reconciliation_reconciled=reconciliation_reconciled,
        )
        for record in reconstruction.records
    )

    return TradeRegistryShadowReport(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        read_only=True,
        diagnostic_only=True,
        broker_mutation_allowed=False,
        runtime_restart_allowed=False,
        reconstruction_schema_version=reconstruction.schema_version,
        canonical_truth_snapshot_id=truth.snapshot_id,
        canonical_truth_classification=truth.classification,
        reconciliation_status=reconciliation_status,
        reconciliation_reconciled=reconciliation_reconciled,
        rows=rows,
        source_paths={
            "shadow_report": str(config.resolve(config.output_path)),
            "reconstruction_sources": ", ".join(reconstruction.source_paths),
            "canonical_truth_snapshot": str(truth_config.resolve(truth_config.output_path)),
            "reconciliation": str(truth_config.resolve(truth_config.reconciliation_path)),
        },
    )


def write_trade_registry_shadow_report(
    *,
    config: TradeRegistryShadowReportConfig,
    report: TradeRegistryShadowReport,
) -> Path:
    path = config.resolve(config.output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _shadow_row(
    *,
    record: TradeRegistryRecord,
    reconstruction: TradeRegistryReconstructionReport,
    truth: TrackBTruthSnapshot,
    reconciliation_status: str,
    reconciliation_reconciled: bool,
) -> TradeRegistryShadowRow:
    missing_links = tuple(item for item in reconstruction.missing_links if item.get("trade_id") == record.trade_id)
    ambiguous = bool(missing_links or record.current_state == TradeCurrentState.REVIEW_REQUIRED)
    agreement, reasons = _registry_reconciliation_agreement(
        record=record,
        reconciliation_reconciled=reconciliation_reconciled,
        truth=truth,
    )
    owner = record.ownership_identity
    first_event = record.event_chain[0] if record.event_chain else None
    return TradeRegistryShadowRow(
        trade_id=record.trade_id,
        lifecycle_id=(owner.lifecycle_id if owner else None) or _event_attr(first_event, "lifecycle_id"),
        lane_id=(owner.lane_id if owner else None) or _event_attr(first_event, "lane_id"),
        thesis_strategy_id=(owner.thesis_strategy_id if owner else None) or _event_attr(first_event, "thesis_strategy_id"),
        symbol=(owner.symbol if owner else None) or _event_attr(first_event, "symbol"),
        con_id=(owner.con_id if owner else None) or _event_attr(first_event, "con_id"),
        local_symbol=(owner.local_symbol if owner else None) or _event_attr(first_event, "local_symbol"),
        event_chain_summary=tuple(_event_summary(event) for event in record.event_chain),
        current_derived_state=record.current_state.value,
        broker_backed_entry=record.broker_backed_entry,
        broker_backed_exit=record.broker_backed_exit,
        reconciliation_status=reconciliation_status,
        reconciliation_reconciled=reconciliation_reconciled,
        truth_classification=truth.classification,
        truth_conflicts=tuple(_truth_conflict_summary(conflict) for conflict in truth.conflicts),
        truth_reason_codes=truth.reason_codes,
        missing_links=missing_links,
        ambiguous_reconstruction=ambiguous,
        registry_agrees_with_reconciliation=agreement,
        agreement_reason_codes=reasons,
    )


def _registry_reconciliation_agreement(
    *,
    record: TradeRegistryRecord,
    reconciliation_reconciled: bool,
    truth: TrackBTruthSnapshot,
) -> tuple[bool, tuple[str, ...]]:
    terminal_flat = record.current_state in {TradeCurrentState.CLOSED_FLAT, TradeCurrentState.CANCELLED}
    record_open = record.open_qty > Decimal("0") and record.current_state in {
        TradeCurrentState.OPEN_MANAGED,
        TradeCurrentState.EXIT_DUE,
        TradeCurrentState.WORKING_EXIT,
    }
    if record.current_state == TradeCurrentState.REVIEW_REQUIRED:
        return (not reconciliation_reconciled, ("REGISTRY_REVIEW_REQUIRED",))
    if reconciliation_reconciled and terminal_flat and truth.broker_truth.broker_position_count == 0:
        return True, ("REGISTRY_TERMINAL_FLAT_MATCHES_RECONCILIATION",)
    if reconciliation_reconciled and record_open and truth.broker_truth.broker_position_count > 0:
        return True, ("REGISTRY_OPEN_MATCHES_RECONCILIATION",)
    if not reconciliation_reconciled:
        return False, ("RECONCILIATION_NOT_CLEAN",)
    return False, ("REGISTRY_RECONCILIATION_STATE_MISMATCH",)


def _event_summary(event: TradeEvent) -> Mapping[str, Any]:
    return {
        "event_type": event.event_type.value,
        "generated_at": event.generated_at.isoformat(),
        "lifecycle_id": event.lifecycle_id,
        "order_id": event.order_id,
        "client_id": event.client_id,
        "perm_id": event.perm_id,
        "exec_id": event.exec_id,
        "reason_codes": list(event.reason_codes),
        "source_artifact_path": event.source_artifact_path,
    }


def _truth_conflict_summary(conflict: Any) -> Mapping[str, Any]:
    return {
        "classification": conflict.classification,
        "question": conflict.question,
        "authoritative_source": conflict.authoritative_source,
        "conflicting_sources": list(conflict.conflicting_sources),
        "reason_codes": list(conflict.reason_codes),
        "review_required": conflict.review_required,
    }


def _event_attr(event: TradeEvent | None, name: str) -> Any:
    return getattr(event, name) if event is not None else None


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, Mapping) else {}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
