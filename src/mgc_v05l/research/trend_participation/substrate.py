"""Durable ATP substrate bundles for reusable feature, candidate, and outcome layers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..platform import read_jsonl_dataset, register_duckdb_catalog, stable_hash, write_dataset_bundle, write_json_manifest
from .models import AtpEntryState, AtpTimingState, ConflictOutcome, FeatureState, TradeRecord
from .outcome_engine import generate_atp_trade_records, trade_records_to_retest_rows
from .phase2_continuation import atp_phase2_variant, classify_entry_states
from .phase3_timing import (
    ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    ATP_TIMING_ACTIVATION_ROLLING_5M,
    VWAP_FAVORABLE,
    classify_timing_states,
)

ATP_SUBSTRATE_ARTIFACT_VERSION = "atp_substrate_v1"
ATP_FEATURE_VERSION = "atp_feature_state_v1"
ATP_CANDIDATE_VERSION = "atp_candidate_state_v2"
ATP_OUTCOME_ENGINE_VERSION = "atp_outcome_engine_v2"


@dataclass(frozen=True)
class AtpFeatureBundle:
    bundle_id: str
    bundle_dir: Path
    manifest_path: Path
    dataset_paths: dict[str, str]
    feature_rows: list[FeatureState]


@dataclass(frozen=True)
class AtpScopeBundle:
    bundle_id: str
    bundle_dir: Path
    manifest_path: Path
    dataset_paths: dict[str, str]
    entry_states: list[AtpEntryState]
    timing_states: list[AtpTimingState]
    trades: list[TradeRecord]
    trade_rows: list[dict[str, Any]]


def ensure_atp_feature_bundle(
    *,
    bundle_root: Path,
    source_db: Path,
    symbol: str,
    selected_sources: dict[str, Any],
    start_timestamp: datetime,
    end_timestamp: datetime,
    feature_scope: str = "rolling_scope",
    feature_rows: Sequence[FeatureState] | None,
) -> AtpFeatureBundle:
    bundle_root = bundle_root.resolve()
    identity = {
        "artifact_version": ATP_SUBSTRATE_ARTIFACT_VERSION,
        "layer": "features",
        "source_db": str(source_db.resolve()),
        "symbol": symbol,
        "selected_sources": selected_sources,
        "start_timestamp": start_timestamp.isoformat(),
        "end_timestamp": end_timestamp.isoformat(),
        "feature_scope": feature_scope,
        "feature_version": ATP_FEATURE_VERSION,
    }
    bundle_id = stable_hash(identity)
    bundle_dir = bundle_root / "feature_bundles" / bundle_id
    manifest_path = bundle_dir / "manifest.json"
    dataset_name = "feature_states"
    if manifest_path.exists():
        manifest = _read_manifest(manifest_path)
        rows = read_jsonl_dataset(Path(manifest["datasets"][dataset_name]["jsonl_path"]))
        return AtpFeatureBundle(
            bundle_id=bundle_id,
            bundle_dir=bundle_dir,
            manifest_path=manifest_path,
            dataset_paths={name: spec["parquet_path"] for name, spec in manifest["datasets"].items()},
            feature_rows=[_feature_state_from_row(row) for row in rows],
        )

    if feature_rows is None:
        raise FileNotFoundError(f"ATP feature bundle not materialized yet: {bundle_dir}")

    dataset_spec = write_dataset_bundle(
        bundle_dir=bundle_dir / "datasets",
        dataset_name=dataset_name,
        rows=[_feature_state_row(feature) for feature in feature_rows],
    )
    duckdb_path = register_duckdb_catalog(
        duckdb_path=bundle_dir / "catalog.duckdb",
        view_to_parquet={dataset_name: Path(dataset_spec["parquet_path"])},
    )
    manifest = {
        "artifact_version": ATP_SUBSTRATE_ARTIFACT_VERSION,
        "bundle_type": "atp_feature_bundle",
        "bundle_id": bundle_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "source_db": str(source_db.resolve()),
        "symbol": symbol,
        "selected_sources": selected_sources,
        "feature_scope": feature_scope,
        "source_date_span": {
            "start_timestamp": start_timestamp.isoformat(),
            "end_timestamp": end_timestamp.isoformat(),
        },
        "versions": {
            "feature_version": ATP_FEATURE_VERSION,
        },
        "datasets": {
            dataset_name: dataset_spec,
        },
        "duckdb_catalog_path": str(duckdb_path.resolve()),
    }
    write_json_manifest(manifest_path, manifest)
    return AtpFeatureBundle(
        bundle_id=bundle_id,
        bundle_dir=bundle_dir,
        manifest_path=manifest_path,
        dataset_paths={dataset_name: dataset_spec["parquet_path"]},
        feature_rows=list(feature_rows),
    )


def ensure_atp_scope_bundle(
    *,
    bundle_root: Path,
    source_db: Path,
    symbol: str,
    selected_sources: dict[str, Any],
    start_timestamp: datetime,
    end_timestamp: datetime,
    allowed_sessions: tuple[str, ...],
    point_value: float,
    bars_1m: Sequence[Any],
    feature_bundle: AtpFeatureBundle,
    entry_activation_basis: str = ATP_TIMING_ACTIVATION_ROLLING_5M,
    quality_bucket_policy: str | None = None,
    allow_pre_5m_context_participation: bool = False,
    sides: tuple[str, ...] = ("LONG",),
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    variant_overrides: Mapping[str, Any] | None = None,
) -> AtpScopeBundle:
    bundle_root = bundle_root.resolve()
    identity = {
        "artifact_version": ATP_SUBSTRATE_ARTIFACT_VERSION,
        "layer": "scope",
        "source_db": str(source_db.resolve()),
        "symbol": symbol,
        "selected_sources": selected_sources,
        "start_timestamp": start_timestamp.isoformat(),
        "end_timestamp": end_timestamp.isoformat(),
        "allowed_sessions": list(allowed_sessions),
        "point_value": point_value,
        "feature_bundle_id": feature_bundle.bundle_id,
        "candidate_version": ATP_CANDIDATE_VERSION,
        "outcome_engine_version": ATP_OUTCOME_ENGINE_VERSION,
        "entry_activation_basis": entry_activation_basis,
        "quality_bucket_policy": quality_bucket_policy,
        "allow_pre_5m_context_participation": allow_pre_5m_context_participation,
        "sides": list(sides),
        "exit_policy": exit_policy,
        "variant_overrides": dict(variant_overrides or {}),
    }
    bundle_id = stable_hash(identity)
    bundle_dir = bundle_root / "scope_bundles" / bundle_id
    manifest_path = bundle_dir / "manifest.json"
    if manifest_path.exists():
        manifest = _read_manifest(manifest_path)
        datasets = manifest["datasets"]
        entry_rows = read_jsonl_dataset(Path(datasets["entry_states"]["jsonl_path"]))
        timing_rows = read_jsonl_dataset(Path(datasets["timing_states"]["jsonl_path"]))
        trade_rows = read_jsonl_dataset(Path(datasets["trade_records"]["jsonl_path"]))
        timing_states = [_timing_state_from_row(row) for row in timing_rows]
        trades = [_trade_record_from_row(row) for row in trade_rows]
        return AtpScopeBundle(
            bundle_id=bundle_id,
            bundle_dir=bundle_dir,
            manifest_path=manifest_path,
            dataset_paths={name: spec["parquet_path"] for name, spec in datasets.items()},
            entry_states=[_entry_state_from_row(row) for row in entry_rows],
            timing_states=timing_states,
            trades=trades,
            trade_rows=trade_records_to_retest_rows(
                trades,
                timing_states_by_decision_id={
                    _timing_state_decision_id(state, variant_overrides=variant_overrides): state
                    for state in timing_states
                },
            ),
        )

    normalized_sides = tuple(
        side
        for side in dict.fromkeys(str(candidate or "LONG").strip().upper() for candidate in sides)
        if side in {"LONG", "SHORT"}
    ) or ("LONG",)
    entry_states: list[AtpEntryState] = []
    timing_states: list[AtpTimingState] = []
    for side in normalized_sides:
        side_entry_states = classify_entry_states(
            feature_rows=feature_bundle.feature_rows,
            allowed_sessions=frozenset(allowed_sessions),
            side=side,
            variant_overrides=variant_overrides,
        )
        side_timing_states = classify_timing_states(
            entry_states=side_entry_states,
            bars_1m=bars_1m,
            entry_activation_basis=entry_activation_basis,
            allow_pre_5m_context_participation=allow_pre_5m_context_participation,
            variant_overrides=variant_overrides,
        )
        entry_states.extend(side_entry_states)
        timing_states.extend(side_timing_states)
    entry_states = sorted(entry_states, key=lambda state: (state.instrument, state.decision_ts, state.side, state.family_name))
    timing_states = sorted(timing_states, key=lambda state: (state.instrument, state.decision_ts, state.side, state.family_name))
    timing_states = _filter_timing_states_for_quality_policy(
        timing_states=timing_states,
        quality_bucket_policy=quality_bucket_policy,
    )
    trades = generate_atp_trade_records(
        timing_states=timing_states,
        bars_1m=bars_1m,
        point_value=point_value,
        feature_rows=feature_bundle.feature_rows,
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
    )

    datasets_dir = bundle_dir / "datasets"
    entry_spec = write_dataset_bundle(
        bundle_dir=datasets_dir,
        dataset_name="entry_states",
        rows=[_entry_state_row(state) for state in entry_states],
    )
    timing_spec = write_dataset_bundle(
        bundle_dir=datasets_dir,
        dataset_name="timing_states",
        rows=[_timing_state_row(state) for state in timing_states],
    )
    trade_spec = write_dataset_bundle(
        bundle_dir=datasets_dir,
        dataset_name="trade_records",
        rows=[_trade_record_row(trade) for trade in trades],
    )
    duckdb_path = register_duckdb_catalog(
        duckdb_path=bundle_dir / "catalog.duckdb",
        view_to_parquet={
            "entry_states": Path(entry_spec["parquet_path"]),
            "timing_states": Path(timing_spec["parquet_path"]),
            "trade_records": Path(trade_spec["parquet_path"]),
        },
    )
    manifest = {
        "artifact_version": ATP_SUBSTRATE_ARTIFACT_VERSION,
        "bundle_type": "atp_scope_bundle",
        "bundle_id": bundle_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "source_db": str(source_db.resolve()),
        "symbol": symbol,
        "selected_sources": selected_sources,
        "source_date_span": {
            "start_timestamp": start_timestamp.isoformat(),
            "end_timestamp": end_timestamp.isoformat(),
        },
        "allowed_sessions": list(allowed_sessions),
        "point_value": point_value,
        "feature_bundle_id": feature_bundle.bundle_id,
        "versions": {
            "feature_version": ATP_FEATURE_VERSION,
            "candidate_version": ATP_CANDIDATE_VERSION,
            "outcome_engine_version": ATP_OUTCOME_ENGINE_VERSION,
        },
        "entry_activation_basis": entry_activation_basis,
        "quality_bucket_policy": quality_bucket_policy,
        "allow_pre_5m_context_participation": allow_pre_5m_context_participation,
        "exit_policy": exit_policy,
        "variant_overrides": dict(variant_overrides or {}),
        "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
        "datasets": {
            "entry_states": entry_spec,
            "timing_states": timing_spec,
            "trade_records": trade_spec,
        },
        "duckdb_catalog_path": str(duckdb_path.resolve()),
    }
    write_json_manifest(manifest_path, manifest)
    return AtpScopeBundle(
        bundle_id=bundle_id,
        bundle_dir=bundle_dir,
        manifest_path=manifest_path,
        dataset_paths={
            "entry_states": entry_spec["parquet_path"],
            "timing_states": timing_spec["parquet_path"],
            "trade_records": trade_spec["parquet_path"],
        },
        entry_states=entry_states,
        timing_states=timing_states,
        trades=trades,
        trade_rows=trade_records_to_retest_rows(
            trades,
            timing_states_by_decision_id={
                _timing_state_decision_id(state, variant_overrides=variant_overrides): state
                for state in timing_states
            },
        ),
    )


def _filter_timing_states_for_quality_policy(
    *,
    timing_states: Sequence[AtpTimingState],
    quality_bucket_policy: str | None,
) -> list[AtpTimingState]:
    policy = str(quality_bucket_policy or "").strip().upper()
    if not policy:
        return list(timing_states)
    if policy == "MEDIUM_HIGH_ONLY":
        return [
            state
            for state in timing_states
            if str((state.feature_snapshot or {}).get("setup_quality_bucket") or "") in {"MEDIUM", "HIGH"}
        ]
    if policy == "HIGH_ONLY":
        return [
            state
            for state in timing_states
            if str((state.feature_snapshot or {}).get("setup_quality_bucket") or "") == "HIGH"
        ]
    if policy == "VWAP_FAVORABLE_ONLY":
        return [state for state in timing_states if state.vwap_price_quality_state == VWAP_FAVORABLE]
    return list(timing_states)


def _feature_state_row(feature: FeatureState) -> dict[str, Any]:
    row = dict(feature.__dict__)
    row["session_date"] = feature.session_date.isoformat()
    row["decision_ts"] = feature.decision_ts.isoformat()
    return row


def _entry_state_row(state: AtpEntryState) -> dict[str, Any]:
    row = dict(state.__dict__)
    row["decision_ts"] = state.decision_ts.isoformat()
    row["session_date"] = state.session_date.isoformat()
    return row


def _timing_state_row(state: AtpTimingState) -> dict[str, Any]:
    row = dict(state.__dict__)
    row["decision_ts"] = state.decision_ts.isoformat()
    row["session_date"] = state.session_date.isoformat()
    row["timing_bar_ts"] = state.timing_bar_ts.isoformat() if state.timing_bar_ts else None
    row["entry_ts"] = state.entry_ts.isoformat() if state.entry_ts else None
    return row


def _trade_record_row(trade: TradeRecord) -> dict[str, Any]:
    row = dict(trade.__dict__)
    row["conflict_outcome"] = trade.conflict_outcome.value
    row["decision_ts"] = trade.decision_ts.isoformat()
    row["entry_ts"] = trade.entry_ts.isoformat()
    row["exit_ts"] = trade.exit_ts.isoformat()
    return row


def _feature_state_from_row(row: dict[str, Any]) -> FeatureState:
    payload = dict(row)
    payload["decision_ts"] = datetime.fromisoformat(str(payload["decision_ts"]))
    payload["session_date"] = date.fromisoformat(str(payload["session_date"]))
    for key in ("atp_bias_reasons", "atp_long_bias_blockers", "atp_short_bias_blockers"):
        payload[key] = tuple(payload.get(key) or [])
    return FeatureState(**payload)


def _entry_state_from_row(row: dict[str, Any]) -> AtpEntryState:
    payload = dict(row)
    payload["decision_ts"] = datetime.fromisoformat(str(payload["decision_ts"]))
    payload["session_date"] = date.fromisoformat(str(payload["session_date"]))
    payload["blocker_codes"] = tuple(payload.get("blocker_codes") or [])
    return AtpEntryState(**payload)


def _timing_state_from_row(row: dict[str, Any]) -> AtpTimingState:
    payload = dict(row)
    payload["decision_ts"] = datetime.fromisoformat(str(payload["decision_ts"]))
    payload["session_date"] = date.fromisoformat(str(payload["session_date"]))
    payload["blocker_codes"] = tuple(payload.get("blocker_codes") or [])
    payload["timing_bar_ts"] = (
        datetime.fromisoformat(str(payload["timing_bar_ts"])) if payload.get("timing_bar_ts") else None
    )
    payload["entry_ts"] = datetime.fromisoformat(str(payload["entry_ts"])) if payload.get("entry_ts") else None
    return AtpTimingState(**payload)


def _trade_record_from_row(row: dict[str, Any]) -> TradeRecord:
    payload = dict(row)
    payload["conflict_outcome"] = ConflictOutcome(str(payload["conflict_outcome"]))
    for key in ("decision_ts", "entry_ts", "exit_ts"):
        payload[key] = datetime.fromisoformat(str(payload[key]))
    return TradeRecord(**payload)


def _read_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _timing_state_decision_id(
    state: AtpTimingState,
    *,
    variant_overrides: Mapping[str, Any] | None = None,
) -> str:
    return (
        f"{state.instrument}|"
        f"{atp_phase2_variant(state.side, variant_overrides=variant_overrides).variant_id}|"
        f"{state.decision_ts.isoformat()}"
    )
