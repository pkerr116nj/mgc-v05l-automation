"""ATP-local helpers for standardized research-platform registry registration."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..research.platform import build_registry_run_row, current_code_version, register_experiment_run, stable_hash

DEFAULT_EXPERIMENT_REGISTRY_ROOT = Path("outputs/research_platform/registry")


def _artifact_payload(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value.resolve())
    return value


def register_atp_report_output(
    *,
    strategy_variant: str,
    payload_json_path: Path,
    artifacts: Mapping[str, Any],
    registry_root: Path = DEFAULT_EXPERIMENT_REGISTRY_ROOT,
) -> dict[str, Any]:
    payload = json.loads(payload_json_path.read_text(encoding="utf-8"))
    date_span = _extract_date_span(payload)
    summary_metrics = _extract_summary_metrics(payload)
    target_rows = _extract_target_rows(strategy_variant=strategy_variant, payload=payload, artifacts=artifacts)
    run_row = build_registry_run_row(
        strategy_family="atp_companion",
        strategy_variant=strategy_variant,
        date_span=date_span,
        code_version=current_code_version(cwd=Path.cwd()),
        data_version=_extract_data_version(payload),
        feature_version=_extract_version(payload, ("manifest", "feature_version")),
        candidate_version=_extract_version(payload, ("manifest", "candidate_version")),
        outcome_engine_version=_extract_version(payload, ("manifest", "outcome_engine_version")),
        config_hash=_extract_hash(payload, "config_hash"),
        control_hash=_extract_hash(payload, "control_hash"),
        target_hash=_extract_target_hash(payload),
        artifacts={key: _artifact_payload(value) for key, value in artifacts.items()},
        summary_metrics=summary_metrics,
        lineage={
            "study": payload.get("study"),
            "registered_from": str(payload_json_path.resolve()),
        },
        generated_at=_extract_generated_at(payload),
    )
    return register_experiment_run(
        registry_root=registry_root.resolve(),
        run_row=run_row,
        target_rows=target_rows,
    )


def _extract_date_span(payload: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("methodology"), Mapping):
        span = payload["methodology"].get("shared_date_span")
        if isinstance(span, Mapping):
            return dict(span)
    if isinstance(payload.get("manifest"), Mapping):
        span = payload["manifest"].get("source_date_span")
        if isinstance(span, Mapping):
            return dict(span)
    if isinstance(payload.get("shared_date_span"), Mapping):
        return dict(payload["shared_date_span"])
    return {
        "start_timestamp": None,
        "end_timestamp": None,
    }


def _extract_generated_at(payload: Mapping[str, Any]) -> str:
    if payload.get("generated_at"):
        return str(payload["generated_at"])
    manifest = payload.get("manifest")
    if isinstance(manifest, Mapping) and manifest.get("generated_at"):
        return str(manifest["generated_at"])
    return datetime.now(UTC).isoformat()


def _extract_hash(payload: Mapping[str, Any], key: str) -> str | None:
    if key in payload and payload.get(key):
        return str(payload[key])
    manifest = payload.get("manifest")
    if isinstance(manifest, Mapping) and manifest.get(key):
        return str(manifest[key])
    return None


def _extract_version(payload: Mapping[str, Any], path: Sequence[str]) -> str | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    if current in (None, ""):
        return None
    return str(current)


def _extract_target_hash(payload: Mapping[str, Any]) -> str | None:
    manifest = payload.get("manifest")
    if isinstance(manifest, Mapping):
        target_hashes = manifest.get("target_hashes")
        if isinstance(target_hashes, Mapping) and target_hashes:
            return stable_hash(dict(target_hashes), length=24)
    return None


def _extract_summary_metrics(payload: Mapping[str, Any]) -> dict[str, Any]:
    ranking = payload.get("ranking")
    if isinstance(ranking, Sequence) and ranking:
        top = ranking[0]
        if isinstance(top, Mapping):
            metrics = top.get("metrics")
            if isinstance(metrics, Mapping):
                return dict(metrics)
    return {}


def _extract_data_version(payload: Mapping[str, Any]) -> str | None:
    methodology = payload.get("methodology")
    if isinstance(methodology, Mapping):
        span = methodology.get("shared_date_span")
        if isinstance(span, Mapping):
            return stable_hash(dict(span), length=24)
    manifest = payload.get("manifest")
    if isinstance(manifest, Mapping):
        span = manifest.get("source_date_span")
        if isinstance(span, Mapping):
            return stable_hash(dict(span), length=24)
    return None


def _extract_target_rows(
    *,
    strategy_variant: str,
    payload: Mapping[str, Any],
    artifacts: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = _matrix_rows(payload)
    manifest = payload.get("manifest")
    target_hashes = manifest.get("target_hashes") if isinstance(manifest, Mapping) else {}
    config_hash = _extract_hash(payload, "config_hash")
    control_hash = _extract_hash(payload, "control_hash")
    generated_at = _extract_generated_at(payload)
    target_entries: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        target_id = str(row.get("target_id") or row.get("strategy_id") or "unknown")
        variant_suffix = str(
            row.get("control_id")
            or row.get("package_id")
            or row.get("case_id")
            or row.get("study_id")
            or row.get("label")
            or "summary"
        )
        target_entries.append(
            {
                "strategy_family": "atp_companion",
                "strategy_variant": f"{strategy_variant}::{target_id}::{variant_suffix}",
                "target_id": target_id,
                "label": row.get("label") or row.get("control_label") or row.get("package_label") or variant_suffix,
                "symbol": row.get("symbol"),
                "allowed_sessions": row.get("sessions") or row.get("allowed_sessions"),
                "record_kind": "experiment_row",
                "analytics_publish": False,
                "config_hash": _nested_config_hash(row) or config_hash,
                "control_hash": control_hash or stable_hash({"variant_suffix": variant_suffix}, length=24),
                "target_hash": (
                    str(target_hashes.get(target_id))
                    if isinstance(target_hashes, Mapping) and target_hashes.get(target_id) is not None
                    else stable_hash({"target_id": target_id}, length=24)
                ),
                "summary_metrics": dict(row.get("metrics") or {}),
                "artifacts": {key: _artifact_payload(value) for key, value in artifacts.items()},
                "generated_at": generated_at,
            }
        )
    return target_entries


def _nested_config_hash(row: Mapping[str, Any]) -> str | None:
    config = row.get("config")
    if isinstance(config, Mapping) and config.get("config_hash"):
        return str(config["config_hash"])
    return None


def _matrix_rows(payload: Mapping[str, Any]) -> Sequence[Any]:
    for key in (
        "results",
        "targeted_exit_matrix",
        "targeted_us_core_filter_matrix",
        "us_early_invalidation_matrix",
        "ranking",
    ):
        value = payload.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return value
    return []
