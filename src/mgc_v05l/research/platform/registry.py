"""Durable project-level experiment registry and comparison catalog."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Mapping, Sequence

from .datasets import json_ready, read_jsonl_dataset, register_duckdb_catalog, stable_hash, write_dataset_bundle, write_json_manifest

REGISTRY_ARTIFACT_VERSION = "research_experiment_registry_v1"


def register_experiment_run(
    *,
    registry_root: Path,
    run_row: Mapping[str, Any],
    target_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    started = perf_counter()
    registry_root = registry_root.resolve()
    registry_root.mkdir(parents=True, exist_ok=True)
    runs_jsonl = registry_root / "runs.jsonl"
    targets_jsonl = registry_root / "targets.jsonl"

    prepared_run = _sanitize_empty_mappings(dict(json_ready(dict(run_row))))
    prepared_run.setdefault("artifact_version", REGISTRY_ARTIFACT_VERSION)
    prepared_run.setdefault("registered_at", datetime.now(UTC).isoformat())
    prepared_run.setdefault("run_id", stable_hash(prepared_run, length=24))

    prepared_targets: list[dict[str, Any]] = []
    for row in target_rows:
        target = _sanitize_empty_mappings(dict(json_ready(dict(row))))
        target.setdefault("artifact_version", REGISTRY_ARTIFACT_VERSION)
        target.setdefault("registered_at", prepared_run["registered_at"])
        target.setdefault("run_id", prepared_run["run_id"])
        target.setdefault(
            "target_entry_id",
            stable_hash(
                {
                    "run_id": prepared_run["run_id"],
                    "target_id": target.get("target_id"),
                    "strategy_variant": target.get("strategy_variant"),
                    "scope_bundle_id": target.get("scope_bundle_id"),
                },
                length=24,
            ),
        )
        prepared_targets.append(target)

    run_rows = read_jsonl_dataset(runs_jsonl)
    target_rows_all = read_jsonl_dataset(targets_jsonl)
    run_rows = [row for row in run_rows if str(row.get("run_id") or "") != str(prepared_run["run_id"])]
    run_rows.append(prepared_run)
    existing_target_ids = {str(row.get("target_entry_id") or "") for row in prepared_targets}
    target_rows_all = [row for row in target_rows_all if str(row.get("target_entry_id") or "") not in existing_target_ids]
    target_rows_all.extend(prepared_targets)
    write_jsonl_started = perf_counter()
    _write_jsonl_rows(runs_jsonl, run_rows)
    _write_jsonl_rows(targets_jsonl, target_rows_all)
    write_jsonl_seconds = perf_counter() - write_jsonl_started

    dataset_started = perf_counter()
    runs_spec = write_dataset_bundle(bundle_dir=registry_root / "datasets", dataset_name="runs", rows=run_rows)
    targets_spec = write_dataset_bundle(bundle_dir=registry_root / "datasets", dataset_name="targets", rows=target_rows_all)
    dataset_seconds = perf_counter() - dataset_started
    duckdb_started = perf_counter()
    duckdb_path = register_duckdb_catalog(
        duckdb_path=registry_root / "catalog.duckdb",
        view_to_parquet={
            "experiment_runs": Path(runs_spec["parquet_path"]),
            "experiment_targets": Path(targets_spec["parquet_path"]),
        },
    )
    duckdb_seconds = perf_counter() - duckdb_started
    manifest = {
        "artifact_version": REGISTRY_ARTIFACT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "run_count": len(run_rows),
        "target_count": len(target_rows_all),
        "datasets": {
            "runs": runs_spec,
            "targets": targets_spec,
        },
        "duckdb_catalog_path": str(duckdb_path.resolve()),
        "latest_run_id": prepared_run["run_id"],
    }
    manifest_started = perf_counter()
    write_json_manifest(registry_root / "manifest.json", manifest)
    manifest_seconds = perf_counter() - manifest_started
    return {
        "run_id": prepared_run["run_id"],
        "registry_root": str(registry_root),
        "manifest_path": str((registry_root / "manifest.json").resolve()),
        "duckdb_catalog_path": str(duckdb_path.resolve()),
        "timing": {
            "write_jsonl_seconds": round(write_jsonl_seconds, 6),
            "dataset_bundle_seconds": round(dataset_seconds, 6),
            "duckdb_seconds": round(duckdb_seconds, 6),
            "manifest_write_seconds": round(manifest_seconds, 6),
            "total_seconds": round(perf_counter() - started, 6),
        },
    }


def read_experiment_registry(*, registry_root: Path) -> dict[str, list[dict[str, Any]]]:
    registry_root = registry_root.resolve()
    return {
        "runs": read_jsonl_dataset(registry_root / "runs.jsonl"),
        "targets": read_jsonl_dataset(registry_root / "targets.jsonl"),
    }


def latest_target_entries(
    *,
    registry_root: Path,
    strategy_family: str | None = None,
    analytics_publishable_only: bool = False,
) -> list[dict[str, Any]]:
    rows = read_experiment_registry(registry_root=registry_root)["targets"]
    if strategy_family:
        rows = [row for row in rows if str(row.get("strategy_family") or "") == strategy_family]
    if analytics_publishable_only:
        rows = [row for row in rows if row.get("analytics_publish") is True]
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.get("strategy_family") or ""),
            str(row.get("strategy_variant") or row.get("target_id") or ""),
            str(row.get("generated_at") or row.get("registered_at") or ""),
        ),
    )
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in ordered:
        key = (
            str(row.get("strategy_family") or ""),
            str(row.get("strategy_variant") or row.get("target_id") or ""),
        )
        latest[key] = row
    return list(latest.values())


def current_code_version(*, cwd: Path | None = None) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str((cwd or Path.cwd()).resolve()),
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return "unknown"
    return completed.stdout.strip() or "unknown"


def build_registry_run_row(
    *,
    strategy_family: str,
    strategy_variant: str,
    date_span: Mapping[str, Any],
    code_version: str | None = None,
    data_version: str | None = None,
    feature_version: str | None = None,
    candidate_version: str | None = None,
    outcome_engine_version: str | None = None,
    config_hash: str | None = None,
    control_hash: str | None = None,
    target_hash: str | None = None,
    bundle_ids: Mapping[str, Any] | None = None,
    artifacts: Mapping[str, Any] | None = None,
    summary_metrics: Mapping[str, Any] | None = None,
    lineage: Mapping[str, Any] | None = None,
    generated_at: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = {
        "run_id": run_id,
        "strategy_family": strategy_family,
        "strategy_variant": strategy_variant,
        "generated_at": generated_at or datetime.now(UTC).isoformat(),
        "code_version": code_version or current_code_version(),
        "data_version": data_version or "unknown",
        "feature_version": feature_version or "unknown",
        "candidate_version": candidate_version or "unknown",
        "outcome_engine_version": outcome_engine_version or "unknown",
        "config_hash": config_hash,
        "control_hash": control_hash,
        "target_hash": target_hash,
        "date_span": dict(json_ready(dict(date_span))),
        "bundle_ids": _sanitize_empty_mappings(dict(json_ready(dict(bundle_ids or {})))),
        "artifacts": _sanitize_empty_mappings(dict(json_ready(dict(artifacts or {})))),
        "summary_metrics": _sanitize_empty_mappings(dict(json_ready(dict(summary_metrics or {})))),
        "lineage": _sanitize_empty_mappings(dict(json_ready(dict(lineage or {})))),
    }
    payload["run_id"] = payload["run_id"] or stable_hash(payload, length=24)
    return payload


def _append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(json_ready(dict(row)), sort_keys=True) + "\n")


def _sanitize_empty_mappings(value: Any) -> Any:
    if isinstance(value, dict):
        if not value:
            return None
        return {str(key): _sanitize_empty_mappings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_empty_mappings(item) for item in value]
    return value


def _write_jsonl_rows(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(json_ready(dict(row)), sort_keys=True) + "\n")
