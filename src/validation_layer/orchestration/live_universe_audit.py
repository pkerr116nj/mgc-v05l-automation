"""Controlled full-universe audit for live and effectively-live strategy candidates."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mgc_v05l.config_models import load_settings_from_files

from ..config.defaults import build_debug_config
from ..data.contracts import ValidationSubject
from ..layer1.atp_adapter import build_strategy_backtest_from_atp_source, load_atp_layer1_source
from ..layer1.atp_optimization import rerun_atp_promotion_add_validation_bundle
from ..layer1.runtime_sqlite_adapter import (
    RuntimeSQLiteLaneSource,
    _database_path_from_url,
    build_runtime_sqlite_strategy_backtest,
)
from ..reporting.json_report import render_json_report
from ..reporting.markdown_report import render_markdown_report
from .pipeline import run_validation_pipeline


REPO_ROOT = Path.cwd()
AUDIT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "validation_layer" / "live_candidate_universe_audit"


@dataclass(frozen=True)
class LiveUniverseItem:
    item_id: str
    item_type: str
    lane_id: str | None
    display_name: str
    runtime_kind: str
    source_family: str
    symbol: str | None
    scope_kind: str
    config_path: Path
    database_path: Path | None
    artifacts_dir: Path | None
    metadata: dict[str, Any]


def _resolve_path(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = REPO_ROOT / resolved
    return resolved.resolve(strict=False)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_live_pilot_config(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith('"') and value.endswith('"'):
            payload[key] = value[1:-1]
        elif value.startswith("'") and value.endswith("'"):
            payload[key] = value[1:-1]
        else:
            payload[key] = value
    if "probationary_paper_lanes_json" in payload:
        payload["probationary_paper_lanes_json"] = json.loads(payload["probationary_paper_lanes_json"])
    return payload


def _required_runtime_tables_exist(database_path: Path) -> bool:
    if not database_path.exists():
        return False
    connection = sqlite3.connect(database_path)
    try:
        tables = {row[0] for row in connection.execute("select name from sqlite_master where type='table'").fetchall()}
    finally:
        connection.close()
    required = {"bars", "processed_bars", "strategy_state_snapshots"}
    return required.issubset(tables)


def _runtime_items_from_config(path: Path, *, scope_kind: str) -> list[LiveUniverseItem]:
    payload = _load_json(path)
    items: list[LiveUniverseItem] = []
    for row in payload.get("lanes", []):
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id:
            continue
        items.append(
            LiveUniverseItem(
                item_id=f"lane::{lane_id}",
                item_type="lane",
                lane_id=lane_id,
                display_name=str(row.get("display_name") or lane_id),
                runtime_kind=str(row.get("runtime_kind") or "unknown_runtime"),
                source_family=str(row.get("source_family") or row.get("strategy_family") or "unknown_family"),
                symbol=str(row.get("symbol")) if row.get("symbol") is not None else None,
                scope_kind=scope_kind,
                config_path=path,
                database_path=_database_path_from_url(row.get("database_url")),
                artifacts_dir=_resolve_path(row.get("artifacts_dir")),
                metadata=dict(row),
            )
        )
    return items


def _live_pilot_items_from_configs() -> list[LiveUniverseItem]:
    configs = [
        REPO_ROOT / "config" / "probationary_pattern_engine_live_atp_companion_v1_gc_asia_us_pilot.yaml",
        REPO_ROOT / "config" / "probationary_pattern_engine_live_atp_companion_v1_asia_us_pilot.yaml",
    ]
    items: list[LiveUniverseItem] = []
    for path in configs:
        if not path.exists():
            continue
        payload = _parse_live_pilot_config(path)
        for lane in payload.get("probationary_paper_lanes_json", []):
            lane_id = str(lane.get("lane_id") or "").strip()
            if not lane_id:
                continue
            items.append(
                LiveUniverseItem(
                    item_id=f"lane::{lane_id}",
                    item_type="lane",
                    lane_id=lane_id,
                    display_name=str(lane.get("display_name") or lane_id),
                    runtime_kind=str(lane.get("runtime_kind") or "atp_companion_live_entry_pilot"),
                    source_family=str(lane.get("strategy_family") or "active_trend_participation_engine"),
                    symbol=str(lane.get("symbol")) if lane.get("symbol") is not None else None,
                    scope_kind="live_pilot",
                    config_path=path,
                    database_path=_database_path_from_url(lane.get("database_url") or payload.get("database_url")),
                    artifacts_dir=_resolve_path(lane.get("artifacts_dir") or payload.get("probationary_artifacts_dir")),
                    metadata={**payload, **lane},
                )
            )
    return items


def build_live_candidate_universe_inventory() -> list[LiveUniverseItem]:
    items = {}
    for path, scope in (
        (REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json", "paper_session"),
        (REPO_ROOT / "outputs" / "probationary_pattern_engine" / "runtime" / "paper_config_in_force.json", "paper_runtime"),
    ):
        if path.exists():
            for item in _runtime_items_from_config(path, scope_kind=scope):
                items[item.item_id] = item
    for item in _live_pilot_items_from_configs():
        items.setdefault(item.item_id, item)

    # Add the one bounded candidate with real attached optimization history.
    items["candidate::atp_promotion_add::promotion_1_075r_favorable_only"] = LiveUniverseItem(
        item_id="candidate::atp_promotion_add::promotion_1_075r_favorable_only",
        item_type="candidate",
        lane_id="atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
        display_name="ATP promotion/add active research candidate (MGC, favorable-only)",
        runtime_kind="atp_promotion_add_review_candidate",
        source_family="active_trend_participation_engine",
        symbol="MGC",
        scope_kind="research_candidate",
        config_path=REPO_ROOT / "config" / "atp_companion_candidate_promotion_1_075r_favorable_only.yaml",
        database_path=_resolve_path("mgc_v05l.replay.sqlite3"),
        artifacts_dir=None,
        metadata={
            "candidate_id": "promotion_1_075r_favorable_only",
            "optimization_history_available": True,
            "source_review": "trend_participation.atp_promotion_add_review",
        },
    )
    return sorted(items.values(), key=lambda item: item.item_id)


def _atp_config_lane_spec(path: Path, lane_id: str) -> dict[str, Any] | None:
    settings = load_settings_from_files([REPO_ROOT / "config" / "base.yaml", path])
    matching = [row for row in settings.probationary_paper_lane_specs if str(row.get("lane_id")) == lane_id]
    if len(matching) != 1:
        return None
    return dict(matching[0])


def _atp_config_match_score(item: LiveUniverseItem, configured_lane: dict[str, Any]) -> int:
    metadata = item.metadata
    score = 0
    weighted_keys = {
        "display_name": 4,
        "symbol": 3,
        "standalone_strategy_id": 5,
        "runtime_kind": 5,
        "lane_mode": 5,
        "strategy_family": 3,
        "quality_bucket_policy": 2,
    }
    for key, weight in weighted_keys.items():
        configured_value = configured_lane.get(key)
        runtime_value = metadata.get(key)
        if configured_value is not None and runtime_value is not None and str(configured_value) == str(runtime_value):
            score += weight

    configured_artifacts_dir = configured_lane.get("artifacts_dir")
    if configured_artifacts_dir is not None and item.artifacts_dir is not None:
        configured_path = _resolve_path(configured_artifacts_dir)
        if configured_path == item.artifacts_dir:
            score += 5
    return score


def _find_atp_source_config(item: LiveUniverseItem) -> Path | None:
    lane_id = str(item.lane_id)
    candidates: list[Path] = []
    for path in sorted((REPO_ROOT / "config").glob("*.yaml")):
        text = path.read_text(encoding="utf-8")
        if lane_id in text:
            candidates.append(path)
    if not candidates:
        return None
    best_path: Path | None = None
    best_score = -1
    for path in candidates:
        configured_lane = _atp_config_lane_spec(path, lane_id)
        if configured_lane is None:
            continue
        score = _atp_config_match_score(item, configured_lane)
        score += max(0, 3 - path.name.count("5m"))
        score += 1 if "live_" not in path.name else 0
        if score > best_score:
            best_score = score
            best_path = path
    if best_path is not None:
        return best_path
    candidates.sort(key=lambda path: (path.name.count("5m"), "live_" in path.name, len(path.name)))
    return candidates[0]


def _can_use_atp_jsonl_adapter(item: LiveUniverseItem) -> bool:
    if not item.runtime_kind.startswith("atp_companion"):
        return False
    if item.artifacts_dir is None:
        return False
    required = (
        item.artifacts_dir / "processed_bars.jsonl",
        item.artifacts_dir / "signals.jsonl",
        item.artifacts_dir / "order_intents.jsonl",
        item.artifacts_dir / "fills.jsonl",
        item.artifacts_dir / "trades.jsonl",
        item.artifacts_dir / "runtime_state.json",
        item.artifacts_dir / "operator_status.json",
        item.artifacts_dir / "reconciliation_events.jsonl",
    )
    return all(path.exists() for path in required)


def _inventory_row(item: LiveUniverseItem) -> dict[str, Any]:
    initial_layer1_status = "already_adapted" if _can_use_atp_jsonl_adapter(item) else "adaptation_needed"
    optimization_history_available = bool(item.metadata.get("optimization_history_available")) or (
        item.lane_id == "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only"
    )
    db_ready = item.database_path is not None and _required_runtime_tables_exist(item.database_path)
    resolved_status = (
        "already_adapted"
        if _can_use_atp_jsonl_adapter(item)
        else "adapted_in_this_pass"
        if item.item_type == "candidate" or db_ready
        else "blocked_missing_evidence"
    )
    likely_evaluable_now = resolved_status != "blocked_missing_evidence"
    blocked_reason = None
    if not likely_evaluable_now:
        if item.database_path is None:
            blocked_reason = "no_database_path"
        elif not item.database_path.exists():
            blocked_reason = "database_missing"
        else:
            blocked_reason = "required_runtime_tables_missing"
    return {
        "item_id": item.item_id,
        "item_type": item.item_type,
        "lane_id": item.lane_id,
        "display_name": item.display_name,
        "runtime_kind": item.runtime_kind,
        "source_family": item.source_family,
        "symbol": item.symbol,
        "scope_kind": item.scope_kind,
        "layer1_status_initial": initial_layer1_status,
        "layer1_status_resolved": resolved_status,
        "optimization_history_available": optimization_history_available,
        "optimization_history_status": "available" if optimization_history_available else "unavailable",
        "likely_evaluable_now": likely_evaluable_now,
        "blocked_reason": blocked_reason,
        "database_path": None if item.database_path is None else str(item.database_path),
        "artifacts_dir": None if item.artifacts_dir is None else str(item.artifacts_dir),
        "operational_only_signal": bool(item.metadata.get("non_approved")) or item.scope_kind == "live_pilot",
    }


def _batch_assignment(row: dict[str, Any]) -> str:
    if row["optimization_history_available"]:
        return "batch_3_optimization_history_capable"
    if row["layer1_status_initial"] == "already_adapted" and row["likely_evaluable_now"]:
        return "batch_1_already_adapted_runnable"
    if row["layer1_status_resolved"] == "adapted_in_this_pass":
        return "batch_2_runtime_adapter_rollout"
    return "batch_4_partial_or_blocked"


def _build_runtime_sqlite_source(item: LiveUniverseItem) -> RuntimeSQLiteLaneSource:
    metadata = dict(item.metadata)
    return RuntimeSQLiteLaneSource(
        lane_id=str(item.lane_id),
        display_name=item.display_name,
        runtime_kind=item.runtime_kind,
        lane_mode=str(metadata.get("lane_mode") or "STANDARD"),
        symbol=str(item.symbol or metadata.get("symbol") or "UNKNOWN"),
        allowed_sessions=tuple(metadata.get("allowed_sessions") or ()),
        source_family=item.source_family,
        strategy_family=str(metadata.get("strategy_family") or item.source_family),
        strategy_identity_root=metadata.get("strategy_identity_root"),
        scope_kind=item.scope_kind,
        config_path=item.config_path,
        database_path=item.database_path,
        artifacts_dir=item.artifacts_dir,
        execution_timeframe=metadata.get("execution_timeframe") or metadata.get("artifact_timeframe"),
        point_value=float(metadata["point_value"]) if metadata.get("point_value") is not None else None,
        trade_size=float(metadata["trade_size"]) if metadata.get("trade_size") is not None else None,
        experimental_status=metadata.get("experimental_status"),
        non_approved=bool(metadata.get("non_approved")),
        tracked_strategy_id=metadata.get("tracked_strategy_id"),
        metadata={
            "config_runtime_kind": metadata.get("runtime_kind"),
            "paper_only": metadata.get("paper_only"),
        },
    )


def _build_subject_for_item(item: LiveUniverseItem) -> tuple[ValidationSubject, dict[str, Any]]:
    if item.item_type == "candidate":
        bundle = rerun_atp_promotion_add_validation_bundle(
            source_sqlite_path=item.database_path or "mgc_v05l.replay.sqlite3",
            instruments=("MGC",),
            point_value=10.0,
            max_windows=8,
            code_version="universe_audit",
        )
        return (
            ValidationSubject(
                strategy_backtest=bundle.strategy_backtest,
                optimization_run=bundle.optimization_run,
            ),
            {
                "strategy_backtest": bundle.strategy_backtest,
                "optimization_run": bundle.optimization_run,
                "optimization_history": bundle.history_payload,
            },
        )

    if _can_use_atp_jsonl_adapter(item):
        source_config_path = _find_atp_source_config(item)
        if source_config_path is None:
            raise ValueError(f"Could not find ATP config source for {item.lane_id}.")
        source = load_atp_layer1_source(
            lane_id=str(item.lane_id),
            subject_label=str(item.lane_id),
            source_config_path=source_config_path,
            lane_dir=item.artifacts_dir,
            runtime_config_in_force_path=item.config_path,
            family_classification="live_lane" if item.scope_kind == "live_pilot" else "paper_lane",
            baseline_reference_path="config/atp_companion_baseline_v1_asia_us.yaml",
            candidate_registry_path=(
                "config/atp_promotion_add_candidate_registry.yaml"
                if "promotion_1_075r_favorable_only" in str(item.lane_id)
                else None
            ),
            candidate_config_path=(
                "config/atp_companion_candidate_promotion_1_075r_favorable_only.yaml"
                if "promotion_1_075r_favorable_only" in str(item.lane_id)
                else None
            ),
        )
        backtest = build_strategy_backtest_from_atp_source(source, code_version="universe_audit")
    else:
        backtest = build_runtime_sqlite_strategy_backtest(_build_runtime_sqlite_source(item), code_version="universe_audit")
    return ValidationSubject(strategy_backtest=backtest), {"strategy_backtest": backtest}


def _json_ready(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return {name: _json_ready(getattr(value, name)) for name in value.__dataclass_fields__}
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _module_failure_tags(report_payload: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    module_map = {row["module_name"]: row for row in report_payload.get("module_results", [])}
    if (module_map.get("layer1_prerequisites") or {}).get("status") == "warn":
        tags.append("provenance_inadequate")
    if any((module_map.get(name) or {}).get("status") in {"warn", "fail"} for name in ("train_bias", "selection_bias", "cscv_pbo")):
        tags.append("optimization_skepticism_concern")
    if (module_map.get("monte_carlo") or {}).get("status") in {"warn", "fail"}:
        tags.append("execution_path_fragility")
    if (module_map.get("market_permutation") or {}).get("status") in {"warn", "fail"}:
        tags.append("market_randomization_weakness")
    if (module_map.get("bootstrap") or {}).get("status") in {"warn", "fail"}:
        tags.append("bootstrap_bound_weakness")
    if (module_map.get("drawdown") or {}).get("status") in {"warn", "fail"}:
        tags.append("drawdown_geometry_failure")
    if (module_map.get("trade_normalization") or {}).get("status") in {"warn", "fail"}:
        tags.append("trade_normalization_failure")
    return tags


def _module_map(report_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["module_name"]: row for row in report_payload.get("module_results", [])}


def _sample_evidence_status(*, trade_count: int | None, minimum_trade_count: int) -> str:
    if trade_count is None:
        return "unknown_sample"
    if trade_count <= 0:
        return "no_realized_trade_sample"
    if trade_count < minimum_trade_count:
        return "thin_trade_sample"
    return "substantive_trade_sample"


def _robustness_evidence_status(report_payload: dict[str, Any]) -> str:
    module_map = _module_map(report_payload)
    robustness_modules = ("bootstrap", "monte_carlo", "market_permutation")
    present = [module_map.get(name) for name in robustness_modules if module_map.get(name) is not None]
    if not present:
        return "unavailable"
    if all(bool(row.get("metrics", {}).get("insufficient_evidence")) for row in present):
        return "insufficient"
    return "available"


def _optimization_skepticism_status(report_payload: dict[str, Any]) -> str:
    module_map = _module_map(report_payload)
    optimization_modules = ("parameter_surface", "train_bias", "selection_bias", "cscv_pbo")
    present = [module_map.get(name) for name in optimization_modules if module_map.get(name) is not None]
    if not present:
        return "unavailable"
    if all(bool(row.get("metrics", {}).get("insufficient_evidence")) for row in present):
        return "insufficient"
    return "available"


def _evidence_profile(*, strategy_payload: dict[str, Any] | None, report_payload: dict[str, Any] | None, minimum_trade_count: int) -> dict[str, Any]:
    if strategy_payload is None:
        return {
            "trade_count": None,
            "bar_count": None,
            "position_count": None,
            "sample_evidence_status": "unknown_sample",
            "robustness_evidence_status": "unavailable",
            "optimization_skepticism_status": "unavailable",
        }

    trade_count = len(strategy_payload.get("trades", []))
    bar_count = len(strategy_payload.get("bar_data", []))
    position_count = len(strategy_payload.get("position_series", []))
    if report_payload is None:
        robustness_status = "unavailable"
        optimization_status = "unavailable"
    else:
        robustness_status = _robustness_evidence_status(report_payload)
        optimization_status = _optimization_skepticism_status(report_payload)
    return {
        "trade_count": trade_count,
        "bar_count": bar_count,
        "position_count": position_count,
        "sample_evidence_status": _sample_evidence_status(
            trade_count=trade_count,
            minimum_trade_count=minimum_trade_count,
        ),
        "robustness_evidence_status": robustness_status,
        "optimization_skepticism_status": optimization_status,
    }


def _taxonomy_for_row(row: dict[str, Any]) -> str:
    if row.get("evaluation_status") == "blocked":
        if row.get("operational_only_signal"):
            return "operationally_useful_not_scientifically_supported"
        return "insufficient_evidence"
    if row.get("operational_only_signal"):
        return "operationally_useful_not_scientifically_supported"
    if row.get("sample_evidence_status") in {"no_realized_trade_sample", "thin_trade_sample"} and row.get(
        "optimization_history_status"
    ) != "available":
        return "insufficient_evidence"
    overall = row.get("overall_status")
    if overall == "insufficient_evidence":
        return "insufficient_evidence"
    if overall == "reject":
        return "fails_promotion_standards"
    return "candidate_for_deeper_scientific_review"


def _family_breakdown(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    families: dict[str, dict[str, int]] = {}
    for row in rows:
        family = str(row["source_family"])
        bucket = families.setdefault(
            family,
            {
                "total": 0,
                "fails_promotion_standards": 0,
                "insufficient_evidence": 0,
                "operationally_useful_not_scientifically_supported": 0,
                "candidate_for_deeper_scientific_review": 0,
            },
        )
        bucket["total"] += 1
        bucket[row["taxonomy"]] += 1
    return dict(sorted(families.items()))


def _summary_recommendations(summary_rows: list[dict[str, Any]]) -> list[str]:
    if any(row["taxonomy"] == "fails_promotion_standards" for row in summary_rows):
        first_fail = next(row for row in summary_rows if row["taxonomy"] == "fails_promotion_standards")
        fail_recommendation = (
            f"Retire `{first_fail['item_id']}` from promotion consideration for now; it has substantive evidence and still fails the skepticism stack."
        )
    else:
        fail_recommendation = "No live candidate currently clears the evidence bar for promotion-oriented confidence."
    return [
        "Treat live-entry pilots and explicitly non-approved surfaces as operationally useful only, not scientific support.",
        "Use insufficient-evidence classifications to drive provenance attachment and longer sample collection instead of granting soft credit for being live.",
        fail_recommendation,
        "Hold deeper scientific review for the small set of candidates that accumulate real trade sample plus optimization history without collapsing under the diagnostics.",
    ]


def _render_universe_summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Live Candidate Universe Audit",
        "",
        "## Inventory",
        f"- Total universe items: `{summary['inventory']['total_items']}`",
        f"- Evaluable now: `{summary['inventory']['evaluable_now']}`",
        f"- Blocked by missing evidence: `{summary['inventory']['blocked']}`",
        "",
        "## Evidence Availability",
        f"- Layer 1 already adapted at start: `{summary['inventory']['initially_adapted']}`",
        f"- Adapted in this pass via runtime SQLite: `{summary['inventory']['adapted_in_this_pass']}`",
        f"- Optimization-history-capable items: `{summary['inventory']['optimization_history_available']}`",
        "",
        "## Taxonomy",
    ]
    for key, value in summary["taxonomy_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Rollout Plan"])
    for key, value in summary["batch_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Sample / Robustness Availability"])
    for key, value in summary["evidence_status_counts"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Clear Scientific Failures"])
    if not summary["clear_failures"]:
        lines.append("- None in the current sweep.")
    else:
        for row in summary["clear_failures"]:
            lines.append(f"- `{row['item_id']}` overall=`{row['overall_status']}` trades=`{row.get('trade_count')}`")
    lines.extend(["", "## Dominant Failure Modes"])
    if not summary["failure_mode_counts"]:
        lines.append("- No substantive failure-mode sample yet beyond evidence gaps.")
    else:
        for key, value in summary["failure_mode_counts"].items():
            lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Deeper Follow-Up Candidates"])
    if not summary["deeper_follow_up"]:
        lines.append("- None in the current sweep.")
    else:
        for row in summary["deeper_follow_up"]:
            lines.append(f"- `{row['item_id']}` overall=`{row['overall_status']}`")
    lines.extend(["", "## Debug / Operational-Only"])
    if not summary["debug_or_operational_only"]:
        lines.append("- None identified.")
    else:
        for row in summary["debug_or_operational_only"]:
            lines.append(f"- `{row['item_id']}` ({row['display_name']})")
    lines.extend(["", "## Insufficient Evidence"])
    if not summary["insufficient_evidence_cases"]:
        lines.append("- None.")
    else:
        for row in summary["insufficient_evidence_cases"][:20]:
            lines.append(
                f"- `{row['item_id']}` sample=`{row.get('sample_evidence_status')}` optimization=`{row.get('optimization_history_status')}`"
            )
        if len(summary["insufficient_evidence_cases"]) > 20:
            lines.append(f"- ... plus `{len(summary['insufficient_evidence_cases']) - 20}` more")
    lines.extend(["", "## Family Breakdown"])
    for family, counts in summary["family_breakdown"].items():
        lines.append(
            f"- `{family}` total=`{counts['total']}` fail=`{counts['fails_promotion_standards']}` "
            f"insufficient=`{counts['insufficient_evidence']}` operational=`{counts['operationally_useful_not_scientifically_supported']}`"
        )
    lines.extend(["", "## Recommendation"])
    lines.extend([f"- {item}" for item in summary["recommendations"]])
    return "\n".join(lines) + "\n"


def run_live_candidate_universe_audit(*, output_dir: str | Path | None = None) -> dict[str, Any]:
    root_dir = _resolve_path(output_dir) or AUDIT_OUTPUT_ROOT
    root_dir.mkdir(parents=True, exist_ok=True)
    items = build_live_candidate_universe_inventory()
    config = build_debug_config()
    inventory_rows = [_inventory_row(item) for item in items]
    inventory_by_id = {row["item_id"]: row for row in inventory_rows}

    evaluations: list[dict[str, Any]] = []
    for item in items:
        row = inventory_by_id[item.item_id]
        row["batch"] = _batch_assignment(row)
        if not row["likely_evaluable_now"]:
            row["evaluation_status"] = "blocked"
            row.update(
                _evidence_profile(
                    strategy_payload=None,
                    report_payload=None,
                    minimum_trade_count=config.thresholds.minimum_trade_count,
                )
            )
            evaluations.append(row)
            continue
        subject_dir = root_dir / item.item_id.replace("::", "__").replace("/", "_")
        subject_dir.mkdir(parents=True, exist_ok=True)
        try:
            subject, artifacts = _build_subject_for_item(item)
            if "strategy_backtest" in artifacts:
                strategy_path = subject_dir / "strategy_backtest.json"
                strategy_path.write_text(json.dumps(_json_ready(artifacts["strategy_backtest"]), indent=2, sort_keys=True) + "\n", encoding="utf-8")
                row["strategy_backtest_json"] = str(strategy_path)
            if "optimization_run" in artifacts:
                optimization_path = subject_dir / "optimization_run.json"
                optimization_path.write_text(json.dumps(_json_ready(artifacts["optimization_run"]), indent=2, sort_keys=True) + "\n", encoding="utf-8")
                row["optimization_run_json"] = str(optimization_path)
            if "optimization_history" in artifacts:
                history_path = subject_dir / "optimization_history.json"
                history_path.write_text(json.dumps(_json_ready(artifacts["optimization_history"]), indent=2, sort_keys=True) + "\n", encoding="utf-8")
                row["optimization_history_json"] = str(history_path)

            report = run_validation_pipeline(subject, config)
            report_json_path = subject_dir / "validation_report.json"
            report_markdown_path = subject_dir / "validation_report.md"
            report_json_path.write_text(render_json_report(report), encoding="utf-8")
            report_markdown_path.write_text(render_markdown_report(report), encoding="utf-8")
            report_payload = json.loads(report_json_path.read_text(encoding="utf-8"))
            row["evaluation_status"] = "completed"
            row["overall_status"] = report_payload["overall_status"]
            row["validation_report_json"] = str(report_json_path)
            row["validation_report_markdown"] = str(report_markdown_path)
            row["module_failure_tags"] = _module_failure_tags(report_payload)
            strategy_payload = _json_ready(artifacts.get("strategy_backtest")) if artifacts.get("strategy_backtest") is not None else None
            row.update(
                _evidence_profile(
                    strategy_payload=strategy_payload,
                    report_payload=report_payload,
                    minimum_trade_count=config.thresholds.minimum_trade_count,
                )
            )
        except Exception as exc:  # noqa: BLE001
            row["evaluation_status"] = "blocked"
            row["blocked_reason"] = f"adaptation_error:{type(exc).__name__}"
            row["blocked_detail"] = str(exc)
            row.update(
                _evidence_profile(
                    strategy_payload=None,
                    report_payload=None,
                    minimum_trade_count=config.thresholds.minimum_trade_count,
                )
            )
        evaluations.append(row)

    for row in evaluations:
        row["taxonomy"] = _taxonomy_for_row(row)

    taxonomy_counts: dict[str, int] = {}
    failure_mode_counts: dict[str, int] = {}
    batch_counts: dict[str, int] = {}
    evidence_status_counts: dict[str, int] = {}
    for row in evaluations:
        taxonomy_counts[row["taxonomy"]] = taxonomy_counts.get(row["taxonomy"], 0) + 1
        batch_counts[row["batch"]] = batch_counts.get(row["batch"], 0) + 1
        evidence_key = f"{row.get('sample_evidence_status')}|robustness:{row.get('robustness_evidence_status')}|optimization:{row.get('optimization_history_status')}"
        evidence_status_counts[evidence_key] = evidence_status_counts.get(evidence_key, 0) + 1
        if row["taxonomy"] != "fails_promotion_standards":
            continue
        for tag in row.get("module_failure_tags", []):
            failure_mode_counts[tag] = failure_mode_counts.get(tag, 0) + 1

    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "inventory": {
            "total_items": len(evaluations),
            "evaluable_now": sum(1 for row in evaluations if row["evaluation_status"] == "completed"),
            "blocked": sum(1 for row in evaluations if row["evaluation_status"] == "blocked"),
            "initially_adapted": sum(1 for row in evaluations if row["layer1_status_initial"] == "already_adapted"),
            "adapted_in_this_pass": sum(1 for row in evaluations if row["layer1_status_resolved"] == "adapted_in_this_pass"),
            "optimization_history_available": sum(1 for row in evaluations if row["optimization_history_available"]),
        },
        "batch_counts": dict(sorted(batch_counts.items())),
        "evidence_status_counts": dict(sorted(evidence_status_counts.items(), key=lambda item: (-item[1], item[0]))),
        "taxonomy_counts": taxonomy_counts,
        "failure_mode_counts": dict(sorted(failure_mode_counts.items(), key=lambda item: (-item[1], item[0]))),
        "clear_failures": [
            row
            for row in evaluations
            if row["taxonomy"] == "fails_promotion_standards"
        ],
        "deeper_follow_up": [
            row
            for row in evaluations
            if row["taxonomy"] == "candidate_for_deeper_scientific_review"
        ],
        "debug_or_operational_only": [
            row
            for row in evaluations
            if row.get("operational_only_signal")
        ],
        "insufficient_evidence_cases": [
            row
            for row in evaluations
            if row["taxonomy"] == "insufficient_evidence"
        ],
        "family_breakdown": _family_breakdown(evaluations),
        "recommendations": _summary_recommendations(evaluations),
    }

    inventory_path = root_dir / "live_candidate_universe_manifest.json"
    inventory_path.write_text(
        json.dumps(
            {
                "generated_at": summary["generated_at"],
                "rows": evaluations,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    summary_json_path = root_dir / "live_candidate_universe_summary.json"
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_markdown_path = root_dir / "live_candidate_universe_summary.md"
    summary_markdown_path.write_text(_render_universe_summary_markdown(summary), encoding="utf-8")
    return {
        "root_dir": str(root_dir),
        "inventory_path": str(inventory_path),
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path),
        "item_count": len(evaluations),
    }
