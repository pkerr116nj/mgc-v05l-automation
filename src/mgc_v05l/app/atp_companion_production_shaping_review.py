"""ATP Companion GC production-shaping package review."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence

from .atp_companion_drawdown_limit_governance import (
    DrawdownGovernanceControl,
    _apply_drawdown_control,
)
from .atp_experiment_registry import register_atp_report_output
from .atp_companion_failure_governance_review import _drawdown_episodes
from .atp_companion_full_history_review import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_SOURCE_DB,
    EvaluationTarget,
    _discover_best_sources,
    _json_ready,
    _materialize_symbol_truth,
    _serialize_datetime,
    _shared_1m_coverage,
    build_targets,
)
from .atp_companion_us_fast_fail_review import (
    _build_candidate_rows,
    _evaluate_scope_with_context,
    _target_hash,
)
from .atp_companion_us_late_pocket_refinement import (
    LatePocketControl,
    _apply_control as _apply_us_late_control,
)
from ..research.trend_participation.experiment_configs import (
    AtpPackageConfig,
    DrawdownGovernanceConfig as DrawdownGovernanceConfigModel,
    EarlyInvalidationConfig,
    SessionScopeConfig,
    config_payload,
)
from ..research.trend_participation.atp_promotion_add_review import default_atp_promotion_add_candidates
from ..research.trend_participation.performance_validation import _trade_metrics


@dataclass(frozen=True)
class PackageDefinition:
    package_id: str
    label: str
    package_config: AtpPackageConfig | None = None
    us_late_control: LatePocketControl | None = None
    governance_control: DrawdownGovernanceControl | None = None


TARGET_IDS = {
    "atp_companion_v1__candidate_gc_asia_us",
    "atp_companion_v1__benchmark_mgc_asia_us",
    "atp_companion_v1__promotion_1_075r_favorable_only",
    "atp_companion_v1__gc_asia__promotion_1_075r_favorable_only",
}

PRIMARY_TARGET = "atp_companion_v1__candidate_gc_asia_us"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-production-shaping-review")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def _targets() -> list[EvaluationTarget]:
    return [target for target in build_targets() if target.target_id in TARGET_IDS]


def _package_defs() -> list[PackageDefinition]:
    us_late_safe = LatePocketControl(
        control_id="us_late_tighter_adverse",
        label="US_LATE 2-bar no-traction 0.25R + adverse 0.65R",
        min_favorable_excursion_r=0.25,
        adverse_excursion_abort_r=0.65,
        logic_mode="all",
    )
    halt_2500 = DrawdownGovernanceControl(
        control_id="halt_only_2500",
        label="Halt-only warning at $2,500",
        threshold_cash=2500.0,
        mode="halt_only",
    )
    halt_3000 = DrawdownGovernanceControl(
        control_id="halt_only_3000",
        label="Halt-only warning at $3,000",
        threshold_cash=3000.0,
        mode="halt_only",
    )
    return [
        PackageDefinition(
            package_id="raw_candidate",
            label="Raw baseline package",
            package_config=AtpPackageConfig(
                package_id="raw_candidate",
                session_scope=SessionScopeConfig(allowed_sessions=("ASIA", "US")),
            ),
        ),
        PackageDefinition(
            package_id="us_late_safeguard",
            label="US_LATE safeguard package",
            package_config=AtpPackageConfig(
                package_id="us_late_safeguard",
                session_scope=SessionScopeConfig(allowed_sessions=("ASIA", "US")),
                early_invalidation=EarlyInvalidationConfig(
                    session_scope="US_LATE",
                    window_bars=2,
                    min_favorable_excursion_r=0.25,
                    adverse_excursion_abort_r=0.65,
                    logic_mode="all",
                ),
            ),
            us_late_control=us_late_safe,
        ),
        PackageDefinition(
            package_id="halt_only_2500",
            label="Outer halt-only governance $2,500",
            package_config=AtpPackageConfig(
                package_id="halt_only_2500",
                session_scope=SessionScopeConfig(allowed_sessions=("ASIA", "US")),
                drawdown_governance=DrawdownGovernanceConfigModel(threshold_cash=2500.0, mode="halt_only"),
            ),
            governance_control=halt_2500,
        ),
        PackageDefinition(
            package_id="halt_only_3000",
            label="Outer halt-only governance $3,000",
            package_config=AtpPackageConfig(
                package_id="halt_only_3000",
                session_scope=SessionScopeConfig(allowed_sessions=("ASIA", "US")),
                drawdown_governance=DrawdownGovernanceConfigModel(threshold_cash=3000.0, mode="halt_only"),
            ),
            governance_control=halt_3000,
        ),
        PackageDefinition(
            package_id="us_late_safeguard__halt_only_3000",
            label="US_LATE safeguard + outer halt-only governance $3,000",
            package_config=AtpPackageConfig(
                package_id="us_late_safeguard__halt_only_3000",
                session_scope=SessionScopeConfig(allowed_sessions=("ASIA", "US")),
                early_invalidation=EarlyInvalidationConfig(
                    session_scope="US_LATE",
                    window_bars=2,
                    min_favorable_excursion_r=0.25,
                    adverse_excursion_abort_r=0.65,
                    logic_mode="all",
                ),
                drawdown_governance=DrawdownGovernanceConfigModel(threshold_cash=3000.0, mode="halt_only"),
            ),
            us_late_control=us_late_safe,
            governance_control=halt_3000,
        ),
    ]


def _package_hash(packages: Sequence[PackageDefinition]) -> str:
    payload = [config_payload(pkg.package_config or {"package_id": pkg.package_id, "label": pkg.label}) for pkg in packages]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _clip_windows_to_rows(
    *,
    rows: Sequence[dict[str, Any]],
    base_trade_windows_by_id: dict[str, list[Any]],
) -> dict[str, list[Any]]:
    clipped: dict[str, list[Any]] = {}
    for row in rows:
        trade_id = str(row.get("trade_id"))
        exit_ts = row.get("exit_ts")
        bars = list(base_trade_windows_by_id.get(trade_id) or [])
        if exit_ts is None:
            clipped[trade_id] = bars
            continue
        clipped[trade_id] = [bar for bar in bars if getattr(bar, "end_ts", None) <= exit_ts]
    return clipped


def _daily_pnl_map(rows: Sequence[dict[str, Any]]) -> dict[str, float]:
    buckets: dict[str, float] = {}
    for row in rows:
        exit_ts = row.get("exit_ts") or row.get("entry_ts")
        if exit_ts is None:
            continue
        day = str(exit_ts.date().isoformat())
        buckets[day] = round(buckets.get(day, 0.0) + float(row.get("pnl_cash") or 0.0), 6)
    return buckets


def _concentration_metrics(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    daily = _daily_pnl_map(rows)
    values = sorted(daily.values(), reverse=True)
    top1 = round(values[0], 4) if values else 0.0
    top3 = round(sum(values[:3]), 4) if values else 0.0
    total = round(sum(float(row.get("pnl_cash") or 0.0) for row in rows), 4)
    survives_without_top1 = round(total - top1, 4)
    survives_without_top3 = round(total - top3, 4)
    return {
        "top_1_day_contribution": top1,
        "top_3_day_contribution": top3,
        "survives_without_top_1": survives_without_top1,
        "survives_without_top_3": survives_without_top3,
    }


def _delta_vs_baseline(
    *,
    metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    worst_episode: float,
    baseline_worst: float,
    concentration: dict[str, Any],
    baseline_concentration: dict[str, Any],
) -> dict[str, Any]:
    return {
        "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(baseline_metrics["net_pnl_cash"]), 4),
        "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(baseline_metrics["max_drawdown"]), 4),
        "profit_factor_delta": round(float(metrics["profit_factor"]) - float(baseline_metrics["profit_factor"]), 4),
        "win_rate_delta": round(float(metrics["win_rate"]) - float(baseline_metrics["win_rate"]), 4),
        "worst_drawdown_episode_delta": round(float(worst_episode) - float(baseline_worst), 4),
        "top_1_day_contribution_delta": round(
            float(concentration["top_1_day_contribution"]) - float(baseline_concentration["top_1_day_contribution"]),
            4,
        ),
        "top_3_day_contribution_delta": round(
            float(concentration["top_3_day_contribution"]) - float(baseline_concentration["top_3_day_contribution"]),
            4,
        ),
        "survives_without_top_1_delta": round(
            float(concentration["survives_without_top_1"]) - float(baseline_concentration["survives_without_top_1"]),
            4,
        ),
        "survives_without_top_3_delta": round(
            float(concentration["survives_without_top_3"]) - float(baseline_concentration["survives_without_top_3"]),
            4,
        ),
    }


def _ranking_score(row: dict[str, Any]) -> tuple[float, float, float, float]:
    delta = row["delta_vs_baseline"]
    return (
        -float(delta["max_drawdown_delta"]),
        float(delta["survives_without_top_3_delta"]),
        float(delta["net_pnl_cash_delta"]),
        -float(row["us_late_false_negative_cost"]["flipped_to_nonpositive_pnl_cash"]),
    )


def run_production_shaping_review(
    *,
    source_db: Path,
    output_dir: Path,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Path]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    started_at = perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    targets = _targets()
    packages = _package_defs()
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    symbol_set = {target.symbol for target in targets}

    discovery_started = perf_counter()
    bar_source_index = _discover_best_sources(symbols=symbol_set, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    discovery_seconds = perf_counter() - discovery_started

    coverage_started = perf_counter()
    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_db, instruments=("MGC", "GC"))
    coverage_seconds = perf_counter() - coverage_started
    run_start = max(shared_start, start_timestamp) if start_timestamp is not None else shared_start
    run_end = min(shared_end, end_timestamp) if end_timestamp is not None else shared_end

    symbol_truths = {
        symbol: _materialize_symbol_truth(
            source_db=source_db,
            symbol=symbol,
            bar_source_index=bar_source_index,
            start_timestamp=run_start,
            end_timestamp=run_end,
        )
        for symbol in sorted(symbol_set)
    }

    scopes = {
        (target.symbol, target.allowed_sessions): _evaluate_scope_with_context(
            symbol_truth=symbol_truths[target.symbol],
            allowed_sessions=target.allowed_sessions,
            point_value=target.point_value,
        )
        for target in targets
    }

    baseline_rows_by_target: dict[str, list[dict[str, Any]]] = {}
    baseline_rows_by_trade_id_by_target: dict[str, dict[str, dict[str, Any]]] = {}
    baseline_metrics_by_target: dict[str, dict[str, Any]] = {}
    baseline_worst_by_target: dict[str, float] = {}
    baseline_concentration_by_target: dict[str, dict[str, Any]] = {}
    for target in targets:
        scope = scopes[(target.symbol, target.allowed_sessions)]
        rows = _build_candidate_rows(target=target, scope=scope, candidate_defs=candidate_defs)
        baseline_rows_by_target[target.target_id] = rows
        baseline_rows_by_trade_id_by_target[target.target_id] = {str(row["trade_id"]): row for row in rows}
        baseline_metrics_by_target[target.target_id] = _trade_metrics(rows, bar_count=scope.bar_count)
        episodes = _drawdown_episodes(rows=rows)
        baseline_worst_by_target[target.target_id] = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
        baseline_concentration_by_target[target.target_id] = _concentration_metrics(rows)

    results: list[dict[str, Any]] = []
    for target in targets:
        scope = scopes[(target.symbol, target.allowed_sessions)]
        baseline_rows = baseline_rows_by_target[target.target_id]
        baseline_rows_by_trade_id = baseline_rows_by_trade_id_by_target[target.target_id]
        baseline_metrics = baseline_metrics_by_target[target.target_id]
        baseline_worst = baseline_worst_by_target[target.target_id]
        baseline_concentration = baseline_concentration_by_target[target.target_id]
        for package in packages:
            package_started = perf_counter()
            current_rows = baseline_rows
            us_late_state = {
                "reason_counts": {"no_traction": 0, "adverse_excursion": 0},
                "reason_net_delta": {"no_traction": 0.0, "adverse_excursion": 0.0},
                "good_winners_harmed_count": 0,
                "good_winners_harmed_pnl_cost": 0.0,
                "flipped_count": 0,
                "flipped_pnl_cost": 0.0,
            }
            effective_windows = scope.trade_windows_by_id

            if package.us_late_control is not None:
                current_rows, us_late_state = _apply_us_late_control(
                    target=target,
                    scope=scope,
                    baseline_rows_by_trade_id=baseline_rows_by_trade_id,
                    control=package.us_late_control,
                    candidate_defs=candidate_defs,
                )
                effective_windows = _clip_windows_to_rows(
                    rows=current_rows,
                    base_trade_windows_by_id=scope.trade_windows_by_id,
                )

            governance_summary = {
                "threshold_breach_count": 0,
                "halted_day_count": 0,
                "halted_session_segment_count": 0,
                "forced_flatten_count": 0,
                "pnl_saved_after_breach_cash": 0.0,
                "pnl_lost_after_breach_cash": 0.0,
                "false_positive_recovery_count": 0,
                "false_positive_pnl_cost_cash": 0.0,
                "material_recovery_threshold_cash": 0.0,
            }
            if package.governance_control is not None:
                current_rows, governance_summary = _apply_drawdown_control(
                    rows=current_rows,
                    trade_windows_by_id=effective_windows,
                    control=package.governance_control,
                )

            metrics = _trade_metrics(current_rows, bar_count=scope.bar_count)
            episodes = _drawdown_episodes(rows=current_rows)
            worst_episode = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
            concentration = _concentration_metrics(current_rows)
            results.append(
                {
                    "target_id": target.target_id,
                    "label": target.label,
                    "symbol": target.symbol,
                    "allowed_sessions": list(target.allowed_sessions),
                    "package_id": package.package_id,
                    "package_label": package.label,
                    "metrics": metrics,
                    "worst_drawdown_episode_loss": round(worst_episode, 4),
                    "governance": governance_summary,
                    "us_late_false_negative_cost": {
                        "flipped_to_nonpositive_count": int(us_late_state["flipped_count"]),
                        "flipped_to_nonpositive_pnl_cash": round(float(us_late_state["flipped_pnl_cost"]), 4),
                        "good_winners_harmed_count": int(us_late_state["good_winners_harmed_count"]),
                        "good_winners_harmed_pnl_cost": round(float(us_late_state["good_winners_harmed_pnl_cost"]), 4),
                    },
                    "concentration": concentration,
                    "delta_vs_baseline": _delta_vs_baseline(
                        metrics=metrics,
                        baseline_metrics=baseline_metrics,
                        worst_episode=worst_episode,
                        baseline_worst=baseline_worst,
                        concentration=concentration,
                        baseline_concentration=baseline_concentration,
                    ),
                    "wall_time_seconds": round(perf_counter() - package_started, 6),
                }
            )

    ranking = sorted(
        [row for row in results if row["target_id"] == PRIMARY_TARGET],
        key=_ranking_score,
        reverse=True,
    )

    manifest = {
        "artifact_version": "atp_production_shaping_review_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_db": str(source_db.resolve()),
        "source_date_span": {
            "start_timestamp": run_start.isoformat(),
            "end_timestamp": run_end.isoformat(),
        },
        "target_hashes": {target.target_id: _target_hash(target) for target in targets},
        "config_hash": _package_hash(packages),
        "provenance": {
            "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
            "benchmark_semantics_changed": False,
            "candidate_package_note": "Packages are explicit production-shaping overlays on top of candidate truth, not benchmark replacements or baseline semantic changes.",
        },
        "timing": {
            "total_wall_seconds": round(perf_counter() - started_at, 6),
            "source_discovery_seconds": round(discovery_seconds, 6),
            "coverage_seconds": round(coverage_seconds, 6),
        },
    }

    payload = {
        "study": "ATP Companion production-shaping package review",
        "manifest": manifest,
        "ranking": ranking,
        "results": results,
    }

    manifest_path = output_dir / "atp_production_shaping_manifest.json"
    json_path = output_dir / "atp_production_shaping_review.json"
    md_path = output_dir / "atp_production_shaping_review.md"
    csv_path = output_dir / "atp_production_shaping_matrix.csv"

    manifest_path.write_text(json.dumps(_json_ready(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "target_id",
                "package_id",
                "net_pnl_cash",
                "max_drawdown",
                "worst_drawdown_episode_loss",
                "profit_factor",
                "win_rate",
                "trade_count",
                "threshold_breach_count",
                "halted_day_count",
                "forced_flatten_count",
                "us_late_false_negative_pnl_cash",
                "pnl_saved_after_breach_cash",
                "pnl_lost_after_breach_cash",
                "top_1_day_contribution",
                "top_3_day_contribution",
                "survives_without_top_1",
                "survives_without_top_3",
                "delta_vs_baseline_net_pnl_cash",
                "delta_vs_baseline_max_drawdown",
                "delta_vs_baseline_worst_episode",
                "delta_vs_baseline_top_1_day",
                "delta_vs_baseline_top_3_day",
                "wall_time_seconds",
            ],
        )
        writer.writeheader()
        for row in results:
            writer.writerow(
                {
                    "target_id": row["target_id"],
                    "package_id": row["package_id"],
                    "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                    "max_drawdown": row["metrics"]["max_drawdown"],
                    "worst_drawdown_episode_loss": row["worst_drawdown_episode_loss"],
                    "profit_factor": row["metrics"]["profit_factor"],
                    "win_rate": row["metrics"]["win_rate"],
                    "trade_count": row["metrics"]["total_trades"],
                    "threshold_breach_count": row["governance"]["threshold_breach_count"],
                    "halted_day_count": row["governance"]["halted_day_count"],
                    "forced_flatten_count": row["governance"]["forced_flatten_count"],
                    "us_late_false_negative_pnl_cash": row["us_late_false_negative_cost"]["flipped_to_nonpositive_pnl_cash"],
                    "pnl_saved_after_breach_cash": row["governance"]["pnl_saved_after_breach_cash"],
                    "pnl_lost_after_breach_cash": row["governance"]["pnl_lost_after_breach_cash"],
                    "top_1_day_contribution": row["concentration"]["top_1_day_contribution"],
                    "top_3_day_contribution": row["concentration"]["top_3_day_contribution"],
                    "survives_without_top_1": row["concentration"]["survives_without_top_1"],
                    "survives_without_top_3": row["concentration"]["survives_without_top_3"],
                    "delta_vs_baseline_net_pnl_cash": row["delta_vs_baseline"]["net_pnl_cash_delta"],
                    "delta_vs_baseline_max_drawdown": row["delta_vs_baseline"]["max_drawdown_delta"],
                    "delta_vs_baseline_worst_episode": row["delta_vs_baseline"]["worst_drawdown_episode_delta"],
                    "delta_vs_baseline_top_1_day": row["delta_vs_baseline"]["top_1_day_contribution_delta"],
                    "delta_vs_baseline_top_3_day": row["delta_vs_baseline"]["top_3_day_contribution_delta"],
                    "wall_time_seconds": row["wall_time_seconds"],
                }
            )

    lines = [
        "# ATP Companion Production-Shaping Package Review",
        "",
        f"- Source DB: `{source_db.resolve()}`",
        f"- Date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Total wall seconds: `{manifest['timing']['total_wall_seconds']}`",
        "",
        "## GC Asia+US Ranked Packages",
        "",
    ]
    for row in ranking:
        lines.extend(
            [
                f"### {row['package_label']}",
                "",
                f"- Net / Max DD / Worst Episode: `{row['metrics']['net_pnl_cash']}` / `{row['metrics']['max_drawdown']}` / `{row['worst_drawdown_episode_loss']}`",
                f"- Delta vs raw baseline net / DD / worst episode: `{row['delta_vs_baseline']['net_pnl_cash_delta']}` / `{row['delta_vs_baseline']['max_drawdown_delta']}` / `{row['delta_vs_baseline']['worst_drawdown_episode_delta']}`",
                f"- Governance breaches / halted days: `{row['governance']['threshold_breach_count']}` / `{row['governance']['halted_day_count']}`",
                f"- US_LATE false-negative pnl cost: `{row['us_late_false_negative_cost']['flipped_to_nonpositive_pnl_cash']}`",
                f"- Top1 / Top3 day contribution: `{row['concentration']['top_1_day_contribution']}` / `{row['concentration']['top_3_day_contribution']}`",
                "",
            ]
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return {
        "manifest_path": manifest_path,
        "json_path": json_path,
        "markdown_path": md_path,
        "comparison_csv_path": csv_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    source_db = Path(args.source_db).resolve()
    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        output_dir = (
            DEFAULT_OUTPUT_ROOT / f"atp_companion_production_shaping_review_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        ).resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_production_shaping_review(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    registry_result = register_atp_report_output(
        strategy_variant="production_shaping_review",
        payload_json_path=Path(result["json_path"]),
        artifacts=result,
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
