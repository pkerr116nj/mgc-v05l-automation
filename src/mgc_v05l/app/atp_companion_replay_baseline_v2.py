"""Build a replay-safe ATP Companion Replay Baseline v2 for future enhancement research."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterator, Sequence

from .atp_companion_full_history_review import (
    DEFAULT_PLATFORM_SUBSTRATE_ROOT,
    DEFAULT_SOURCE_DB,
    _discover_best_sources,
    _materialize_symbol_truth,
    _trade_windows_by_id,
    build_replay_truth_manifest,
    build_targets,
    iter_replay_truth_trade_rows,
)
from ..research.platform import ensure_symbol_context_bundle, stable_hash
from ..research.trend_participation.performance_validation import _trade_metrics, summarize_trade_distribution_diagnostics
from ..research.trend_participation.atp_promotion_add_review import default_atp_promotion_add_candidates
from ..research.trend_participation.substrate import (
    _read_manifest,
    _timing_state_decision_id,
    _timing_state_from_row,
    _trade_record_from_row,
)
from ..research.platform import read_jsonl_dataset
from ..research.trend_participation.outcome_engine import trade_records_to_retest_rows

REPO_ROOT = Path.cwd()
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_replay_baseline_v2"
DEFAULT_MIN_START = datetime.fromisoformat("2024-01-01T18:01:00-05:00")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-replay-baseline-v2")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--scope-bundle-id", default=None, help="Optional explicit ATP substrate scope bundle id.")
    return parser


def _scope_bundle_manifest_path(scope_bundle_id: str) -> Path:
    return DEFAULT_PLATFORM_SUBSTRATE_ROOT / "scope_bundles" / scope_bundle_id / "manifest.json"


def _load_scope_bundle(scope_bundle_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = _read_manifest(_scope_bundle_manifest_path(scope_bundle_id))
    datasets = manifest["datasets"]
    timing_rows = read_jsonl_dataset(Path(datasets["timing_states"]["jsonl_path"]))
    trade_rows = read_jsonl_dataset(Path(datasets["trade_records"]["jsonl_path"]))
    timing_states = [_timing_state_from_row(row) for row in timing_rows]
    trades = [_trade_record_from_row(row) for row in trade_rows]
    replay_rows = trade_records_to_retest_rows(
        trades,
        timing_states_by_decision_id={_timing_state_decision_id(state): state for state in timing_states},
    )
    return manifest, replay_rows


def _find_latest_mgc_asia_us_scope_bundle() -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for manifest_path in (DEFAULT_PLATFORM_SUBSTRATE_ROOT / "scope_bundles").glob("*/manifest.json"):
        manifest = _read_manifest(manifest_path)
        if manifest.get("symbol") != "MGC":
            continue
        if list(manifest.get("allowed_sessions") or []) != ["ASIA", "US"]:
            continue
        span = manifest.get("source_date_span") or {}
        end_value = span.get("end_timestamp") or span.get("end_ts")
        start_value = span.get("start_timestamp") or span.get("start_ts")
        candidates.append(
            {
                "bundle_id": manifest.get("bundle_id"),
                "manifest_path": str(manifest_path.resolve()),
                "row_count": int((((manifest.get("datasets") or {}).get("trade_records") or {}).get("row_count")) or 0),
                "start_timestamp": start_value,
                "end_timestamp": end_value,
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda row: (
            str(row.get("end_timestamp") or ""),
            int(row.get("row_count") or 0),
            str(row.get("bundle_id") or ""),
        ),
        reverse=True,
    )
    return candidates[0]


def _manifest_1m_span(manifest: dict[str, Any]) -> tuple[datetime, datetime]:
    selected_1m = (manifest.get("selected_sources") or {}).get("1m") or {}
    start_value = selected_1m.get("start_ts") or selected_1m.get("start_timestamp")
    end_value = selected_1m.get("end_ts") or selected_1m.get("end_timestamp")
    if start_value and end_value:
        return datetime.fromisoformat(str(start_value)), datetime.fromisoformat(str(end_value))
    span = manifest.get("source_date_span") or {}
    return datetime.fromisoformat(str(span["start_timestamp"])), datetime.fromisoformat(str(span["end_timestamp"]))


def _base_position_rows(trade_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in trade_rows:
        trade = row["trade_record"]
        rows.append(
            {
                "trade_id": row["trade_id"],
                "entry_ts": trade.entry_ts,
                "exit_ts": trade.exit_ts,
                "decision_ts": trade.decision_ts,
                "entry_price": float(trade.entry_price),
                "exit_price": float(trade.exit_price),
                "stop_price": float(trade.stop_price),
                "pnl_cash": float(trade.pnl_cash),
                "mfe_points": float(trade.mfe_points),
                "mae_points": float(trade.mae_points),
                "hold_minutes": float(trade.hold_minutes),
                "bars_held_1m": int(trade.bars_held_1m),
                "side": trade.side,
                "session_segment": trade.session_segment,
                "family": trade.family,
                "exit_reason": trade.exit_reason,
                "added": False,
                "add_pnl_cash": 0.0,
                "add_reason": None,
                "add_price_quality_state": None,
            }
        )
    return rows


def _write_jsonl(path: Path, rows: Iterator[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _summary_from_trade_rows(trade_rows: Sequence[dict[str, Any]], *, bar_count: int) -> dict[str, Any]:
    position_rows = _base_position_rows(trade_rows)
    metrics = _trade_metrics(position_rows, bar_count=bar_count)
    distribution = summarize_trade_distribution_diagnostics(position_rows)
    pnl_values = [float(row["pnl_cash"]) for row in position_rows]
    asia_net = round(sum(float(row["pnl_cash"]) for row in position_rows if str(row["session_segment"]) == "ASIA"), 4)
    us_net = round(sum(float(row["pnl_cash"]) for row in position_rows if str(row["session_segment"]) == "US"), 4)
    return {
        "trade_count": int(metrics["total_trades"]),
        "net_pnl_cash": round(float(metrics["net_pnl_cash"]), 4),
        "average_trade_pnl_cash": round(float(metrics["average_trade_pnl_cash"]), 4),
        "median_trade_pnl_cash": round(float(median(pnl_values)), 4) if pnl_values else 0.0,
        "win_rate": round(float(metrics["win_rate"]), 4),
        "profit_factor": round(float(metrics["profit_factor"]), 4),
        "max_drawdown": round(float(metrics["max_drawdown"]), 4),
        "largest_win": round(max(pnl_values), 4) if pnl_values else 0.0,
        "largest_loss": round(min(pnl_values), 4) if pnl_values else 0.0,
        "max_consecutive_losers": int(distribution["max_consecutive_losses"]),
        "asia_net_pnl_cash": asia_net,
        "us_net_pnl_cash": us_net,
        "london_diagnostic_status": "DIAGNOSTIC_ONLY_NOT_EXECUTED",
    }


def _summary_row(summary: dict[str, Any], *, classification: str, scope_bundle_id: str, source_mode: str) -> dict[str, Any]:
    return {
        "baseline_id": "atp_companion_replay_baseline_v2",
        "classification": classification,
        "scope_bundle_id": scope_bundle_id,
        "source_mode": source_mode,
        **summary,
    }


def _validation_observed_from_replay_jsonl(path: Path) -> dict[str, Any]:
    rows = list(_iter_jsonl(path))
    position_rows = []
    for row in rows:
        trade = dict(row["trade_record"])
        position_rows.append(
            {
                "entry_ts": datetime.fromisoformat(str(trade["entry_ts"])),
                "decision_ts": datetime.fromisoformat(str(trade["decision_ts"])),
                "pnl_cash": float(trade["pnl_cash"]),
                "mfe_points": float(trade["mfe_points"]),
                "mae_points": float(trade["mae_points"]),
                "hold_minutes": float(trade["hold_minutes"]),
                "bars_held_1m": int(trade["bars_held_1m"]),
                "side": trade["side"],
                "session_segment": trade["session_segment"],
            }
        )
    metrics = _trade_metrics(position_rows, bar_count=max(len(rows), 1))
    distribution = summarize_trade_distribution_diagnostics(position_rows)
    pnl_values = [float(row["pnl_cash"]) for row in position_rows]
    return {
        "trade_count": int(metrics["total_trades"]),
        "net_pnl_cash": round(float(metrics["net_pnl_cash"]), 4),
        "average_trade_pnl_cash": round(float(metrics["average_trade_pnl_cash"]), 4),
        "median_trade_pnl_cash": round(float(median(pnl_values)), 4) if pnl_values else 0.0,
        "win_rate": round(float(metrics["win_rate"]), 4),
        "profit_factor": round(float(metrics["profit_factor"]), 4),
        "max_drawdown": round(float(metrics["max_drawdown"]), 4),
        "largest_win": round(max(pnl_values), 4) if pnl_values else 0.0,
        "largest_loss": round(min(pnl_values), 4) if pnl_values else 0.0,
        "max_consecutive_losers": int(distribution["max_consecutive_losses"]),
        "asia_net_pnl_cash": round(sum(float(row["pnl_cash"]) for row in position_rows if str(row["session_segment"]) == "ASIA"), 4),
        "us_net_pnl_cash": round(sum(float(row["pnl_cash"]) for row in position_rows if str(row["session_segment"]) == "US"), 4),
    }


def _validation_rows(expected: dict[str, Any], observed: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    tolerance = 0.0002
    for metric in (
        "trade_count",
        "net_pnl_cash",
        "average_trade_pnl_cash",
        "median_trade_pnl_cash",
        "win_rate",
        "profit_factor",
        "max_drawdown",
        "largest_win",
        "largest_loss",
        "max_consecutive_losers",
        "asia_net_pnl_cash",
        "us_net_pnl_cash",
    ):
        expected_value = expected[metric]
        observed_value = observed[metric]
        if isinstance(expected_value, int) and isinstance(observed_value, int):
            passed = expected_value == observed_value
            tol = 0
        else:
            passed = abs(float(expected_value) - float(observed_value)) <= tolerance
            tol = tolerance
        rows.append(
            {
                "metric": metric,
                "expected": expected_value,
                "observed": observed_value,
                "tolerance": tol,
                "passed": passed,
            }
        )
    return rows


def _classification(rows: Sequence[dict[str, Any]]) -> str:
    if all(bool(row["passed"]) for row in rows):
        return "REPLAY_BASELINE_V2_BUILT"
    if any(bool(row["passed"]) for row in rows):
        return "REPLAY_BASELINE_V2_PARTIAL"
    return "REPLAY_BASELINE_V2_BLOCKED"


def _render_markdown(*, summary: dict[str, Any], classification: str, manifest_path: Path, validation_rows: Sequence[dict[str, Any]]) -> str:
    lines = [
        "# ATP Companion Replay Baseline v2",
        "",
        f"- Classification: `{classification}`",
        "- Old ATP Companion v1 remains materialized-only and is not controlling truth for future research.",
        "- Current Replay Baseline v2 is replay-safe and reproduces itself from materialized replay records.",
        "- Future ATP enhancement research should use current replay-safe v2 as the controlling reproducible baseline unless we explicitly choose otherwise.",
        "- No threshold sensitivity, exit redesign, drawdown governance, live execution, IBKR, or broker changes were run in this build step.",
        f"- Replay manifest: `{manifest_path}`",
        "",
        "## Baseline Summary",
        f"- Trade count: `{summary['trade_count']}`",
        f"- Net P/L: `{summary['net_pnl_cash']}`",
        f"- Average trade: `{summary['average_trade_pnl_cash']}`",
        f"- Median trade: `{summary['median_trade_pnl_cash']}`",
        f"- Win rate: `{summary['win_rate']}`",
        f"- Profit factor: `{summary['profit_factor']}`",
        f"- Max drawdown: `{summary['max_drawdown']}`",
        f"- Largest win / loss: `{summary['largest_win']}` / `{summary['largest_loss']}`",
        f"- Max consecutive losers: `{summary['max_consecutive_losers']}`",
        f"- Asia contribution: `{summary['asia_net_pnl_cash']}`",
        f"- U.S. contribution: `{summary['us_net_pnl_cash']}`",
        f"- London status: `{summary['london_diagnostic_status']}`",
        "",
        "## Replay Validation",
    ]
    for row in validation_rows:
        lines.append(f"- `{row['metric']}` expected=`{row['expected']}` observed=`{row['observed']}` pass=`{row['passed']}`")
    return "\n".join(lines) + "\n"


def build_replay_baseline_v2(*, source_db: Path, output_dir: Path, scope_bundle_id: str | None = None) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    source_mode = "existing_scope_bundle"
    selected_bundle = scope_bundle_id
    if selected_bundle is None:
        latest = _find_latest_mgc_asia_us_scope_bundle()
        if latest is not None:
            selected_bundle = str(latest["bundle_id"])
        else:
            source_mode = "generated_scope_bundle"
            bar_source_index = _discover_best_sources(symbols={"MGC"}, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
            selection = bar_source_index["MGC"]["1m"]
            run_start = max(DEFAULT_MIN_START, datetime.fromisoformat(str(selection.start_ts)))
            run_end = datetime.fromisoformat(str(selection.end_ts))
            symbol_truth = _materialize_symbol_truth(
                bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
                source_db=source_db,
                symbol="MGC",
                bar_source_index=bar_source_index,
                start_timestamp=run_start,
                end_timestamp=run_end,
            )
            from .atp_companion_full_history_review import _evaluate_materialized_scope

            scope_truth = _evaluate_materialized_scope(
                bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
                symbol_truth=symbol_truth,
                allowed_sessions=("ASIA", "US"),
                point_value=10.0,
            )
            selected_bundle = str(scope_truth.scope_bundle_id)

    if selected_bundle is None:
        raise RuntimeError("Unable to identify or generate an MGC ASIA+US ATP scope bundle for Replay Baseline v2.")

    manifest = _read_manifest(_scope_bundle_manifest_path(selected_bundle))
    trade_rows = _load_scope_bundle(selected_bundle)[1]
    run_start, run_end = _manifest_1m_span(manifest)

    bar_source_index = _discover_best_sources(symbols={"MGC"}, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    context_bundle = ensure_symbol_context_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        symbol="MGC",
        bar_source_index=bar_source_index,
        start_timestamp=run_start,
        end_timestamp=run_end,
    )
    if context_bundle is None:
        raise RuntimeError("Replay Baseline v2 could not materialize the MGC 1m/5m context bundle for the selected scope span.")
    reference_candidate = next(candidate for candidate in default_atp_promotion_add_candidates() if candidate.candidate_id == "promotion_1_075r_favorable_only")
    trade_windows_by_scope = {
        ("MGC", ("ASIA", "US")): _trade_windows_by_id(bars_1m=context_bundle["bars_1m"], trade_rows=trade_rows),
    }
    baseline_target = next(target for target in build_targets() if target.target_id == "atp_companion_v1__benchmark_mgc_asia_us")

    manifest_payload = build_replay_truth_manifest(
        source_db=source_db,
        run_start=run_start,
        run_end=run_end,
        baseline_target=baseline_target,
        data_substrate={
            "selected_sources": manifest.get("selected_sources"),
            "scope_bundle_id": selected_bundle,
            "feature_bundle_id": manifest.get("feature_bundle_id"),
        },
        review_config={
            "baseline_id": "atp_companion_replay_baseline_v2",
            "scope_bundle_id": selected_bundle,
            "entry_activation_basis": manifest.get("entry_activation_basis"),
            "quality_bucket_policy": manifest.get("quality_bucket_policy"),
            "exit_policy": manifest.get("exit_policy"),
            "variant_overrides": manifest.get("variant_overrides") or {},
        },
        scope_bundle_ids={"MGC:ASIA/US": selected_bundle},
        feature_bundle_ids={"MGC": manifest.get("feature_bundle_id")},
        context_bundle_ids={"MGC": context_bundle["context_bundle_id"]},
        reference_candidate=reference_candidate,
    )

    replay_manifest_path = output_dir / "atp_companion_replay_baseline_v2_manifest.json"
    benchmark_rows_path = output_dir / "atp_companion_replay_baseline_v2_benchmark_position_rows.jsonl"
    replay_trades_path = output_dir / "atp_companion_replay_baseline_v2_trades.jsonl"
    summary_csv_path = output_dir / "atp_companion_replay_baseline_v2_summary.csv"
    summary_md_path = output_dir / "atp_companion_replay_baseline_v2_summary.md"
    validation_csv_path = output_dir / "atp_companion_replay_baseline_v2_validation_report.csv"
    validation_md_path = output_dir / "atp_companion_replay_baseline_v2_validation_report.md"
    validation_json_path = output_dir / "atp_companion_replay_baseline_v2_validation_report.json"

    _write_jsonl(benchmark_rows_path, iter(_base_position_rows(trade_rows)))
    _write_jsonl(
        replay_trades_path,
        iter_replay_truth_trade_rows(
            asia_scope=type(
                "ReplayBaselineScope",
                (),
                {
                    "symbol": "MGC",
                    "allowed_sessions": ("ASIA", "US"),
                    "point_value": float(manifest.get("point_value") or 10.0),
                    "trade_rows": trade_rows,
                },
            )(),
            trade_windows_by_scope=trade_windows_by_scope,
            reference_candidate=reference_candidate,
        ),
    )

    summary = _summary_from_trade_rows(trade_rows, bar_count=len(context_bundle["bars_1m"]))
    observed = _validation_observed_from_replay_jsonl(replay_trades_path)
    validation_rows = _validation_rows(summary, observed)
    classification = _classification(validation_rows)

    manifest_payload.update(
        {
            "baseline_id": "atp_companion_replay_baseline_v2",
            "classification": classification,
            "generator_module": "src/mgc_v05l/app/atp_companion_replay_baseline_v2.py",
            "command": f"PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.atp_companion_replay_baseline_v2 --output-dir {output_dir}",
            "instruments": ["MGC"],
            "sessions": ["ASIA", "US"],
            "session_treatment": {
                "ASIA": "EXECUTABLE_BASELINE_SCOPE",
                "US": "EXECUTABLE_BASELINE_SCOPE",
                "LONDON": "DIAGNOSTIC_ONLY_NOT_EXECUTED",
            },
            "entry_exit_semantics": {
                "execution_model": manifest.get("execution_model"),
                "entry_activation_basis": manifest.get("entry_activation_basis"),
                "exit_policy": manifest.get("exit_policy"),
                "quality_bucket_policy": manifest.get("quality_bucket_policy"),
            },
            "source_hashes": {
                "selected_sources_hash": stable_hash(manifest.get("selected_sources") or {}),
                "variant_overrides_hash": stable_hash(manifest.get("variant_overrides") or {}),
                "bundle_id": selected_bundle,
                "feature_bundle_id": manifest.get("feature_bundle_id"),
            },
            "artifacts": {
                "benchmark_position_rows_path": str(benchmark_rows_path.resolve()),
                "replay_trades_path": str(replay_trades_path.resolve()),
                "summary_csv_path": str(summary_csv_path.resolve()),
                "validation_csv_path": str(validation_csv_path.resolve()),
            },
            "summary_metrics": summary,
            "validation_reference": {
                "expected": summary,
                "observed": observed,
            },
            "source_mode": source_mode,
        }
    )
    replay_manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    _write_csv(summary_csv_path, [_summary_row(summary, classification=classification, scope_bundle_id=selected_bundle, source_mode=source_mode)])
    summary_md_path.write_text(_render_markdown(summary=summary, classification=classification, manifest_path=replay_manifest_path, validation_rows=validation_rows), encoding="utf-8")
    _write_csv(validation_csv_path, validation_rows)
    validation_md_path.write_text(_render_markdown(summary=summary, classification=classification, manifest_path=replay_manifest_path, validation_rows=validation_rows), encoding="utf-8")
    validation_json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "classification": classification,
                "expected": summary,
                "observed": observed,
                "validation_rows": validation_rows,
                "replay_manifest_path": str(replay_manifest_path.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "manifest_path": replay_manifest_path,
        "summary_csv_path": summary_csv_path,
        "summary_md_path": summary_md_path,
        "validation_csv_path": validation_csv_path,
        "validation_md_path": validation_md_path,
        "validation_json_path": validation_json_path,
        "replay_trades_path": replay_trades_path,
        "benchmark_rows_path": benchmark_rows_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    source_db = Path(args.source_db).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "mgc_asia_us_latest")
    artifacts = build_replay_baseline_v2(source_db=source_db, output_dir=output_dir, scope_bundle_id=args.scope_bundle_id)
    print(f"Wrote replay manifest -> {artifacts['manifest_path']}")
    print(f"Wrote replay trades -> {artifacts['replay_trades_path']}")
    print(f"Wrote benchmark rows -> {artifacts['benchmark_rows_path']}")
    print(f"Wrote summary csv -> {artifacts['summary_csv_path']}")
    print(f"Wrote validation csv -> {artifacts['validation_csv_path']}")


if __name__ == "__main__":
    main()
