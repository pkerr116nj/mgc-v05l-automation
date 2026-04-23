"""Rolling walk-forward research for GC/MGC NY-early short policies."""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .gc_mgc_ny_early_short_meta_label_research import (
    CandidateRow,
    _extract_candidates,
    _predict_probability,
    _train_logistic_regression,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_JSON = (
    REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short_archive_v1" / "gc_mgc_ny_early_short_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short_walkforward"
DEFAULT_SOURCE_VARIANT = "ny_early_short_v2_failed_pop_3m"
DEFAULT_MIN_TRAIN_DATES = 20
DEFAULT_TEST_WINDOW_DATES = 5


@dataclass(frozen=True)
class PolicySpec:
    policy_id: str
    description: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-ny-early-short-walkforward-research")
    parser.add_argument("--source-json", default=str(DEFAULT_SOURCE_JSON), help="Path to NY-early short research JSON.")
    parser.add_argument("--source-variant", default=DEFAULT_SOURCE_VARIANT, help="Source candidate variant id.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    parser.add_argument("--min-train-dates", type=int, default=DEFAULT_MIN_TRAIN_DATES, help="Minimum dates in first train window.")
    parser.add_argument("--test-window-dates", type=int, default=DEFAULT_TEST_WINDOW_DATES, help="Dates per test fold.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_ny_early_short_walkforward_research(
        source_json=Path(args.source_json),
        source_variant=str(args.source_variant),
        output_dir=Path(args.output_dir),
        min_train_dates=int(args.min_train_dates),
        test_window_dates=int(args.test_window_dates),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_ny_early_short_walkforward_research(
    *,
    source_json: Path | None = None,
    source_variant: str = DEFAULT_SOURCE_VARIANT,
    output_dir: Path | None = None,
    min_train_dates: int = DEFAULT_MIN_TRAIN_DATES,
    test_window_dates: int = DEFAULT_TEST_WINDOW_DATES,
) -> dict[str, Any]:
    resolved_source_json = Path(source_json or DEFAULT_SOURCE_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    report = json.loads(resolved_source_json.read_text(encoding="utf-8"))
    candidates = _extract_candidates(report=report, source_variant=source_variant)
    unique_dates = sorted({row.trade_date for row in candidates})
    folds = _build_walkforward_folds(unique_dates, min_train_dates=min_train_dates, test_window_dates=test_window_dates)
    policies = build_policy_specs()

    fold_reports: list[dict[str, Any]] = []
    aggregate_policy_results: dict[str, list[float]] = {policy.policy_id: [] for policy in policies}
    for fold_index, fold in enumerate(folds, start=1):
        train_rows = [row for row in candidates if row.trade_date in fold["train_dates"]]
        test_rows = [row for row in candidates if row.trade_date in fold["test_dates"]]
        model = _train_logistic_regression(train_rows)
        predicted = [
            {
                "row": row,
                "probability": _predict_probability(row, model=model),
            }
            for row in test_rows
        ]
        by_date_symbol = {
            (item["row"].trade_date, item["row"].symbol): item
            for item in predicted
        }
        policy_summaries = {}
        for policy in policies:
            selected = _apply_policy(policy=policy, predicted_rows=predicted, by_date_symbol=by_date_symbol)
            net_pnls = [item["row"].net_pnl_points for item in selected]
            aggregate_policy_results[policy.policy_id].extend(net_pnls)
            policy_summaries[policy.policy_id] = {
                "description": policy.description,
                "summary": _performance_summary(net_pnls),
            }
        fold_reports.append(
            {
                "fold_index": fold_index,
                "train_start_date": fold["train_dates"][0],
                "train_end_date": fold["train_dates"][-1],
                "test_start_date": fold["test_dates"][0],
                "test_end_date": fold["test_dates"][-1],
                "train_date_count": len(fold["train_dates"]),
                "test_date_count": len(fold["test_dates"]),
                "train_trade_count": len(train_rows),
                "test_trade_count": len(test_rows),
                "policy_summaries": policy_summaries,
            }
        )

    aggregate_policy_reports = {
        policy.policy_id: {
            "description": policy.description,
            "summary": _performance_summary(aggregate_policy_results[policy.policy_id]),
        }
        for policy in policies
    }
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_ny_early_short_walkforward",
        "source_json": str(resolved_source_json),
        "source_variant": source_variant,
        "min_train_dates": min_train_dates,
        "test_window_dates": test_window_dates,
        "fold_count": len(fold_reports),
        "fold_reports": fold_reports,
        "aggregate_policy_reports": aggregate_policy_reports,
    }
    json_path = resolved_output_dir / "gc_mgc_ny_early_short_walkforward_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_ny_early_short_walkforward_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_ny_early_short_walkforward_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "fold_count": len(fold_reports),
        "aggregate_policy_reports": aggregate_policy_reports,
    }


def build_policy_specs() -> tuple[PolicySpec, ...]:
    return (
        PolicySpec(
            policy_id="pooled_p_060",
            description="Take all test trades with predicted probability >= 0.60.",
        ),
        PolicySpec(
            policy_id="pooled_p_065",
            description="Take all test trades with predicted probability >= 0.65.",
        ),
        PolicySpec(
            policy_id="mgc_with_gc_confirm_070",
            description="Take MGC only when MGC probability >= 0.60 and same-date GC probability >= 0.70.",
        ),
    )


def _build_walkforward_folds(unique_dates: list[str], *, min_train_dates: int, test_window_dates: int) -> list[dict[str, list[str]]]:
    folds: list[dict[str, list[str]]] = []
    start = min_train_dates
    while start < len(unique_dates):
        test_dates = unique_dates[start : start + test_window_dates]
        if not test_dates:
            break
        train_dates = unique_dates[:start]
        folds.append({"train_dates": train_dates, "test_dates": test_dates})
        start += test_window_dates
    return folds


def _apply_policy(
    *,
    policy: PolicySpec,
    predicted_rows: list[dict[str, Any]],
    by_date_symbol: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    if policy.policy_id == "pooled_p_060":
        return [item for item in predicted_rows if item["probability"] >= 0.60]
    if policy.policy_id == "pooled_p_065":
        return [item for item in predicted_rows if item["probability"] >= 0.65]
    if policy.policy_id == "mgc_with_gc_confirm_070":
        selected: list[dict[str, Any]] = []
        for item in predicted_rows:
            row: CandidateRow = item["row"]
            if row.symbol != "MGC":
                continue
            if item["probability"] < 0.60:
                continue
            gc_item = by_date_symbol.get((row.trade_date, "GC"))
            if gc_item is None:
                continue
            if gc_item["probability"] < 0.70:
                continue
            selected.append(item)
        return selected
    return []


def _performance_summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "trade_count": 0,
            "average_net_pnl_points": None,
            "median_net_pnl_points": None,
            "net_profit_factor": None,
            "win_rate": None,
        }
    winners = [value for value in values if value > 0.0]
    losers = [value for value in values if value <= 0.0]
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    return {
        "trade_count": len(values),
        "average_net_pnl_points": round(statistics.fmean(values), 4),
        "median_net_pnl_points": round(statistics.median(values), 4),
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "win_rate": round(len(winners) / len(values), 4),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC NY Early Short Walk-Forward Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Source JSON: `{payload['source_json']}`",
        f"- Source variant: `{payload['source_variant']}`",
        f"- Folds: `{payload['fold_count']}`",
        "",
        "## Aggregate Policies",
        "",
    ]
    for policy_id, payload_row in payload["aggregate_policy_reports"].items():
        lines.append(f"- `{policy_id}`: `{payload_row['summary']}`")
    lines.extend(["", "## Folds", ""])
    for fold in payload["fold_reports"]:
        lines.append(
            f"- Fold {fold['fold_index']}: train `{fold['train_start_date']}` to `{fold['train_end_date']}`, test `{fold['test_start_date']}` to `{fold['test_end_date']}`"
        )
        for policy_id, summary in fold["policy_summaries"].items():
            lines.append(f"  - `{policy_id}`: `{summary['summary']}`")
    return "\n".join(lines)
