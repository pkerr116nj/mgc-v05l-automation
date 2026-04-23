"""Meta-label research for GC/MGC London-late long candidate pools."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_JSON = (
    REPO_ROOT / "outputs" / "reports" / "gc_mgc_london_late_long_archive_v4" / "gc_mgc_london_late_long_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_london_late_meta_label"
DEFAULT_SOURCE_VARIANTS = ("london_late_long_v5_3m_soft", "london_late_long_v7_3m_meta")
FEATURE_NAMES = (
    "pre_context_to_setup_range_ratio",
    "setup_abs_efficiency",
    "setup_close_location",
    "setup_vwap_displacement",
    "setup_green_share",
    "setup_volume_ratio",
    "setup_score",
)


@dataclass(frozen=True)
class CandidateRow:
    symbol: str
    trade_date: str
    source_variant: str
    label: int
    net_pnl_points: float
    features: dict[str, float]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-london-late-meta-label-research")
    parser.add_argument(
        "--source-json",
        default=str(DEFAULT_SOURCE_JSON),
        help="Path to a gc_mgc_london_late_long_research JSON artifact.",
    )
    parser.add_argument(
        "--source-variant",
        action="append",
        default=None,
        help="Source candidate variant to model. May be supplied multiple times.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    parser.add_argument("--train-fraction", type=float, default=0.7, help="Chronological training split fraction.")
    parser.add_argument(
        "--signal-symbol",
        default=None,
        help="Optional signal-discovery symbol to train on, e.g. GC.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_london_late_meta_label_research(
        source_json=Path(args.source_json),
        source_variants=tuple(args.source_variant or DEFAULT_SOURCE_VARIANTS),
        output_dir=Path(args.output_dir),
        train_fraction=float(args.train_fraction),
        signal_symbol=str(args.signal_symbol).strip().upper() if args.signal_symbol else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_london_late_meta_label_research(
    *,
    source_json: Path | None = None,
    source_variants: tuple[str, ...] | None = DEFAULT_SOURCE_VARIANTS,
    output_dir: Path | None = DEFAULT_OUTPUT_DIR,
    train_fraction: float = 0.7,
    signal_symbol: str | None = None,
) -> dict[str, Any]:
    resolved_source_json = Path(source_json or DEFAULT_SOURCE_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_variants = tuple(source_variants or DEFAULT_SOURCE_VARIANTS)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    report = json.loads(resolved_source_json.read_text(encoding="utf-8"))

    dataset_rows: list[dict[str, Any]] = []
    variant_reports: list[dict[str, Any]] = []
    for source_variant in resolved_variants:
        candidates = _extract_candidates(report=report, source_variant=source_variant)
        modeling_candidates = [row for row in candidates if row.symbol == signal_symbol] if signal_symbol else candidates
        if len(modeling_candidates) < 40:
            variant_reports.append(
                {
                    "source_variant": source_variant,
                    "status": "insufficient_candidates",
                    "candidate_count": len(modeling_candidates),
                }
            )
            continue
        split = _split_candidates(modeling_candidates, train_fraction=train_fraction)
        model = _train_logistic_regression(split["train"])
        threshold = _select_probability_threshold(
            rows=split["train"],
            model=model,
            minimum_fraction=0.2,
        )
        variant_dataset_rows = _materialize_dataset_rows(
            source_variant=source_variant,
            model=model,
            threshold=threshold,
            train_rows=split["train"],
            test_rows=split["test"],
            all_rows=candidates,
            signal_symbol=signal_symbol,
        )
        dataset_rows.extend(variant_dataset_rows)
        variant_reports.append(
            _variant_report(
                source_variant=source_variant,
                model=model,
                threshold=threshold,
                train_rows=split["train"],
                test_rows=split["test"],
                all_rows=candidates,
                signal_symbol=signal_symbol,
            )
        )

    dataset_path = resolved_output_dir / "gc_mgc_london_late_meta_label_dataset.csv"
    _write_dataset_csv(dataset_path, dataset_rows)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_london_late_meta_label",
        "source_json": str(resolved_source_json),
        "source_variants": list(resolved_variants),
        "signal_symbol": signal_symbol,
        "feature_names": list(FEATURE_NAMES),
        "dataset_path": str(dataset_path),
        "variant_reports": variant_reports,
    }
    json_path = resolved_output_dir / "gc_mgc_london_late_meta_label_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_london_late_meta_label_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_london_late_meta_label_research",
        "artifact_paths": {
            "json": str(json_path),
            "markdown": str(markdown_path),
            "dataset_csv": str(dataset_path),
        },
        "variant_reports": variant_reports,
    }


def _extract_candidates(*, report: dict[str, Any], source_variant: str) -> list[CandidateRow]:
    rows: list[CandidateRow] = []
    for symbol, payload in report["symbol_reports"].items():
        sessions = payload["variants"][source_variant]["sessions"]
        for session in sessions:
            if not session.get("entered") or session.get("net_pnl_points") is None:
                continue
            features = {
                name: float(session[name])
                for name in FEATURE_NAMES
                if session.get(name) is not None
            }
            if len(features) != len(FEATURE_NAMES):
                continue
            rows.append(
                CandidateRow(
                    symbol=symbol,
                    trade_date=str(session["trade_date"]),
                    source_variant=source_variant,
                    label=1 if float(session["net_pnl_points"]) > 0.0 else 0,
                    net_pnl_points=float(session["net_pnl_points"]),
                    features=features,
                )
            )
    rows.sort(key=lambda row: (row.trade_date, row.symbol))
    return rows


def _split_candidates(rows: list[CandidateRow], *, train_fraction: float) -> dict[str, list[CandidateRow]]:
    unique_dates = sorted({row.trade_date for row in rows})
    train_date_count = max(1, min(len(unique_dates) - 1, math.floor(len(unique_dates) * train_fraction)))
    train_dates = set(unique_dates[:train_date_count])
    train = [row for row in rows if row.trade_date in train_dates]
    test = [row for row in rows if row.trade_date not in train_dates]
    return {"train": train, "test": test}


def _train_logistic_regression(rows: list[CandidateRow]) -> dict[str, Any]:
    means: dict[str, float] = {}
    stdevs: dict[str, float] = {}
    for feature_name in FEATURE_NAMES:
        values = [row.features[feature_name] for row in rows]
        means[feature_name] = statistics.fmean(values)
        stdevs[feature_name] = statistics.pstdev(values) or 1.0

    weights = {feature_name: 0.0 for feature_name in FEATURE_NAMES}
    intercept = 0.0
    learning_rate = 0.08
    l2 = 0.01
    for _ in range(1200):
        grad_b = 0.0
        grad_w = {feature_name: 0.0 for feature_name in FEATURE_NAMES}
        for row in rows:
            score = intercept + sum(
                weights[feature_name] * _standardize(row.features[feature_name], means[feature_name], stdevs[feature_name])
                for feature_name in FEATURE_NAMES
            )
            prob = _sigmoid(score)
            error = prob - row.label
            grad_b += error
            for feature_name in FEATURE_NAMES:
                grad_w[feature_name] += error * _standardize(
                    row.features[feature_name],
                    means[feature_name],
                    stdevs[feature_name],
                )
        scale = max(len(rows), 1)
        intercept -= learning_rate * (grad_b / scale)
        for feature_name in FEATURE_NAMES:
            penalty = l2 * weights[feature_name]
            weights[feature_name] -= learning_rate * ((grad_w[feature_name] / scale) + penalty)

    return {
        "intercept": intercept,
        "weights": weights,
        "means": means,
        "stdevs": stdevs,
    }


def _predict_probability(row: CandidateRow, *, model: dict[str, Any]) -> float:
    score = float(model["intercept"])
    for feature_name in FEATURE_NAMES:
        score += float(model["weights"][feature_name]) * _standardize(
            row.features[feature_name],
            float(model["means"][feature_name]),
            float(model["stdevs"][feature_name]),
        )
    return _sigmoid(score)


def _select_probability_threshold(
    *,
    rows: list[CandidateRow],
    model: dict[str, Any],
    minimum_fraction: float,
) -> float:
    minimum_count = max(12, math.floor(len(rows) * minimum_fraction))
    best_threshold = 0.5
    best_score = float("-inf")
    baseline_avg = _average_net_pnl(rows)
    for threshold in [0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75]:
        selected = [row for row in rows if _predict_probability(row, model=model) >= threshold]
        if len(selected) < minimum_count:
            continue
        avg_net = _average_net_pnl(selected)
        if avg_net <= baseline_avg:
            continue
        score = avg_net * math.sqrt(len(selected))
        if score > best_score:
            best_threshold = threshold
            best_score = score
    return best_threshold


def _materialize_dataset_rows(
    *,
    source_variant: str,
    model: dict[str, Any],
    threshold: float,
    train_rows: list[CandidateRow],
    test_rows: list[CandidateRow],
    all_rows: list[CandidateRow],
    signal_symbol: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    paired_test_by_date = _paired_rows_by_date(
        signal_test_rows=test_rows,
        all_rows=all_rows,
        model=model,
        threshold=threshold,
        signal_symbol=signal_symbol,
    )
    for split_name, split_rows in (("train", train_rows), ("test", test_rows)):
        for row in split_rows:
            probability = _predict_probability(row, model=model)
            rows.append(
                {
                    "source_variant": source_variant,
                    "split": split_name,
                    "symbol": row.symbol,
                    "trade_date": row.trade_date,
                    "label": row.label,
                    "net_pnl_points": row.net_pnl_points,
                    "predicted_probability": round(probability, 6),
                    "selected": probability >= threshold,
                    **row.features,
                }
            )
    if signal_symbol is not None:
        for trade_date, paired_rows in paired_test_by_date.items():
            for row in paired_rows:
                rows.append(
                    {
                        "source_variant": source_variant,
                        "split": "paired_test",
                        "symbol": row.symbol,
                        "trade_date": row.trade_date,
                        "label": row.label,
                        "net_pnl_points": row.net_pnl_points,
                        "predicted_probability": None,
                        "selected": True,
                        **row.features,
                    }
                )
    return rows


def _variant_report(
    *,
    source_variant: str,
    model: dict[str, Any],
    threshold: float,
    train_rows: list[CandidateRow],
    test_rows: list[CandidateRow],
    all_rows: list[CandidateRow],
    signal_symbol: str | None,
) -> dict[str, Any]:
    report = {
        "source_variant": source_variant,
        "threshold": threshold,
        "signal_symbol": signal_symbol,
        "feature_weights": {
            feature_name: round(float(model["weights"][feature_name]), 6)
            for feature_name in sorted(FEATURE_NAMES, key=lambda name: abs(float(model["weights"][name])), reverse=True)
        },
        "train_summary": _split_summary(train_rows, model=model, threshold=threshold),
        "test_summary": _split_summary(test_rows, model=model, threshold=threshold),
    }
    if signal_symbol is not None:
        report["paired_execution_test_summary"] = _paired_execution_summary(
            signal_test_rows=test_rows,
            all_rows=all_rows,
            model=model,
            threshold=threshold,
            signal_symbol=signal_symbol,
        )
    return report


def _split_summary(rows: list[CandidateRow], *, model: dict[str, Any], threshold: float) -> dict[str, Any]:
    selected = [row for row in rows if _predict_probability(row, model=model) >= threshold]
    by_symbol = {}
    for symbol in sorted({row.symbol for row in rows}):
        symbol_rows = [row for row in rows if row.symbol == symbol]
        symbol_selected = [row for row in selected if row.symbol == symbol]
        by_symbol[symbol] = {
            "baseline": _performance_summary(symbol_rows),
            "filtered": _performance_summary(symbol_selected),
        }
    return {
        "candidate_count": len(rows),
        "selected_count": len(selected),
        "selection_rate": round(len(selected) / len(rows), 4) if rows else None,
        "baseline": _performance_summary(rows),
        "filtered": _performance_summary(selected),
        "by_symbol": by_symbol,
    }


def _paired_execution_summary(
    *,
    signal_test_rows: list[CandidateRow],
    all_rows: list[CandidateRow],
    model: dict[str, Any],
    threshold: float,
    signal_symbol: str,
) -> dict[str, Any]:
    selected_dates = {
        row.trade_date for row in signal_test_rows if _predict_probability(row, model=model) >= threshold
    }
    test_dates = {row.trade_date for row in signal_test_rows}
    execution_symbols = sorted({row.symbol for row in all_rows if row.symbol != signal_symbol})
    by_symbol: dict[str, Any] = {}
    combined_selected: list[CandidateRow] = []
    combined_baseline: list[CandidateRow] = []
    for symbol in execution_symbols:
        baseline_rows = [row for row in all_rows if row.symbol == symbol and row.trade_date in test_dates]
        selected_rows = [row for row in baseline_rows if row.trade_date in selected_dates]
        combined_baseline.extend(baseline_rows)
        combined_selected.extend(selected_rows)
        by_symbol[symbol] = {
            "baseline": _performance_summary(baseline_rows),
            "filtered": _performance_summary(selected_rows),
        }
    return {
        "signal_selected_trade_dates": len(selected_dates),
        "baseline": _performance_summary(combined_baseline),
        "filtered": _performance_summary(combined_selected),
        "by_symbol": by_symbol,
    }


def _paired_rows_by_date(
    *,
    signal_test_rows: list[CandidateRow],
    all_rows: list[CandidateRow],
    model: dict[str, Any],
    threshold: float,
    signal_symbol: str | None,
) -> dict[str, list[CandidateRow]]:
    if signal_symbol is None:
        return {}
    selected_dates = {
        row.trade_date for row in signal_test_rows if _predict_probability(row, model=model) >= threshold
    }
    paired: dict[str, list[CandidateRow]] = {}
    for trade_date in selected_dates:
        paired[trade_date] = [
            row
            for row in all_rows
            if row.trade_date == trade_date and row.symbol != signal_symbol
        ]
    return paired


def _performance_summary(rows: list[CandidateRow]) -> dict[str, Any]:
    winners = [row for row in rows if row.net_pnl_points > 0.0]
    losers = [row for row in rows if row.net_pnl_points <= 0.0]
    gross_profit = sum(row.net_pnl_points for row in winners)
    gross_loss = abs(sum(row.net_pnl_points for row in losers))
    return {
        "trade_count": len(rows),
        "win_rate": round(len(winners) / len(rows), 4) if rows else None,
        "average_net_pnl_points": round(_average_net_pnl(rows), 4) if rows else None,
        "median_net_pnl_points": round(statistics.median(row.net_pnl_points for row in rows), 4) if rows else None,
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
    }


def _average_net_pnl(rows: list[CandidateRow]) -> float:
    return statistics.fmean(row.net_pnl_points for row in rows) if rows else 0.0


def _write_dataset_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _standardize(value: float, mean: float, stdev: float) -> float:
    return (value - mean) / stdev if stdev else 0.0


def _sigmoid(value: float) -> float:
    clipped = max(min(value, 30.0), -30.0)
    return 1.0 / (1.0 + math.exp(-clipped))


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC London Late Meta-Label Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Source JSON: `{payload['source_json']}`",
        "",
    ]
    for variant in payload["variant_reports"]:
        lines.append(f"## {variant['source_variant']}")
        if variant.get("status") == "insufficient_candidates":
            lines.append("")
            lines.append(f"- Insufficient candidates: `{variant['candidate_count']}`")
            lines.append("")
            continue
        lines.append("")
        lines.append(f"- Probability threshold: `{variant['threshold']}`")
        lines.append("- Top feature weights:")
        for feature_name, weight in variant["feature_weights"].items():
            lines.append(f"  - `{feature_name}`: `{weight}`")
        lines.append("- Test baseline vs filtered:")
        test_summary = variant["test_summary"]
        lines.append(
            f"  - baseline trades `{test_summary['baseline']['trade_count']}`, avg net `{test_summary['baseline']['average_net_pnl_points']}`, PF `{test_summary['baseline']['net_profit_factor']}`"
        )
        lines.append(
            f"  - filtered trades `{test_summary['filtered']['trade_count']}`, avg net `{test_summary['filtered']['average_net_pnl_points']}`, PF `{test_summary['filtered']['net_profit_factor']}`"
        )
        paired = variant.get("paired_execution_test_summary")
        if paired is not None:
            lines.append("- Paired execution on selected signal dates:")
            lines.append(
                f"  - filtered trades `{paired['filtered']['trade_count']}`, avg net `{paired['filtered']['average_net_pnl_points']}`, PF `{paired['filtered']['net_profit_factor']}`"
            )
        lines.append("")
    return "\n".join(lines)
