"""Threshold frontier research for GC/MGC NY-early short meta-label outputs."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_META_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "gc_mgc_ny_early_short_meta_label_archive_v1"
    / "gc_mgc_ny_early_short_meta_label_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short_meta_threshold"
DEFAULT_THRESHOLDS = (0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80)


@dataclass(frozen=True)
class ThresholdRow:
    threshold: float
    trade_count: int
    average_net_pnl_points: float | None
    median_net_pnl_points: float | None
    net_profit_factor: float | None
    win_rate: float | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-ny-early-short-meta-threshold-research")
    parser.add_argument("--meta-json", default=str(DEFAULT_META_JSON), help="Path to NY-early short meta-label JSON.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_ny_early_short_meta_threshold_research(
        meta_json=Path(args.meta_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_ny_early_short_meta_threshold_research(
    *,
    meta_json: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_meta_json = Path(meta_json or DEFAULT_META_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    meta_report = json.loads(resolved_meta_json.read_text(encoding="utf-8"))
    dataset_path = Path(meta_report["dataset_path"])
    rows = _load_test_rows(dataset_path)

    overall = [_threshold_row(rows, threshold) for threshold in DEFAULT_THRESHOLDS]
    by_symbol = {
        symbol: [_threshold_row([row for row in rows if row["symbol"] == symbol], threshold) for threshold in DEFAULT_THRESHOLDS]
        for symbol in sorted({row["symbol"] for row in rows})
    }
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_ny_early_short_meta_threshold",
        "meta_json": str(resolved_meta_json),
        "dataset_path": str(dataset_path),
        "thresholds": list(DEFAULT_THRESHOLDS),
        "overall": [asdict(row) for row in overall],
        "by_symbol": {symbol: [asdict(row) for row in rows_] for symbol, rows_ in by_symbol.items()},
    }
    json_path = resolved_output_dir / "gc_mgc_ny_early_short_meta_threshold_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_ny_early_short_meta_threshold_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_ny_early_short_meta_threshold_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "overall": [asdict(row) for row in overall],
    }


def _load_test_rows(dataset_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["split"] != "test":
                continue
            rows.append(
                {
                    "symbol": row["symbol"],
                    "trade_date": row["trade_date"],
                    "predicted_probability": float(row["predicted_probability"]),
                    "net_pnl_points": float(row["net_pnl_points"]),
                }
            )
    return rows


def _threshold_row(rows: list[dict[str, Any]], threshold: float) -> ThresholdRow:
    selected = [row["net_pnl_points"] for row in rows if row["predicted_probability"] >= threshold]
    if not selected:
        return ThresholdRow(
            threshold=threshold,
            trade_count=0,
            average_net_pnl_points=None,
            median_net_pnl_points=None,
            net_profit_factor=None,
            win_rate=None,
        )
    winners = [value for value in selected if value > 0.0]
    losers = [value for value in selected if value <= 0.0]
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    ordered = sorted(selected)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        median = ordered[midpoint]
    else:
        median = (ordered[midpoint - 1] + ordered[midpoint]) / 2.0
    return ThresholdRow(
        threshold=threshold,
        trade_count=len(selected),
        average_net_pnl_points=round(sum(selected) / len(selected), 4),
        median_net_pnl_points=round(median, 4),
        net_profit_factor=round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        win_rate=round(len(winners) / len(selected), 4),
    )


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC NY Early Short Meta Threshold Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Meta JSON: `{payload['meta_json']}`",
        "",
        "## Overall",
        "",
    ]
    for row in payload["overall"]:
        lines.append(
            f"- `p >= {row['threshold']}`: trades `{row['trade_count']}`, avg net `{row['average_net_pnl_points']}`, PF `{row['net_profit_factor']}`, win rate `{row['win_rate']}`"
        )
    lines.extend(["", "## By Symbol", ""])
    for symbol, rows in payload["by_symbol"].items():
        lines.append(f"### {symbol}")
        lines.append("")
        for row in rows:
            lines.append(
                f"- `p >= {row['threshold']}`: trades `{row['trade_count']}`, avg net `{row['average_net_pnl_points']}`, PF `{row['net_profit_factor']}`, win rate `{row['win_rate']}`"
            )
        lines.append("")
    return "\n".join(lines)
