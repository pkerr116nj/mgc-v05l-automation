"""Confirmation-policy research for filtered GC/MGC NY-early short trades."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
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
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short_confirmation_policy"


@dataclass(frozen=True)
class PolicySpec:
    policy_id: str
    description: str
    gc_probability_threshold: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-ny-early-short-confirmation-policy-research")
    parser.add_argument("--meta-json", default=str(DEFAULT_META_JSON), help="Path to NY-early short meta-label JSON.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_ny_early_short_confirmation_policy_research(
        meta_json=Path(args.meta_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_ny_early_short_confirmation_policy_research(
    *,
    meta_json: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_meta_json = Path(meta_json or DEFAULT_META_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    meta_report = json.loads(resolved_meta_json.read_text(encoding="utf-8"))
    selected_rows = _load_selected_test_rows(Path(meta_report["dataset_path"]))
    policies = build_policy_specs()

    baseline_all = _performance_summary([row["net_pnl_points"] for row in selected_rows])
    baseline_gc = _performance_summary([row["net_pnl_points"] for row in selected_rows if row["symbol"] == "GC"])
    baseline_mgc = _performance_summary([row["net_pnl_points"] for row in selected_rows if row["symbol"] == "MGC"])
    by_date = _group_by_date(selected_rows)

    policy_reports = []
    for policy in policies:
        mgc_rows = [
            date_rows["MGC"]
            for date_rows in by_date.values()
            if "MGC" in date_rows and "GC" in date_rows and date_rows["GC"]["predicted_probability"] >= policy.gc_probability_threshold
        ]
        gc_rows = [
            date_rows["GC"]
            for date_rows in by_date.values()
            if "GC" in date_rows and date_rows["GC"]["predicted_probability"] >= policy.gc_probability_threshold
        ]
        both_rows = [
            trade
            for date_rows in by_date.values()
            if "MGC" in date_rows and "GC" in date_rows and date_rows["GC"]["predicted_probability"] >= policy.gc_probability_threshold
            for trade in (date_rows["GC"], date_rows["MGC"])
        ]
        policy_reports.append(
            {
                **asdict(policy),
                "gc_selected_summary": _performance_summary([row["net_pnl_points"] for row in gc_rows]),
                "mgc_execution_summary": _performance_summary([row["net_pnl_points"] for row in mgc_rows]),
                "dual_execution_summary": _performance_summary([row["net_pnl_points"] for row in both_rows]),
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_ny_early_short_confirmation_policy",
        "meta_json": str(resolved_meta_json),
        "baseline_all_selected": baseline_all,
        "baseline_gc_selected": baseline_gc,
        "baseline_mgc_selected": baseline_mgc,
        "policy_reports": policy_reports,
    }
    json_path = resolved_output_dir / "gc_mgc_ny_early_short_confirmation_policy_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_ny_early_short_confirmation_policy_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_ny_early_short_confirmation_policy_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "policy_reports": policy_reports,
    }


def build_policy_specs() -> tuple[PolicySpec, ...]:
    return (
        PolicySpec(
            policy_id="gc_confirm_065",
            description="Require GC selected and GC predicted probability >= 0.65 before taking MGC.",
            gc_probability_threshold=0.65,
        ),
        PolicySpec(
            policy_id="gc_confirm_070",
            description="Require GC selected and GC predicted probability >= 0.70 before taking MGC.",
            gc_probability_threshold=0.70,
        ),
        PolicySpec(
            policy_id="gc_confirm_075",
            description="Require GC selected and GC predicted probability >= 0.75 before taking MGC.",
            gc_probability_threshold=0.75,
        ),
        PolicySpec(
            policy_id="gc_confirm_080",
            description="Require GC selected and GC predicted probability >= 0.80 before taking MGC.",
            gc_probability_threshold=0.80,
        ),
    )


def _load_selected_test_rows(dataset_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["split"] != "test":
                continue
            if str(row["selected"]).lower() != "true":
                continue
            rows.append(
                {
                    "source_variant": row["source_variant"],
                    "symbol": row["symbol"],
                    "trade_date": row["trade_date"],
                    "predicted_probability": float(row["predicted_probability"]),
                    "net_pnl_points": float(row["net_pnl_points"]),
                }
            )
    return rows


def _group_by_date(rows: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        grouped[row["trade_date"]][row["symbol"]] = row
    return grouped


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
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        median = sorted_values[midpoint]
    else:
        median = (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0
    return {
        "trade_count": len(values),
        "average_net_pnl_points": round(sum(values) / len(values), 4),
        "median_net_pnl_points": round(median, 4),
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "win_rate": round(len(winners) / len(values), 4),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC NY Early Short Confirmation Policy Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Meta JSON: `{payload['meta_json']}`",
        "",
        "## Baseline",
        "",
        f"- All selected: `{payload['baseline_all_selected']}`",
        f"- GC selected: `{payload['baseline_gc_selected']}`",
        f"- MGC selected: `{payload['baseline_mgc_selected']}`",
        "",
        "## Policies",
        "",
    ]
    for policy in payload["policy_reports"]:
        lines.append(f"### {policy['policy_id']}")
        lines.append("")
        lines.append(f"- {policy['description']}")
        lines.append(f"- GC-selected summary: `{policy['gc_selected_summary']}`")
        lines.append(f"- MGC execution summary: `{policy['mgc_execution_summary']}`")
        lines.append(f"- Dual execution summary: `{policy['dual_execution_summary']}`")
        lines.append("")
    return "\n".join(lines)
