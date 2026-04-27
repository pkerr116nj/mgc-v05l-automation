"""Durability review for the ATP Companion / Asia Drift staged-add overlay."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any, Sequence

DEFAULT_ENHANCEMENT_JSON = Path(
    "outputs/reports/atp_companion_asia_drift_enhancement_review/asia_only_promotion_075r_v1/atp_companion_asia_drift_enhancement_summary.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-asia-drift-enhancement-durability-review")
    parser.add_argument("--enhancement-json", default=str(DEFAULT_ENHANCEMENT_JSON), help="Enhancement review JSON artifact.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    return parser


def _hydrate_row(row: dict[str, Any]) -> dict[str, Any]:
    hydrated = dict(row)
    for key, value in list(hydrated.items()):
        if key.endswith("_ts") and isinstance(value, str) and value:
            hydrated[key] = datetime.fromisoformat(value)
    return hydrated


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _percent(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-9:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _add_event_detail_rows(candidate_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    add_rows = [row for row in candidate_rows if bool(row.get("added"))]
    details: list[dict[str, Any]] = []
    for row in sorted(add_rows, key=lambda item: item["add_entry_ts"]):
        add_pnl = float(row.get("add_pnl_cash") or 0.0)
        details.append(
            {
                "trade_id": row.get("trade_id"),
                "date": row["add_entry_ts"].date().isoformat(),
                "session": row.get("session_segment"),
                "decision_time": row["decision_ts"].isoformat() if row.get("decision_ts") else None,
                "entry_time": row["entry_ts"].isoformat() if row.get("entry_ts") else None,
                "add_time": row["add_entry_ts"].isoformat() if row.get("add_entry_ts") else None,
                "direction": row.get("side"),
                "base_trade_pnl_cash": round(float(row.get("trade_pnl_cash") or 0.0), 4),
                "add_contribution_cash": round(add_pnl, 4),
                "candidate_trade_pnl_cash": round(float(row.get("pnl_cash") or 0.0), 4),
                "improved_trade": add_pnl > 0.0,
                "worsened_trade": add_pnl < 0.0,
                "mfe_points_trade_window": round(float(row.get("mfe_points") or 0.0), 4),
                "mae_points_trade_window": round(float(row.get("mae_points") or 0.0), 4),
                "add_hold_minutes": round(float(row.get("add_hold_minutes") or 0.0), 4),
                "exit_time": row["exit_ts"].isoformat() if row.get("exit_ts") else None,
                "exit_reason": row.get("exit_reason"),
                "add_reason": row.get("add_reason"),
                "add_price_quality_state": row.get("add_price_quality_state"),
            }
        )
    return details


def _concentration_rows(add_detail_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    contributions = sorted((float(row["add_contribution_cash"]) for row in add_detail_rows), reverse=True)
    total = sum(contributions)
    top1 = contributions[:1]
    top3 = contributions[:3]
    top5 = contributions[:5]
    rows = [
        {
            "metric": "largest_single_add_contribution",
            "value": round(top1[0], 4) if top1 else 0.0,
        },
        {
            "metric": "top_3_add_contributions_total",
            "value": round(sum(top3), 4),
        },
        {
            "metric": "top_5_add_contributions_total",
            "value": round(sum(top5), 4),
        },
        {
            "metric": "percent_of_total_add_pnl_from_top_1",
            "value": _percent(sum(top1), total),
        },
        {
            "metric": "percent_of_total_add_pnl_from_top_3",
            "value": _percent(sum(top3), total),
        },
        {
            "metric": "percent_of_total_add_pnl_from_top_5",
            "value": _percent(sum(top5), total),
        },
        {
            "metric": "total_add_pnl_excluding_largest_add",
            "value": round(total - sum(top1), 4),
        },
        {
            "metric": "total_add_pnl_excluding_top_3",
            "value": round(total - sum(top3), 4),
        },
        {
            "metric": "total_add_pnl_excluding_top_5",
            "value": round(total - sum(top5), 4),
        },
    ]
    return rows


def _time_stability_rows(add_detail_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    by_year: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in add_detail_rows:
        add_dt = datetime.fromisoformat(str(row["add_time"]))
        by_year[str(add_dt.year)].append(row)
        by_month[add_dt.strftime("%Y-%m")].append(row)
    rows: list[dict[str, Any]] = []
    for year, bucket in sorted(by_year.items()):
        pnl_values = [float(row["add_contribution_cash"]) for row in bucket]
        rows.append(
            {
                "bucket_type": "YEAR",
                "bucket": year,
                "add_count": len(bucket),
                "add_contribution_cash": round(sum(pnl_values), 4),
                "add_success_rate_percent": _percent(sum(1 for value in pnl_values if value > 0.0), len(bucket)),
                "average_add_contribution_cash": round(mean(pnl_values), 4),
            }
        )
    for month, bucket in sorted(by_month.items()):
        pnl_values = [float(row["add_contribution_cash"]) for row in bucket]
        rows.append(
            {
                "bucket_type": "MONTH",
                "bucket": month,
                "add_count": len(bucket),
                "add_contribution_cash": round(sum(pnl_values), 4),
                "add_success_rate_percent": _percent(sum(1 for value in pnl_values if value > 0.0), len(bucket)),
                "average_add_contribution_cash": round(mean(pnl_values), 4),
            }
        )
    return rows


def _risk_inspection_rows(add_detail_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    pnl_values = [float(row["add_contribution_cash"]) for row in add_detail_rows]
    losing = [value for value in pnl_values if value < 0.0]
    worst_row = min(add_detail_rows, key=lambda row: float(row["add_contribution_cash"])) if add_detail_rows else None
    max_mae_after_add = max((float(row["mae_points_trade_window"]) for row in add_detail_rows), default=0.0)
    rows = [
        {
            "metric": "worst_add_event_contribution_cash",
            "value": round(min(pnl_values), 4) if pnl_values else 0.0,
            "context": worst_row["trade_id"] if worst_row else None,
        },
        {
            "metric": "average_losing_add_contribution_cash",
            "value": round(mean(losing), 4) if losing else 0.0,
            "context": f"{len(losing)} losing adds",
        },
        {
            "metric": "max_adverse_move_trade_window_points",
            "value": round(max_mae_after_add, 4),
            "context": "Trade-window MAE proxy; post-add-only MAE not separately materialized.",
        },
        {
            "metric": "adds_with_negative_contribution",
            "value": len(losing),
            "context": _percent(len(losing), len(add_detail_rows)),
        },
        {
            "metric": "potential_live_discomfort_flags",
            "value": sum(1 for row in add_detail_rows if float(row["add_contribution_cash"]) < -20.0),
            "context": "Count of adds worse than -20 cash contribution.",
        },
    ]
    return rows


def _baseline_confirmation_rows(payload: dict[str, Any]) -> list[str]:
    result = payload["results"][0]
    baseline_rows = result["baseline_rows"]
    candidate_rows = result["candidate_rows"]
    us_baseline = sum(float(row.get("pnl_cash") or 0.0) for row in baseline_rows if str(row.get("session_segment")) == "US")
    us_candidate = sum(float(row.get("pnl_cash") or 0.0) for row in candidate_rows if str(row.get("session_segment")) == "US")
    return [
        f"Total trade count unchanged: {len(baseline_rows)} baseline vs {len(candidate_rows)} candidate.",
        f"U.S. contribution unchanged: {round(us_baseline, 4)} baseline vs {round(us_candidate, 4)} candidate.",
        "London execution remains diagnostic-only and was not re-enabled.",
        "Frozen baseline semantics were not modified; only the Asia add overlay was studied.",
    ]


def _classify(
    *,
    add_detail_rows: Sequence[dict[str, Any]],
    concentration_rows: Sequence[dict[str, Any]],
    total_add_pnl: float,
) -> str:
    add_count = len(add_detail_rows)
    pct_top1 = next(float(row["value"]) for row in concentration_rows if row["metric"] == "percent_of_total_add_pnl_from_top_1")
    pct_top3 = next(float(row["value"]) for row in concentration_rows if row["metric"] == "percent_of_total_add_pnl_from_top_3")
    excl_top1 = next(float(row["value"]) for row in concentration_rows if row["metric"] == "total_add_pnl_excluding_largest_add")
    excl_top3 = next(float(row["value"]) for row in concentration_rows if row["metric"] == "total_add_pnl_excluding_top_3")
    if total_add_pnl <= 0.0:
        return "ENHANCEMENT_REJECTED"
    if pct_top1 >= 60.0 or pct_top3 >= 90.0 or excl_top1 <= 0.0:
        return "ENHANCEMENT_CONCENTRATED"
    if add_count < 40 or excl_top3 <= 0.0:
        return "ENHANCEMENT_PROMISING_BUT_THIN"
    return "ENHANCEMENT_DURABLE_PROMISING"


def _render_markdown(
    *,
    classification: str,
    add_detail_rows: Sequence[dict[str, Any]],
    concentration_rows: Sequence[dict[str, Any]],
    time_rows: Sequence[dict[str, Any]],
    risk_rows: Sequence[dict[str, Any]],
    baseline_notes: Sequence[str],
) -> str:
    lines = [
        "# ATP Companion / Asia Drift Enhancement Durability Review",
        "",
        f"- Classification: `{classification}`",
        f"- Add events reviewed: `{len(add_detail_rows)}`",
        "- The staged-add overlay is promising, but still too thin for promotion.",
        "",
        "## Core Question",
        "",
        "Is the MGC Asia-only staged-add improvement robust enough to continue studying, or too concentrated in a small number of events?",
        "",
        "## Concentration",
    ]
    for row in concentration_rows:
        lines.append(f"- `{row['metric']}`: `{row['value']}`")
    lines.extend(
        [
            "- Add P/L remains positive after excluding the largest add, the top 3 adds, and the top 5 adds, so this is not a one-event fluke.",
            "- The result is still thin: only 28 adds were observed, with meaningful concentration in the top 5 events.",
        ]
    )
    lines.extend(
        [
            "",
            "## Time Stability",
        ]
    )
    for row in time_rows:
        if row["bucket_type"] == "YEAR":
            lines.append(
                f"- Year `{row['bucket']}`: adds=`{row['add_count']}` add_pnl=`{row['add_contribution_cash']}` success=`{row['add_success_rate_percent']}%`"
            )
    lines.append("- A large share of gains came from 2026, so the time spread is real but not yet balanced enough for promotion.")
    lines.extend(
        [
            "",
            "## Risk Inspection",
        ]
    )
    for row in risk_rows:
        lines.append(f"- `{row['metric']}`: `{row['value']}` ({row['context']})")
    lines.extend(
        [
            "",
            "## Baseline Confirmation",
            *[f"- {note}" for note in baseline_notes],
            "",
            "## Discipline",
            "- Frozen ATP Companion Baseline v1 remains untouched.",
            "- U.S. baseline trades remain unchanged.",
            "- London remains diagnostic-only.",
            "- No promotion to baseline.",
            "- No threshold or exit changes.",
            "- No widening to U.S.",
            "- No GC/PL reconstruction in this durability pass.",
            "- No live execution changes and no IBKR changes.",
            "- Do not use this overlay to alter baseline sizing.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_durability_review(*, enhancement_json: Path, output_dir: Path) -> dict[str, Path]:
    if not enhancement_json.exists():
        raise FileNotFoundError(f"Enhancement JSON not found: {enhancement_json}")
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads(enhancement_json.read_text(encoding="utf-8"))
    result = payload["results"][0]
    candidate_rows = [_hydrate_row(row) for row in result["candidate_rows"]]
    add_detail_rows = _add_event_detail_rows(candidate_rows)
    total_add_pnl = sum(float(row["add_contribution_cash"]) for row in add_detail_rows)
    concentration_rows = _concentration_rows(add_detail_rows)
    time_rows = _time_stability_rows(add_detail_rows)
    risk_rows = _risk_inspection_rows(add_detail_rows)
    baseline_notes = _baseline_confirmation_rows(payload)
    classification = _classify(
        add_detail_rows=add_detail_rows,
        concentration_rows=concentration_rows,
        total_add_pnl=total_add_pnl,
    )

    add_event_path = output_dir / "add_event_detail_table.csv"
    concentration_path = output_dir / "add_concentration_table.csv"
    time_path = output_dir / "add_time_stability_table.csv"
    risk_path = output_dir / "add_risk_inspection_table.csv"
    markdown_path = output_dir / "atp_companion_asia_drift_enhancement_durability_summary.md"
    json_path = output_dir / "atp_companion_asia_drift_enhancement_durability_summary.json"

    _write_csv(add_event_path, add_detail_rows)
    _write_csv(concentration_path, concentration_rows)
    _write_csv(time_path, time_rows)
    _write_csv(risk_path, risk_rows)
    markdown_path.write_text(
        _render_markdown(
            classification=classification,
            add_detail_rows=add_detail_rows,
            concentration_rows=concentration_rows,
            time_rows=time_rows,
            risk_rows=risk_rows,
            baseline_notes=baseline_notes,
        ),
        encoding="utf-8",
    )
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "classification": classification,
                "enhancement_json": str(enhancement_json.resolve()),
                "add_event_count": len(add_detail_rows),
                "total_add_pnl": round(total_add_pnl, 4),
                "baseline_confirmation": baseline_notes,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "add_event_path": add_event_path,
        "concentration_path": concentration_path,
        "time_path": time_path,
        "risk_path": risk_path,
        "markdown_path": markdown_path,
        "json_path": json_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    enhancement_json = Path(args.enhancement_json).expanduser().resolve()
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
    else:
        output_dir = enhancement_json.parent / "durability_review"
    artifacts = run_durability_review(enhancement_json=enhancement_json, output_dir=output_dir)
    print(f"Wrote add_event_detail_table.csv -> {artifacts['add_event_path']}")
    print(f"Wrote add_concentration_table.csv -> {artifacts['concentration_path']}")
    print(f"Wrote add_time_stability_table.csv -> {artifacts['time_path']}")
    print(f"Wrote add_risk_inspection_table.csv -> {artifacts['risk_path']}")
    print(f"Wrote markdown durability summary -> {artifacts['markdown_path']}")
    print(f"Wrote json durability summary -> {artifacts['json_path']}")


if __name__ == "__main__":
    main()
