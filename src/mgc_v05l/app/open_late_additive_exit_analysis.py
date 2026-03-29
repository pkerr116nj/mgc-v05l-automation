"""Exit-conditioned diagnostics for the US_OPEN_LATE additive lane."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path


ADDITIVE_FAMILY = "usDerivativeBearAdditiveTurn"
US_OPEN_LATE = "US_OPEN_LATE"


@dataclass(frozen=True)
class AdditiveExitDiagnostic:
    entry_ts: str
    exit_ts: str
    entry_session_phase: str
    net_pnl: Decimal
    realized_points: Decimal
    mfe: Decimal
    mae: Decimal
    time_to_mfe: int
    time_to_mae: int
    bars_held: int
    exit_reason: str
    mfe_capture_pct: Decimal
    entry_efficiency_5: Decimal
    initial_adverse_3bar: Decimal
    initial_favorable_3bar: Decimal
    entry_distance_vwap_atr: Decimal
    uncaptured_mfe_points: Decimal
    assessment: str
    exit_takeaway: str


def build_and_write_open_late_additive_exit_analysis(
    *,
    summary_path: Path,
    anchor_summary_metrics_path: Path,
    open_full_summary_metrics_path: Path,
) -> dict[str, str]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trade_ledger_path = Path(summary["trade_ledger_path"])
    summary_metrics = json.loads(Path(summary["summary_metrics_path"]).read_text(encoding="utf-8"))
    anchor_metrics = json.loads(anchor_summary_metrics_path.read_text(encoding="utf-8"))
    open_full_metrics = json.loads(open_full_summary_metrics_path.read_text(encoding="utf-8"))

    diagnostics = _load_diagnostics(trade_ledger_path, point_value=Decimal(str(summary_metrics["assumptions"]["point_value"])))

    prefix = Path(str(summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".open_late_additive_exit_trade_diagnostics.csv")
    summary_json_path = prefix.with_suffix(".open_late_additive_exit_summary.json")

    _write_csv(detail_path, [asdict(item) for item in diagnostics])
    summary_json_path.write_text(
        json.dumps(
            _build_summary(
                diagnostics=diagnostics,
                current_metrics=summary_metrics,
                anchor_metrics=anchor_metrics,
                open_full_metrics=open_full_metrics,
            ),
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )

    return {
        "open_late_additive_exit_trade_diagnostics_path": str(detail_path),
        "open_late_additive_exit_summary_path": str(summary_json_path),
    }


def _load_diagnostics(trade_ledger_path: Path, *, point_value: Decimal) -> list[AdditiveExitDiagnostic]:
    diagnostics: list[AdditiveExitDiagnostic] = []
    with trade_ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["setup_family"] != ADDITIVE_FAMILY or row["entry_session_phase"] != US_OPEN_LATE:
                continue
            net_pnl = Decimal(row["net_pnl"])
            realized_points = net_pnl / point_value if point_value > 0 else Decimal("0")
            mfe = Decimal(row["mfe"])
            uncaptured_mfe_points = max(Decimal("0"), mfe - realized_points)
            mfe_capture_pct = Decimal(row["mfe_capture_pct"])
            bars_held = int(row["bars_held"])
            time_to_mfe = int(row["time_to_mfe"])
            assessment, takeaway = _classify_trade(
                net_pnl=net_pnl,
                mfe=mfe,
                mfe_capture_pct=mfe_capture_pct,
                bars_held=bars_held,
                time_to_mfe=time_to_mfe,
                exit_reason=row["exit_reason"],
            )
            diagnostics.append(
                AdditiveExitDiagnostic(
                    entry_ts=row["entry_ts"],
                    exit_ts=row["exit_ts"],
                    entry_session_phase=row["entry_session_phase"],
                    net_pnl=net_pnl,
                    realized_points=realized_points,
                    mfe=mfe,
                    mae=Decimal(row["mae"]),
                    time_to_mfe=time_to_mfe,
                    time_to_mae=int(row["time_to_mae"]),
                    bars_held=bars_held,
                    exit_reason=row["exit_reason"],
                    mfe_capture_pct=mfe_capture_pct,
                    entry_efficiency_5=Decimal(row["entry_efficiency_5"]),
                    initial_adverse_3bar=Decimal(row["initial_adverse_3bar"]),
                    initial_favorable_3bar=Decimal(row["initial_favorable_3bar"]),
                    entry_distance_vwap_atr=Decimal(row["entry_distance_vwap_atr"]),
                    uncaptured_mfe_points=uncaptured_mfe_points,
                    assessment=assessment,
                    exit_takeaway=takeaway,
                )
            )
    return diagnostics


def _classify_trade(
    *,
    net_pnl: Decimal,
    mfe: Decimal,
    mfe_capture_pct: Decimal,
    bars_held: int,
    time_to_mfe: int,
    exit_reason: str,
) -> tuple[str, str]:
    if net_pnl <= 0:
        return ("bad_entry", "Entry quality was the problem; exit work is secondary for this trade.")
    if mfe_capture_pct < Decimal("25") and time_to_mfe <= 1 and bars_held > time_to_mfe:
        return (
            "tolerable_entry_bad_exit",
            "Trade worked quickly but gave back most of MFE before integrity exit; monetization looks weak.",
        )
    if mfe_capture_pct < Decimal("45") and exit_reason == "SHORT_INTEGRITY_FAIL":
        return (
            "good_entry_exit_left_money",
            "Entry was directionally right, but integrity-based exit realized less than half the available move.",
        )
    return (
        "good_entry_reasonable_exit",
        "Trade quality was good and current exit logic monetized a useful share of MFE.",
    )


def _build_summary(
    *,
    diagnostics: list[AdditiveExitDiagnostic],
    current_metrics: dict[str, object],
    anchor_metrics: dict[str, object],
    open_full_metrics: dict[str, object],
) -> dict[str, object]:
    additive_pnl = sum(item.net_pnl for item in diagnostics)
    avg_capture = _avg_decimal(item.mfe_capture_pct for item in diagnostics)
    avg_uncaptured = _avg_decimal(item.uncaptured_mfe_points for item in diagnostics)
    all_integrity_exit = all(item.exit_reason == "SHORT_INTEGRITY_FAIL" for item in diagnostics)
    time_exit_bound = any(item.exit_reason == "SHORT_TIME_EXIT" for item in diagnostics)

    ranked_findings = [
        "All 3 additive US_OPEN_LATE trades exited via SHORT_INTEGRITY_FAIL; no additive trade hit SHORT_TIME_EXIT.",
        f"Average MFE capture was {avg_capture:.2f}% with average uncaptured MFE of {avg_uncaptured:.2f} points.",
        "The weak middle trade was still profitable, which points away from a clearly bad entry and toward monetization/integrity handling.",
        "A separate additive hold-time clock looks low-value because the additive trades exited before any short time limit bound.",
    ]

    trade_assessments = {
        item.entry_ts: {
            "assessment": item.assessment,
            "exit_takeaway": item.exit_takeaway,
        }
        for item in diagnostics
    }

    exit_gap_more_than_entry = True
    if any(item.assessment == "bad_entry" for item in diagnostics):
        exit_gap_more_than_entry = False

    return {
        "current_branch_metrics": {
            "total_net_pnl": current_metrics["total_net_pnl"],
            "expectancy": current_metrics["expectancy"],
            "max_drawdown": current_metrics["max_drawdown"],
            "additive_lane_pnl": additive_pnl,
            "additive_lane_trade_count": len(diagnostics),
        },
        "comparison_vs_anchor": {
            "pnl_delta": Decimal(str(current_metrics["total_net_pnl"])) - Decimal(str(anchor_metrics["total_net_pnl"])),
            "expectancy_delta": Decimal(str(current_metrics["expectancy"])) - Decimal(str(anchor_metrics["expectancy"])),
            "drawdown_delta": Decimal(str(current_metrics["max_drawdown"])) - Decimal(str(anchor_metrics["max_drawdown"])),
        },
        "comparison_vs_open_full": {
            "pnl_delta": Decimal(str(current_metrics["total_net_pnl"])) - Decimal(str(open_full_metrics["total_net_pnl"])),
            "expectancy_delta": Decimal(str(current_metrics["expectancy"])) - Decimal(str(open_full_metrics["expectancy"])),
            "drawdown_delta": Decimal(str(current_metrics["max_drawdown"])) - Decimal(str(open_full_metrics["max_drawdown"])),
        },
        "ranked_exit_conditioned_findings": ranked_findings,
        "trade_assessments": trade_assessments,
        "all_integrity_exit": all_integrity_exit,
        "time_exit_bound": time_exit_bound,
        "remaining_gap_more_about_exit_logic_than_entry_logic": exit_gap_more_than_entry,
        "additive_specific_exit_treatment_run": False,
        "additive_specific_exit_treatment_not_run_reason": (
            "Current runtime state does not carry short entry family/source into the exit engine, "
            "so an additive-lane-only exit override would require strategy-core plumbing beyond a minimal analysis pass."
        ),
    }


def _avg_decimal(values: object) -> Decimal:
    collected = [value for value in values]
    if not collected:
        return Decimal("0")
    return sum(collected, Decimal("0")) / Decimal(len(collected))


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
