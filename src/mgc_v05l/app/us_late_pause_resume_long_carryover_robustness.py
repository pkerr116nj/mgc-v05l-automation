"""Robustness split for US_LATE long branch carryover vs standard fills."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


EARLY_END = datetime.fromisoformat("2025-10-26T23:55:00-04:00")
MIDDLE_END = datetime.fromisoformat("2026-01-06T03:05:00-05:00")


@dataclass(frozen=True)
class BranchTrade:
    entry_ts: datetime
    exit_ts: datetime
    entry_session: str
    entry_session_phase: str
    net_pnl: float


def build_and_write_us_late_pause_resume_long_carryover_robustness(
    *,
    family_trades_csv_path: Path,
) -> dict[str, str]:
    trades = _load_trades(family_trades_csv_path)
    bucket_rows = _build_bucket_rows(trades)
    slice_rows = _build_slice_rows(trades)
    summary = _build_summary(bucket_rows=bucket_rows, slice_rows=slice_rows)

    prefix = Path(str(family_trades_csv_path).removesuffix(".csv"))
    bucket_path = prefix.with_suffix(".carryover_robustness_buckets.csv")
    slice_path = prefix.with_suffix(".carryover_robustness_slices.csv")
    summary_path = prefix.with_suffix(".carryover_robustness_summary.json")

    _write_csv(bucket_path, bucket_rows)
    _write_csv(slice_path, slice_rows)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "carryover_robustness_bucket_path": str(bucket_path),
        "carryover_robustness_slice_path": str(slice_path),
        "carryover_robustness_summary_path": str(summary_path),
    }


def _load_trades(path: Path) -> list[BranchTrade]:
    trades: list[BranchTrade] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            trades.append(
                BranchTrade(
                    entry_ts=datetime.fromisoformat(row["entry_ts"]),
                    exit_ts=datetime.fromisoformat(row["exit_ts"]),
                    entry_session=row["entry_session"],
                    entry_session_phase=row["entry_session_phase"],
                    net_pnl=float(row["net_pnl"]),
                )
            )
    return trades


def _bucket_name(trade: BranchTrade) -> str:
    if trade.entry_ts.hour == 17 and trade.entry_ts.minute == 55:
        return "carryover_1755"
    return "standard_us_fill"


def _slice_name(trade: BranchTrade) -> str:
    if trade.entry_ts <= EARLY_END:
        return "early"
    if trade.entry_ts <= MIDDLE_END:
        return "middle"
    return "recent"


def _build_bucket_rows(trades: list[BranchTrade]) -> list[dict[str, Any]]:
    rows = []
    for bucket in ("standard_us_fill", "carryover_1755"):
        bucket_trades = [trade for trade in trades if _bucket_name(trade) == bucket]
        rows.append(
            {
                "bucket": bucket,
                "trade_count": len(bucket_trades),
                "total_net_pnl": _sum_pnl(bucket_trades),
                "expectancy": _avg_pnl(bucket_trades),
                "avg_winner": _avg([trade.net_pnl for trade in bucket_trades if trade.net_pnl > 0]),
                "avg_loser": _avg([trade.net_pnl for trade in bucket_trades if trade.net_pnl <= 0]),
                "win_rate": _ratio(sum(1 for trade in bucket_trades if trade.net_pnl > 0), len(bucket_trades)),
            }
        )
    return rows


def _build_slice_rows(trades: list[BranchTrade]) -> list[dict[str, Any]]:
    rows = []
    for bucket in ("standard_us_fill", "carryover_1755"):
        for slice_name in ("early", "middle", "recent"):
            slice_trades = [
                trade for trade in trades if _bucket_name(trade) == bucket and _slice_name(trade) == slice_name
            ]
            rows.append(
                {
                    "bucket": bucket,
                    "slice": slice_name,
                    "trade_count": len(slice_trades),
                    "total_net_pnl": _sum_pnl(slice_trades),
                    "expectancy": _avg_pnl(slice_trades),
                }
            )
    return rows


def _build_summary(*, bucket_rows: list[dict[str, Any]], slice_rows: list[dict[str, Any]]) -> dict[str, Any]:
    bucket_map = {row["bucket"]: row for row in bucket_rows}
    return {
        "standard_us_fill": bucket_map["standard_us_fill"],
        "carryover_1755": bucket_map["carryover_1755"],
        "slice_rows": slice_rows,
        "recent_without_carryover_pnl": next(
            row["total_net_pnl"]
            for row in slice_rows
            if row["bucket"] == "standard_us_fill" and row["slice"] == "recent"
        ),
        "recommendation_hint": (
            "promote_only_with_carryover_rule"
            if float(bucket_map["carryover_1755"]["total_net_pnl"]) > 0
            and float(bucket_map["standard_us_fill"]["total_net_pnl"]) > 0
            else "hold"
        ),
    }


def _sum_pnl(trades: list[BranchTrade]) -> float:
    return round(sum(trade.net_pnl for trade in trades), 10)


def _avg_pnl(trades: list[BranchTrade]) -> float:
    if not trades:
        return 0.0
    return round(_sum_pnl(trades) / len(trades), 10)


def _avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 10)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 10)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Split US_LATE long branch performance by carryover fills.")
    parser.add_argument("family_trades_csv")
    args = parser.parse_args()
    outputs = build_and_write_us_late_pause_resume_long_carryover_robustness(
        family_trades_csv_path=Path(args.family_trades_csv)
    )
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
