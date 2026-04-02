"""Compare replay families across control/treatment and flag direct trade collisions."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LedgerTrade:
    entry_ts: datetime
    exit_ts: datetime
    direction: str
    setup_family: str
    net_pnl: float
    exit_reason: str
    bars_held: int
    entry_session_phase: str
    raw: dict[str, str]


def build_and_write_family_collision_audit(
    *,
    control_summary_path: Path,
    treatment_summary_path: Path,
    target_family: str,
    new_family: str,
) -> dict[str, str]:
    control_summary = json.loads(control_summary_path.read_text(encoding="utf-8"))
    treatment_summary = json.loads(treatment_summary_path.read_text(encoding="utf-8"))

    control_trades = _load_trades(Path(control_summary["trade_ledger_path"]))
    treatment_trades = _load_trades(Path(treatment_summary["trade_ledger_path"]))

    control_target = {trade.entry_ts.isoformat(): trade for trade in control_trades if trade.setup_family == target_family}
    treatment_target = {trade.entry_ts.isoformat(): trade for trade in treatment_trades if trade.setup_family == target_family}
    treatment_new_family = [trade for trade in treatment_trades if trade.setup_family == new_family]

    changed_rows: list[dict[str, Any]] = []
    structural_count = 0

    for entry_key in sorted(set(control_target) | set(treatment_target)):
        control_trade = control_target.get(entry_key)
        treatment_trade = treatment_target.get(entry_key)

        if control_trade and not treatment_trade:
            change_type = "removed_in_treatment"
        elif treatment_trade and not control_trade:
            change_type = "added_in_treatment"
        else:
            assert control_trade is not None and treatment_trade is not None
            if _same_trade(control_trade, treatment_trade):
                continue
            change_type = "modified_in_treatment"

        reference_trade = treatment_trade or control_trade
        assert reference_trade is not None
        overlaps = _find_overlaps(reference_trade, treatment_new_family)
        interaction_type = "structural_position_conflict" if overlaps else "incidental_timing_shift"
        if overlaps:
            structural_count += 1

        changed_rows.append(
            {
                "target_family": target_family,
                "new_family": new_family,
                "change_type": change_type,
                "interaction_type": interaction_type,
                "entry_ts": entry_key,
                "control_exit_ts": control_trade.exit_ts.isoformat() if control_trade else "",
                "treatment_exit_ts": treatment_trade.exit_ts.isoformat() if treatment_trade else "",
                "control_net_pnl": _format_float(control_trade.net_pnl if control_trade else None),
                "treatment_net_pnl": _format_float(treatment_trade.net_pnl if treatment_trade else None),
                "control_exit_reason": control_trade.exit_reason if control_trade else "",
                "treatment_exit_reason": treatment_trade.exit_reason if treatment_trade else "",
                "control_bars_held": control_trade.bars_held if control_trade else "",
                "treatment_bars_held": treatment_trade.bars_held if treatment_trade else "",
                "entry_session_phase": reference_trade.entry_session_phase,
                "overlap_count": len(overlaps),
                "overlap_new_family_entries": "|".join(trade.entry_ts.isoformat() for trade in overlaps),
                "overlap_new_family_exits": "|".join(trade.exit_ts.isoformat() for trade in overlaps),
                "overlap_new_family_pnl": "|".join(_format_float(trade.net_pnl) for trade in overlaps),
            }
        )

    changed_target_pnl_delta = (
        sum(trade.net_pnl for trade in treatment_target.values())
        - sum(trade.net_pnl for trade in control_target.values())
    )
    new_family_pnl = sum(trade.net_pnl for trade in treatment_new_family)

    summary = {
        "target_family": target_family,
        "new_family": new_family,
        "control_trade_count": len(control_target),
        "treatment_trade_count": len(treatment_target),
        "control_pnl": sum(trade.net_pnl for trade in control_target.values()),
        "treatment_pnl": sum(trade.net_pnl for trade in treatment_target.values()),
        "target_family_pnl_delta": changed_target_pnl_delta,
        "new_family_trade_count": len(treatment_new_family),
        "new_family_pnl": new_family_pnl,
        "changed_trade_count": len(changed_rows),
        "structural_change_count": structural_count,
        "changed_trades": changed_rows,
        "interaction_verdict": (
            "structural"
            if structural_count > 0
            else "incidental"
        ),
    }

    prefix = Path(str(treatment_summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".family_collision_audit_detail.csv")
    summary_path = prefix.with_suffix(".family_collision_audit_summary.json")

    _write_csv(detail_path, changed_rows)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "family_collision_audit_detail_path": str(detail_path),
        "family_collision_audit_summary_path": str(summary_path),
    }


def _load_trades(path: Path) -> list[LedgerTrade]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = []
        for raw in csv.DictReader(handle):
            rows.append(
                LedgerTrade(
                    entry_ts=datetime.fromisoformat(raw["entry_ts"]),
                    exit_ts=datetime.fromisoformat(raw["exit_ts"]),
                    direction=raw["direction"],
                    setup_family=raw["setup_family"],
                    net_pnl=float(raw["net_pnl"]),
                    exit_reason=raw["exit_reason"],
                    bars_held=int(raw["bars_held"]) if raw["bars_held"] else 0,
                    entry_session_phase=raw["entry_session_phase"],
                    raw=raw,
                )
            )
    return rows


def _same_trade(left: LedgerTrade, right: LedgerTrade) -> bool:
    return (
        left.exit_ts == right.exit_ts
        and left.net_pnl == right.net_pnl
        and left.exit_reason == right.exit_reason
        and left.bars_held == right.bars_held
    )


def _find_overlaps(target_trade: LedgerTrade, candidate_trades: list[LedgerTrade]) -> list[LedgerTrade]:
    overlaps: list[LedgerTrade] = []
    for candidate in candidate_trades:
        if candidate.exit_ts <= target_trade.entry_ts:
            continue
        if candidate.entry_ts >= target_trade.exit_ts:
            continue
        overlaps.append(candidate)
    return overlaps


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.1f}"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Audit target-family collisions between control and treatment replays.")
    parser.add_argument("control_summary")
    parser.add_argument("treatment_summary")
    parser.add_argument("--target-family", default="firstBullSnapTurn")
    parser.add_argument("--new-family", default="asiaEarlyPauseResumeShortTurn")
    args = parser.parse_args()

    outputs = build_and_write_family_collision_audit(
        control_summary_path=Path(args.control_summary),
        treatment_summary_path=Path(args.treatment_summary),
        target_family=args.target_family,
        new_family=args.new_family,
    )
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
