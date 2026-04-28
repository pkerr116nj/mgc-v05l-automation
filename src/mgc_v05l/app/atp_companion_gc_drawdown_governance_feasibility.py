"""Drawdown-governance feasibility pass for the replay-safe GC ATP expression."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

REPO_ROOT = Path.cwd()
DEFAULT_GC_TRADES_JSONL = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_replay_baseline_v2_gc_expression"
    / "gc_asia_us_latest"
    / "atp_companion_replay_baseline_v2_gc_trades.jsonl"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_gc_drawdown_governance"

CAP_GRID: tuple[float, ...] = (2500.0, 5000.0, 7500.0, 10000.0, 15000.0, 20000.0)
RESET_POLICIES: tuple[str, ...] = ("REST_OF_DAY", "REST_OF_WEEK", "REST_OF_MONTH", "MANUAL_REVIEW_ONLY")


@dataclass(frozen=True)
class GovernancePolicy:
    cap_cash: float
    reset_policy: str

    @property
    def governance_id(self) -> str:
        return f"GC_DD_{int(self.cap_cash)}__{self.reset_policy}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-gc-drawdown-governance-feasibility")
    parser.add_argument("--gc-trades-jsonl", default=str(DEFAULT_GC_TRADES_JSONL), help="Replay-safe GC trade JSONL.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    return parser


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
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


def _write_markdown(path: Path, text: str) -> None:
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _load_trades(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in _iter_jsonl(path):
        trade = raw["trade_record"]
        entry_ts = datetime.fromisoformat(str(trade["entry_ts"]).replace(" ", "T"))
        exit_ts = datetime.fromisoformat(str(trade["exit_ts"]).replace(" ", "T"))
        rows.append(
            {
                "trade_id": str(raw["trade_id"]),
                "instrument": str(raw["instrument"]),
                "session": str(raw["session"]),
                "direction": str(raw["direction"]),
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_date": entry_ts.date().isoformat(),
                "entry_year": entry_ts.year,
                "entry_month": entry_ts.month,
                "entry_iso_week": entry_ts.isocalendar().week,
                "exit_reason": str(raw["exit_reason"]),
                "trade_pnl_cash": float(raw["baseline_pnl_cash"]),
            }
        )
    rows.sort(key=lambda row: (row["entry_ts"], row["exit_ts"], row["trade_id"]))
    return rows


def _max_drawdown(pnls: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return round(max_drawdown, 4)


def _profit_factor(pnls: Sequence[float]) -> float | None:
    winners = [value for value in pnls if value > 0]
    losers = [abs(value) for value in pnls if value < 0]
    gross_profit = sum(winners)
    gross_loss = sum(losers)
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return gross_profit
    return None


def _max_losing_streak(pnls: Sequence[float]) -> int:
    streak = 0
    best = 0
    for pnl in pnls:
        if pnl < 0:
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def _drawdown_to_profit_ratio(net_pnl_cash: float, max_drawdown: float) -> float | None:
    if net_pnl_cash <= 0:
        return None
    return max_drawdown / net_pnl_cash


def _policy_allows_resume(*, policy: str, breach_trade: dict[str, Any], candidate_trade: dict[str, Any]) -> bool:
    if policy == "MANUAL_REVIEW_ONLY":
        return False
    if policy == "REST_OF_DAY":
        return candidate_trade["entry_ts"].date() > breach_trade["entry_ts"].date()
    if policy == "REST_OF_WEEK":
        breach_key = (breach_trade["entry_ts"].isocalendar().year, breach_trade["entry_ts"].isocalendar().week)
        candidate_key = (candidate_trade["entry_ts"].isocalendar().year, candidate_trade["entry_ts"].isocalendar().week)
        return candidate_key > breach_key
    if policy == "REST_OF_MONTH":
        breach_key = (breach_trade["entry_ts"].year, breach_trade["entry_ts"].month)
        candidate_key = (candidate_trade["entry_ts"].year, candidate_trade["entry_ts"].month)
        return candidate_key > breach_key
    raise ValueError(f"Unsupported reset policy: {policy}")


def _baseline_metrics(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(trade["trade_pnl_cash"]) for trade in trades]
    net = round(sum(pnls), 4)
    asia = round(sum(float(trade["trade_pnl_cash"]) for trade in trades if trade["session"] == "ASIA"), 4)
    us = round(sum(float(trade["trade_pnl_cash"]) for trade in trades if trade["session"] == "US"), 4)
    return {
        "trade_count": len(trades),
        "net_pnl_cash": net,
        "max_drawdown": _max_drawdown(pnls),
        "drawdown_to_profit_ratio": round(float(_drawdown_to_profit_ratio(net, _max_drawdown(pnls))), 4)
        if _drawdown_to_profit_ratio(net, _max_drawdown(pnls)) is not None
        else None,
        "asia_contribution_cash": asia,
        "us_contribution_cash": us,
        "profit_factor": _profit_factor(pnls),
    }


def _simulate_policy(trades: Sequence[dict[str, Any]], policy: GovernancePolicy) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    retained_rows: list[dict[str, Any]] = []
    trade_ledger: list[dict[str, Any]] = []
    stop_events: list[dict[str, Any]] = []
    active_stop: dict[str, Any] | None = None
    stop_counter = 0

    for trade in trades:
        if active_stop is not None and _policy_allows_resume(policy=policy.reset_policy, breach_trade=active_stop["breach_trade"], candidate_trade=trade):
            active_stop["suspension_end_ts"] = trade["entry_ts"].isoformat()
            active_stop["recovery_time_days"] = round(
                (trade["entry_ts"] - active_stop["suspension_start_dt"]).total_seconds() / 86400.0,
                4,
            )
            active_stop = None

        equity_before = equity
        drawdown_before = peak - equity
        if active_stop is not None:
            pnl = float(trade["trade_pnl_cash"])
            event_id = str(active_stop["stop_event_id"])
            active_stop["skipped_trade_count"] += 1
            active_stop["skipped_net_pnl_cash"] += pnl
            if pnl > 0:
                active_stop["skipped_winning_trades"] += 1
            elif pnl < 0:
                active_stop["skipped_losing_trades"] += 1
            trade_ledger.append(
                {
                    "governance_id": policy.governance_id,
                    "cap_cash": policy.cap_cash,
                    "reset_policy": policy.reset_policy,
                    "trade_id": trade["trade_id"],
                    "entry_ts": trade["entry_ts"].isoformat(),
                    "exit_ts": trade["exit_ts"].isoformat(),
                    "session": trade["session"],
                    "direction": trade["direction"],
                    "exit_reason": trade["exit_reason"],
                    "trade_pnl_cash": round(pnl, 4),
                    "retained_or_skipped": "SKIPPED",
                    "retained_flag": False,
                    "cumulative_equity_before_trade": round(equity_before, 4),
                    "cumulative_equity_after_trade": "",
                    "active_drawdown_before_trade": round(drawdown_before, 4),
                    "active_drawdown_after_trade": "",
                    "skip_reason": f"Suspended after {event_id}",
                    "stop_event_id": event_id,
                }
            )
            continue

        pnl = float(trade["trade_pnl_cash"])
        equity += pnl
        peak = max(peak, equity)
        drawdown_after = peak - equity
        max_drawdown = max(max_drawdown, drawdown_after)
        retained_rows.append(dict(trade))
        trade_ledger.append(
            {
                "governance_id": policy.governance_id,
                "cap_cash": policy.cap_cash,
                "reset_policy": policy.reset_policy,
                "trade_id": trade["trade_id"],
                "entry_ts": trade["entry_ts"].isoformat(),
                "exit_ts": trade["exit_ts"].isoformat(),
                "session": trade["session"],
                "direction": trade["direction"],
                "exit_reason": trade["exit_reason"],
                "trade_pnl_cash": round(pnl, 4),
                "retained_or_skipped": "RETAINED",
                "retained_flag": True,
                "cumulative_equity_before_trade": round(equity_before, 4),
                "cumulative_equity_after_trade": round(equity, 4),
                "active_drawdown_before_trade": round(drawdown_before, 4),
                "active_drawdown_after_trade": round(drawdown_after, 4),
                "skip_reason": "",
                "stop_event_id": "",
            }
        )

        if drawdown_after >= policy.cap_cash:
            stop_counter += 1
            active_stop = {
                "governance_id": policy.governance_id,
                "stop_event_id": f"{policy.governance_id}__STOP_{stop_counter:03d}",
                "cap_cash": round(policy.cap_cash, 4),
                "reset_policy": policy.reset_policy,
                "breach_timestamp": trade["exit_ts"].isoformat(),
                "breach_trade_id": trade["trade_id"],
                "breach_session": trade["session"],
                "active_drawdown_at_breach": round(drawdown_after, 4),
                "stop_event_depth": round(drawdown_after, 4),
                "suspension_start": trade["exit_ts"].isoformat(),
                "suspension_start_dt": trade["exit_ts"],
                "suspension_end_ts": None,
                "recovery_time_days": None,
                "skipped_trade_count": 0,
                "skipped_net_pnl_cash": 0.0,
                "skipped_winning_trades": 0,
                "skipped_losing_trades": 0,
                "breach_trade": trade,
            }
            stop_events.append(active_stop)

    if active_stop is not None:
        active_stop["suspension_end_ts"] = None
        active_stop["recovery_time_days"] = None

    clean_events: list[dict[str, Any]] = []
    for event in stop_events:
        clean_events.append(
            {
                "governance_id": event["governance_id"],
                "stop_event_id": event["stop_event_id"],
                "cap_cash": event["cap_cash"],
                "reset_policy": event["reset_policy"],
                "breach_timestamp": event["breach_timestamp"],
                "breach_trade_id": event["breach_trade_id"],
                "breach_session": event["breach_session"],
                "active_drawdown_at_breach": event["active_drawdown_at_breach"],
                "stop_event_depth": event["stop_event_depth"],
                "suspension_start": event["suspension_start"],
                "suspension_end_ts": event["suspension_end_ts"] or "",
                "recovery_time_days": round(float(event["recovery_time_days"]), 4) if event["recovery_time_days"] is not None else "",
                "skipped_trade_count": int(event["skipped_trade_count"]),
                "skipped_net_pnl_cash": round(float(event["skipped_net_pnl_cash"]), 4),
                "skipped_winning_trades": int(event["skipped_winning_trades"]),
                "skipped_losing_trades": int(event["skipped_losing_trades"]),
            }
        )

    pnls = [float(row["trade_pnl_cash"]) for row in retained_rows]
    net_pnl_cash = round(sum(pnls), 4)
    trade_count_retained = len(retained_rows)
    skipped_rows = [row for row in trade_ledger if row["retained_or_skipped"] == "SKIPPED"]
    skipped_pnls = [float(row["trade_pnl_cash"]) for row in skipped_rows]
    year_rows: dict[int, dict[str, Any]] = {}
    for year in sorted({int(trade["entry_year"]) for trade in trades}):
        year_rows[year] = {
            "governance_id": policy.governance_id,
            "cap_cash": policy.cap_cash,
            "reset_policy": policy.reset_policy,
            "year": year,
            "retained_net_pnl_cash": 0.0,
            "retained_trade_count": 0,
            "skipped_trade_count": 0,
            "stop_event_count": 0,
            "max_drawdown_within_year": 0.0,
            "asia_contribution_cash": 0.0,
            "us_contribution_cash": 0.0,
        }
    year_pnls: dict[int, list[float]] = {year: [] for year in year_rows}
    for ledger_row in trade_ledger:
        year = int(ledger_row["entry_ts"][:4])
        if ledger_row["retained_or_skipped"] == "RETAINED":
            pnl = float(ledger_row["trade_pnl_cash"])
            year_rows[year]["retained_net_pnl_cash"] += pnl
            year_rows[year]["retained_trade_count"] += 1
            year_pnls[year].append(pnl)
            if ledger_row["session"] == "ASIA":
                year_rows[year]["asia_contribution_cash"] += pnl
            elif ledger_row["session"] == "US":
                year_rows[year]["us_contribution_cash"] += pnl
        else:
            year_rows[year]["skipped_trade_count"] += 1
    for event in clean_events:
        year_rows[int(event["breach_timestamp"][:4])]["stop_event_count"] += 1
    for year, values in year_pnls.items():
        year_rows[year]["max_drawdown_within_year"] = _max_drawdown(values)
        year_rows[year]["retained_net_pnl_cash"] = round(year_rows[year]["retained_net_pnl_cash"], 4)
        year_rows[year]["asia_contribution_cash"] = round(year_rows[year]["asia_contribution_cash"], 4)
        year_rows[year]["us_contribution_cash"] = round(year_rows[year]["us_contribution_cash"], 4)

    pf = _profit_factor(pnls)
    drawdown_ratio = _drawdown_to_profit_ratio(net_pnl_cash, max_drawdown)
    summary = {
        "governance_id": policy.governance_id,
        "cap_cash": round(policy.cap_cash, 4),
        "reset_policy": policy.reset_policy,
        "trade_count_retained": trade_count_retained,
        "trade_count_skipped": len(skipped_rows),
        "retained_net_pnl_cash": net_pnl_cash,
        "average_retained_trade_cash": round(net_pnl_cash / max(trade_count_retained, 1), 4),
        "median_retained_trade_cash": round(float(median(pnls)), 4) if pnls else 0.0,
        "win_rate_retained": round((sum(1 for pnl in pnls if pnl > 0) / max(len(pnls), 1) * 100.0), 4) if pnls else 0.0,
        "profit_factor_retained": round(float(pf), 4) if pf is not None else "",
        "max_drawdown_after_governance": round(max_drawdown, 4),
        "drawdown_to_profit_ratio": round(float(drawdown_ratio), 4) if drawdown_ratio is not None else "",
        "max_consecutive_losers_retained": _max_losing_streak(pnls),
        "stop_event_count": len(clean_events),
        "average_stop_event_depth": round(sum(float(event["stop_event_depth"]) for event in clean_events) / max(len(clean_events), 1), 4)
        if clean_events
        else 0.0,
        "largest_stop_event": round(max((float(event["stop_event_depth"]) for event in clean_events), default=0.0), 4),
        "average_recovery_time_days": round(
            sum(float(event["recovery_time_days"]) for event in clean_events if event["recovery_time_days"] != "") / max(sum(1 for event in clean_events if event["recovery_time_days"] != ""), 1),
            4,
        )
        if any(event["recovery_time_days"] != "" for event in clean_events)
        else "",
        "winning_trades_skipped": sum(1 for row in skipped_rows if float(row["trade_pnl_cash"]) > 0),
        "losing_trades_skipped": sum(1 for row in skipped_rows if float(row["trade_pnl_cash"]) < 0),
        "net_pnl_of_skipped_trades": round(sum(skipped_pnls), 4),
        "asia_contribution_after_governance": round(sum(float(row["trade_pnl_cash"]) for row in retained_rows if row["session"] == "ASIA"), 4),
        "us_contribution_after_governance": round(sum(float(row["trade_pnl_cash"]) for row in retained_rows if row["session"] == "US"), 4),
        "largest_retained_win": round(max(pnls), 4) if pnls else 0.0,
        "largest_retained_loss": round(min(pnls), 4) if pnls else 0.0,
        "psychological_survivability": "",
        "expectancy_effect": "",
    }
    return trade_ledger, clean_events, {"summary": summary, "year_rows": list(year_rows.values())}


def _annotate_summary_rows(summary_rows: list[dict[str, Any]], *, baseline: dict[str, Any]) -> None:
    baseline_net = float(baseline["net_pnl_cash"])
    baseline_dd = float(baseline["max_drawdown"])
    for row in summary_rows:
        retained_pct = (float(row["retained_net_pnl_cash"]) / baseline_net * 100.0) if baseline_net != 0 else 0.0
        dd_reduction = baseline_dd - float(row["max_drawdown_after_governance"])
        trade_retention_pct = float(row["trade_count_retained"]) / max(int(baseline["trade_count"]), 1) * 100.0
        if float(row["max_drawdown_after_governance"]) <= 5000.0 and retained_pct >= 50.0:
            psych = "PRACTICALLY_TOLERABLE"
        elif float(row["max_drawdown_after_governance"]) <= 10000.0 and retained_pct >= 40.0:
            psych = "IMPROVED_BUT_STILL_LARGE"
        else:
            psych = "STILL_LARGE"
        if retained_pct >= 70.0:
            expectancy = "PRESERVED"
        elif retained_pct >= 40.0:
            expectancy = "DEGRADED_BUT_PRESENT"
        elif retained_pct > 0.0:
            expectancy = "HEAVILY_DEGRADED"
        else:
            expectancy = "DESTROYED"
        row["retained_net_pnl_vs_ungated_gc_pct"] = round(retained_pct, 4)
        row["drawdown_reduction_cash"] = round(dd_reduction, 4)
        row["trade_count_retained_pct"] = round(trade_retention_pct, 4)
        row["psychological_survivability"] = psych
        row["expectancy_effect"] = expectancy


def _classification(summary_rows: Sequence[dict[str, Any]]) -> str:
    rows = list(summary_rows)
    if rows and all(str(row.get("psychological_survivability", "STILL_LARGE")) == "STILL_LARGE" for row in rows):
        fivek_rows = [row for row in rows if float(row["cap_cash"]) == 5000.0]
        fivek_destroyed = all(float(row["retained_net_pnl_vs_ungated_gc_pct"]) < 30.0 for row in fivek_rows)
        if fivek_destroyed:
            return "GC_DRAWDOWN_TOO_LARGE_FOR_CURRENT_USE"
    practical = [
        row
        for row in rows
        if float(row["cap_cash"]) <= 7500.0
        and float(row["retained_net_pnl_cash"]) > 0.0
        and float(row["retained_net_pnl_vs_ungated_gc_pct"]) >= 50.0
        and float(row["trade_count_retained_pct"]) >= 50.0
        and float(row["drawdown_reduction_cash"]) > 5000.0
    ]
    fivek_rows = [row for row in rows if float(row["cap_cash"]) == 5000.0]
    mid = [
        row
        for row in rows
        if float(row["cap_cash"]) <= 10000.0
        and float(row["retained_net_pnl_cash"]) > 0.0
        and float(row["retained_net_pnl_vs_ungated_gc_pct"]) >= 35.0
        and float(row["drawdown_reduction_cash"]) > 5000.0
    ]
    high = [
        row
        for row in rows
        if float(row["cap_cash"]) >= 15000.0
        and float(row["retained_net_pnl_cash"]) > 0.0
        and float(row["retained_net_pnl_vs_ungated_gc_pct"]) >= 50.0
    ]
    if practical:
        return "GC_DRAWDOWN_GOVERNANCE_PROMISING"
    if mid:
        return "GC_DRAWDOWN_GOVERNANCE_MIXED"
    if high:
        fivek_destroyed = all(float(row["retained_net_pnl_vs_ungated_gc_pct"]) < 25.0 for row in fivek_rows)
        if fivek_destroyed:
            return "GC_DRAWDOWN_TOO_LARGE_FOR_CURRENT_USE"
        return "GC_DRAWDOWN_GOVERNANCE_MIXED"
    return "GC_DRAWDOWN_GOVERNANCE_TOO_RESTRICTIVE"


def _render_markdown(*, classification: str, baseline: dict[str, Any], summary_rows: Sequence[dict[str, Any]]) -> str:
    fivek_rows = [row for row in summary_rows if float(row["cap_cash"]) == 5000.0]
    best_practical = sorted(
        [row for row in summary_rows if float(row["cap_cash"]) <= 10000.0],
        key=lambda row: (float(row["retained_net_pnl_vs_ungated_gc_pct"]), float(row["drawdown_reduction_cash"])),
        reverse=True,
    )[0]
    high_rows = sorted(
        [row for row in summary_rows if float(row["cap_cash"]) >= 15000.0],
        key=lambda row: (float(row["retained_net_pnl_vs_ungated_gc_pct"]), -float(row["max_drawdown_after_governance"])),
        reverse=True,
    )
    high_best = high_rows[0] if high_rows else None
    lines = [
        "# ATP Companion GC Drawdown-Governance Feasibility",
        "",
        f"- Classification: `{classification}`",
        "- This is risk-governance feasibility only. It does not change the GC expression semantics.",
        "- This pass does not recommend live or paper GC execution.",
        "- No threshold sensitivity, exit redesign, staged adds, IBKR, broker, or old-v1 comparison work was run.",
        "- London remains diagnostic-only.",
        "",
        "## Ungated GC Reference",
        f"- Ungated GC net P/L: `{baseline['net_pnl_cash']}`",
        f"- Ungated GC max drawdown: `{baseline['max_drawdown']}`",
        f"- Ungated GC drawdown-to-profit ratio: `{baseline['drawdown_to_profit_ratio']}`",
        f"- Ungated GC Asia contribution: `{baseline['asia_contribution_cash']}`",
        f"- Ungated GC U.S. contribution: `{baseline['us_contribution_cash']}`",
        "",
        "## Practical Boundary",
    ]
    for row in fivek_rows:
        lines.append(
            f"- `$5,000` / `{row['reset_policy']}` retained `{row['retained_net_pnl_cash']}` "
            f"(`{row['retained_net_pnl_vs_ungated_gc_pct']}%`) with max DD `{row['max_drawdown_after_governance']}` "
            f"and expectancy effect `{row['expectancy_effect']}`"
        )
    lines.extend(
        [
            "",
        "## Strongest Practical Zone",
        f"- Best sub-`$10,000` retained P/L row: `{best_practical['governance_id']}`",
        f"- Retained net P/L: `{best_practical['retained_net_pnl_cash']}` (`{best_practical['retained_net_pnl_vs_ungated_gc_pct']}%` of ungated GC)",
        f"- Max drawdown after governance: `{best_practical['max_drawdown_after_governance']}`",
        f"- Drawdown reduction: `{best_practical['drawdown_reduction_cash']}`",
        f"- Trade retention: `{best_practical['trade_count_retained_pct']}%`",
        f"- Psychological survivability: `{best_practical['psychological_survivability']}`",
        "- Even the strongest sub-`$10,000` row still leaves realized drawdown far above a practically tolerable boundary.",
    ]
    )
    if high_best is not None:
        lines.extend(
            [
                "",
                "## Higher-Cap Context",
                f"- Best `$15,000-$20,000` row: `{high_best['governance_id']}`",
                f"- Retained net P/L: `{high_best['retained_net_pnl_cash']}` (`{high_best['retained_net_pnl_vs_ungated_gc_pct']}%`)",
                f"- Max drawdown after governance: `{high_best['max_drawdown_after_governance']}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Discipline",
            "- Do not treat this as a final cap recommendation.",
            "- Do not treat `$5,000` as a number to force.",
            "- The only question here is whether governance can make the GC expression practically tolerable without destroying too much edge.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_governance_feasibility(*, gc_trades_jsonl: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades = _load_trades(gc_trades_jsonl)
    baseline = _baseline_metrics(trades)

    summary_rows: list[dict[str, Any]] = []
    stop_event_rows: list[dict[str, Any]] = []
    retained_vs_skipped_rows: list[dict[str, Any]] = []
    year_rows: list[dict[str, Any]] = []

    for cap in CAP_GRID:
        for reset_policy in RESET_POLICIES:
            policy = GovernancePolicy(cap_cash=cap, reset_policy=reset_policy)
            trade_ledger, stop_events, payload = _simulate_policy(trades, policy)
            summary_rows.append(payload["summary"])
            stop_event_rows.extend(stop_events)
            retained_vs_skipped_rows.extend(trade_ledger)
            year_rows.extend(payload["year_rows"])

    _annotate_summary_rows(summary_rows, baseline=baseline)
    classification = _classification(summary_rows)
    for row in summary_rows:
        row["classification"] = classification
    summary_md = _render_markdown(classification=classification, baseline=baseline, summary_rows=summary_rows)

    summary_csv_path = output_dir / "atp_gc_drawdown_governance_summary.csv"
    stop_events_csv_path = output_dir / "atp_gc_drawdown_stop_events.csv"
    retained_vs_skipped_csv_path = output_dir / "atp_gc_drawdown_retained_vs_skipped_trades.csv"
    year_csv_path = output_dir / "atp_gc_drawdown_year_decomposition.csv"
    summary_md_path = output_dir / "atp_gc_drawdown_governance_summary.md"

    _write_csv(summary_csv_path, summary_rows)
    _write_csv(stop_events_csv_path, stop_event_rows)
    _write_csv(retained_vs_skipped_csv_path, retained_vs_skipped_rows)
    _write_csv(year_csv_path, year_rows)
    _write_markdown(summary_md_path, summary_md)

    return {
        "summary_csv_path": summary_csv_path,
        "stop_events_csv_path": stop_events_csv_path,
        "retained_vs_skipped_csv_path": retained_vs_skipped_csv_path,
        "year_csv_path": year_csv_path,
        "summary_md_path": summary_md_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_us_latest")
    artifacts = build_governance_feasibility(
        gc_trades_jsonl=Path(args.gc_trades_jsonl).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
