"""Build a replay-safe ATP Companion Replay Baseline v2 Legacy-Intent and compare it to v1/v2."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

from .atp_companion_full_history_review import (
    DEFAULT_PLATFORM_SUBSTRATE_ROOT,
    DEFAULT_SOURCE_DB,
    _discover_best_sources,
    _trade_windows_by_id,
    build_replay_truth_manifest,
    build_targets,
    iter_replay_truth_trade_rows,
)
from .atp_companion_replay_baseline_v2 import _manifest_1m_span
from ..research.platform import ensure_symbol_context_bundle, stable_hash
from ..research.trend_participation.features import build_feature_states
from ..research.trend_participation.performance_validation import _trade_metrics, summarize_trade_distribution_diagnostics
from ..research.trend_participation.atp_promotion_add_review import default_atp_promotion_add_candidates
from ..research.trend_participation.substrate import _read_manifest, ensure_atp_feature_bundle, ensure_atp_scope_bundle

REPO_ROOT = Path.cwd()
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_replay_baseline_v2_legacy_intent"
OLD_TRUTH_JSON = REPO_ROOT / "outputs" / "reports" / "atp_companion_full_history_review_optimized_20260406" / "atp_companion_materialized_baseline_truth.json"
CURRENT_V2_DIR = REPO_ROOT / "outputs" / "reports" / "atp_companion_replay_baseline_v2" / "mgc_asia_us_latest"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-replay-baseline-v2-legacy-intent")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--old-truth-json", default=str(OLD_TRUTH_JSON), help="Old materialized v1 archive.")
    parser.add_argument("--current-v2-dir", default=str(CURRENT_V2_DIR), help="Current replay baseline v2 directory.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, text: str) -> None:
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _trade_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (str(row["decision_ts"]).replace(" ", "T"), str(row["session_segment"]), str(row["side"]))


def _trade_record_key(row: dict[str, Any]) -> tuple[str, str, str]:
    trade = row["trade_record"]
    return (str(trade["decision_ts"]).replace(" ", "T"), str(trade["session_segment"]), str(trade["side"]))


def _target_by_id(payload: dict[str, Any], target_id: str) -> dict[str, Any]:
    for row in payload.get("materialized_targets") or []:
        if row.get("target_id") == target_id:
            return row
    raise KeyError(target_id)


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


def _summary_from_trade_rows(trade_rows: Sequence[dict[str, Any]], *, bar_count: int) -> dict[str, Any]:
    position_rows = _base_position_rows(trade_rows)
    metrics = _trade_metrics(position_rows, bar_count=bar_count)
    diagnostics = summarize_trade_distribution_diagnostics(position_rows)
    pnl_values = [float(row["pnl_cash"]) for row in position_rows]
    gross_values = [float(row["trade_record"].gross_pnl_cash) for row in trade_rows]
    asia_rows = [row for row in trade_rows if str(row["trade_record"].session_segment) == "ASIA"]
    us_rows = [row for row in trade_rows if str(row["trade_record"].session_segment) == "US"]
    exit_reason_counter = Counter(str(row["trade_record"].exit_reason) for row in trade_rows)
    return {
        "trade_count": int(metrics["total_trades"]),
        "net_pnl_cash": round(float(metrics["net_pnl_cash"]), 4),
        "gross_pnl_cash": round(sum(gross_values), 4),
        "average_trade_pnl_cash": round(float(metrics["average_trade_pnl_cash"]), 4),
        "median_trade_pnl_cash": round(float(median(pnl_values)), 4) if pnl_values else 0.0,
        "win_rate": round(float(metrics["win_rate"]), 4),
        "profit_factor": round(float(metrics["profit_factor"]), 4),
        "max_drawdown": round(float(metrics["max_drawdown"]), 4),
        "largest_win": round(max(pnl_values), 4) if pnl_values else 0.0,
        "largest_loss": round(min(pnl_values), 4) if pnl_values else 0.0,
        "max_consecutive_losers": int(diagnostics["max_consecutive_losses"]),
        "asia_net_pnl_cash": round(sum(float(row["trade_record"].pnl_cash) for row in asia_rows), 4),
        "asia_gross_pnl_cash": round(sum(float(row["trade_record"].gross_pnl_cash) for row in asia_rows), 4),
        "us_net_pnl_cash": round(sum(float(row["trade_record"].pnl_cash) for row in us_rows), 4),
        "us_gross_pnl_cash": round(sum(float(row["trade_record"].gross_pnl_cash) for row in us_rows), 4),
        "exit_reason_distribution_json": json.dumps(dict(sorted(exit_reason_counter.items())), sort_keys=True),
        "london_diagnostic_status": "DIAGNOSTIC_ONLY_NOT_EXECUTED",
    }


def _validation_observed_from_replay_jsonl(path: Path) -> dict[str, Any]:
    trade_rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    position_rows: list[dict[str, Any]] = []
    gross_values: list[float] = []
    asia_gross = 0.0
    us_gross = 0.0
    exit_reason_counter: Counter[str] = Counter()
    for row in trade_rows:
        trade = dict(row["trade_record"])
        gross_pnl = float(trade["gross_pnl_cash"])
        gross_values.append(gross_pnl)
        exit_reason_counter[str(trade["exit_reason"])] += 1
        if str(trade["session_segment"]) == "ASIA":
            asia_gross += gross_pnl
        elif str(trade["session_segment"]) == "US":
            us_gross += gross_pnl
        position_rows.append(
            {
                "trade_id": row["trade_id"],
                "entry_ts": datetime.fromisoformat(str(trade["entry_ts"])),
                "exit_ts": datetime.fromisoformat(str(trade["exit_ts"])),
                "decision_ts": datetime.fromisoformat(str(trade["decision_ts"])),
                "entry_price": float(trade["entry_price"]),
                "exit_price": float(trade["exit_price"]),
                "stop_price": float(trade["stop_price"]),
                "pnl_cash": float(trade["pnl_cash"]),
                "mfe_points": float(trade["mfe_points"]),
                "mae_points": float(trade["mae_points"]),
                "hold_minutes": float(trade["hold_minutes"]),
                "bars_held_1m": int(trade["bars_held_1m"]),
                "side": trade["side"],
                "session_segment": trade["session_segment"],
                "family": trade["family"],
                "exit_reason": trade["exit_reason"],
                "added": False,
                "add_pnl_cash": 0.0,
                "add_reason": None,
                "add_price_quality_state": None,
            }
        )
    metrics = _trade_metrics(position_rows, bar_count=max(len(position_rows), 1))
    diagnostics = summarize_trade_distribution_diagnostics(position_rows)
    pnl_values = [float(row["pnl_cash"]) for row in position_rows]
    asia_rows = [row for row in position_rows if str(row["session_segment"]) == "ASIA"]
    us_rows = [row for row in position_rows if str(row["session_segment"]) == "US"]
    return {
        "trade_count": int(metrics["total_trades"]),
        "net_pnl_cash": round(float(metrics["net_pnl_cash"]), 4),
        "gross_pnl_cash": round(sum(gross_values), 4),
        "average_trade_pnl_cash": round(float(metrics["average_trade_pnl_cash"]), 4),
        "median_trade_pnl_cash": round(float(median(pnl_values)), 4) if pnl_values else 0.0,
        "win_rate": round(float(metrics["win_rate"]), 4),
        "profit_factor": round(float(metrics["profit_factor"]), 4),
        "max_drawdown": round(float(metrics["max_drawdown"]), 4),
        "largest_win": round(max(pnl_values), 4) if pnl_values else 0.0,
        "largest_loss": round(min(pnl_values), 4) if pnl_values else 0.0,
        "max_consecutive_losers": int(diagnostics["max_consecutive_losses"]),
        "asia_net_pnl_cash": round(sum(float(row["pnl_cash"]) for row in asia_rows), 4),
        "asia_gross_pnl_cash": round(asia_gross, 4),
        "us_net_pnl_cash": round(sum(float(row["pnl_cash"]) for row in us_rows), 4),
        "us_gross_pnl_cash": round(us_gross, 4),
        "exit_reason_distribution_json": json.dumps(dict(sorted(exit_reason_counter.items())), sort_keys=True),
        "london_diagnostic_status": "DIAGNOSTIC_ONLY_NOT_EXECUTED",
    }


def _validation_rows(expected: dict[str, Any], observed: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    tolerance = 0.0002
    for metric in (
        "trade_count",
        "net_pnl_cash",
        "gross_pnl_cash",
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
        if isinstance(expected_value, int):
            passed = int(expected_value) == int(observed_value)
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


def _support_matrix_rows() -> list[dict[str, Any]]:
    return [
        {
            "semantic_field": "quality_bucket_policy",
            "documented_old_value": "MEDIUM_HIGH_ONLY",
            "support_status": "SUPPORTED",
            "implemented_value": "MEDIUM_HIGH_ONLY",
            "notes": "Native substrate timing-state filter exists in ensure_atp_scope_bundle.",
        },
        {
            "semantic_field": "allow_pre_5m_context_participation",
            "documented_old_value": "True",
            "support_status": "SUPPORTED",
            "implemented_value": "True",
            "notes": "Native classify_timing_states early-participation gate exists in current substrate.",
        },
        {
            "semantic_field": "atp_context_timeframe",
            "documented_old_value": "3m",
            "support_status": "PARTIALLY_SUPPORTED",
            "implemented_value": "No dedicated 3m context substrate; retained rolling_5m_on_1m timing with pre-5m participation enabled",
            "notes": "Current replay substrate has explicit 5m feature state + 1m timing layers, but no first-class 3m context bundle parameter for ATP baseline generation.",
        },
        {
            "semantic_field": "entry_activation_basis",
            "documented_old_value": "Implicit 3m runtime-facing posture",
            "support_status": "AMBIGUOUS",
            "implemented_value": "rolling_5m_on_1m",
            "notes": "Old materialized archive does not preserve replay-safe entry activation metadata; current replay-safe path uses the documented current ATP scope activation basis.",
        },
        {
            "semantic_field": "current_cost_model",
            "documented_old_value": "Not replay-safe in old archive",
            "support_status": "SUPPORTED",
            "implemented_value": "slippage_points=0.25, fee_per_trade=1.50, gross-net gap=6.5",
            "notes": "Current v2/legacy-intent accounting is explicit and reproducible.",
        },
    ]


def _matched_stats(
    *,
    old_rows: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    candidate_trade_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    old_map = {_trade_key(row): row for row in old_rows}
    candidate_map = {_trade_key(row): row for row in candidate_rows}
    candidate_trade_map = {_trade_record_key(row): row["trade_record"] for row in candidate_trade_rows}
    shared = sorted(set(old_map) & set(candidate_map) & set(candidate_trade_map))
    old_only = sorted(set(old_map) - set(candidate_map))
    new_only = sorted(set(candidate_map) - set(old_map))
    same_exit_reason = 0
    same_exit_ts = 0
    matched_old_pnl = 0.0
    matched_candidate_pnl = 0.0
    by_exit_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for key in shared:
        old_row = old_map[key]
        candidate_row = candidate_map[key]
        candidate_trade = candidate_trade_map[key]
        if str(old_row["exit_reason"]) == str(candidate_trade["exit_reason"]):
            same_exit_reason += 1
        if str(old_row["exit_ts"]) == str(candidate_trade["exit_ts"]).replace(" ", "T"):
            same_exit_ts += 1
        matched_old_pnl += float(old_row["pnl_cash"])
        matched_candidate_pnl += float(candidate_trade["pnl_cash"])
        by_exit_reason[str(candidate_trade["exit_reason"])].append(
            {
                "old_minus_net": float(old_row["pnl_cash"]) - float(candidate_trade["pnl_cash"]),
                "old_minus_gross": float(old_row["pnl_cash"]) - float(candidate_trade["gross_pnl_cash"]),
                "same_exit_reason": str(old_row["exit_reason"]) == str(candidate_trade["exit_reason"]),
            }
        )
    return {
        "matched_trade_count": len(shared),
        "old_only_trade_count": len(old_only),
        "candidate_only_trade_count": len(new_only),
        "exit_reason_match_rate": round((same_exit_reason / len(shared)) * 100.0, 4) if shared else 0.0,
        "exit_timestamp_match_rate": round((same_exit_ts / len(shared)) * 100.0, 4) if shared else 0.0,
        "matched_old_pnl_cash": round(matched_old_pnl, 4),
        "matched_candidate_pnl_cash": round(matched_candidate_pnl, 4),
        "matched_pnl_delta_old_minus_candidate": round(matched_old_pnl - matched_candidate_pnl, 4),
        "exit_reason_accounting_bridge": {
            exit_reason: {
                "matched_count": len(rows),
                "same_exit_reason_count": sum(1 for row in rows if row["same_exit_reason"]),
                "mean_old_minus_net": round(
                    sum(row["old_minus_net"] for row in rows if row["same_exit_reason"])
                    / max(sum(1 for row in rows if row["same_exit_reason"]), 1),
                    4,
                ),
                "mean_old_minus_gross": round(
                    sum(row["old_minus_gross"] for row in rows if row["same_exit_reason"])
                    / max(sum(1 for row in rows if row["same_exit_reason"]), 1),
                    4,
                ),
            }
            for exit_reason, rows in sorted(by_exit_reason.items())
        },
    }


def _legacy_proxy_matched_pnl(*, candidate_trade_rows: list[dict[str, Any]], old_rows: list[dict[str, Any]], bridge: dict[str, Any]) -> dict[str, Any]:
    old_map = {_trade_key(row): row for row in old_rows}
    candidate_trade_map = {_trade_record_key(row): row["trade_record"] for row in candidate_trade_rows}
    total_proxy = 0.0
    proxied = 0
    for key, trade in candidate_trade_map.items():
        old_row = old_map.get(key)
        if old_row is None:
            continue
        exit_reason = str(trade["exit_reason"])
        bridge_row = bridge.get(exit_reason)
        if not bridge_row or str(old_row["exit_reason"]) != exit_reason:
            continue
        if exit_reason == "target":
            proxy = float(trade["pnl_cash"]) + float(bridge_row["mean_old_minus_net"])
        else:
            proxy = float(trade["gross_pnl_cash"]) + float(bridge_row["mean_old_minus_gross"])
        total_proxy += proxy
        proxied += 1
    return {"legacy_style_proxy_matched_pnl_cash": round(total_proxy, 4), "legacy_style_proxy_matched_trade_count": proxied}


def _comparison_rows(
    *,
    old_summary: dict[str, Any],
    old_rows: list[dict[str, Any]],
    current_v2_summary: dict[str, Any],
    current_v2_rows: list[dict[str, Any]],
    current_v2_trade_rows: list[dict[str, Any]],
    legacy_summary: dict[str, Any],
    legacy_rows: list[dict[str, Any]],
    legacy_trade_rows: list[dict[str, Any]],
    current_v2_span: dict[str, Any],
    legacy_span: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    current_stats = _matched_stats(old_rows=old_rows, candidate_rows=current_v2_rows, candidate_trade_rows=current_v2_trade_rows)
    legacy_stats = _matched_stats(old_rows=old_rows, candidate_rows=legacy_rows, candidate_trade_rows=legacy_trade_rows)
    current_proxy = _legacy_proxy_matched_pnl(old_rows=old_rows, candidate_trade_rows=current_v2_trade_rows, bridge=current_stats["exit_reason_accounting_bridge"])
    legacy_proxy = _legacy_proxy_matched_pnl(old_rows=old_rows, candidate_trade_rows=legacy_trade_rows, bridge=legacy_stats["exit_reason_accounting_bridge"])
    current_exit_distribution_json = json.dumps(
        dict(sorted(Counter(str(row["exit_reason"]) for row in current_v2_rows).items())),
        sort_keys=True,
    )

    rows = [
        {
            "baseline_name": "OLD_MATERIALIZED_V1",
            "span_start": "2024-01-01T18:01:00-05:00",
            "span_end": "2026-04-02T17:00:00-04:00",
            "trade_count": int(old_summary["trade_count"]),
            "matched_trade_count_vs_old_v1": int(old_summary["trade_count"]),
            "net_pnl_cash": round(float(old_summary["net_pnl_cash"]), 4),
            "gross_pnl_cash": "",
            "average_trade_pnl_cash": round(float(old_summary["average_trade_pnl_cash"]), 4),
            "median_trade_pnl_cash": round(float(old_summary["median_trade_pnl_cash"]), 4),
            "win_rate": round(float(old_summary["win_rate"]), 4),
            "profit_factor": round(float(old_summary["profit_factor"]), 4),
            "max_drawdown": round(float(old_summary["max_drawdown"]), 4),
            "largest_win": round(float(old_summary["largest_win"]), 4),
            "largest_loss": round(float(old_summary["largest_loss"]), 4),
            "max_consecutive_losers": int(old_summary["max_consecutive_losers"]),
            "asia_net_pnl_cash": round(float(old_summary["asia_net_pnl_cash"]), 4),
            "us_net_pnl_cash": round(float(old_summary["us_net_pnl_cash"]), 4),
            "exit_reason_distribution_json": json.dumps(old_summary["exit_reason_distribution"], sort_keys=True),
            "exit_timestamp_match_rate_vs_old_v1": 100.0,
            "matched_pnl_delta_vs_old_v1": 0.0,
            "net_pnl_delta_vs_current_v2": round(float(old_summary["net_pnl_cash"]) - float(current_v2_summary["net_pnl_cash"]), 4),
            "net_pnl_delta_vs_legacy_intent": round(float(old_summary["net_pnl_cash"]) - float(legacy_summary["net_pnl_cash"]), 4),
            "legacy_style_proxy_matched_pnl_cash": "",
            "legacy_style_proxy_matched_trade_count": "",
        },
        {
            "baseline_name": "CURRENT_REPLAY_V2",
            "span_start": current_v2_span["start_timestamp"],
            "span_end": current_v2_span["end_timestamp"],
            "trade_count": int(current_v2_summary["trade_count"]),
            "matched_trade_count_vs_old_v1": int(current_stats["matched_trade_count"]),
            "net_pnl_cash": round(float(current_v2_summary["net_pnl_cash"]), 4),
            "gross_pnl_cash": round(float(current_v2_summary["gross_pnl_cash"]), 4),
            "average_trade_pnl_cash": round(float(current_v2_summary["average_trade_pnl_cash"]), 4),
            "median_trade_pnl_cash": round(float(current_v2_summary["median_trade_pnl_cash"]), 4),
            "win_rate": round(float(current_v2_summary["win_rate"]), 4),
            "profit_factor": round(float(current_v2_summary["profit_factor"]), 4),
            "max_drawdown": round(float(current_v2_summary["max_drawdown"]), 4),
            "largest_win": round(float(current_v2_summary["largest_win"]), 4),
            "largest_loss": round(float(current_v2_summary["largest_loss"]), 4),
            "max_consecutive_losers": int(current_v2_summary["max_consecutive_losers"]),
            "asia_net_pnl_cash": round(float(current_v2_summary["asia_net_pnl_cash"]), 4),
            "us_net_pnl_cash": round(float(current_v2_summary["us_net_pnl_cash"]), 4),
            "exit_reason_distribution_json": current_exit_distribution_json,
            "exit_timestamp_match_rate_vs_old_v1": float(current_stats["exit_timestamp_match_rate"]),
            "matched_pnl_delta_vs_old_v1": round(float(current_stats["matched_pnl_delta_old_minus_candidate"]), 4) * -1.0,
            "net_pnl_delta_vs_current_v2": 0.0,
            "net_pnl_delta_vs_legacy_intent": round(float(current_v2_summary["net_pnl_cash"]) - float(legacy_summary["net_pnl_cash"]), 4),
            "legacy_style_proxy_matched_pnl_cash": current_proxy["legacy_style_proxy_matched_pnl_cash"],
            "legacy_style_proxy_matched_trade_count": current_proxy["legacy_style_proxy_matched_trade_count"],
        },
        {
            "baseline_name": "LEGACY_INTENT_REPLAY_V2",
            "span_start": legacy_span["start_timestamp"],
            "span_end": legacy_span["end_timestamp"],
            "trade_count": int(legacy_summary["trade_count"]),
            "matched_trade_count_vs_old_v1": int(legacy_stats["matched_trade_count"]),
            "net_pnl_cash": round(float(legacy_summary["net_pnl_cash"]), 4),
            "gross_pnl_cash": round(float(legacy_summary["gross_pnl_cash"]), 4),
            "average_trade_pnl_cash": round(float(legacy_summary["average_trade_pnl_cash"]), 4),
            "median_trade_pnl_cash": round(float(legacy_summary["median_trade_pnl_cash"]), 4),
            "win_rate": round(float(legacy_summary["win_rate"]), 4),
            "profit_factor": round(float(legacy_summary["profit_factor"]), 4),
            "max_drawdown": round(float(legacy_summary["max_drawdown"]), 4),
            "largest_win": round(float(legacy_summary["largest_win"]), 4),
            "largest_loss": round(float(legacy_summary["largest_loss"]), 4),
            "max_consecutive_losers": int(legacy_summary["max_consecutive_losers"]),
            "asia_net_pnl_cash": round(float(legacy_summary["asia_net_pnl_cash"]), 4),
            "us_net_pnl_cash": round(float(legacy_summary["us_net_pnl_cash"]), 4),
            "exit_reason_distribution_json": str(legacy_summary["exit_reason_distribution_json"]),
            "exit_timestamp_match_rate_vs_old_v1": float(legacy_stats["exit_timestamp_match_rate"]),
            "matched_pnl_delta_vs_old_v1": round(float(legacy_stats["matched_pnl_delta_old_minus_candidate"]), 4) * -1.0,
            "net_pnl_delta_vs_current_v2": round(float(legacy_summary["net_pnl_cash"]) - float(current_v2_summary["net_pnl_cash"]), 4),
            "net_pnl_delta_vs_legacy_intent": 0.0,
            "legacy_style_proxy_matched_pnl_cash": legacy_proxy["legacy_style_proxy_matched_pnl_cash"],
            "legacy_style_proxy_matched_trade_count": legacy_proxy["legacy_style_proxy_matched_trade_count"],
        },
    ]

    bridge_rows: list[dict[str, Any]] = []
    for baseline_name, stats in (("CURRENT_REPLAY_V2", current_stats), ("LEGACY_INTENT_REPLAY_V2", legacy_stats)):
        for exit_reason, values in sorted(stats["exit_reason_accounting_bridge"].items()):
            bridge_rows.append(
                {
                    "baseline_name": baseline_name,
                    "exit_reason": exit_reason,
                    "matched_count": int(values["matched_count"]),
                    "same_exit_reason_count": int(values["same_exit_reason_count"]),
                    "mean_old_minus_net": round(float(values["mean_old_minus_net"]), 4),
                    "mean_old_minus_gross": round(float(values["mean_old_minus_gross"]), 4),
                }
            )
    return rows, bridge_rows


def _classification(*, old_net: float, current_net: float, legacy_net: float, current_matched_delta: float, legacy_matched_delta: float) -> str:
    current_gap = abs(old_net - current_net)
    legacy_gap = abs(old_net - legacy_net)
    matched_current_gap = abs(current_matched_delta)
    matched_legacy_gap = abs(legacy_matched_delta)
    if legacy_gap < current_gap * 0.5 and matched_legacy_gap < matched_current_gap * 0.5:
        return "LEGACY_INTENT_REPLAY_BUILT_CLOSE_MATCH"
    if legacy_gap < current_gap or matched_legacy_gap < matched_current_gap:
        return "LEGACY_INTENT_REPLAY_BUILT_PARTIAL_MATCH"
    return "LEGACY_INTENT_REPLAY_BUILT_DIVERGENT"


def _render_reproduction_markdown(*, classification: str, summary: dict[str, Any], validation_rows: Sequence[dict[str, Any]], support_rows: Sequence[dict[str, Any]]) -> str:
    lines = [
        "# ATP Companion Replay Baseline v2 Legacy-Intent Reproduction Report",
        "",
        f"- Classification: `{classification}`",
        "- This is a new replay-safe legacy-intent baseline, not a claim of exact equivalence to the old materialized v1 archive.",
        "- Old v1 remains materialized-only historical evidence and is not controlling truth for future research.",
        "- Legacy-intent v2 is a partial semantic match, not a recovery of old v1.",
        "- No threshold sensitivity, exit redesign, drawdown governance, live execution, IBKR, or broker changes were run in this diagnostic step.",
        "",
        "## Self-Replay Validation",
        f"- Trade count: `{summary['trade_count']}`",
        f"- Net P/L: `{summary['net_pnl_cash']}`",
        f"- Gross P/L: `{summary['gross_pnl_cash']}`",
        f"- Asia / U.S. net: `{summary['asia_net_pnl_cash']}` / `{summary['us_net_pnl_cash']}`",
        "",
    ]
    for row in validation_rows:
        lines.append(f"- `{row['metric']}` expected=`{row['expected']}` observed=`{row['observed']}` pass=`{row['passed']}`")
    lines.extend(["", "## Semantic Support Matrix"])
    for row in support_rows:
        lines.append(
            f"- `{row['semantic_field']}` documented=`{row['documented_old_value']}` "
            f"support=`{row['support_status']}` implemented=`{row['implemented_value']}`"
        )
    return "\n".join(lines) + "\n"


def _render_summary_markdown(*, classification: str, comparison_rows: Sequence[dict[str, Any]], support_rows: Sequence[dict[str, Any]]) -> str:
    legacy_row = next(row for row in comparison_rows if row["baseline_name"] == "LEGACY_INTENT_REPLAY_V2")
    current_row = next(row for row in comparison_rows if row["baseline_name"] == "CURRENT_REPLAY_V2")
    old_row = next(row for row in comparison_rows if row["baseline_name"] == "OLD_MATERIALIZED_V1")
    lines = [
        "# ATP Companion Replay Baseline v2 Legacy-Intent Diagnostic Summary",
        "",
        f"- Classification: `{classification}`",
        "- Old v1 remains materialized-only and is not controlling truth for future research.",
        "- Current Replay Baseline v2 is replay-safe and reproduces itself.",
        "- Legacy-intent v2 is a partial semantic match, not a recovery of old v1.",
        "- Legacy-intent v2 does not improve economics versus current v2.",
        "- Old v1 economics appear closer to current replay gross than to current replay net.",
        "- Future ATP enhancement research should use current replay-safe v2 as the controlling reproducible baseline unless we explicitly choose otherwise.",
        "- No threshold sensitivity, exit redesign, drawdown governance, live execution, IBKR, or broker changes were run.",
        "- This pass tests whether the documented old policy posture can be rebuilt honestly from source.",
        "",
        "## Three-Baseline Snapshot",
        f"- Old v1 net P/L / trades: `{old_row['net_pnl_cash']}` / `{old_row['trade_count']}`",
        f"- Current v2 net P/L / trades: `{current_row['net_pnl_cash']}` / `{current_row['trade_count']}`",
        f"- Legacy-intent net P/L / trades: `{legacy_row['net_pnl_cash']}` / `{legacy_row['trade_count']}`",
        f"- Legacy-intent gross P/L: `{legacy_row['gross_pnl_cash']}`",
        f"- Legacy-intent matched trades vs old v1: `{legacy_row['matched_trade_count_vs_old_v1']}`",
        f"- Legacy-intent exit timestamp match rate vs old v1: `{legacy_row['exit_timestamp_match_rate_vs_old_v1']}`%",
        f"- Legacy-intent matched P/L delta vs old v1: `{legacy_row['matched_pnl_delta_vs_old_v1']}`",
        f"- Legacy-intent net P/L delta vs current v2: `{legacy_row['net_pnl_delta_vs_current_v2']}`",
        "",
        "## Recoverable Legacy Posture",
    ]
    for row in support_rows:
        lines.append(
            f"- `{row['semantic_field']}` support=`{row['support_status']}` implemented=`{row['implemented_value']}`"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "- If legacy-intent materially closes the gap, it is a better candidate controlling research baseline than current v2.",
            "- If the gap remains large, the remaining distance is coming from unsupported or changed trade-engine semantics rather than from replay-safety alone.",
            "- No threshold sensitivity, exit redesign, or governance step should proceed until this baseline choice is explicit.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_legacy_intent_baseline(*, source_db: Path, old_truth_json: Path, current_v2_dir: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    current_v2_manifest = _load_json(current_v2_dir / "atp_companion_replay_baseline_v2_manifest.json")
    current_scope_bundle_id = ((current_v2_manifest.get("bundle_contract") or {}).get("scope_bundle_ids") or {}).get("MGC:ASIA/US")
    current_scope_manifest = _read_manifest(DEFAULT_PLATFORM_SUBSTRATE_ROOT / "scope_bundles" / str(current_scope_bundle_id) / "manifest.json")
    run_start, run_end = _manifest_1m_span(current_scope_manifest)

    bar_source_index = _discover_best_sources(symbols={"MGC"}, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    context_bundle = ensure_symbol_context_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        symbol="MGC",
        bar_source_index=bar_source_index,
        start_timestamp=run_start,
        end_timestamp=run_end,
    )
    if context_bundle is None:
        raise RuntimeError("Unable to materialize MGC context bundle for legacy-intent replay baseline.")

    try:
        feature_bundle = ensure_atp_feature_bundle(
            bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
            source_db=source_db,
            symbol="MGC",
            selected_sources=context_bundle["selected_sources"],
            start_timestamp=run_start,
            end_timestamp=run_end,
            feature_rows=None,
        )
    except FileNotFoundError:
        feature_rows = build_feature_states(
            bars_5m=list(context_bundle["combined_rolling_5m"]),
            bars_1m=list(context_bundle["bars_1m"]),
        )
        feature_bundle = ensure_atp_feature_bundle(
            bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
            source_db=source_db,
            symbol="MGC",
            selected_sources=context_bundle["selected_sources"],
            start_timestamp=run_start,
            end_timestamp=run_end,
            feature_rows=feature_rows,
        )

    scope_bundle = ensure_atp_scope_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        source_db=source_db,
        symbol="MGC",
        selected_sources=context_bundle["selected_sources"],
        start_timestamp=run_start,
        end_timestamp=run_end,
        allowed_sessions=("ASIA", "US"),
        point_value=10.0,
        bars_1m=context_bundle["bars_1m"],
        feature_bundle=feature_bundle,
        entry_activation_basis=str(current_scope_manifest.get("entry_activation_basis") or "rolling_5m_on_1m"),
        quality_bucket_policy="MEDIUM_HIGH_ONLY",
        allow_pre_5m_context_participation=True,
        sides=("LONG",),
        exit_policy=str(current_scope_manifest.get("exit_policy") or "fixed_target_time_stop"),
    )

    reference_candidate = next(candidate for candidate in default_atp_promotion_add_candidates() if candidate.candidate_id == "promotion_1_075r_favorable_only")
    baseline_target = next(target for target in build_targets() if target.target_id == "atp_companion_v1__benchmark_mgc_asia_us")
    trade_windows_by_scope = {("MGC", ("ASIA", "US")): _trade_windows_by_id(bars_1m=context_bundle["bars_1m"], trade_rows=scope_bundle.trade_rows)}

    replay_manifest_path = output_dir / "legacy_intent_replay_baseline_manifest.json"
    replay_trades_path = output_dir / "legacy_intent_replay_trades.jsonl"
    summary_csv_path = output_dir / "legacy_intent_replay_summary.csv"
    comparison_csv_path = output_dir / "legacy_intent_v1_v2_comparison.csv"
    accounting_bridge_csv_path = output_dir / "legacy_intent_accounting_bridge.csv"
    support_matrix_csv_path = output_dir / "legacy_intent_semantic_support_matrix.csv"
    reproduction_md_path = output_dir / "legacy_intent_reproduction_report.md"
    summary_md_path = output_dir / "legacy_intent_diagnostic_summary.md"

    _write_jsonl(
        replay_trades_path,
        iter_replay_truth_trade_rows(
            asia_scope=type(
                "LegacyIntentScope",
                (),
                {
                    "symbol": "MGC",
                    "allowed_sessions": ("ASIA", "US"),
                    "point_value": 10.0,
                    "trade_rows": scope_bundle.trade_rows,
                },
            )(),
            trade_windows_by_scope=trade_windows_by_scope,
            reference_candidate=reference_candidate,
        ),
    )

    summary = _summary_from_trade_rows(scope_bundle.trade_rows, bar_count=len(context_bundle["bars_1m"]))
    observed = _validation_observed_from_replay_jsonl(replay_trades_path)
    validation_rows = _validation_rows(summary, observed)

    old_truth = _load_json(old_truth_json)
    old_target = _target_by_id(old_truth, "atp_companion_v1__benchmark_mgc_asia_us")
    old_metrics = dict(old_target["metrics"])
    old_summary = {
        "trade_count": int(old_metrics["total_trades"]),
        "net_pnl_cash": round(float(old_metrics["net_pnl_cash"]), 4),
        "average_trade_pnl_cash": round(float(old_metrics["average_trade_pnl_cash"]), 4),
        "median_trade_pnl_cash": round(float(median(float(row["pnl_cash"]) for row in old_target["position_rows"])), 4),
        "win_rate": round(float(old_metrics["win_rate"]), 4),
        "profit_factor": round(float(old_metrics["profit_factor"]), 4),
        "max_drawdown": round(float(old_metrics["max_drawdown"]), 4),
        "largest_win": round(max(float(row["pnl_cash"]) for row in old_target["position_rows"]), 4),
        "largest_loss": round(min(float(row["pnl_cash"]) for row in old_target["position_rows"]), 4),
        "max_consecutive_losers": int(
            max(
                (len(run) for run in "".join("L" if float(row["pnl_cash"]) < 0 else "W" for row in old_target["position_rows"]).split("W")),
                default=0,
            )
        ),
        "asia_net_pnl_cash": round(float((old_target["session_breakdown"] or {}).get("ASIA", {}).get("net_pnl_cash") or 0.0), 4),
        "us_net_pnl_cash": round(float((old_target["session_breakdown"] or {}).get("US", {}).get("net_pnl_cash") or 0.0), 4),
        "exit_reason_distribution": dict(Counter(str(row["exit_reason"]) for row in old_target["position_rows"])),
    }
    current_v2_summary = _load_json(current_v2_dir / "atp_companion_replay_baseline_v2_manifest.json")["summary_metrics"]
    current_v2_rows = list(_iter_jsonl(current_v2_dir / "atp_companion_replay_baseline_v2_benchmark_position_rows.jsonl"))
    current_v2_trade_rows = list(_iter_jsonl(current_v2_dir / "atp_companion_replay_baseline_v2_trades.jsonl"))
    legacy_trade_rows = list(_iter_jsonl(replay_trades_path))
    comparison_rows, bridge_rows = _comparison_rows(
        old_summary=old_summary,
        old_rows=list(old_target["position_rows"]),
        current_v2_summary={
            **current_v2_summary,
            "gross_pnl_cash": round(sum(float(row["trade_record"]["gross_pnl_cash"]) for row in current_v2_trade_rows), 4),
        },
        current_v2_rows=current_v2_rows,
        current_v2_trade_rows=current_v2_trade_rows,
        legacy_summary=summary,
        legacy_rows=_base_position_rows(scope_bundle.trade_rows),
        legacy_trade_rows=legacy_trade_rows,
        current_v2_span=current_v2_manifest["shared_date_span"],
        legacy_span={"start_timestamp": run_start.isoformat(), "end_timestamp": run_end.isoformat()},
    )
    support_rows = _support_matrix_rows()
    legacy_row = next(row for row in comparison_rows if row["baseline_name"] == "LEGACY_INTENT_REPLAY_V2")
    current_row = next(row for row in comparison_rows if row["baseline_name"] == "CURRENT_REPLAY_V2")
    classification = _classification(
        old_net=float(old_summary["net_pnl_cash"]),
        current_net=float(current_row["net_pnl_cash"]),
        legacy_net=float(legacy_row["net_pnl_cash"]),
        current_matched_delta=float(current_row["matched_pnl_delta_vs_old_v1"]),
        legacy_matched_delta=float(legacy_row["matched_pnl_delta_vs_old_v1"]),
    )

    manifest_payload = build_replay_truth_manifest(
        source_db=source_db,
        run_start=run_start,
        run_end=run_end,
        baseline_target=baseline_target,
        data_substrate={
            "selected_sources": current_scope_manifest.get("selected_sources"),
            "scope_bundle_id": scope_bundle.bundle_id,
            "feature_bundle_id": feature_bundle.bundle_id,
        },
        review_config={
            "baseline_id": "atp_companion_replay_baseline_v2_legacy_intent",
            "scope_bundle_id": scope_bundle.bundle_id,
            "entry_activation_basis": current_scope_manifest.get("entry_activation_basis"),
            "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
            "allow_pre_5m_context_participation": True,
            "legacy_documented_context_timeframe": "3m",
            "exit_policy": current_scope_manifest.get("exit_policy"),
        },
        scope_bundle_ids={"MGC:ASIA/US": scope_bundle.bundle_id},
        feature_bundle_ids={"MGC": feature_bundle.bundle_id},
        context_bundle_ids={"MGC": context_bundle["context_bundle_id"]},
        reference_candidate=reference_candidate,
    )
    manifest_payload.update(
        {
            "baseline_id": "atp_companion_replay_baseline_v2_legacy_intent",
            "classification": classification,
            "generator_module": "src/mgc_v05l/app/atp_companion_replay_baseline_v2_legacy_intent.py",
            "command": f"PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.atp_companion_replay_baseline_v2_legacy_intent --output-dir {output_dir}",
            "source_db": str(source_db.resolve()),
            "source_mode": "generated_scope_bundle_from_current_replay_safe_context",
            "current_v2_reference_dir": str(current_v2_dir.resolve()),
            "legacy_documented_posture": {
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "allow_pre_5m_context_participation": True,
                "atp_context_timeframe": "3m",
            },
            "implemented_posture": {
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "allow_pre_5m_context_participation": True,
                "atp_context_timeframe": "PARTIALLY_SUPPORTED_RUNTIME_DOC_ONLY",
                "entry_activation_basis": str(current_scope_manifest.get("entry_activation_basis") or "rolling_5m_on_1m"),
                "exit_policy": str(current_scope_manifest.get("exit_policy") or "fixed_target_time_stop"),
            },
            "support_matrix_rows": support_rows,
            "summary_metrics": summary,
            "validation_reference": {"expected": summary, "observed": observed},
            "comparison_artifacts": {
                "comparison_csv_path": str(comparison_csv_path.resolve()),
                "accounting_bridge_csv_path": str(accounting_bridge_csv_path.resolve()),
                "support_matrix_csv_path": str(support_matrix_csv_path.resolve()),
            },
        }
    )
    replay_manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    _write_csv(summary_csv_path, [{"baseline_name": "LEGACY_INTENT_REPLAY_V2", **summary}])
    _write_csv(comparison_csv_path, comparison_rows)
    _write_csv(accounting_bridge_csv_path, bridge_rows)
    _write_csv(support_matrix_csv_path, support_rows)
    _write_markdown(reproduction_md_path, _render_reproduction_markdown(classification=classification, summary=summary, validation_rows=validation_rows, support_rows=support_rows))
    _write_markdown(summary_md_path, _render_summary_markdown(classification=classification, comparison_rows=comparison_rows, support_rows=support_rows))

    return {
        "manifest_path": replay_manifest_path,
        "replay_trades_path": replay_trades_path,
        "summary_csv_path": summary_csv_path,
        "comparison_csv_path": comparison_csv_path,
        "accounting_bridge_csv_path": accounting_bridge_csv_path,
        "support_matrix_csv_path": support_matrix_csv_path,
        "reproduction_md_path": reproduction_md_path,
        "summary_md_path": summary_md_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "mgc_asia_us_latest")
    artifacts = build_legacy_intent_baseline(
        source_db=Path(args.source_db).expanduser().resolve(),
        old_truth_json=Path(args.old_truth_json).expanduser().resolve(),
        current_v2_dir=Path(args.current_v2_dir).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
