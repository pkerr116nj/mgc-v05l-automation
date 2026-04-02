"""Approval-pass evaluation for narrowed futures strategy lanes."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .quant_futures import StrategyResearchArtifacts, _clip, _expectancy, _top_positive_share, _walk_forward_positive_ratio
from .quant_futures_phase2a import _build_symbol_store
from .quant_futures_phase2c import (
    Phase2CTrade,
    Phase2CVariantSpec,
    _build_perturbation_neighbors,
    _build_phase2c_variants,
    _evaluate_variant,
)

BREAKOUT_APPROVAL_IDS = (
    "phase2c.breakout.metals_only.no_london.baseline",
    "phase2c.breakout.metals_only.us_unknown.baseline",
    "phase2c.breakout.metals_only.us_unknown.soft_0_56",
    "phase2c.breakout.fx_metals_full.us_unknown.soft_0_56",
    "phase2c.breakout.fx_metals_full.no_london.baseline",
)

FAILED_APPROVAL_IDS = (
    "phase2c.failed.core4.no_us.baseline",
    "phase2c.failed.core4_plus_qc.no_us.baseline",
    "phase2c.failed.core4_plus_zt.no_us.baseline",
    "phase2c.failed.core4_plus_qc_zt.no_us.baseline",
    "phase2c.failed.core4_plus_qc_zt.no_us.soft_0_30",
    "phase2c.failed.core4.no_us.no_extension",
    "phase2c.failed.core4.no_us.no_failed_breakout",
)

MEAN_REVERSION_SANDBOX_IDS = ("phase2c.meanrev.metals.us_only",)


def run_quant_futures_approval_pass(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    symbols: tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
) -> StrategyResearchArtifacts:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir or Path.cwd() / "outputs" / "reports" / "quant_futures_approval_pass").resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    variants = [variant for variant in _build_phase2c_variants() if variant.variant_id in set(BREAKOUT_APPROVAL_IDS + FAILED_APPROVAL_IDS + MEAN_REVERSION_SANDBOX_IDS)]
    selected_symbols = tuple(symbols) if symbols else tuple(sorted({symbol for variant in variants for symbol in variant.symbols}))
    symbol_store = _build_symbol_store(
        database_path=resolved_database_path,
        execution_timeframe=execution_timeframe,
        symbols=selected_symbols,
    )

    evaluated = [_evaluate_variant(variant=variant, symbol_store=symbol_store) for variant in variants]
    rows_by_id = {row["variant"]["variant_id"]: row for row in evaluated}

    breakout_rows = [row for row in evaluated if row["variant"]["variant_id"] in BREAKOUT_APPROVAL_IDS]
    failed_rows = [row for row in evaluated if row["variant"]["variant_id"] in FAILED_APPROVAL_IDS and row["variant"]["role"] == "lane"]
    failed_driver_rows = [row for row in evaluated if row["variant"]["variant_id"] in FAILED_APPROVAL_IDS and row["variant"]["role"] != "lane"]
    mean_reversion_row = next((row for row in evaluated if row["variant"]["variant_id"] in MEAN_REVERSION_SANDBOX_IDS), None)

    for row in breakout_rows + failed_rows:
        row["approval"] = _build_approval_assessment(row=row, symbol_store=symbol_store)
        row["summary"]["approval_score"] = _approval_score(row["summary"], row["approval"], row["variant"])
        row["approval_verdict"] = _approval_verdict(row=row)
        row["deployment_status"] = _deployment_status(row=row)

    breakout_rows.sort(key=lambda row: (-row["summary"]["approval_score"], -row["approval"]["cost_expectancy_r_025"], row["variant"]["variant_id"]))
    failed_rows.sort(key=lambda row: (-row["summary"]["approval_score"], -row["approval"]["cost_expectancy_r_025"], row["variant"]["variant_id"]))

    breakout_best = breakout_rows[0]
    failed_best = failed_rows[0]

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_path": str(resolved_database_path),
        "execution_timeframe": execution_timeframe,
        "symbols_tested": list(selected_symbols),
        "phase": "approval_pass",
        "approval_standard": {
            "must_hold": [
                "positive post-cost under tougher assumptions",
                "survive stricter leave-one-slice-out checks",
                "coherent economic explanation",
                "stable local strategy lane rather than a thin parameter artifact",
            ]
        },
        "approval_pass_verdicts": {
            "breakout_long": _lane_report_breakout(breakout_rows=breakout_rows, rows_by_id=rows_by_id),
            "failed_move_short": _lane_report_failed(failed_rows=failed_rows, driver_rows=failed_driver_rows, rows_by_id=rows_by_id),
            "mean_reversion_sandbox": _lane_report_mean_reversion(mean_reversion_row),
        },
        "recommended_final_shortlist_definition": {
            "breakout_long": _final_shortlist_definition_breakout(breakout_best),
            "failed_move_short": _final_shortlist_definition_failed(failed_best),
        },
        "permanent_exclusions": _permanent_exclusions(breakout_best=breakout_best, failed_best=failed_best),
        "deployment_readiness": {
            "breakout_long": breakout_best["deployment_status"],
            "failed_move_short": failed_best["deployment_status"],
        },
    }

    json_path = resolved_output_dir / "quant_futures_approval_pass.json"
    markdown_path = resolved_output_dir / "quant_futures_approval_pass.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return StrategyResearchArtifacts(json_path=json_path, markdown_path=markdown_path, report=report)


def _build_approval_assessment(
    *,
    row: dict[str, Any],
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    variant = Phase2CVariantSpec(**row["variant"])
    trades = row["trades"]
    strict_summary = _strict_summary(trades=trades, symbols=variant.symbols, sessions=variant.allowed_sessions)
    leave_one_symbol_out = _strict_leave_one_symbol_out(trades=trades, symbols=variant.symbols, sessions=variant.allowed_sessions)
    leave_one_session_out = _strict_leave_one_session_out(trades=trades, symbols=variant.symbols, sessions=variant.allowed_sessions)
    subperiod = _strict_subperiod(trades)
    perturbation = _strict_perturbation(variant=variant, symbol_store=symbol_store)
    return {
        **strict_summary,
        "leave_one_symbol_out": leave_one_symbol_out,
        "leave_one_session_out": leave_one_session_out,
        "leave_one_symbol_out_positive_ratio_020": round(sum(1 for row in leave_one_symbol_out if row["cost_expectancy_r_020"] > 0.0) / float(max(len(leave_one_symbol_out), 1)), 6),
        "leave_one_symbol_out_positive_ratio_025": round(sum(1 for row in leave_one_symbol_out if row["cost_expectancy_r_025"] > 0.0) / float(max(len(leave_one_symbol_out), 1)), 6),
        "leave_one_session_out_positive_ratio_020": round(sum(1 for row in leave_one_session_out if row["cost_expectancy_r_020"] > 0.0) / float(max(len(leave_one_session_out), 1)), 6),
        "leave_one_session_out_positive_ratio_025": round(sum(1 for row in leave_one_session_out if row["cost_expectancy_r_025"] > 0.0) / float(max(len(leave_one_session_out), 1)), 6),
        "subperiod": subperiod,
        "perturbation": perturbation,
    }


def _strict_summary(
    *,
    trades: list[Phase2CTrade],
    symbols: tuple[str, ...],
    sessions: tuple[str, ...],
) -> dict[str, Any]:
    r_values = [trade.r_multiple for trade in trades]
    per_symbol = []
    for symbol in symbols:
        symbol_values = [trade.r_multiple for trade in trades if trade.symbol == symbol]
        per_symbol.append(
            {
                "symbol": symbol,
                "trade_count": len(symbol_values),
                "cost_expectancy_r_020": _expectancy([value - 0.20 for value in symbol_values]),
                "cost_expectancy_r_025": _expectancy([value - 0.25 for value in symbol_values]),
            }
        )
    per_session = []
    for session in sessions:
        session_values = [trade.r_multiple for trade in trades if trade.entry_session == session]
        if not session_values:
            continue
        per_session.append(
            {
                "session": session,
                "trade_count": len(session_values),
                "cost_expectancy_r_020": _expectancy([value - 0.20 for value in session_values]),
                "cost_expectancy_r_025": _expectancy([value - 0.25 for value in session_values]),
            }
        )
    symbol_totals: dict[str, float] = defaultdict(float)
    for trade in trades:
        symbol_totals[trade.symbol] += trade.r_multiple
    total = sum(r_values)
    dominant_symbol = max(symbol_totals, key=symbol_totals.get) if symbol_totals else None
    dominant_symbol_total = symbol_totals.get(dominant_symbol, 0.0) if dominant_symbol else 0.0
    return {
        "trade_count": len(r_values),
        "cost_expectancy_r_020": _expectancy([value - 0.20 for value in r_values]),
        "cost_expectancy_r_025": _expectancy([value - 0.25 for value in r_values]),
        "positive_symbol_share_cost_020": round(sum(1 for row in per_symbol if row["cost_expectancy_r_020"] > 0.0) / float(max(len(symbols), 1)), 6),
        "positive_symbol_share_cost_025": round(sum(1 for row in per_symbol if row["cost_expectancy_r_025"] > 0.0) / float(max(len(symbols), 1)), 6),
        "walk_forward_positive_ratio_cost_020": _walk_forward_positive_ratio_cost(trades, 0.20),
        "walk_forward_positive_ratio_cost_025": _walk_forward_positive_ratio_cost(trades, 0.25),
        "per_symbol": per_symbol,
        "per_session": per_session,
        "concentration": {
            "dominant_symbol": dominant_symbol,
            "dominant_symbol_share_of_total_r": round(dominant_symbol_total / total, 6) if total != 0.0 else 0.0,
            "top_3_positive_share": _top_positive_share(r_values, 3),
        },
    }


def _strict_leave_one_symbol_out(
    *,
    trades: list[Phase2CTrade],
    symbols: tuple[str, ...],
    sessions: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows = []
    for symbol in symbols:
        subset = [trade for trade in trades if trade.symbol != symbol]
        if not subset:
            continue
        summary = _strict_summary(trades=subset, symbols=tuple(sym for sym in symbols if sym != symbol), sessions=sessions)
        rows.append(
            {
                "excluded_symbol": symbol,
                "trade_count": summary["trade_count"],
                "cost_expectancy_r_020": summary["cost_expectancy_r_020"],
                "cost_expectancy_r_025": summary["cost_expectancy_r_025"],
            }
        )
    return rows


def _strict_leave_one_session_out(
    *,
    trades: list[Phase2CTrade],
    symbols: tuple[str, ...],
    sessions: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows = []
    present_sessions = sorted({trade.entry_session for trade in trades})
    for session in present_sessions:
        subset = [trade for trade in trades if trade.entry_session != session]
        if not subset:
            continue
        summary = _strict_summary(trades=subset, symbols=symbols, sessions=tuple(sess for sess in sessions if sess != session))
        rows.append(
            {
                "excluded_session": session,
                "trade_count": summary["trade_count"],
                "cost_expectancy_r_020": summary["cost_expectancy_r_020"],
                "cost_expectancy_r_025": summary["cost_expectancy_r_025"],
            }
        )
    return rows


def _strict_subperiod(trades: list[Phase2CTrade]) -> dict[str, Any]:
    if not trades:
        return {
            "positive_ratio_cost_020": 0.0,
            "positive_ratio_cost_025": 0.0,
            "worst_cost_expectancy_r_025": 0.0,
        }
    ordered = sorted(trades, key=lambda trade: trade.entry_ts)
    bucket_size = max(len(ordered) // 6, 1)
    rows = []
    for start in range(0, len(ordered), bucket_size):
        bucket = ordered[start : start + bucket_size]
        values = [trade.r_multiple for trade in bucket]
        rows.append(
            {
                "trade_count": len(bucket),
                "cost_expectancy_r_020": _expectancy([value - 0.20 for value in values]),
                "cost_expectancy_r_025": _expectancy([value - 0.25 for value in values]),
            }
        )
    return {
        "windows": rows,
        "positive_ratio_cost_020": round(sum(1 for row in rows if row["cost_expectancy_r_020"] > 0.0) / float(len(rows)), 6),
        "positive_ratio_cost_025": round(sum(1 for row in rows if row["cost_expectancy_r_025"] > 0.0) / float(len(rows)), 6),
        "worst_cost_expectancy_r_025": min((row["cost_expectancy_r_025"] for row in rows), default=0.0),
    }


def _strict_perturbation(
    *,
    variant: Phase2CVariantSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    rows = [_evaluate_variant(variant=neighbor, symbol_store=symbol_store) for neighbor in _build_perturbation_neighbors(variant)]
    strict_rows = [_strict_summary(trades=row["trades"], symbols=tuple(row["variant"]["symbols"]), sessions=tuple(row["variant"]["allowed_sessions"])) for row in rows]
    return {
        "tested_neighbors": len(strict_rows),
        "positive_ratio_cost_020": round(sum(1 for row in strict_rows if row["cost_expectancy_r_020"] > 0.0) / float(max(len(strict_rows), 1)), 6),
        "positive_ratio_cost_025": round(sum(1 for row in strict_rows if row["cost_expectancy_r_025"] > 0.0) / float(max(len(strict_rows), 1)), 6),
        "median_cost_expectancy_r_025": _expectancy([row["cost_expectancy_r_025"] for row in strict_rows]),
        "worst_cost_expectancy_r_025": min((row["cost_expectancy_r_025"] for row in strict_rows), default=0.0),
    }


def _walk_forward_positive_ratio_cost(trades: list[Phase2CTrade], cost_per_trade: float) -> float:
    if not trades:
        return 0.0
    ordered = sorted(trades, key=lambda row: row.entry_ts)
    bucket_size = max(len(ordered) // 4, 1)
    positive = 0
    total = 0
    for start in range(0, len(ordered), bucket_size):
        bucket = ordered[start : start + bucket_size]
        if not bucket:
            continue
        total += 1
        adjusted = [row.r_multiple - cost_per_trade for row in bucket]
        if _expectancy(adjusted) > 0.0:
            positive += 1
    return round(positive / float(total), 6) if total else 0.0


def _approval_score(summary: dict[str, Any], approval: dict[str, Any], variant: dict[str, Any]) -> float:
    cost020_score = _clip((approval["cost_expectancy_r_020"] + 0.04) / 0.18, 0.0, 1.0)
    cost025_score = _clip((approval["cost_expectancy_r_025"] + 0.04) / 0.16, 0.0, 1.0)
    breadth020 = approval["positive_symbol_share_cost_020"]
    breadth025 = approval["positive_symbol_share_cost_025"]
    leave_symbol = approval["leave_one_symbol_out_positive_ratio_020"]
    leave_session = approval["leave_one_session_out_positive_ratio_020"]
    subperiod = approval["subperiod"]["positive_ratio_cost_020"]
    perturb = approval["perturbation"]["positive_ratio_cost_020"]
    simplicity = _clip((6.0 - float(variant["complexity_points"])) / 4.0, 0.0, 1.0)
    concentration_penalty = _clip(approval["concentration"]["dominant_symbol_share_of_total_r"] / 0.60, 0.0, 1.0)
    worst_bucket_penalty = _clip(-approval["subperiod"]["worst_cost_expectancy_r_025"] / 0.15, 0.0, 1.0)
    score = 100.0 * (
        0.22 * cost025_score
        + 0.18 * cost020_score
        + 0.10 * breadth025
        + 0.08 * breadth020
        + 0.10 * leave_symbol
        + 0.08 * leave_session
        + 0.08 * subperiod
        + 0.08 * perturb
        + 0.04 * approval["walk_forward_positive_ratio_cost_020"]
        + 0.04 * approval["walk_forward_positive_ratio_cost_025"]
        + 0.04 * simplicity
        - 0.08 * concentration_penalty
        - 0.06 * worst_bucket_penalty
    )
    return round(score, 4)


def _approval_verdict(*, row: dict[str, Any]) -> dict[str, Any]:
    approval = row["approval"]
    approved = bool(
        approval["cost_expectancy_r_020"] > 0.0
        and approval["cost_expectancy_r_025"] > 0.0
        and approval["positive_symbol_share_cost_020"] >= 0.50
        and approval["leave_one_symbol_out_positive_ratio_020"] >= 0.75
        and approval["leave_one_session_out_positive_ratio_020"] >= 0.50
        and approval["subperiod"]["positive_ratio_cost_020"] >= 0.50
        and approval["perturbation"]["positive_ratio_cost_020"] >= 0.50
        and approval["concentration"]["dominant_symbol_share_of_total_r"] <= 0.65
    )
    return {
        "approved": approved,
        "cost_expectancy_r_020": approval["cost_expectancy_r_020"],
        "cost_expectancy_r_025": approval["cost_expectancy_r_025"],
        "leave_one_symbol_out_positive_ratio_020": approval["leave_one_symbol_out_positive_ratio_020"],
        "leave_one_session_out_positive_ratio_020": approval["leave_one_session_out_positive_ratio_020"],
        "subperiod_positive_ratio_cost_020": approval["subperiod"]["positive_ratio_cost_020"],
        "perturbation_positive_ratio_cost_020": approval["perturbation"]["positive_ratio_cost_020"],
    }


def _deployment_status(*, row: dict[str, Any]) -> str:
    verdict = row["approval_verdict"]
    if not verdict["approved"]:
        return "probationary_tracking_only"
    if row["approval"]["cost_expectancy_r_025"] > 0.05 and row["approval"]["leave_one_symbol_out_positive_ratio_020"] >= 1.0:
        return "operator_baseline_candidate"
    return "probationary_tracking_only"


def _lane_report_breakout(*, breakout_rows: list[dict[str, Any]], rows_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    best = breakout_rows[0]
    return {
        "approval_pass_verdict": best["approval_verdict"],
        "best_candidate": _render_candidate(best),
        "ranked_candidates": [_render_candidate(row) for row in breakout_rows],
        "durable_core_test": {
            "metals_only_no_london_baseline": _render_candidate(rows_by_id["phase2c.breakout.metals_only.no_london.baseline"]),
            "soft_scored_us_unknown_variant": _render_candidate(rows_by_id["phase2c.breakout.fx_metals_full.us_unknown.soft_0_56"]),
        },
        "six_j_scope_test": {
            "with_6J": _render_candidate(rows_by_id["phase2c.breakout.fx_metals_full.us_unknown.soft_0_56"]),
            "without_6J_metals_only": _render_candidate(rows_by_id["phase2c.breakout.metals_only.us_unknown.soft_0_56"]),
            "recommended_in_scope": "6J" in best["variant"]["symbols"] and _symbol_cost(best, "6J") > 0.0,
        },
        "clean_edge_explanation": (
            "Metals continuation works when short-horizon breakout acceptance follows prior compression and the trade is kept out of London. The softer continuation score improves selectivity, but the durable economic core is metals-led continuation rather than broad cross-asset momentum."
        ),
    }


def _lane_report_failed(*, failed_rows: list[dict[str, Any]], driver_rows: list[dict[str, Any]], rows_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    best = failed_rows[0]
    driver_by_id = {row["variant"]["variant_id"]: row for row in driver_rows}
    return {
        "approval_pass_verdict": best["approval_verdict"],
        "best_candidate": _render_candidate(best),
        "ranked_candidates": [_render_candidate(row) for row in failed_rows],
        "core_vs_additive_slices": {
            "core4": _render_candidate(rows_by_id["phase2c.failed.core4.no_us.baseline"]),
            "core4_plus_qc": _render_candidate(rows_by_id["phase2c.failed.core4_plus_qc.no_us.baseline"]),
            "core4_plus_zt": _render_candidate(rows_by_id["phase2c.failed.core4_plus_zt.no_us.baseline"]),
            "core4_plus_qc_zt": _render_candidate(rows_by_id["phase2c.failed.core4_plus_qc_zt.no_us.baseline"]),
        },
        "indispensable_components": {
            "extension_required": _component_delta(
                keep_row=rows_by_id["phase2c.failed.core4.no_us.baseline"],
                drop_row=driver_by_id["phase2c.failed.core4.no_us.no_extension"],
            ),
            "failed_breakout_required": _component_delta(
                keep_row=rows_by_id["phase2c.failed.core4.no_us.baseline"],
                drop_row=driver_by_id["phase2c.failed.core4.no_us.no_failed_breakout"],
            ),
            "soft_reversal_score_needed": _component_delta(
                keep_row=rows_by_id["phase2c.failed.core4_plus_qc_zt.no_us.baseline"],
                drop_row=rows_by_id["phase2c.failed.core4_plus_qc_zt.no_us.soft_0_30"],
            ),
        },
        "clean_edge_explanation": (
            "The lane captures failed upside excursions that reverse outside the US session after extension has already stretched the market. The structural core is extension plus failed breakout rejection in CL/ES/6E/6J, with QC and ZT only modest additive satellites."
        ),
    }


def _lane_report_mean_reversion(mean_reversion_row: dict[str, Any] | None) -> dict[str, Any]:
    if mean_reversion_row is None:
        return {"recommendation": "discard"}
    trades = mean_reversion_row["trades"]
    approval = _strict_summary(
        trades=trades,
        symbols=tuple(mean_reversion_row["variant"]["symbols"]),
        sessions=tuple(mean_reversion_row["variant"]["allowed_sessions"]),
    )
    return {
        "best_candidate": {
            "variant_id": mean_reversion_row["variant"]["variant_id"],
            "cost_expectancy_r_020": approval["cost_expectancy_r_020"],
            "cost_expectancy_r_025": approval["cost_expectancy_r_025"],
            "trade_count": approval["trade_count"],
        },
        "recommendation": "keep_small_sandbox_only" if approval["cost_expectancy_r_020"] <= 0.0 else "keep_small_sandbox_only",
    }


def _render_candidate(row: dict[str, Any]) -> dict[str, Any]:
    approval = row.get("approval")
    payload = {
        "variant_id": row["variant"]["variant_id"],
        "cluster_name": row["variant"]["cluster_name"],
        "session_name": row["variant"]["session_name"],
        "trade_count": row["summary"]["trade_count"],
        "cost_expectancy_r_010": row["summary"]["cost_expectancy_r_010"],
        "cost_expectancy_r_015": row["summary"]["cost_expectancy_r_015"],
        "approval_score": row["summary"].get("approval_score"),
    }
    if approval is not None:
        payload.update(
            {
                "cost_expectancy_r_020": approval["cost_expectancy_r_020"],
                "cost_expectancy_r_025": approval["cost_expectancy_r_025"],
                "positive_symbol_share_cost_020": approval["positive_symbol_share_cost_020"],
                "leave_one_symbol_out_positive_ratio_020": approval["leave_one_symbol_out_positive_ratio_020"],
                "leave_one_session_out_positive_ratio_020": approval["leave_one_session_out_positive_ratio_020"],
                "deployment_status": row["deployment_status"],
            }
        )
    return payload


def _component_delta(*, keep_row: dict[str, Any], drop_row: dict[str, Any]) -> dict[str, Any]:
    keep_cost = _strict_summary(
        trades=keep_row["trades"],
        symbols=tuple(keep_row["variant"]["symbols"]),
        sessions=tuple(keep_row["variant"]["allowed_sessions"]),
    )
    drop_cost = _strict_summary(
        trades=drop_row["trades"],
        symbols=tuple(drop_row["variant"]["symbols"]),
        sessions=tuple(drop_row["variant"]["allowed_sessions"]),
    )
    return {
        "with_component_cost_expectancy_r_025": keep_cost["cost_expectancy_r_025"],
        "without_component_cost_expectancy_r_025": drop_cost["cost_expectancy_r_025"],
        "delta_cost_expectancy_r_025": round(keep_cost["cost_expectancy_r_025"] - drop_cost["cost_expectancy_r_025"], 6),
    }


def _symbol_cost(row: dict[str, Any], symbol: str) -> float:
    for payload in row["approval"]["per_symbol"]:
        if payload["symbol"] == symbol:
            return float(payload["cost_expectancy_r_025"])
    return 0.0


def _final_shortlist_definition_breakout(best_row: dict[str, Any]) -> dict[str, Any]:
    include_6j = best_row["variant"]["cluster_name"] == "fx_metals_full" and _symbol_cost(best_row, "6J") > 0.0
    symbols = list(best_row["variant"]["symbols"])
    if not include_6j and "6J" in symbols:
        symbols = [symbol for symbol in symbols if symbol != "6J"]
    return {
        "approved": best_row["approval_verdict"]["approved"],
        "recommended_variant_id": best_row["variant"]["variant_id"],
        "symbols": symbols,
        "sessions": list(best_row["variant"]["allowed_sessions"]),
        "core_logic": "compression -> breakout acceptance -> continuation, outside London bias",
    }


def _final_shortlist_definition_failed(best_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "approved": best_row["approval_verdict"]["approved"],
        "recommended_variant_id": best_row["variant"]["variant_id"],
        "symbols": list(best_row["variant"]["symbols"]),
        "sessions": list(best_row["variant"]["allowed_sessions"]),
        "core_logic": "extension + failed breakout rejection + no-US filter",
    }


def _permanent_exclusions(*, breakout_best: dict[str, Any], failed_best: dict[str, Any]) -> list[str]:
    rows = [
        "Broad cross-universe versions of both lanes remain permanently excluded.",
        "Broad mean reversion remains permanently excluded from shortlist consideration.",
        "Breakout-long London exposure should remain excluded from the approval shortlist.",
    ]
    if _symbol_cost(breakout_best, "6J") <= 0.0:
        rows.append("6J should be excluded from the breakout approval shortlist definition.")
    if failed_best["variant"]["cluster_name"] != "core4_plus_qc_zt":
        rows.append("Thin additive slices that do not improve the failed-move core should remain excluded.")
    return rows


def _render_markdown(report: dict[str, Any]) -> str:
    breakout = report["approval_pass_verdicts"]["breakout_long"]
    failed = report["approval_pass_verdicts"]["failed_move_short"]
    lines = [
        "# Quant Futures Approval Pass",
        "",
        f"Execution timeframe: {report['execution_timeframe']}",
        "",
        "## Approval Verdicts",
        (
            f"- Breakout long: approved={breakout['approval_pass_verdict']['approved']}, "
            f"deployment={breakout['best_candidate']['deployment_status']}, "
            f"best={breakout['best_candidate']['variant_id']}"
        ),
        (
            f"- Failed-move short: approved={failed['approval_pass_verdict']['approved']}, "
            f"deployment={failed['best_candidate']['deployment_status']}, "
            f"best={failed['best_candidate']['variant_id']}"
        ),
        "",
        "## Final Shortlist",
        f"- Breakout long: {report['recommended_final_shortlist_definition']['breakout_long']['recommended_variant_id']}",
        f"- Failed-move short: {report['recommended_final_shortlist_definition']['failed_move_short']['recommended_variant_id']}",
        "",
        "## Exclusions",
    ]
    for item in report["permanent_exclusions"]:
        lines.append(f"- {item}")
    return "\n".join(lines)
