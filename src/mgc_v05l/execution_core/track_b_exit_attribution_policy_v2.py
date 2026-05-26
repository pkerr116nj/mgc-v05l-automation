"""Research-only exit attribution and Exit Policy v2 shadow framework.

This module never submits, cancels, modifies, closes, flattens, or grants
broker/lifecycle authority. It reads Track B PAPER trade artifacts and Phase-1
candles to estimate whether exits captured favorable movement.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


DEFAULT_ATTRIBUTION_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_exit_attribution_review.json"
)
DEFAULT_SHADOW_PATH = (
    Path("outputs") / "track_b_execution_core" / "research_shadow" / "latest_exit_policy_v2_shadow.json"
)
DEFAULT_MARKDOWN_PATH = Path("docs") / "track_b_exit_attribution_and_policy_v2.md"
DEFAULT_LEDGER_PATH = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "track_b_paper_trade_ledger.jsonl"
)

ENTRY_BAD = "ENTRY_BAD"
ENTRY_GOOD_EXIT_GOOD = "ENTRY_GOOD_EXIT_GOOD"
ENTRY_GOOD_EXIT_TOO_EARLY = "ENTRY_GOOD_EXIT_TOO_EARLY"
ENTRY_GOOD_EXIT_TOO_LATE = "ENTRY_GOOD_EXIT_TOO_LATE"
ENTRY_GOOD_ORDER_MANAGEMENT_BAD = "ENTRY_GOOD_ORDER_MANAGEMENT_BAD"
INCONCLUSIVE = "INCONCLUSIVE"

NO_BROKER_AUTHORITY_FLAGS = {
    "shadow_only": True,
    "submit_allowed": False,
    "broker_mutation_allowed": False,
    "lifecycle_authority": False,
    "not_order_authority": True,
    "not_lifecycle_authority": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "dashboard_projection_consumed": False,
}


@dataclass(frozen=True)
class ExitPolicyV2Config:
    repo_root: Path
    attribution_path: Path = DEFAULT_ATTRIBUTION_PATH
    shadow_path: Path = DEFAULT_SHADOW_PATH
    markdown_path: Path = DEFAULT_MARKDOWN_PATH
    ledger_path: Path = DEFAULT_LEDGER_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def run_exit_attribution_policy_v2(
    *,
    config: ExitPolicyV2Config,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    trades = _closed_track_b_trades(config.resolve(config.ledger_path))
    candle_cache: dict[str, list[dict[str, Any]]] = {}
    attributions = []
    shadows = []
    for trade in trades:
        instrument = str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper()
        candles = candle_cache.setdefault(instrument, _load_phase1_5m_candles(config=config, instrument=instrument))
        attribution = _attribute_trade(trade=trade, candles=candles)
        attributions.append(attribution)
        shadows.append(_shadow_policy_alternatives(trade=trade, attribution=attribution, candles=candles))

    summary = _summary(attributions=attributions, shadows=shadows)
    attribution_payload = {
        "schema_version": "track_b_exit_attribution_review_v1",
        "generated_at": actual_now.isoformat(),
        "classification": _review_classification(attributions),
        **NO_BROKER_AUTHORITY_FLAGS,
        "closed_trade_count": len(trades),
        "attributed_trade_count": len(attributions),
        "summary": summary["attribution_summary"],
        "trades": attributions,
        "source_artifacts": {
            "trade_ledger": str(config.resolve(config.ledger_path)),
        },
    }
    shadow_payload = {
        "schema_version": "track_b_exit_policy_v2_shadow_v1",
        "generated_at": actual_now.isoformat(),
        "classification": _shadow_classification(shadows),
        **NO_BROKER_AUTHORITY_FLAGS,
        "shadow_policy_ids": [
            "CURRENT_ACTUAL_MANAGED_EXIT",
            "FIXED_3X5M_TIMEBOX",
            "PARTICIPATION_AWARE_HOLD_EXTENSION",
            "PROFIT_HARVEST_EXIT",
            "THESIS_FAILURE_PARTICIPATION_DECAY_EXIT",
        ],
        "summary": summary["shadow_summary"],
        "promotion_gates": _promotion_gates(),
        "shadow_evaluations": shadows,
    }
    write_json_atomic(config.resolve(config.attribution_path), to_jsonable(attribution_payload))
    write_json_atomic(config.resolve(config.shadow_path), to_jsonable(shadow_payload))
    _write_markdown(config.resolve(config.markdown_path), attribution_payload, shadow_payload)
    return {
        "classification": "EXIT_ATTRIBUTION_POLICY_V2_READY",
        "attribution_path": str(config.resolve(config.attribution_path)),
        "shadow_path": str(config.resolve(config.shadow_path)),
        "markdown_path": str(config.resolve(config.markdown_path)),
        "closed_trade_count": len(trades),
        "attribution_summary": summary["attribution_summary"],
        "shadow_summary": summary["shadow_summary"],
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _closed_track_b_trades(path: Path) -> list[dict[str, Any]]:
    rows = _read_jsonl(path)
    closed: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("final_position_status") or "").upper() != "CLOSED_FLAT":
            continue
        if row.get("entry_fill_confirmed") is not True or row.get("exit_fill_confirmed") is not True:
            continue
        trade_id = str(row.get("trade_id") or row.get("lifecycle_id") or "")
        if not trade_id:
            continue
        previous = closed.get(trade_id)
        if previous is None or str(row.get("created_at") or "") >= str(previous.get("created_at") or ""):
            closed[trade_id] = dict(row)
    return sorted(closed.values(), key=lambda item: str(item.get("entry_timestamp") or ""))


def _attribute_trade(*, trade: Mapping[str, Any], candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    entry_time = _parse_time(trade.get("entry_timestamp"))
    exit_time = _parse_time(trade.get("exit_timestamp"))
    entry_price = _decimal(trade.get("entry_fill_price"))
    exit_price = _decimal(trade.get("exit_fill_price")) or _decimal(trade.get("exit_limit_price"))
    side = str(trade.get("side") or "").upper()
    trade_candles = _candles_between(candles, entry_time, exit_time)
    mfe_mae = _mfe_mae(side=side, entry_price=entry_price, candles=trade_candles)
    realized = _directional_points(side=side, entry_price=entry_price, exit_price=exit_price)
    giveback = None
    if mfe_mae["mfe"] is not None and realized is not None:
        giveback = max(Decimal("0"), mfe_mae["mfe"] - realized)
    classification, reasons = _classify_exit(realized=realized, mfe=mfe_mae["mfe"], mae=mfe_mae["mae"], giveback=giveback, candle_count=len(trade_candles), exit_price_known=exit_price is not None)
    return {
        "trade_id": trade.get("trade_id"),
        "lifecycle_id": trade.get("lifecycle_id"),
        "strategy_id": trade.get("strategy_id"),
        "lane_id": trade.get("lane_id"),
        "instrument": str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper(),
        "local_symbol": trade.get("local_symbol"),
        "side": side,
        "quantity": trade.get("quantity"),
        "entry_time": _iso(entry_time),
        "entry_price": _string(entry_price),
        "exit_time": _iso(exit_time),
        "exit_price": _string(exit_price),
        "realized_points": _string(realized),
        "realized_pnl": trade.get("realized_pnl"),
        "mfe_points": _string(mfe_mae["mfe"]),
        "mae_points": _string(mfe_mae["mae"]),
        "giveback_from_mfe_points": _string(giveback),
        "mfe_capture_ratio": _ratio(realized, mfe_mae["mfe"]),
        "time_in_trade_minutes": _minutes_between(entry_time, exit_time),
        "actual_exit_profile": trade.get("managed_exit_policy_id") or trade.get("exit_profile_id"),
        "classification": classification,
        "classification_reasons": reasons,
        "candle_window": {
            "available": bool(trade_candles),
            "bar_count": len(trade_candles),
            "first_bar_start": _iso(_bar_time(trade_candles[0], "bar_start")) if trade_candles else None,
            "last_bar_end": _iso(_bar_time(trade_candles[-1], "bar_end")) if trade_candles else None,
        },
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _shadow_policy_alternatives(*, trade: Mapping[str, Any], attribution: Mapping[str, Any], candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    entry_time = _parse_time(trade.get("entry_timestamp"))
    entry_price = _decimal(trade.get("entry_fill_price"))
    side = str(trade.get("side") or "").upper()
    forward_candles = _candles_after(candles, entry_time, max_minutes=90)
    policies = [
        _policy_current_actual(trade=trade, attribution=attribution),
        _policy_fixed_timebox(side=side, entry_price=entry_price, candles=forward_candles, bars=3),
        _policy_participation_hold(side=side, entry_price=entry_price, candles=forward_candles),
        _policy_profit_harvest(side=side, entry_price=entry_price, candles=forward_candles),
        _policy_thesis_failure(side=side, entry_price=entry_price, candles=forward_candles),
    ]
    best = _best_policy(policies)
    return {
        "trade_id": trade.get("trade_id"),
        "lifecycle_id": trade.get("lifecycle_id"),
        "strategy_id": trade.get("strategy_id"),
        "lane_id": trade.get("lane_id"),
        "instrument": str(trade.get("instrument_family") or trade.get("instrument") or trade.get("symbol") or "").upper(),
        "side": side,
        "entry_time": attribution.get("entry_time"),
        "actual_exit_profile": attribution.get("actual_exit_profile"),
        "actual_classification": attribution.get("classification"),
        "best_shadow_policy_id": best.get("policy_id") if best else None,
        "recommendation": _policy_recommendation(best, attribution),
        "policies": policies,
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _policy_current_actual(*, trade: Mapping[str, Any], attribution: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy_id": "CURRENT_ACTUAL_MANAGED_EXIT",
        "classification": "ACTUAL_EXIT_OBSERVED",
        "exit_time": attribution.get("exit_time"),
        "exit_price": attribution.get("exit_price"),
        "net_points": attribution.get("realized_points"),
        "mfe_capture_ratio": attribution.get("mfe_capture_ratio"),
        "basis": "ledger_actual_exit_price" if attribution.get("exit_price") is not None else "broker_flat_close_without_fill_price",
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _policy_fixed_timebox(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]], bars: int) -> dict[str, Any]:
    if entry_price is None or len(candles) < bars:
        return _policy_unclear("FIXED_3X5M_TIMEBOX", "insufficient_forward_candles")
    candle = candles[bars - 1]
    exit_price = _decimal(candle.get("close"))
    return _policy_result("FIXED_3X5M_TIMEBOX", side, entry_price, exit_price, candle, f"close_after_{bars}_completed_5m_bars")


def _policy_participation_hold(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if entry_price is None or len(candles) < 3:
        return _policy_unclear("PARTICIPATION_AWARE_HOLD_EXTENSION", "insufficient_forward_candles")
    prior_close: Decimal | None = None
    adverse_streak = 0
    selected = candles[min(len(candles), 12) - 1]
    for candle in candles[:12]:
        close = _decimal(candle.get("close"))
        if close is None:
            continue
        if prior_close is not None:
            favorable = close >= prior_close if side == "LONG" else close <= prior_close
            adverse_streak = 0 if favorable else adverse_streak + 1
            if adverse_streak >= 2:
                selected = candle
                break
        prior_close = close
    return _policy_result("PARTICIPATION_AWARE_HOLD_EXTENSION", side, entry_price, _decimal(selected.get("close")), selected, "hold_until_two_adverse_closes_or_60m")


def _policy_profit_harvest(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if entry_price is None or not candles:
        return _policy_unclear("PROFIT_HARVEST_EXIT", "insufficient_forward_candles")
    peak = Decimal("0")
    selected = candles[min(len(candles), 12) - 1]
    threshold = _profit_threshold(str(candles[0].get("symbol") or ""))
    for candle in candles[:12]:
        high = _decimal(candle.get("high"))
        low = _decimal(candle.get("low"))
        close = _decimal(candle.get("close"))
        if high is None or low is None or close is None:
            continue
        favorable = (high - entry_price) if side == "LONG" else (entry_price - low)
        peak = max(peak, favorable)
        current = (close - entry_price) if side == "LONG" else (entry_price - close)
        if peak >= threshold and peak - current >= peak * Decimal("0.40"):
            selected = candle
            break
    return _policy_result("PROFIT_HARVEST_EXIT", side, entry_price, _decimal(selected.get("close")), selected, "exit_on_40pct_giveback_after_material_mfe")


def _policy_thesis_failure(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if entry_price is None or not candles:
        return _policy_unclear("THESIS_FAILURE_PARTICIPATION_DECAY_EXIT", "insufficient_forward_candles")
    selected = candles[min(len(candles), 6) - 1]
    adverse_threshold = Decimal("8") if str(candles[0].get("symbol") or "").upper() == "MNQ" else Decimal("3")
    for candle in candles[:6]:
        low = _decimal(candle.get("low"))
        high = _decimal(candle.get("high"))
        if low is None or high is None:
            continue
        adverse = (entry_price - low) if side == "LONG" else (high - entry_price)
        if adverse >= adverse_threshold:
            selected = candle
            break
    return _policy_result("THESIS_FAILURE_PARTICIPATION_DECAY_EXIT", side, entry_price, _decimal(selected.get("close")), selected, "exit_on_adverse_excursion_or_30m")


def _policy_result(policy_id: str, side: str, entry_price: Decimal, exit_price: Decimal | None, candle: Mapping[str, Any], basis: str) -> dict[str, Any]:
    net = _directional_points(side=side, entry_price=entry_price, exit_price=exit_price)
    return {
        "policy_id": policy_id,
        "classification": "SHADOW_EXIT_SIMULATED" if net is not None else "SHADOW_EXIT_UNCLEAR",
        "exit_time": candle.get("bar_end") or candle.get("timestamp"),
        "exit_price": _string(exit_price),
        "net_points": _string(net),
        "basis": basis,
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _policy_unclear(policy_id: str, reason: str) -> dict[str, Any]:
    return {
        "policy_id": policy_id,
        "classification": "SHADOW_EXIT_UNCLEAR",
        "blocker": reason,
        "exit_time": None,
        "exit_price": None,
        "net_points": None,
        **NO_BROKER_AUTHORITY_FLAGS,
    }


def _summary(*, attributions: Sequence[Mapping[str, Any]], shadows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    class_counts = Counter(str(item.get("classification") or INCONCLUSIVE) for item in attributions)
    by_strategy: dict[str, Counter[str]] = defaultdict(Counter)
    for item in attributions:
        by_strategy[str(item.get("strategy_id") or "UNKNOWN")][str(item.get("classification") or INCONCLUSIVE)] += 1
    recommendations = Counter(str(item.get("recommendation") or "COLLECT_MORE_EVIDENCE") for item in shadows)
    return {
        "attribution_summary": {
            "classification_counts": dict(class_counts),
            "by_strategy": {strategy: dict(counts) for strategy, counts in sorted(by_strategy.items())},
            "top_underperformance_root_causes": _root_causes(class_counts),
        },
        "shadow_summary": {
            "recommendation_counts": dict(recommendations),
            "best_policy_counts": dict(Counter(str(item.get("best_shadow_policy_id") or "NONE") for item in shadows)),
            "recommended_next_slice": "Collect fill-price-complete closed trades and compare fixed time-box against profit-harvest on timestamp-coherent windows.",
        },
    }


def _classify_exit(*, realized: Decimal | None, mfe: Decimal | None, mae: Decimal | None, giveback: Decimal | None, candle_count: int, exit_price_known: bool) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if not exit_price_known:
        return ENTRY_GOOD_ORDER_MANAGEMENT_BAD, ["exit_fill_price_missing_after_broker_effect_close"]
    if realized is None or mfe is None or mae is None or candle_count == 0:
        return INCONCLUSIVE, ["timestamp_coherent_candles_or_prices_missing"]
    if mfe <= Decimal("0") and realized < Decimal("0"):
        return ENTRY_BAD, ["no_favorable_excursion_and_realized_loss"]
    capture = realized / mfe if mfe > 0 else Decimal("0")
    if realized > 0 and capture >= Decimal("0.60"):
        return ENTRY_GOOD_EXIT_GOOD, ["captured_at_least_60pct_of_mfe"]
    if mfe > 0 and giveback is not None and giveback >= mfe * Decimal("0.50"):
        reasons.append("gave_back_at_least_half_of_mfe")
        return ENTRY_GOOD_EXIT_TOO_LATE, reasons
    if realized <= 0 and mfe > abs(mae or Decimal("0")):
        return ENTRY_GOOD_EXIT_TOO_LATE, ["positive_mfe_but_exit_failed_to_capture"]
    if realized > 0 and capture < Decimal("0.35"):
        return ENTRY_GOOD_EXIT_TOO_EARLY, ["small_capture_of_available_mfe"]
    return INCONCLUSIVE, ["mixed_or_small_sample_exit_evidence"]


def _root_causes(class_counts: Counter[str]) -> list[dict[str, Any]]:
    mapping = {
        ENTRY_GOOD_ORDER_MANAGEMENT_BAD: "Missing or incomplete close fill/price evidence prevents reliable P&L and exit-quality attribution.",
        ENTRY_GOOD_EXIT_TOO_LATE: "Trades showed favorable excursion but gave back too much before exit.",
        ENTRY_GOOD_EXIT_TOO_EARLY: "Trades exited with low MFE capture; participation-aware hold may be worth shadowing.",
        ENTRY_BAD: "Entry quality did not create favorable excursion.",
        INCONCLUSIVE: "Timestamp-coherent candle or fill evidence is incomplete.",
    }
    return [
        {"cause": mapping[key], "classification": key, "count": count}
        for key, count in class_counts.most_common()
        if key in mapping and count
    ]


def _promotion_gates() -> list[dict[str, Any]]:
    return [
        {"gate": "minimum_sample_size", "requirement": "At least 30 timestamp-coherent broker-effect closed trades per strategy/exit-family."},
        {"gate": "lower_giveback", "requirement": "Shadow policy reduces median giveback from MFE versus current exit."},
        {"gate": "improved_mfe_capture", "requirement": "Shadow policy improves realized/MFE capture without relying on hindsight-only prices."},
        {"gate": "no_worse_mae", "requirement": "MAE distribution is no worse than current exit for the same entry set."},
        {"gate": "no_lifecycle_failures", "requirement": "No added duplicate close, unmanaged order, modify, or lifecycle persistence failures."},
        {"gate": "explainable_rules_only", "requirement": "Rules use participation, MFE/giveback, thesis failure, and session/regime evidence; no black-box promotion."},
    ]


def _write_markdown(path: Path, attribution: Mapping[str, Any], shadow: Mapping[str, Any]) -> None:
    summary = attribution.get("summary") if isinstance(attribution.get("summary"), Mapping) else {}
    shadow_summary = shadow.get("summary") if isinstance(shadow.get("summary"), Mapping) else {}
    lines = [
        "# Track B Exit Attribution and Policy v2",
        "",
        "Status: research/shadow only. No broker, order, lifecycle, live-money, or paper_proof authority is granted.",
        "",
        "## Current Findings",
        f"- Closed broker-effect trades reviewed: {attribution.get('closed_trade_count', 0)}",
        f"- Attribution classification counts: `{json.dumps(summary.get('classification_counts', {}), sort_keys=True)}`",
        f"- Shadow recommendation counts: `{json.dumps(shadow_summary.get('recommendation_counts', {}), sort_keys=True)}`",
        "",
        "## Top Root Causes",
    ]
    for item in summary.get("top_underperformance_root_causes", []) or []:
        lines.append(f"- {item.get('classification')}: {item.get('cause')} ({item.get('count')})")
    lines.extend(
        [
            "",
            "## Shadow Policies",
            "- CURRENT_ACTUAL_MANAGED_EXIT",
            "- FIXED_3X5M_TIMEBOX",
            "- PARTICIPATION_AWARE_HOLD_EXTENSION",
            "- PROFIT_HARVEST_EXIT",
            "- THESIS_FAILURE_PARTICIPATION_DECAY_EXIT",
            "",
            "## Promotion Gates",
        ]
    )
    for gate in shadow.get("promotion_gates", []) or []:
        lines.append(f"- {gate.get('gate')}: {gate.get('requirement')}")
    lines.extend(
        [
            "",
            "## Recommended Next Slice",
            str(shadow_summary.get("recommended_next_slice") or "Collect more timestamp-coherent closed-trade evidence before changing live exits."),
            "",
            "## Safety",
            "- `submit_allowed=false`",
            "- `broker_mutation_allowed=false`",
            "- `lifecycle_authority=false`",
            "- Existing managed-exit roster behavior is unchanged.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _load_phase1_5m_candles(*, config: ExitPolicyV2Config, instrument: str) -> list[dict[str, Any]]:
    if not instrument:
        return []
    path = config.repo_root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / instrument / "5m" / "latest_runtime_candles.json"
    payload = _read_json(path)
    rows = []
    for row in payload.get("bars") or payload.get("candles") or []:
        if isinstance(row, Mapping):
            rows.append({**dict(row), "symbol": instrument})
    return sorted(rows, key=lambda item: str(item.get("bar_start") or item.get("timestamp") or ""))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return rows
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _candles_between(candles: Sequence[Mapping[str, Any]], start: datetime | None, end: datetime | None) -> list[Mapping[str, Any]]:
    if start is None or end is None:
        return []
    selected = []
    for candle in candles:
        bar_start = _bar_time(candle, "bar_start") or _bar_time(candle, "timestamp")
        bar_end = _bar_time(candle, "bar_end") or bar_start
        if bar_end is not None and bar_end <= start:
            continue
        if bar_start is not None and bar_start > end:
            continue
        selected.append(candle)
    return selected


def _candles_after(candles: Sequence[Mapping[str, Any]], start: datetime | None, *, max_minutes: int) -> list[Mapping[str, Any]]:
    if start is None:
        return []
    end = start + timedelta(minutes=max_minutes)
    return _candles_between(candles, start, end)


def _mfe_mae(*, side: str, entry_price: Decimal | None, candles: Sequence[Mapping[str, Any]]) -> dict[str, Decimal | None]:
    if entry_price is None or not candles:
        return {"mfe": None, "mae": None}
    mfe = Decimal("0")
    mae = Decimal("0")
    for candle in candles:
        high = _decimal(candle.get("high"))
        low = _decimal(candle.get("low"))
        if high is None or low is None:
            continue
        if side == "LONG":
            mfe = max(mfe, high - entry_price)
            mae = min(mae, low - entry_price)
        else:
            mfe = max(mfe, entry_price - low)
            mae = min(mae, entry_price - high)
    return {"mfe": mfe, "mae": mae}


def _directional_points(*, side: str, entry_price: Decimal | None, exit_price: Decimal | None) -> Decimal | None:
    if entry_price is None or exit_price is None:
        return None
    return exit_price - entry_price if side == "LONG" else entry_price - exit_price


def _best_policy(policies: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    scored = [(policy, _decimal(policy.get("net_points"))) for policy in policies if policy.get("policy_id") != "CURRENT_ACTUAL_MANAGED_EXIT"]
    scored = [(policy, net) for policy, net in scored if net is not None]
    if not scored:
        return None
    return max(scored, key=lambda item: item[1] or Decimal("-999999"))[0]


def _policy_recommendation(best: Mapping[str, Any] | None, attribution: Mapping[str, Any]) -> str:
    if not best:
        return "COLLECT_MORE_FORWARD_EVIDENCE"
    if attribution.get("classification") in {ENTRY_GOOD_EXIT_TOO_EARLY, ENTRY_GOOD_EXIT_TOO_LATE, ENTRY_GOOD_ORDER_MANAGEMENT_BAD}:
        return "SHADOW_POLICY_COMPARISON_WORTH_COLLECTING"
    return "KEEP_LIVE_EXIT_UNCHANGED_COLLECT_SHADOW"


def _review_classification(attributions: Sequence[Mapping[str, Any]]) -> str:
    if not attributions:
        return "EXIT_ATTRIBUTION_NO_CLOSED_TRADES"
    if any(item.get("classification") in {ENTRY_GOOD_EXIT_TOO_EARLY, ENTRY_GOOD_EXIT_TOO_LATE, ENTRY_GOOD_ORDER_MANAGEMENT_BAD} for item in attributions):
        return "EXIT_ATTRIBUTION_REVIEW_ACTIONABLE_DIAGNOSTICS"
    return "EXIT_ATTRIBUTION_REVIEW_READY"


def _shadow_classification(shadows: Sequence[Mapping[str, Any]]) -> str:
    if not shadows:
        return "EXIT_POLICY_V2_SHADOW_NO_TRADES"
    if any(item.get("recommendation") == "SHADOW_POLICY_COMPARISON_WORTH_COLLECTING" for item in shadows):
        return "EXIT_POLICY_V2_SHADOW_RECOMMENDATIONS_READY"
    return "EXIT_POLICY_V2_SHADOW_COLLECT_MORE_EVIDENCE"


def _profit_threshold(symbol: str) -> Decimal:
    return Decimal("10") if symbol.upper() in {"MNQ", "NQ", "ES", "MES"} else Decimal("3")


def _ratio(numerator: Decimal | None, denominator: Decimal | None) -> str | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return _string(numerator / denominator)


def _minutes_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() / 60.0, 3)


def _bar_time(candle: Mapping[str, Any], key: str) -> datetime | None:
    return _parse_time(candle.get(key))


def _parse_time(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.normalize())


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Track B exit attribution and Exit Policy v2 shadow artifacts.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run_exit_attribution_policy_v2(config=ExitPolicyV2Config(repo_root=args.repo_root.expanduser().resolve()))
    if args.json:
        print(json.dumps(to_jsonable(report), indent=2, sort_keys=True))
    else:
        print(json.dumps({"classification": report["classification"], "attribution_path": report["attribution_path"], "shadow_path": report["shadow_path"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
