"""Shadow-only trend participation hold extension for Track B exits.

This module evaluates whether a timebox-due PAPER position would have had
enough continuation evidence to justify a shadow hold extension.  It is
diagnostic only: it does not submit, cancel, close, modify, restart, or mutate
broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = "track_b_trend_participation_hold_extension_shadow_v1"
DEFAULT_LEDGER_PATH = Path("outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl")
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "exit_shadow"
    / "latest_trend_participation_hold_extension_replay.json"
)

EXIT_NOW = "EXIT_NOW"
EXTEND_HOLD = "EXTEND_HOLD"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"

NO_AUTHORITY_FLAGS: dict[str, bool] = {
    "read_only": True,
    "shadow_only": True,
    "submit_authority": False,
    "submit_allowed": False,
    "broker_mutation_allowed": False,
    "cancel_allowed": False,
    "close_allowed": False,
    "lifecycle_authority": False,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
}


@dataclass(frozen=True)
class TrendHoldCandle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    timeframe: str = "5m_completed"

    @property
    def range_points(self) -> float:
        return max(0.0, self.high - self.low)


@dataclass(frozen=True)
class TrendHoldExtensionConfig:
    repo_root: Path = REPO_ROOT
    ledger_path: Path = DEFAULT_LEDGER_PATH
    output_path: Path = DEFAULT_OUTPUT_PATH
    trading_date: str | None = None
    extension_minutes: tuple[int, ...] = (5, 10, 15)

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def evaluate_trend_participation_hold_extension(
    *,
    side: str,
    due_at: datetime,
    completed_candles: Sequence[TrendHoldCandle | Mapping[str, Any]],
    lane_id: str | None = None,
    instrument: str | None = None,
    managed_exit_policy_id: str | None = None,
    future_candles_for_offline_replay: Sequence[TrendHoldCandle | Mapping[str, Any]] = (),
    extension_minutes: Sequence[int] = (5, 10, 15),
) -> dict[str, Any]:
    """Return a shadow hold-extension recommendation.

    ``completed_candles`` must already be restricted to candles completed at or
    before ``due_at``.  Future candles are accepted only for offline
    hypothetical labels and never influence the recommendation.
    """

    actual_due_at = _require_dt(due_at, "due_at")
    normalized = tuple(
        sorted((_coerce_candle(c) for c in completed_candles), key=lambda item: item.timestamp)
    )
    signal_candles = tuple(c for c in normalized if c.timestamp + timedelta(minutes=5) <= actual_due_at)
    feature_candles = signal_candles[-3:]
    actual_side = str(side or "").upper()
    recommendation = INSUFFICIENT_EVIDENCE
    reason_codes: list[str] = []
    features: dict[str, Any] = {
        "side": actual_side,
        "lane_id": lane_id,
        "instrument": instrument,
        "managed_exit_policy_id": managed_exit_policy_id,
        "due_at": actual_due_at.isoformat(),
        "completed_5m_candle_count": len(signal_candles),
        "feature_window_count": len(feature_candles),
        "uses_future_data_for_recommendation": False,
    }
    if actual_side not in {"LONG", "SHORT"}:
        reason_codes.append("unknown_side")
    elif len(feature_candles) < 3:
        reason_codes.append("fewer_than_3_completed_5m_candles")
    else:
        first, prev, last = feature_candles
        avg_range = sum(max(c.range_points, 0.0) for c in feature_candles) / 3.0
        directional_delta = last.close - first.close
        last_close_delta = last.close - prev.close
        body_points = last.close - last.open
        body_strength = abs(body_points) / last.range_points if last.range_points > 0 else 0.0
        if actual_side == "LONG":
            close_location = (last.close - last.low) / last.range_points if last.range_points > 0 else 0.5
            directional_score = directional_delta
            last_close_favorable = last_close_delta > 0
            body_favorable = body_points >= 0
            adverse_impulse = last.close < prev.close and last.low < prev.low
            min_slope = max(0.25 * avg_range, 0.0)
            favorable = directional_score > min_slope and last_close_favorable and close_location >= 0.60
        else:
            close_location = (last.high - last.close) / last.range_points if last.range_points > 0 else 0.5
            directional_score = -directional_delta
            last_close_favorable = last_close_delta < 0
            body_favorable = body_points <= 0
            adverse_impulse = last.close > prev.close and last.high > prev.high
            # Shorts are deliberately more conservative in this first shadow
            # overlay because today's London Late short sample degraded after
            # the timebox more often than it improved.
            min_slope = max(0.75 * avg_range, 0.0)
            favorable = directional_score > min_slope and last_close_favorable and close_location >= 0.65
        features.update(
            {
                "first_candle_timestamp": first.timestamp.isoformat(),
                "last_candle_timestamp": last.timestamp.isoformat(),
                "avg_range_points": avg_range,
                "directional_delta_points": directional_delta,
                "directional_score_points": directional_score,
                "last_close_delta_points": last_close_delta,
                "last_body_points": body_points,
                "last_body_strength": body_strength,
                "last_close_location_in_favorable_range": close_location,
                "last_close_favorable": last_close_favorable,
                "last_body_favorable": body_favorable,
                "adverse_impulse": adverse_impulse,
                "min_required_slope_points": min_slope,
            }
        )
        if favorable and body_favorable and body_strength >= 0.15 and not adverse_impulse:
            recommendation = EXTEND_HOLD
            reason_codes.append("trend_participation_favorable_no_reversal")
        else:
            recommendation = EXIT_NOW
            if not favorable:
                reason_codes.append("continuation_evidence_insufficient")
            if not body_favorable:
                reason_codes.append("last_body_not_favorable")
            if body_strength < 0.15:
                reason_codes.append("last_body_weak")
            if adverse_impulse:
                reason_codes.append("adverse_impulse_detected")

    exit_reference_price = feature_candles[-1].close if feature_candles else None
    hypothetical = _hypothetical_extension_results(
        side=actual_side,
        reference_price=exit_reference_price,
        due_at=actual_due_at,
        future_candles=tuple(_coerce_candle(c) for c in future_candles_for_offline_replay),
        extension_minutes=extension_minutes,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        **NO_AUTHORITY_FLAGS,
        "recommendation": recommendation,
        "would_have_extended": recommendation == EXTEND_HOLD,
        "reason_codes": reason_codes,
        "features": features,
        "hypothetical_offline_results": hypothetical,
    }


def build_trend_participation_hold_extension_replay(
    *,
    config: TrendHoldExtensionConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _require_dt(now or datetime.now(UTC), "now")
    trading_date = config.trading_date or actual_now.date().isoformat()
    trades = _completed_noisemaker_trades(config=config, trading_date=trading_date)
    rows: list[dict[str, Any]] = []
    for trade in trades:
        candles = _load_lane_5m_candles(
            repo_root=config.repo_root,
            lane_id=str(trade["lane_id"]),
            start=trade["entry_time"] - timedelta(minutes=20),
            end=trade["exit_time"] + timedelta(minutes=max(config.extension_minutes) + 10),
        )
        completed = tuple(c for c in candles if c.timestamp + timedelta(minutes=5) <= trade["exit_time"])
        future = tuple(c for c in candles if c.timestamp >= trade["exit_time"])
        decision = evaluate_trend_participation_hold_extension(
            side=str(trade["side"]),
            due_at=trade["exit_time"],
            completed_candles=completed,
            future_candles_for_offline_replay=future,
            extension_minutes=config.extension_minutes,
            lane_id=str(trade["lane_id"]),
            instrument=str(trade["instrument"]),
            managed_exit_policy_id=str(trade.get("managed_exit_policy_id") or ""),
        )
        rows.append(
            {
                "lifecycle_id": trade["lifecycle_id"],
                "lane_id": trade["lane_id"],
                "instrument": trade["instrument"],
                "side": trade["side"],
                "entry_time": trade["entry_time"].isoformat(),
                "exit_time": trade["exit_time"].isoformat(),
                "hold_minutes": (trade["exit_time"] - trade["entry_time"]).total_seconds() / 60.0,
                "entry_fill_price": trade["entry_fill_price"],
                "exit_fill_price": trade["exit_fill_price"],
                "managed_exit_policy_id": trade.get("managed_exit_policy_id"),
                "shadow_decision": decision,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "trading_date": trading_date,
        **NO_AUTHORITY_FLAGS,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "runtime_restarted": False,
        "trade_count": len(rows),
        "summary": _summary(rows),
        "trades": rows,
        "source_artifacts": {
            "ledger_path": str(config.resolve(config.ledger_path)),
            "lane_sqlite_pattern": "mgc_v05l.probationary.paper__{lane_id}.sqlite3",
        },
        "artifact_paths": {"report": str(config.resolve(config.output_path))},
    }


def write_trend_participation_hold_extension_replay(
    *,
    config: TrendHoldExtensionConfig,
    payload: Mapping[str, Any],
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay shadow-only trend participation hold extension decisions.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--ledger-path", default=str(DEFAULT_LEDGER_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--trading-date")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrendHoldExtensionConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        ledger_path=Path(args.ledger_path),
        output_path=Path(args.output_path),
        trading_date=args.trading_date,
    )
    payload = build_trend_participation_hold_extension_replay(config=config)
    write_trend_participation_hold_extension_replay(config=config, payload=payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"{payload['trade_count']} trades: "
            f"{payload['summary']['recommendation_counts'].get(EXTEND_HOLD, 0)} EXTEND_HOLD, "
            f"{payload['summary']['recommendation_counts'].get(EXIT_NOW, 0)} EXIT_NOW"
        )
    return 0


def _completed_noisemaker_trades(*, config: TrendHoldExtensionConfig, trading_date: str) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    ledger_path = config.resolve(config.ledger_path)
    for line in ledger_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        lane_id = str(row.get("strategy_id") or "")
        entry_time = _parse_dt(row.get("entry_timestamp"))
        if "active_participation" not in lane_id:
            continue
        if row.get("final_position_status") != "CLOSED_FLAT":
            continue
        if entry_time is None or entry_time.date().isoformat() != trading_date:
            continue
        lifecycle_id = str(row.get("lifecycle_id") or "")
        if lifecycle_id:
            latest[lifecycle_id] = row

    trades: list[dict[str, Any]] = []
    for lifecycle_id, row in latest.items():
        report_path = Path(str(row.get("paper_lifecycle_report_path") or ""))
        if not report_path.is_absolute():
            report_path = config.repo_root / report_path
        report = _read_json(report_path)
        entry_fill = _mapping(report.get("entry_fill"))
        close_fill = _mapping(report.get("close_fill"))
        entry_time = _parse_dt(entry_fill.get("filled_at") or row.get("entry_timestamp"))
        exit_time = _parse_dt(close_fill.get("filled_at") or row.get("exit_timestamp"))
        entry_price = _float(entry_fill.get("price") or row.get("entry_fill_price") or row.get("entry_price"))
        exit_price = _float(close_fill.get("price") or row.get("exit_fill_price") or row.get("exit_price"))
        if entry_time is None or exit_time is None or entry_price is None or exit_price is None:
            continue
        lane_id = str(row.get("strategy_id") or "")
        trades.append(
            {
                "lifecycle_id": lifecycle_id,
                "lane_id": lane_id,
                "instrument": "MNQ" if lane_id.startswith("mnq_") else "MES" if lane_id.startswith("mes_") else "",
                "side": "LONG" if lane_id.endswith("_long") else "SHORT" if lane_id.endswith("_short") else "",
                "entry_time": entry_time,
                "exit_time": exit_time,
                "entry_fill_price": entry_price,
                "exit_fill_price": exit_price,
                "managed_exit_policy_id": report.get("managed_exit_policy_id") or row.get("managed_exit_policy_id"),
            }
        )
    return sorted(trades, key=lambda item: item["entry_time"])


def _load_lane_5m_candles(*, repo_root: Path, lane_id: str, start: datetime, end: datetime) -> tuple[TrendHoldCandle, ...]:
    db_path = repo_root / f"mgc_v05l.probationary.paper__{lane_id}.sqlite3"
    if not db_path.exists():
        return ()
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            select timestamp, open, high, low, close
            from bars
            where timeframe = '1m'
              and data_source = 'databento_live'
              and timestamp >= ?
              and timestamp <= ?
            order by timestamp
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()
    finally:
        conn.close()
    grouped: dict[datetime, list[tuple[datetime, float, float, float, float]]] = defaultdict(list)
    for ts, opn, high, low, close in rows:
        parsed = _parse_dt(ts)
        if parsed is None:
            continue
        bucket = parsed.replace(minute=(parsed.minute // 5) * 5, second=0, microsecond=0)
        grouped[bucket].append((parsed, float(opn), float(high), float(low), float(close)))
    candles: list[TrendHoldCandle] = []
    for bucket, items in sorted(grouped.items()):
        ordered = sorted(items, key=lambda item: item[0])
        candles.append(
            TrendHoldCandle(
                timestamp=bucket,
                open=ordered[0][1],
                high=max(item[2] for item in ordered),
                low=min(item[3] for item in ordered),
                close=ordered[-1][4],
            )
        )
    return tuple(candles)


def _hypothetical_extension_results(
    *,
    side: str,
    reference_price: float | None,
    due_at: datetime,
    future_candles: Sequence[TrendHoldCandle],
    extension_minutes: Sequence[int],
) -> dict[str, Any]:
    if reference_price is None or side not in {"LONG", "SHORT"}:
        return {"available": False, "reason": "missing_reference_price_or_side"}
    rows: dict[str, Any] = {"available": True, "reference_price": reference_price, "due_at": due_at.isoformat()}
    for minutes in extension_minutes:
        window = tuple(c for c in future_candles if c.timestamp < due_at + timedelta(minutes=int(minutes)))
        key = f"plus_{int(minutes)}m"
        if not window:
            rows[key] = {"available": False}
            continue
        close_price = window[-1].close
        if side == "LONG":
            points = close_price - reference_price
            max_favorable = max(0.0, max(c.high for c in window) - reference_price)
            max_adverse = max(0.0, reference_price - min(c.low for c in window))
        else:
            points = reference_price - close_price
            max_favorable = max(0.0, reference_price - min(c.low for c in window))
            max_adverse = max(0.0, max(c.high for c in window) - reference_price)
        rows[key] = {
            "available": True,
            "points_vs_exit_now": points,
            "close_price": close_price,
            "max_favorable_points": max_favorable,
            "max_adverse_points": max_adverse,
            "window_candle_count": len(window),
        }
    return rows


def _summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    recommendation_counts: dict[str, int] = defaultdict(int)
    by_side: dict[str, dict[str, Any]] = {}
    by_lane: dict[str, dict[str, Any]] = {}
    for row in rows:
        decision = _mapping(row.get("shadow_decision"))
        recommendation = str(decision.get("recommendation") or INSUFFICIENT_EVIDENCE)
        recommendation_counts[recommendation] += 1
        side = str(row.get("side") or "")
        lane = str(row.get("lane_id") or "")
        _accumulate(by_side, side, row)
        _accumulate(by_lane, lane, row)
    return {
        "recommendation_counts": dict(recommendation_counts),
        "by_side": by_side,
        "by_lane": by_lane,
    }


def _accumulate(target: dict[str, dict[str, Any]], key: str, row: Mapping[str, Any]) -> None:
    bucket = target.setdefault(
        key,
        {"trade_count": 0, "extend_hold": 0, "exit_now": 0, "insufficient_evidence": 0, "plus_15m_improved": 0, "plus_15m_worsened": 0, "plus_15m_net_points": 0.0},
    )
    bucket["trade_count"] += 1
    decision = _mapping(row.get("shadow_decision"))
    recommendation = str(decision.get("recommendation") or INSUFFICIENT_EVIDENCE)
    if recommendation == EXTEND_HOLD:
        bucket["extend_hold"] += 1
    elif recommendation == EXIT_NOW:
        bucket["exit_now"] += 1
    else:
        bucket["insufficient_evidence"] += 1
    plus_15 = _mapping(_mapping(decision.get("hypothetical_offline_results")).get("plus_15m"))
    points = _float(plus_15.get("points_vs_exit_now"))
    if points is not None:
        bucket["plus_15m_net_points"] += points
        if points > 0:
            bucket["plus_15m_improved"] += 1
        elif points < 0:
            bucket["plus_15m_worsened"] += 1


def _coerce_candle(value: TrendHoldCandle | Mapping[str, Any]) -> TrendHoldCandle:
    if isinstance(value, TrendHoldCandle):
        return value
    timestamp = _parse_dt(value.get("timestamp") or value.get("start_ts") or value.get("end_ts"))
    if timestamp is None:
        raise ValueError("candle timestamp is required")
    return TrendHoldCandle(
        timestamp=timestamp,
        open=float(value["open"]),
        high=float(value["high"]),
        low=float(value["low"]),
        close=float(value["close"]),
        timeframe=str(value.get("timeframe") or "5m_completed"),
    )


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _require_dt(value: datetime, name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
