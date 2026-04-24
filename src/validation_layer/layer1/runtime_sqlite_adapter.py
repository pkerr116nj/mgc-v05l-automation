"""Generic Layer 1 adapter for probationary runtime SQLite-backed lanes."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ..data.contracts import EquityPoint, PositionPoint, PriceBar, StrategyBacktest, TradeRecord
from .producer_contract import (
    build_data_provenance,
    build_execution_assumptions,
    build_strategy_backtest_provenance,
    build_trade_validation_metadata,
    full_sample_split_window,
)


@dataclass(frozen=True)
class RuntimeSQLiteLaneSource:
    lane_id: str
    display_name: str
    runtime_kind: str
    lane_mode: str
    symbol: str
    allowed_sessions: tuple[str, ...]
    source_family: str
    strategy_family: str
    strategy_identity_root: str | None
    scope_kind: str
    config_path: Path
    database_path: Path | None
    artifacts_dir: Path | None
    execution_timeframe: str | None
    point_value: float | None
    trade_size: float | None
    experimental_status: str | None = None
    non_approved: bool = False
    tracked_strategy_id: str | None = None
    metadata: dict[str, Any] | None = None


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _parse_bar_id_timestamp(bar_id: str | None) -> datetime | None:
    if not bar_id:
        return None
    parts = str(bar_id).split("|")
    if len(parts) < 3:
        return None
    return _parse_iso_timestamp(parts[-1])


def _timeframe_minutes(label: str | None) -> int:
    value = str(label or "1m").strip().lower()
    if value.endswith("m"):
        try:
            return max(1, int(value[:-1]))
        except ValueError:
            return 1
    return 1


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    return float(value)


def _database_path_from_url(database_url: str | None) -> Path | None:
    if not database_url:
        return None
    text = str(database_url).strip()
    if not text.startswith("sqlite:///"):
        return None
    relative = text.removeprefix("sqlite:///")
    candidate = Path(relative)
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    candidate = candidate.resolve(strict=False)
    if candidate.exists():
        return candidate
    name = candidate.name
    if "mgc_v05l.probationary__" in name:
        alternate = candidate.with_name(name.replace("mgc_v05l.probationary__", "mgc_v05l.probationary.paper__"))
        if alternate.exists():
            return alternate
    return candidate


def _session_label_from_bar_row(row: sqlite3.Row) -> str:
    if int(row["session_us"] or 0):
        return "US"
    if int(row["session_london"] or 0):
        return "LONDON"
    if int(row["session_asia"] or 0):
        return "ASIA"
    return "UNKNOWN"


def _build_equity_curve(trades: tuple[TradeRecord, ...], start: datetime, starting_equity: float = 100_000.0) -> tuple[EquityPoint, ...]:
    equity = starting_equity
    curve = [EquityPoint(timestamp=start, equity=equity)]
    for trade in sorted(trades, key=lambda item: item.exit_time):
        equity += float(trade.net_pnl)
        curve.append(EquityPoint(timestamp=trade.exit_time, equity=equity, session_label=trade.session_label))
    return tuple(curve)


def _build_position_series(trades: tuple[TradeRecord, ...], start: datetime) -> tuple[PositionPoint, ...]:
    deltas: dict[datetime, float] = {start: 0.0}
    for trade in trades:
        signed = float(trade.qty) if trade.direction == "long" else -float(trade.qty)
        deltas[trade.entry_time] = deltas.get(trade.entry_time, 0.0) + signed
        deltas[trade.exit_time] = deltas.get(trade.exit_time, 0.0) - signed
    running = 0.0
    rows: list[PositionPoint] = []
    for timestamp in sorted(deltas):
        running += deltas[timestamp]
        rows.append(PositionPoint(timestamp=timestamp, position=running, exposure=running))
    return tuple(rows)


def build_runtime_sqlite_strategy_backtest(
    source: RuntimeSQLiteLaneSource,
    *,
    code_version: str,
    strategy_name: str | None = None,
) -> StrategyBacktest:
    if source.database_path is None:
        raise ValueError(f"Runtime SQLite adapter requires a database path for {source.lane_id}.")
    if not source.database_path.exists():
        raise ValueError(f"Runtime SQLite database does not exist for {source.lane_id}: {source.database_path}")

    connection = sqlite3.connect(source.database_path)
    connection.row_factory = sqlite3.Row
    try:
        processed_count = connection.execute(
            "select count(*) from processed_bars where lane_id = ?",
            (source.lane_id,),
        ).fetchone()[0]
        if processed_count <= 0:
            raise ValueError(f"Runtime SQLite adapter found no processed bars for {source.lane_id}.")

        price_rows = connection.execute(
            """
            select distinct
                b.bar_id,
                b.symbol,
                b.start_ts,
                b.end_ts,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.session_asia,
                b.session_london,
                b.session_us,
                b.data_source,
                pb.standalone_strategy_id
            from processed_bars pb
            join bars b on b.bar_id = pb.bar_id
            where pb.lane_id = ?
            order by b.end_ts asc
            """,
            (source.lane_id,),
        ).fetchall()
        price_bars = tuple(
            PriceBar(
                timestamp=_parse_iso_timestamp(row["end_ts"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"] or 0.0),
                session_label=_session_label_from_bar_row(row),
                metadata={
                    "bar_id": row["bar_id"],
                    "source": row["data_source"],
                    "standalone_strategy_id": row["standalone_strategy_id"],
                },
            )
            for row in price_rows
        )
        if not price_bars:
            raise ValueError(f"Runtime SQLite adapter could not materialize price bars for {source.lane_id}.")
        price_bar_sessions = {str(row["bar_id"]): _session_label_from_bar_row(row) for row in price_rows}

        trade_rows = connection.execute(
            """
            select *
            from trade_outcomes
            order by trade_id asc
            """
        ).fetchall()
        fill_count = connection.execute("select count(*) from fills").fetchone()[0]
        intent_count = connection.execute("select count(*) from order_intents").fetchone()[0]

        timeframe_minutes = _timeframe_minutes(source.execution_timeframe)
        converted_trades: list[TradeRecord] = []
        for row in trade_rows:
            entry_time = _parse_bar_id_timestamp(row["entry_bar_id"])
            exit_time = _parse_bar_id_timestamp(row["exit_bar_id"]) or (
                entry_time + timedelta(minutes=max(1, int(row["bars_held"] or 1) * timeframe_minutes))
                if entry_time is not None
                else _parse_iso_timestamp(row["created_at"])
            )
            if entry_time is None or exit_time is None:
                continue
            session_label = price_bar_sessions.get(str(row["entry_bar_id"])) or price_bar_sessions.get(str(row["exit_bar_id"])) or (
                source.allowed_sessions[0] if source.allowed_sessions else "UNKNOWN"
            )
            converted_trades.append(
                TradeRecord(
                    entry_time=entry_time,
                    exit_time=exit_time,
                    direction=str(row["side"]).lower(),
                    qty=_coerce_float(row["size"], default=1.0),
                    gross_pnl=_coerce_float(row["pnl"]),
                    net_pnl=_coerce_float(row["pnl"]),
                    mae=abs(_coerce_float(row["mae"])),
                    mfe=abs(_coerce_float(row["mfe"])),
                    holding_bars=max(1, int(row["bars_held"] or 1)),
                    holding_minutes=max(1.0, (exit_time - entry_time).total_seconds() / 60.0),
                    session_label=session_label,
                    validation_metadata=build_trade_validation_metadata(
                        trade_id=f"{source.lane_id}:{row['trade_id']}",
                        signal_id=str(row["entry_bar_id"]),
                        setup_family=str(row["entry_family"] or source.source_family),
                        setup_variant=str(row["entry_reason"] or source.lane_id),
                        decision_time=entry_time,
                        exit_reason=str(row["exit_reason"]) if row["exit_reason"] is not None else None,
                        execution_model=source.runtime_kind,
                        fill_policy="runtime_recorded_fill_lifecycle" if fill_count and intent_count else "runtime_trade_outcome_reconstruction",
                        tags={
                            "entry_reason": row["entry_reason"],
                            "quality_score_at_entry": _coerce_float(row["quality_score_at_entry"]),
                            "size_recommendation_at_entry": _coerce_float(row["size_recommendation_at_entry"]),
                        },
                    ),
                    metadata={
                        "trade_id": int(row["trade_id"]),
                        "entry_bar_id": row["entry_bar_id"],
                        "exit_bar_id": row["exit_bar_id"],
                        "ticker": row["ticker"],
                        "timeframe": row["timeframe"],
                        "pnl_embedded_runtime_truth": True,
                    },
                )
            )
    finally:
        connection.close()

    mapped_trades = tuple(converted_trades)
    start = price_bars[0].timestamp
    end = price_bars[-1].timestamp
    execution_mode = "live_execution_pilot" if source.scope_kind == "live_pilot" else "paper_runtime"
    artifact_references = {
        "runtime_config": str(source.config_path),
        "runtime_database": str(source.database_path),
    }
    if source.artifacts_dir is not None:
        artifact_references["artifacts_dir"] = str(source.artifacts_dir)
        operator_status = source.artifacts_dir / "operator_status.json"
        if operator_status.exists():
            artifact_references["operator_status"] = str(operator_status)

    provenance = build_strategy_backtest_provenance(
        producer_id="probationary_runtime.sqlite_lane_artifacts",
        producer_family=source.source_family or source.runtime_kind,
        study_mode=execution_mode,
        split_method=f"{source.scope_kind}_observed_sample",
        split_windows=(full_sample_split_window(start=start, end=end, split_id=f"{source.scope_kind}_observed_sample"),),
        execution_assumptions=build_execution_assumptions(
            fill_policy="runtime_recorded_fill_lifecycle" if fill_count and intent_count else "runtime_trade_outcome_reconstruction",
            entry_fill_basis="trade_outcomes.entry_bar_id",
            exit_fill_basis="trade_outcomes.exit_bar_id_or_created_at",
            execution_mode=execution_mode,
            slippage_model_name="runtime_not_separately_persisted_or_embedded",
            slippage_model_version="v1",
            slippage_parameters={"fills_present": bool(fill_count), "trade_outcomes_present": True},
            fee_model_name="runtime_not_separately_persisted_or_embedded",
            fee_model_version="v1",
            fee_parameters={"fills_present": bool(fill_count), "trade_outcomes_present": True},
            conservative_bias="adapter uses persisted runtime outcomes without expanding hidden gross-vs-net assumptions",
            notes=(source.runtime_kind, source.lane_mode, source.display_name),
        ),
        data_provenance=build_data_provenance(
            source_type="probationary_runtime_sqlite",
            data_source="bars+processed_bars+trade_outcomes+state_snapshots",
            data_version=f"{source.lane_id}:{end.isoformat()}",
            provenance_id=f"runtime_sqlite:{source.lane_id}:{end.isoformat()}",
            feature_version=source.strategy_identity_root,
            code_version=code_version,
            coverage_start=start,
            coverage_end=end,
            artifact_ids={"lane_id": source.lane_id, "scope_kind": source.scope_kind},
            tags=(source.runtime_kind, source.scope_kind),
        ),
        artifact_references=artifact_references,
        tags=(
            "layer1_ready",
            "runtime_sqlite_adapter",
            source.scope_kind,
            "non_approved" if source.non_approved else "approval_candidate_surface",
        ),
    )
    return StrategyBacktest(
        strategy_name=strategy_name or f"{source.source_family or source.runtime_kind}::{source.lane_id}",
        symbol=source.symbol,
        timeframe=source.execution_timeframe or "1m",
        parameters={
            "lane_id": source.lane_id,
            "runtime_kind": source.runtime_kind,
            "lane_mode": source.lane_mode,
            "allowed_sessions": source.allowed_sessions,
            "trade_size": source.trade_size,
            "point_value": source.point_value,
        },
        bar_data=price_bars,
        trades=mapped_trades,
        equity_curve=_build_equity_curve(mapped_trades, start=start),
        position_series=_build_position_series(mapped_trades, start=start),
        provenance=provenance,
        metadata={
            "runtime_scope_kind": source.scope_kind,
            "source_family": source.source_family,
            "strategy_family": source.strategy_family,
            "experimental_status": source.experimental_status,
            "non_approved": source.non_approved,
            "tracked_strategy_id": source.tracked_strategy_id,
            **(source.metadata or {}),
        },
    )

