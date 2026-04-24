"""Dedicated Layer 1 adapter boundary for ATP companion / staged participation artifacts."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from mgc_v05l.config_models import load_settings_from_files

from ..data.contracts import EquityPoint, PositionPoint, PriceBar, StrategyBacktest, TradeRecord
from .producer_contract import (
    build_data_provenance,
    build_execution_assumptions,
    build_strategy_backtest_provenance,
    build_trade_validation_metadata,
    full_sample_split_window,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class AtpLayer1Source:
    lane_id: str
    subject_label: str
    source_config_path: Path
    lane_dir: Path
    runtime_config_in_force_path: Path
    family_classification: str
    baseline_reference_path: Path | None = None
    candidate_registry_path: Path | None = None
    candidate_config_path: Path | None = None


def load_atp_layer1_source(
    *,
    lane_id: str,
    subject_label: str,
    source_config_path: str | Path,
    lane_dir: str | Path,
    runtime_config_in_force_path: str | Path,
    family_classification: str,
    baseline_reference_path: str | Path | None = None,
    candidate_registry_path: str | Path | None = None,
    candidate_config_path: str | Path | None = None,
) -> AtpLayer1Source:
    return AtpLayer1Source(
        lane_id=lane_id,
        subject_label=subject_label,
        source_config_path=_resolve_path(source_config_path),
        lane_dir=_resolve_path(lane_dir),
        runtime_config_in_force_path=_resolve_path(runtime_config_in_force_path),
        family_classification=family_classification,
        baseline_reference_path=_resolve_path(baseline_reference_path) if baseline_reference_path is not None else None,
        candidate_registry_path=_resolve_path(candidate_registry_path) if candidate_registry_path is not None else None,
        candidate_config_path=_resolve_path(candidate_config_path) if candidate_config_path is not None else None,
    )


def build_strategy_backtest_from_atp_source(
    source: AtpLayer1Source,
    *,
    code_version: str,
    strategy_name: str | None = None,
) -> StrategyBacktest:
    _validate_source_paths(source)
    configured_lane = _load_configured_lane_spec(source)
    runtime_state = _load_json(source.lane_dir / "runtime_state.json")
    operator_status = _load_json(source.lane_dir / "operator_status.json")
    _validate_runtime_truth(source, runtime_state, operator_status)
    runtime_lane = _load_runtime_lane_spec(source)
    _validate_lane_identity(source, configured_lane, runtime_lane, operator_status)
    if source.candidate_registry_path is not None and source.candidate_config_path is not None:
        _validate_candidate_registry(source)

    processed_bars_rows = _load_jsonl(source.lane_dir / "processed_bars.jsonl")
    trade_rows = _load_jsonl(source.lane_dir / "trades.jsonl")
    signal_rows = _load_jsonl(source.lane_dir / "signals.jsonl")
    order_intent_rows = _load_jsonl(source.lane_dir / "order_intents.jsonl")
    fill_rows = _load_jsonl(source.lane_dir / "fills.jsonl")
    reconciliation_rows = _load_jsonl(source.lane_dir / "reconciliation_events.jsonl")

    _validate_required_artifacts(
        source=source,
        processed_bars_rows=processed_bars_rows,
        trade_rows=trade_rows,
        signal_rows=signal_rows,
        order_intent_rows=order_intent_rows,
        fill_rows=fill_rows,
        reconciliation_rows=reconciliation_rows,
    )

    price_bars = tuple(_price_bar_from_row(row) for row in processed_bars_rows if str(row.get("lane_id")) == source.lane_id)
    lifecycle_records = tuple(runtime_state["authoritative_trade_lifecycle_records"])
    signals_by_decision = _signals_by_decision(signal_rows)
    converted_trades = tuple(
        _trade_record_from_atp_artifacts(
            source=source,
            trade_row=trade_row,
            lifecycle_record=_find_lifecycle_record(lifecycle_records, trade_id=str(trade_row["trade_id"])),
            signal_row=signals_by_decision.get(str(trade_row["decision_id"])),
            bar_rows=processed_bars_rows,
            configured_lane=configured_lane,
            order_intent_rows=order_intent_rows,
            fill_rows=fill_rows,
        )
        for trade_row in trade_rows
    )
    mapped_trades = tuple(sorted(converted_trades, key=lambda item: (item.entry_time, item.metadata.get("trade_id", ""))))
    if price_bars:
        start = min(price_bars[0].timestamp, mapped_trades[0].entry_time)
        end = max(price_bars[-1].timestamp, mapped_trades[-1].exit_time)
    else:
        start = mapped_trades[0].entry_time
        end = mapped_trades[-1].exit_time

    fill_policy = str(runtime_state.get("active_entry_model", "CURRENT_CANDLE_VWAP")).lower()
    source_config_label = str(source.source_config_path.relative_to(REPO_ROOT)) if source.source_config_path.is_relative_to(REPO_ROOT) else str(source.source_config_path)
    artifact_references = {
        "source_config": str(source.source_config_path),
        "runtime_config_in_force": str(source.runtime_config_in_force_path),
        "lane_dir": str(source.lane_dir),
        "processed_bars": str(source.lane_dir / "processed_bars.jsonl"),
        "signals": str(source.lane_dir / "signals.jsonl"),
        "order_intents": str(source.lane_dir / "order_intents.jsonl"),
        "fills": str(source.lane_dir / "fills.jsonl"),
        "trades": str(source.lane_dir / "trades.jsonl"),
        "runtime_state": str(source.lane_dir / "runtime_state.json"),
        "operator_status": str(source.lane_dir / "operator_status.json"),
        "reconciliation_events": str(source.lane_dir / "reconciliation_events.jsonl"),
    }
    if source.baseline_reference_path is not None:
        artifact_references["baseline_reference_config"] = str(source.baseline_reference_path)
    if source.candidate_registry_path is not None:
        artifact_references["candidate_registry_config"] = str(source.candidate_registry_path)
    if source.candidate_config_path is not None:
        artifact_references["candidate_config"] = str(source.candidate_config_path)

    provenance = build_strategy_backtest_provenance(
        producer_id="probationary_runtime.atp_companion_lane_artifacts",
        producer_family="atp_companion_staged_participation",
        study_mode="paper_runtime_artifact_adaptation",
        split_method="paper_runtime_observed_sample",
        split_windows=(full_sample_split_window(start=start, end=end, split_id="paper_runtime_observed_sample"),),
        execution_assumptions=build_execution_assumptions(
            fill_policy=fill_policy,
            entry_fill_basis="paper_runtime_current_candle_vwap_fill_ledger",
            exit_fill_basis="paper_runtime_recorded_exit_fill_ledger",
            execution_mode="paper_runtime",
            slippage_model_name="paper_runtime_recorded_slippage_cost",
            slippage_model_version="v1",
            slippage_parameters={"recorded_default": 0.0, "artifact_field": "slippage_cost"},
            fee_model_name="paper_runtime_recorded_fee_cost",
            fee_model_version="v1",
            fee_parameters={"recorded_default": 0.0, "artifact_field": "fees_paid"},
            conservative_bias="adapter preserves recorded runtime fills and costs without inventing extra friction",
            notes=(
                source.family_classification,
                source_config_label,
            ),
        ),
        data_provenance=build_data_provenance(
            source_type="probationary_paper_runtime_lane_artifacts",
            data_source="processed_bars+runtime_state+trade_lifecycle_records",
            data_version=f"{source.lane_id}:{end.isoformat()}",
            provenance_id=f"atp_companion:{source.lane_id}:{end.isoformat()}",
            feature_version=str(
                (runtime_lane or {}).get("strategy_identity_root")
                or configured_lane.get("strategy_identity_root")
                or "ATP_COMPANION_V1"
            ),
            code_version=code_version,
            coverage_start=start,
            coverage_end=end,
            artifact_ids={
                "lane_id": source.lane_id,
                "standalone_strategy_id": str((runtime_lane or {}).get("standalone_strategy_id") or configured_lane.get("standalone_strategy_id", "")),
                "runtime_kind": str((runtime_lane or {}).get("runtime_kind") or configured_lane.get("runtime_kind", "")),
            },
            tags=(source.family_classification, "atp_companion", "layer1_ready"),
        ),
        artifact_references=artifact_references,
        tags=(
            "layer1_ready",
            "atp_companion",
            source.family_classification,
            "runtime_config_entry_present" if runtime_lane is not None else "runtime_config_entry_missing",
        ),
    )
    return StrategyBacktest(
        strategy_name=strategy_name or f"atp_companion::{source.lane_id}",
        symbol=str(configured_lane["symbol"]),
        timeframe=str(operator_status.get("execution_timeframe") or configured_lane.get("execution_timeframe", "1m")),
        parameters={
            "lane_id": source.lane_id,
            "lane_mode": configured_lane.get("lane_mode"),
            "runtime_kind": configured_lane.get("runtime_kind"),
            "participation_policy": configured_lane.get("participation_policy"),
            "max_concurrent_entries": configured_lane.get("max_concurrent_entries"),
            "max_adds_after_entry": configured_lane.get("max_adds_after_entry"),
            "quality_bucket_policy": configured_lane.get("quality_bucket_policy"),
            "session_restriction": configured_lane.get("session_restriction"),
            "allowed_sessions": tuple(configured_lane.get("allowed_sessions", ()) or ()),
            "candidate_id": configured_lane.get("candidate_id"),
            "candidate_origin": configured_lane.get("candidate_origin"),
        },
        bar_data=price_bars,
        trades=mapped_trades,
        equity_curve=_build_equity_curve(mapped_trades, start=start),
        position_series=_build_position_series(mapped_trades, start=start),
        provenance=provenance,
        metadata={
            "pilot_family": "atp_companion_staged_participation",
            "family_classification": source.family_classification,
            "lane_identity": {
                "lane_id": source.lane_id,
                "display_name": operator_status.get("display_name") or configured_lane.get("display_name"),
                "standalone_strategy_id": configured_lane.get("standalone_strategy_id"),
                "runtime_kind": configured_lane.get("runtime_kind"),
                "lane_mode": configured_lane.get("lane_mode"),
            },
            "baseline_reference_config": None if source.baseline_reference_path is None else str(source.baseline_reference_path),
            "candidate_registry_config": None if source.candidate_registry_path is None else str(source.candidate_registry_path),
            "candidate_config": None if source.candidate_config_path is None else str(source.candidate_config_path),
            "truth_basis": {
                "runtime_state_authoritative": True,
                "runtime_config_entry_present": runtime_lane is not None,
                "trade_lifecycle_records": len(lifecycle_records),
                "reconciliation_events": len(reconciliation_rows),
                "price_bar_window_start": None if not price_bars else price_bars[0].timestamp.isoformat(),
                "price_bar_window_end": None if not price_bars else price_bars[-1].timestamp.isoformat(),
            },
        },
    )


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = REPO_ROOT / resolved
    return resolved.resolve(strict=False)


def _validate_source_paths(source: AtpLayer1Source) -> None:
    required = (
        source.source_config_path,
        source.lane_dir,
        source.runtime_config_in_force_path,
        source.lane_dir / "processed_bars.jsonl",
        source.lane_dir / "signals.jsonl",
        source.lane_dir / "order_intents.jsonl",
        source.lane_dir / "fills.jsonl",
        source.lane_dir / "trades.jsonl",
        source.lane_dir / "runtime_state.json",
        source.lane_dir / "operator_status.json",
        source.lane_dir / "reconciliation_events.jsonl",
    )
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise ValueError(
            "ATP Layer 1 adaptation requires complete provenance artifacts. Missing paths: "
            + ", ".join(missing)
        )


def _load_configured_lane_spec(source: AtpLayer1Source) -> dict[str, Any]:
    settings = load_settings_from_files([REPO_ROOT / "config" / "base.yaml", source.source_config_path])
    matching = [row for row in settings.probationary_paper_lane_specs if str(row.get("lane_id")) == source.lane_id]
    if len(matching) != 1:
        raise ValueError(
            f"ATP source config {source.source_config_path} must define exactly one matching lane for {source.lane_id}."
        )
    return dict(matching[0])


def _load_runtime_lane_spec(source: AtpLayer1Source) -> dict[str, Any] | None:
    payload = _load_json(source.runtime_config_in_force_path)
    matching = [row for row in payload.get("lanes", []) if str(row.get("lane_id")) == source.lane_id]
    if not matching:
        return None
    if len(matching) != 1:
        raise ValueError(f"Runtime config in force contains ambiguous entries for {source.lane_id}.")
    return dict(matching[0])


def _validate_lane_identity(
    source: AtpLayer1Source,
    configured_lane: dict[str, Any],
    runtime_lane: dict[str, Any] | None,
    operator_status: dict[str, Any],
) -> None:
    if runtime_lane is not None:
        mismatches: list[str] = []
        for key in (
            "lane_id",
            "symbol",
            "standalone_strategy_id",
            "runtime_kind",
            "lane_mode",
            "strategy_family",
            "quality_bucket_policy",
            "artifacts_dir",
        ):
            configured_value = configured_lane.get(key)
            runtime_value = runtime_lane.get(key)
            if runtime_value in (None, ""):
                continue
            if key == "artifacts_dir":
                configured_path = _resolve_path(str(configured_value))
                runtime_path = _resolve_path(str(runtime_value))
                if configured_path != runtime_path:
                    mismatches.append(key)
                continue
            if str(configured_value) != str(runtime_value):
                mismatches.append(key)
        if mismatches:
            raise ValueError(
                "ATP Layer 1 adaptation refused due to config/runtime drift for "
                f"{source.lane_id}: {', '.join(sorted(mismatches))}"
            )

    operator_checks = {
        "lane_id": operator_status.get("lane_id"),
        "symbol": operator_status.get("symbol"),
        "display_name": operator_status.get("display_name"),
    }
    if str(operator_checks["lane_id"]) != str(source.lane_id):
        raise ValueError(f"ATP operator status lane_id mismatch for {source.lane_id}.")
    if operator_checks["symbol"] is not None and str(operator_checks["symbol"]) != str(configured_lane.get("symbol")):
        raise ValueError(f"ATP operator status symbol mismatch for {source.lane_id}.")
    if not str(operator_checks["display_name"] or "").strip():
        raise ValueError(f"ATP operator status is missing display_name for {source.lane_id}.")


def _validate_candidate_registry(source: AtpLayer1Source) -> None:
    assert source.candidate_registry_path is not None
    assert source.candidate_config_path is not None
    registry_text = source.candidate_registry_path.read_text(encoding="utf-8")
    candidate_text = source.candidate_config_path.read_text(encoding="utf-8")
    if 'advanced_candidate_id: "promotion_1_075r_favorable_only"' not in registry_text:
        raise ValueError("ATP candidate registry no longer marks promotion_1_075r_favorable_only as the active candidate.")
    relative_candidate_path = str(source.candidate_config_path.relative_to(REPO_ROOT))
    if relative_candidate_path not in registry_text:
        raise ValueError("ATP candidate registry does not reference the expected candidate config path.")
    if 'candidate_id: "promotion_1_075r_favorable_only"' not in candidate_text:
        raise ValueError("ATP candidate config is missing the expected candidate_id.")


def _validate_runtime_truth(source: AtpLayer1Source, runtime_state: dict[str, Any], operator_status: dict[str, Any]) -> None:
    if not bool(runtime_state.get("authoritative_trade_lifecycle_available")):
        raise ValueError(f"ATP runtime_state for {source.lane_id} is not authoritative enough for Layer 1 adaptation.")
    if not runtime_state.get("authoritative_trade_lifecycle_records"):
        raise ValueError(f"ATP runtime_state for {source.lane_id} contains no authoritative lifecycle records.")
    if str(operator_status.get("active_entry_model", "")) != str(runtime_state.get("active_entry_model", "")):
        raise ValueError(f"ATP operator status and runtime state disagree on active_entry_model for {source.lane_id}.")


def _validate_required_artifacts(
    *,
    source: AtpLayer1Source,
    processed_bars_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    signal_rows: list[dict[str, Any]],
    order_intent_rows: list[dict[str, Any]],
    fill_rows: list[dict[str, Any]],
    reconciliation_rows: list[dict[str, Any]],
) -> None:
    if not processed_bars_rows:
        raise ValueError(f"ATP processed bars are missing for {source.lane_id}.")
    if not trade_rows:
        raise ValueError(f"ATP trade ledger is empty for {source.lane_id}; adaptation would be too lossy.")
    if not signal_rows:
        raise ValueError(f"ATP signal lineage is missing for {source.lane_id}.")
    if not order_intent_rows or not fill_rows:
        raise ValueError(f"ATP order-intent/fill lineage is incomplete for {source.lane_id}.")
    if not reconciliation_rows:
        raise ValueError(f"ATP reconciliation lineage is missing for {source.lane_id}.")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _price_bar_from_row(row: dict[str, Any]) -> PriceBar:
    return PriceBar(
        timestamp=datetime.fromisoformat(str(row["end_ts"])),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row.get("volume", 0.0) or 0.0),
        session_label=str(row.get("session_segment") or row.get("session_label") or ""),
        metadata={
            "symbol": row.get("symbol"),
            "lane_id": row.get("lane_id"),
            "provenance": row.get("provenance"),
            "experimental_status": row.get("experimental_status"),
        },
    )


def _signals_by_decision(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_decision: dict[str, dict[str, Any]] = {}
    for row in rows:
        decision_id = str(row.get("decision_id", "")).strip()
        if not decision_id:
            continue
        chosen = by_decision.get(decision_id)
        if chosen is None or bool(row.get("signal_passed_flag")):
            by_decision[decision_id] = row
    return by_decision


def _find_lifecycle_record(records: tuple[dict[str, Any], ...], *, trade_id: str) -> dict[str, Any]:
    for row in records:
        if str(row.get("trade_id")) == trade_id:
            return row
    raise ValueError(f"ATP runtime_state is missing lifecycle record for trade {trade_id}.")


def _trade_record_from_atp_artifacts(
    *,
    source: AtpLayer1Source,
    trade_row: dict[str, Any],
    lifecycle_record: dict[str, Any],
    signal_row: dict[str, Any] | None,
    bar_rows: list[dict[str, Any]],
    configured_lane: dict[str, Any],
    order_intent_rows: list[dict[str, Any]],
    fill_rows: list[dict[str, Any]],
) -> TradeRecord:
    decision_id = str(trade_row["decision_id"])
    entry_time = datetime.fromisoformat(str(trade_row["entry_timestamp"]))
    exit_time = datetime.fromisoformat(str(trade_row["exit_timestamp"]))
    entry_fill_matches = [
        row for row in fill_rows
        if str(row.get("lane_id")) == source.lane_id
        and datetime.fromisoformat(str(row["fill_timestamp"])) == entry_time
    ]
    exit_fill_matches = [
        row for row in fill_rows
        if str(row.get("lane_id")) == source.lane_id
        and datetime.fromisoformat(str(row["fill_timestamp"])) == exit_time
    ]
    if not entry_fill_matches or not exit_fill_matches:
        raise ValueError(
            f"ATP fill lineage is incomplete for trade {trade_row['trade_id']} in lane {source.lane_id}."
        )
    intent_matches = [
        row for row in order_intent_rows
        if str(row.get("lane_id")) == source.lane_id
        and datetime.fromisoformat(str(row["submitted_at"])) in {entry_time, exit_time}
    ]
    if len(intent_matches) < 2:
        raise ValueError(
            f"ATP order-intent lineage is incomplete for trade {trade_row['trade_id']} in lane {source.lane_id}."
        )

    direction = str(trade_row["direction"]).lower()
    trade_bars = [
        row for row in bar_rows
        if str(row.get("lane_id")) == source.lane_id
        and entry_time <= datetime.fromisoformat(str(row["end_ts"])) <= exit_time
    ]
    entry_price = float(Decimal(str(trade_row["entry_price"])))
    holding_minutes = max(0.0, (exit_time - entry_time).total_seconds() / 60.0)
    holding_bars = _holding_bars_from_trade(
        lifecycle_record=lifecycle_record,
        configured_lane=configured_lane,
        holding_minutes=holding_minutes,
    )
    if trade_bars:
        mae, mfe = _excursion_from_bars(direction=direction, entry_price=entry_price, trade_bars=trade_bars)
        excursion_source = "processed_bars_window"
    else:
        mae, mfe = 0.0, 0.0
        excursion_source = "unavailable_rolling_bar_window"
    signal_snapshot = {} if signal_row is None else dict(signal_row.get("feature_snapshot", {}))
    return TradeRecord(
        entry_time=entry_time,
        exit_time=exit_time,
        direction=direction,
        qty=float(trade_row["quantity"]),
        gross_pnl=float(Decimal(str(trade_row["gross_pnl"]))),
        net_pnl=float(Decimal(str(trade_row["realized_pnl"]))),
        mae=mae,
        mfe=mfe,
        holding_bars=holding_bars,
        holding_minutes=holding_minutes,
        session_label=str(signal_row.get("session_segment") if signal_row is not None else ""),
        validation_metadata=build_trade_validation_metadata(
            trade_id=str(trade_row["trade_id"]),
            signal_id=decision_id,
            setup_family=str(trade_row["setup_family"]),
            setup_variant=None if signal_row is None else str(signal_row.get("variant_id")),
            decision_time=datetime.fromisoformat(str(trade_row["decision_ts"])),
            exit_reason=str(trade_row["exit_reason"]),
            execution_model=str(lifecycle_record.get("entry_model") or configured_lane.get("runtime_kind")),
            fill_policy=str(lifecycle_record.get("entry_model") or "CURRENT_CANDLE_VWAP").lower(),
            slippage_cost=float(Decimal(str(trade_row.get("slippage_cost", "0")))),
            fee_cost=float(Decimal(str(trade_row.get("fees_paid", "0")))),
            tags={
                "lane_id": source.lane_id,
                "family_classification": source.family_classification,
                "decision_context_linkage_available": bool(trade_row.get("decision_context_linkage_available")),
                "quality_bucket_policy": trade_row.get("quality_bucket_policy"),
                "setup_signature": trade_row.get("setup_signature"),
                "setup_state_signature": trade_row.get("setup_state_signature"),
                "signal_snapshot": signal_snapshot,
                "excursion_source": excursion_source,
            },
        ),
        metadata={
            "trade_id": trade_row["trade_id"],
            "entry_source_family": trade_row.get("entry_source_family"),
            "primary_exit_reason": lifecycle_record.get("primary_exit_reason"),
            "pnl_truth_basis": lifecycle_record.get("pnl_truth_basis"),
            "source_resolution": lifecycle_record.get("source_resolution"),
            "runtime_truth_provenance": lifecycle_record.get("truth_provenance", {}),
            "entry_order_intent_ids": [row["order_intent_id"] for row in intent_matches if datetime.fromisoformat(str(row["submitted_at"])) == entry_time],
            "exit_order_intent_ids": [row["order_intent_id"] for row in intent_matches if datetime.fromisoformat(str(row["submitted_at"])) == exit_time],
            "excursion_source": excursion_source,
        },
    )


def _excursion_from_bars(*, direction: str, entry_price: float, trade_bars: list[dict[str, Any]]) -> tuple[float, float]:
    if direction == "long":
        mfe = max(float(row["high"]) - entry_price for row in trade_bars)
        mae = min(float(row["low"]) - entry_price for row in trade_bars)
    else:
        mfe = max(entry_price - float(row["low"]) for row in trade_bars)
        mae = min(entry_price - float(row["high"]) for row in trade_bars)
    return round(mae, 6), round(mfe, 6)


def _holding_bars_from_trade(
    *,
    lifecycle_record: dict[str, Any],
    configured_lane: dict[str, Any],
    holding_minutes: float,
) -> int:
    bars_held = lifecycle_record.get("bars_held_1m")
    if bars_held is not None:
        try:
            return max(1, int(bars_held))
        except (TypeError, ValueError):
            pass
    timeframe_minutes = _timeframe_to_minutes(str(configured_lane.get("execution_timeframe", "1m")))
    return max(1, int(round(holding_minutes / max(1, timeframe_minutes))))


def _timeframe_to_minutes(value: str) -> int:
    normalized = value.strip().lower()
    if normalized.endswith("m"):
        return max(1, int(normalized[:-1]))
    if normalized.endswith("h"):
        return max(1, int(normalized[:-1]) * 60)
    return 1


def _build_equity_curve(
    trades: tuple[TradeRecord, ...],
    *,
    start: datetime,
    starting_equity: float = 100_000.0,
) -> tuple[EquityPoint, ...]:
    equity = starting_equity
    curve = [EquityPoint(timestamp=start, equity=equity)]
    for trade in sorted(trades, key=lambda item: item.exit_time):
        equity += trade.net_pnl
        curve.append(EquityPoint(timestamp=trade.exit_time, equity=equity, session_label=trade.session_label))
    return tuple(curve)


def _build_position_series(trades: tuple[TradeRecord, ...], *, start: datetime) -> tuple[PositionPoint, ...]:
    deltas: dict[datetime, float] = defaultdict(float)
    deltas[start] += 0.0
    for trade in sorted(trades, key=lambda item: item.entry_time):
        signed_qty = trade.qty if trade.direction == "long" else -trade.qty
        deltas[trade.entry_time] += signed_qty
        deltas[trade.exit_time] -= signed_qty

    running_position = 0.0
    positions: list[PositionPoint] = []
    for timestamp in sorted(deltas):
        running_position += deltas[timestamp]
        positions.append(PositionPoint(timestamp=timestamp, position=running_position))
    return tuple(positions)
