"""Canonical read-only Track B trade outcome layer."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_STRATEGY_PERFORMANCE_DIR = DEFAULT_OUTPUT_ROOT / "strategy_performance"
DEFAULT_CANONICAL_TRADE_RECORDS = DEFAULT_STRATEGY_PERFORMANCE_DIR / "canonical_trade_records.jsonl"
DEFAULT_SIDE_SESSION_REPLAY = DEFAULT_STRATEGY_PERFORMANCE_DIR / "side_session_attribution" / "side_session_trade_replay.jsonl"
DEFAULT_CRFD_ROWS = DEFAULT_OUTPUT_ROOT / "research" / "canonical_research_feature_dataset" / "research_feature_dataset.jsonl"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "trade_outcome_layer"

OUTCOMES_JSONL = "canonical_trade_outcomes.jsonl"
SUMMARY_JSON = "latest_trade_outcome_summary.json"
SUMMARY_MD = "latest_trade_outcome_summary.md"
CONTRACT_MD = "trade_outcome_layer_contract.md"
DATA_QUALITY_MD = "trade_outcome_data_quality.md"

SCHEMA_VERSION = "track_b_canonical_trade_outcome_v1"
SUMMARY_SCHEMA_VERSION = "track_b_trade_outcome_summary_v1"


@dataclass(frozen=True)
class TradeOutcomeLayerResult:
    outcomes: list[dict[str, Any]]
    summary: dict[str, Any]
    outcomes_path: Path
    summary_path: Path
    summary_markdown_path: Path
    contract_path: Path
    data_quality_path: Path


def run_trade_outcome_layer(
    *,
    canonical_records_path: Path = DEFAULT_CANONICAL_TRADE_RECORDS,
    side_session_replay_path: Path = DEFAULT_SIDE_SESSION_REPLAY,
    crfd_rows_path: Path = DEFAULT_CRFD_ROWS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
    jsonl_config: BoundedJsonlConfig | None = None,
) -> TradeOutcomeLayerResult:
    generated_at = _coerce_now(now)
    canonical_records = _read_jsonl(canonical_records_path)
    side_session_rows = _read_jsonl(side_session_replay_path)
    crfd_rows = _read_jsonl(crfd_rows_path)
    outcomes = build_trade_outcomes(
        canonical_records,
        side_session_rows=side_session_rows,
        crfd_rows=crfd_rows,
        generated_at=generated_at,
        source_paths={
            "canonical_records": canonical_records_path,
            "side_session_replay": side_session_replay_path,
            "crfd_rows": crfd_rows_path,
        },
    )
    summary = build_trade_outcome_summary(
        outcomes,
        canonical_records=canonical_records,
        generated_at=generated_at,
        source_paths={
            "canonical_records": canonical_records_path,
            "side_session_replay": side_session_replay_path,
            "crfd_rows": crfd_rows_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    outcomes_path = output_dir / OUTCOMES_JSONL
    write_bounded_jsonl(outcomes_path, outcomes, config=jsonl_config)
    snapshot_config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    summary_path = output_dir / SUMMARY_JSON
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config)
    summary_md = output_dir / SUMMARY_MD
    summary_md.write_text(render_trade_outcome_summary_markdown(summary), encoding="utf-8")
    contract_path = output_dir / CONTRACT_MD
    contract_path.write_text(render_trade_outcome_contract_markdown(), encoding="utf-8")
    data_quality_path = output_dir / DATA_QUALITY_MD
    data_quality_path.write_text(render_trade_outcome_quality_markdown(summary), encoding="utf-8")
    return TradeOutcomeLayerResult(
        outcomes=outcomes,
        summary=summary,
        outcomes_path=outcomes_path,
        summary_path=summary_path,
        summary_markdown_path=summary_md,
        contract_path=contract_path,
        data_quality_path=data_quality_path,
    )


def build_trade_outcomes(
    canonical_records: Sequence[Mapping[str, Any]],
    *,
    side_session_rows: Sequence[Mapping[str, Any]] = (),
    crfd_rows: Sequence[Mapping[str, Any]] = (),
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> list[dict[str, Any]]:
    replay_index = _side_session_index(side_session_rows)
    crfd_index = _CrfdIndex(crfd_rows)
    outcomes: list[dict[str, Any]] = []
    for row in canonical_records:
        if not _is_completed_paired_trade(row):
            continue
        replay = replay_index.get(_trade_join_key(row))
        entry_time = _parse_datetime(row.get("entry_time"))
        crfd = crfd_index.latest_at_or_before(contract=str(row.get("symbol") or row.get("local_symbol") or ""), timestamp=entry_time)
        outcomes.append(_build_outcome_record(row, replay=replay, crfd=crfd, generated_at=generated_at, source_paths=source_paths or {}))
    return outcomes


def build_trade_outcome_summary(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    canonical_records: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    total_records = len(canonical_records)
    unpaired = [row for row in canonical_records if not _is_completed_paired_trade(row)]
    data_quality_flags = _counts(flag for outcome in outcomes for flag in outcome.get("data_quality_flags", ()))
    strategy_coverage = _counts(str(outcome.get("lane_id") or "UNKNOWN") for outcome in outcomes)
    exit_coverage = _counts(str(outcome.get("exit_policy") or outcome.get("exit_reason") or "UNKNOWN") for outcome in outcomes)
    session_coverage = _counts(str(outcome.get("session_at_entry") or "UNKNOWN") for outcome in outcomes)
    regime_join_count = sum(1 for outcome in outcomes if outcome.get("gre_label_at_entry") or outcome.get("vwap_relation_at_entry") or outcome.get("avwap_relation_at_entry"))
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "analytics_only": True,
        "diagnostic_only": True,
        "production_effect": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "canonical_record_count": total_records,
            "outcome_count": len(outcomes),
            "incomplete_or_unpaired_count": len(unpaired),
            "pnl_proxy_available_count": sum(1 for outcome in outcomes if outcome.get("realized_pnl_proxy") is not None),
            "r_proxy_available_count": sum(1 for outcome in outcomes if outcome.get("realized_r_proxy") is not None),
            "mfe_available_count": sum(1 for outcome in outcomes if outcome.get("mfe_points") is not None),
            "mae_available_count": sum(1 for outcome in outcomes if outcome.get("mae_points") is not None),
            "average_realized_points": _average(_numeric_values(outcomes, "realized_points")),
            "average_pnl_proxy": _average(_numeric_values(outcomes, "realized_pnl_proxy")),
            "average_r_proxy": _average(_numeric_values(outcomes, "realized_r_proxy")),
            "average_mfe": _average(_numeric_values(outcomes, "mfe_points")),
            "average_mae": _average(_numeric_values(outcomes, "mae_points")),
            "average_hold_seconds": _average(_numeric_values(outcomes, "hold_seconds")),
            "median_hold_seconds": _median(_numeric_values(outcomes, "hold_seconds")),
            "regime_join_coverage": _rate(regime_join_count, len(outcomes)),
        },
        "coverage": {
            "strategy_count": len(strategy_coverage),
            "exit_policy_count": len(exit_coverage),
            "session_count": len(session_coverage),
            "strategy_coverage": strategy_coverage,
            "exit_policy_coverage": exit_coverage,
            "session_coverage": session_coverage,
        },
        "data_quality": {
            "top_flags": data_quality_flags,
            "unpaired_or_incomplete_examples": _sample_unpaired(unpaired),
            "unpaired_or_incomplete_count": len(unpaired),
        },
        "safety_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "db_mutation": False,
        },
    }


def _build_outcome_record(
    row: Mapping[str, Any],
    *,
    replay: Mapping[str, Any] | None,
    crfd: Mapping[str, Any] | None,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str],
) -> dict[str, Any]:
    flags: list[str] = []
    entry_price = _number(row.get("entry_price"))
    exit_price = _number(row.get("exit_price"))
    side = str(row.get("side") or row.get("entry_side") or "UNKNOWN").upper()
    realized_points = _number(row.get("realized_pnl_points"))
    if realized_points is None:
        realized_points = _number(row.get("mark_to_market_pnl_points"))
    if realized_points is None and entry_price is not None and exit_price is not None:
        realized_points = _realized_points_from_prices(side=side, entry_price=entry_price, exit_price=exit_price)
        flags.append("realized_points_computed_from_prices")
    if realized_points is None:
        flags.append("missing_realized_points")

    pnl_proxy = _number(row.get("realized_pnl_currency"))
    if pnl_proxy is None:
        pnl_proxy = _number(row.get("realized_pnl_proxy"))
    if pnl_proxy is None:
        point_value = _number(row.get("point_value") or row.get("dollar_per_point"))
        quantity = _number(row.get("quantity") or row.get("qty"))
        if realized_points is not None and point_value is not None and quantity is not None:
            pnl_proxy = realized_points * point_value * quantity
            flags.append("pnl_proxy_computed_from_point_value")
    if pnl_proxy is None:
        flags.append("missing_pnl_proxy")

    mfe = _number(row.get("mfe_points"))
    mae = _number(row.get("mae_points"))
    if replay:
        if mfe is None:
            mfe = _number(replay.get("mfe_points"))
        if mae is None:
            mae = _number(replay.get("mae_points"))
    if mfe is None:
        flags.append("missing_mfe")
    if mae is None:
        flags.append("missing_mae")

    hold_seconds = _number(row.get("hold_seconds"))
    if hold_seconds is None:
        hold_seconds = _seconds_between(row.get("entry_time"), row.get("exit_time"))
    if hold_seconds is None:
        flags.append("missing_hold_seconds")

    if crfd is None:
        flags.append("missing_crfd_regime_join")

    flags.append("missing_realized_r_proxy")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "trade_outcome_id": _outcome_id(row),
        "entry_trade_id": row.get("entry_trade_id") or row.get("trade_id"),
        "exit_trade_id": row.get("exit_trade_id") or row.get("exit_order_id") or row.get("trade_id"),
        "strategy_id": row.get("strategy_id") or row.get("lane_id") or "UNKNOWN",
        "lane_id": row.get("lane_id") or "UNKNOWN",
        "instrument": row.get("symbol") or row.get("instrument") or _root_symbol(row.get("local_symbol")),
        "contract": row.get("local_symbol") or row.get("contract") or row.get("symbol"),
        "side": side,
        "quantity": _number(row.get("quantity") or row.get("qty")),
        "entry_time": row.get("entry_time"),
        "exit_time": row.get("exit_time"),
        "hold_seconds": hold_seconds,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "realized_points": realized_points,
        "realized_pnl_proxy": pnl_proxy,
        "realized_r_proxy": None,
        "mfe_points": mfe,
        "mae_points": mae,
        "mfe_capture_ratio": _ratio(realized_points, mfe),
        "mae_to_realized_ratio": _ratio(mae, realized_points),
        "exit_policy": row.get("exit_policy") or row.get("exit_reason"),
        "exit_reason": row.get("exit_reason") or row.get("exit_policy"),
        "session_at_entry": row.get("session_at_entry") or row.get("session_label"),
        "session_at_exit": row.get("session_at_exit"),
        "gre_label_at_entry": crfd.get("gre_label") if crfd else None,
        "gre_confidence_at_entry": crfd.get("gre_confidence") if crfd else None,
        "vwap_relation_at_entry": crfd.get("vwap_relation") if crfd else None,
        "avwap_relation_at_entry": crfd.get("avwap_relation_globex_session_open_18et") if crfd else None,
        "data_quality_flags": sorted(set(flags)),
        "source_refs": {
            "canonical_trade_records": str(source_paths.get("canonical_records", "")),
            "side_session_replay": str(source_paths.get("side_session_replay", "")) if replay else None,
            "crfd_rows": str(source_paths.get("crfd_rows", "")) if crfd else None,
            "source_trade_id": row.get("trade_id"),
        },
        "diagnostic_only": True,
    }


def render_trade_outcome_summary_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall") or {}
    return "\n".join(
        [
            "# Trade Outcome Summary",
            "",
            f"- Generated at: {summary.get('generated_at')}",
            f"- Outcomes: {overall.get('outcome_count')}",
            f"- Incomplete/unpaired: {overall.get('incomplete_or_unpaired_count')}",
            f"- P&L proxy available: {overall.get('pnl_proxy_available_count')}",
            f"- R proxy available: {overall.get('r_proxy_available_count')}",
            f"- MFE available: {overall.get('mfe_available_count')}",
            f"- MAE available: {overall.get('mae_available_count')}",
            f"- Average realized points: {overall.get('average_realized_points')}",
            f"- Average P&L proxy: {overall.get('average_pnl_proxy')}",
            f"- Average hold seconds: {overall.get('average_hold_seconds')}",
            "",
        ]
    )


def render_trade_outcome_quality_markdown(summary: Mapping[str, Any]) -> str:
    quality = summary.get("data_quality") or {}
    lines = ["# Trade Outcome Data Quality", "", f"- Incomplete/unpaired count: {quality.get('unpaired_or_incomplete_count')}", ""]
    lines.append("## Top Flags")
    for flag, count in (quality.get("top_flags") or {}).items():
        lines.append(f"- {flag}: {count}")
    lines.append("")
    return "\n".join(lines)


def render_trade_outcome_contract_markdown() -> str:
    return "\n".join(
        [
            "# Trade Outcome Layer Contract",
            "",
            "Canonical trade outcomes are analytics-only records for completed paired trades.",
            "Unpaired or analytically incomplete source records remain in the data-quality report unless a future schema explicitly models incomplete outcomes.",
            "",
            "## Guarantees",
            "- One outcome row per completed paired trade.",
            "- No fabricated P&L, R, MFE, or MAE values.",
            "- Explicit data-quality flags for missing or derived fields.",
            "- Bounded JSONL publication.",
            "- No broker, runtime, Managed Exit, strategy, or trading-gate authority.",
            "",
        ]
    )


def _is_completed_paired_trade(row: Mapping[str, Any]) -> bool:
    return row.get("pairing_status") == "PAIRED" and row.get("trade_status") == "CLOSED"


def _outcome_id(row: Mapping[str, Any]) -> str:
    raw = "|".join(
        str(row.get(key) or "")
        for key in ("trade_id", "lane_id", "symbol", "local_symbol", "entry_time", "exit_time", "side")
    )
    return f"trade_outcome_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _realized_points_from_prices(*, side: str, entry_price: float, exit_price: float) -> float | None:
    if side == "LONG":
        return round(exit_price - entry_price, 10)
    if side == "SHORT":
        return round(entry_price - exit_price, 10)
    return None


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / abs(denominator), 6)


def _side_session_index(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    result: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in rows:
        result[(str(row.get("lane_id") or ""), str(row.get("entry_time") or ""), str(row.get("exit_time") or ""))] = row
    return result


def _trade_join_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(row.get("lane_id") or ""), str(row.get("entry_time") or ""), str(row.get("exit_time") or ""))


class _CrfdIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_contract: dict[str, list[tuple[datetime, Mapping[str, Any]]]] = {}
        for row in rows:
            ts = _parse_datetime(row.get("observation_time"))
            contract = str(row.get("contract") or "").upper()
            if ts is None or not contract:
                continue
            by_contract.setdefault(contract, []).append((ts, row))
        self._rows = {contract: sorted(values, key=lambda item: item[0]) for contract, values in by_contract.items()}

    def latest_at_or_before(self, *, contract: str, timestamp: datetime | None) -> Mapping[str, Any] | None:
        if timestamp is None:
            return None
        rows = self._rows.get(_root_symbol(contract), ()) or self._rows.get(str(contract or "").upper(), ())
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate


def _sample_unpaired(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    keys = ("pairing_status", "trade_status", "trade_id", "lane_id", "symbol", "entry_time", "exit_time")
    return [{key: row.get(key) for key in keys} for row in rows[:20]]


def _numeric_values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    return [value for value in (_number(row.get(key)) for row in rows) if value is not None]


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def _rate(part: int, whole: int) -> float | None:
    if whole <= 0:
        return None
    return round(part / whole, 6)


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _seconds_between(start: Any, end: Any) -> float | None:
    start_ts = _parse_datetime(start)
    end_ts = _parse_datetime(end)
    if start_ts is None or end_ts is None:
        return None
    return max((end_ts - start_ts).total_seconds(), 0.0)


def _root_symbol(value: Any) -> str:
    root = "".join(ch for ch in str(value or "").upper() if ch.isalpha())
    if root.startswith("MGC"):
        return "MGC"
    if root.startswith("GC"):
        return "GC"
    if root.startswith("MNQ"):
        return "MNQ"
    if root.startswith("NQ"):
        return "NQ"
    if root.startswith("MES"):
        return "MES"
    if root.startswith("ES"):
        return "ES"
    return root


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    return rows


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
