"""Pattern Engine v1 scanning and ranking utilities."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path

from .dataset import PatternEngineContext, load_pattern_engine_contexts
from .families import PatternFamilySpec, default_pattern_family_specs
from .primitives import build_pattern_primitive_points


@dataclass(frozen=True)
class PatternMatchRow:
    family_name: str
    direction: str
    session_phase: str
    anchor_timestamp: str
    phase_sequence: str
    primitive_signature: str
    move_5bar: Decimal
    move_10bar: Decimal
    move_20bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal


@dataclass(frozen=True)
class PatternSummaryRow:
    family_name: str
    direction: str
    session_phase: str
    match_count: int
    estimated_value_mfe20: Decimal
    avg_move_10bar: Decimal
    avg_mfe_20bar: Decimal
    avg_mae_20bar: Decimal
    dominant_sequence: str
    dominant_sequence_share: Decimal


@dataclass(frozen=True)
class PatternSequenceSummaryRow:
    family_name: str
    direction: str
    phase_sequence: str
    match_count: int
    estimated_value_mfe20: Decimal
    avg_move_10bar: Decimal


def build_pattern_engine_v1_report(
    *,
    replay_db_path: Path,
    ticker: str = "MGC",
    timeframe: str = "5m",
    family_specs: tuple[PatternFamilySpec, ...] | None = None,
) -> tuple[list[PatternMatchRow], list[PatternSummaryRow], list[PatternSequenceSummaryRow], dict[str, object]]:
    contexts = load_pattern_engine_contexts(replay_db_path=replay_db_path, ticker=ticker, timeframe=timeframe)
    primitives = build_pattern_primitive_points(contexts)
    specs = family_specs or default_pattern_family_specs()
    matches = _scan_matches(contexts, primitives, specs)
    family_rows = _build_family_summary_rows(matches)
    sequence_rows = _build_sequence_summary_rows(matches)
    payload = {
        "ticker": ticker,
        "timeframe": timeframe,
        "context_count": len(contexts),
        "family_count": len(specs),
        "match_count": len(matches),
        "primitive_vocabulary": [
            "slope_state",
            "curvature_state",
            "expansion_state",
            "pullback_state",
            "breakout_state",
            "failure_state",
            "ema_location_state",
            "extrema_distance_state",
            "volume_context",
            "close_strength_state",
        ],
        "family_rankings": [asdict(row) for row in family_rows[:12]],
        "sequence_rankings": [asdict(row) for row in sequence_rows[:12]],
    }
    return matches, family_rows, sequence_rows, payload


def write_pattern_engine_v1_report(
    *,
    detail_path: Path,
    family_summary_path: Path,
    sequence_summary_path: Path,
    summary_json_path: Path,
    matches: list[PatternMatchRow],
    family_rows: list[PatternSummaryRow],
    sequence_rows: list[PatternSequenceSummaryRow],
    summary_payload: dict[str, object],
) -> None:
    _write_csv(detail_path, [asdict(row) for row in matches])
    _write_csv(family_summary_path, [asdict(row) for row in family_rows])
    _write_csv(sequence_summary_path, [asdict(row) for row in sequence_rows])
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _scan_matches(
    contexts: list[PatternEngineContext],
    primitives,
    family_specs: tuple[PatternFamilySpec, ...],
) -> list[PatternMatchRow]:
    rows: list[PatternMatchRow] = []
    for index in range(len(contexts)):
        for spec in family_specs:
            match = spec.matcher(contexts, primitives, index)
            if match is None:
                continue
            metrics = _forward_metrics(contexts, index, match.direction)
            rows.append(
                PatternMatchRow(
                    family_name=match.family_name,
                    direction=match.direction,
                    session_phase=match.session_phase,
                    anchor_timestamp=match.anchor_timestamp,
                    phase_sequence=" > ".join(step.phase_name for step in match.steps),
                    primitive_signature=" > ".join(step.primitive_signature for step in match.steps),
                    move_5bar=metrics["move_5bar"],
                    move_10bar=metrics["move_10bar"],
                    move_20bar=metrics["move_20bar"],
                    mfe_20bar=metrics["mfe_20bar"],
                    mae_20bar=metrics["mae_20bar"],
                )
            )
    return rows


def _forward_metrics(contexts: list[PatternEngineContext], index: int, direction: str) -> dict[str, Decimal]:
    anchor_close = contexts[index].close
    close_5 = contexts[min(index + 5, len(contexts) - 1)].close
    close_10 = contexts[min(index + 10, len(contexts) - 1)].close
    close_20 = contexts[min(index + 20, len(contexts) - 1)].close
    forward_window = contexts[index + 1 : min(index + 21, len(contexts))]
    if direction == "SHORT":
        move_5 = anchor_close - close_5
        move_10 = anchor_close - close_10
        move_20 = anchor_close - close_20
        mfe_20 = max((anchor_close - item.low for item in forward_window), default=Decimal("0"))
        mae_20 = max((item.high - anchor_close for item in forward_window), default=Decimal("0"))
    else:
        move_5 = close_5 - anchor_close
        move_10 = close_10 - anchor_close
        move_20 = close_20 - anchor_close
        mfe_20 = max((item.high - anchor_close for item in forward_window), default=Decimal("0"))
        mae_20 = max((anchor_close - item.low for item in forward_window), default=Decimal("0"))
    return {
        "move_5bar": move_5,
        "move_10bar": move_10,
        "move_20bar": move_20,
        "mfe_20bar": mfe_20,
        "mae_20bar": mae_20,
    }


def _build_family_summary_rows(matches: list[PatternMatchRow]) -> list[PatternSummaryRow]:
    grouped: dict[tuple[str, str, str], list[PatternMatchRow]] = defaultdict(list)
    for row in matches:
        grouped[(row.family_name, row.direction, row.session_phase)].append(row)
    summary_rows: list[PatternSummaryRow] = []
    for (family_name, direction, session_phase), bucket in grouped.items():
        sequence_counts = Counter(item.phase_sequence for item in bucket)
        dominant_sequence, dominant_count = sequence_counts.most_common(1)[0]
        summary_rows.append(
            PatternSummaryRow(
                family_name=family_name,
                direction=direction,
                session_phase=session_phase,
                match_count=len(bucket),
                estimated_value_mfe20=sum((item.mfe_20bar for item in bucket), Decimal("0")),
                avg_move_10bar=_avg(item.move_10bar for item in bucket),
                avg_mfe_20bar=_avg(item.mfe_20bar for item in bucket),
                avg_mae_20bar=_avg(item.mae_20bar for item in bucket),
                dominant_sequence=dominant_sequence,
                dominant_sequence_share=_ratio(dominant_count, len(bucket)),
            )
        )
    summary_rows.sort(key=lambda row: (row.estimated_value_mfe20, row.match_count), reverse=True)
    return summary_rows


def _build_sequence_summary_rows(matches: list[PatternMatchRow]) -> list[PatternSequenceSummaryRow]:
    grouped: dict[tuple[str, str, str], list[PatternMatchRow]] = defaultdict(list)
    for row in matches:
        grouped[(row.family_name, row.direction, row.phase_sequence)].append(row)
    summary_rows: list[PatternSequenceSummaryRow] = []
    for (family_name, direction, phase_sequence), bucket in grouped.items():
        summary_rows.append(
            PatternSequenceSummaryRow(
                family_name=family_name,
                direction=direction,
                phase_sequence=phase_sequence,
                match_count=len(bucket),
                estimated_value_mfe20=sum((item.mfe_20bar for item in bucket), Decimal("0")),
                avg_move_10bar=_avg(item.move_10bar for item in bucket),
            )
        )
    summary_rows.sort(key=lambda row: (row.estimated_value_mfe20, row.match_count), reverse=True)
    return summary_rows


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        rows = [{"status": "no_rows"}]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _avg(values) -> Decimal:
    values = list(values)
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ratio(numerator: int, denominator: int) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator)
