"""Research scaffold for a GC/MGC-only London-open acceptance continuation sibling branch."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..app.session_phase_labels import label_session_phase
from ..config_models import StrategySettings, load_settings_from_files
from ..domain.models import Bar


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_london_open_acceptance_continuation_long"
DEFAULT_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "live.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
)
DEFAULT_LANE_DATABASES = {
    "GC": REPO_ROOT / "mgc_v05l.probationary.paper__gc_asia_early_normal_breakout_retest_hold_long.sqlite3",
    "MGC": REPO_ROOT / "mgc_v05l.probationary.paper__mgc_asia_early_normal_breakout_retest_hold_long.sqlite3",
}
FIRST_THREE_LONDON_OPEN_BARS = (time(3, 5), time(3, 10), time(3, 15))


@dataclass(frozen=True)
class StoredFeature:
    atr: Decimal
    velocity: Decimal
    bar_range: Decimal


@dataclass(frozen=True)
class StoredBarContext:
    bar: Bar
    feature: StoredFeature


@dataclass(frozen=True)
class BranchEvaluation:
    signal_timestamp: str
    symbol: str
    session_label: str
    prior_high: str
    breakout_high: str
    breakout_close: str
    signal_low: str
    signal_close: str
    breakout_normalized_slope: str
    breakout_range_expansion_ratio: str
    baseline_current_family_passed: bool
    baseline_detail: dict[str, bool | str]
    sibling_branch_passed: bool
    sibling_detail: dict[str, bool | str]


def run_gc_mgc_london_open_acceptance_continuation_research(
    *,
    output_dir: str | Path | None = None,
    gc_lane_database_path: str | Path | None = None,
    mgc_lane_database_path: str | Path | None = None,
    inspected_session_date: date | None = None,
) -> dict[str, Path]:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files(list(DEFAULT_CONFIG_PATHS))
    target_date = inspected_session_date or date(2026, 3, 24)
    database_paths = {
        "GC": Path(gc_lane_database_path).resolve() if gc_lane_database_path else DEFAULT_LANE_DATABASES["GC"],
        "MGC": Path(mgc_lane_database_path).resolve() if mgc_lane_database_path else DEFAULT_LANE_DATABASES["MGC"],
    }

    symbol_payloads: dict[str, Any] = {}
    inspected_move: dict[str, list[dict[str, Any]]] = {}
    for symbol, database_path in database_paths.items():
        contexts = _load_stored_contexts(database_path=database_path, symbol=symbol)
        branch_rows = _evaluate_symbol(contexts=contexts, settings=settings)
        sibling_rows = [row for row in branch_rows if row.sibling_branch_passed]
        baseline_rows = [row for row in branch_rows if row.baseline_current_family_passed]
        inspected_rows = [
            asdict(row)
            for row in branch_rows
            if datetime.fromisoformat(row.signal_timestamp).astimezone(settings.timezone_info).date() == target_date
            and datetime.fromisoformat(row.signal_timestamp).astimezone(settings.timezone_info).time() in FIRST_THREE_LONDON_OPEN_BARS
        ]
        symbol_payloads[symbol] = {
            "database_path": str(database_path),
            "loaded_bar_count": len(contexts),
            "candidate_count_current_family": len(baseline_rows),
            "candidate_count_sibling_branch": len(sibling_rows),
            "stored_sample_trade_count_proxy": len(sibling_rows),
            "direct_overlap_with_existing_gc_mgc_family_candidates": len(
                {row.signal_timestamp for row in sibling_rows} & {row.signal_timestamp for row in baseline_rows}
            ),
            "first_three_london_open_evaluations": [asdict(row) for row in branch_rows if _is_first_three_london_open(row.signal_timestamp, settings)],
        }
        inspected_move[symbol] = inspected_rows

    gc_0315 = next(
        (
            row
            for row in inspected_move.get("GC", [])
            if row["signal_timestamp"] == "2026-03-24T03:15:00-04:00"
        ),
        None,
    )
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_branch_id": "gc_mgc_london_open_acceptance_continuation_long",
        "status": "research_scaffold_only",
        "definition": {
            "instruments": ["GC", "MGC"],
            "session": "LONDON_OPEN",
            "window": "first_3_completed_5m_bars_only",
            "direction": "LONG",
            "prior_bar_high_broken_by_breakout_bar": True,
            "breakout_bar_close_greater_equal_prior_bar_close": True,
            "breakout_normalized_slope_min": "0.25",
            "breakout_normalized_slope_max": "1.10",
            "breakout_range_expansion_ratio_min": "0.10",
            "breakout_range_expansion_ratio_max": "0.50",
            "signal_bar_low_greater_equal_breakout_high": True,
            "signal_bar_close_greater_equal_breakout_high_plus_0_35_atr": True,
            "anti_churn_priority_note": "Existing anti-churn, one-position, and priority rules remain unchanged; this artifact only defines the structural sibling branch.",
        },
        "inspected_session_date": target_date.isoformat(),
        "before_after_on_inspected_move": inspected_move,
        "sample_summary": symbol_payloads,
        "overlap_and_chase_review": {
            "existing_gc_mgc_family_overlap": {
                symbol: {
                    "direct_overlap_with_existing_gc_mgc_family_candidates": payload["direct_overlap_with_existing_gc_mgc_family_candidates"],
                    "verdict": (
                        "no_direct_overlap_in_stored_sample"
                        if payload["direct_overlap_with_existing_gc_mgc_family_candidates"] == 0
                        else "overlap_present"
                    ),
                }
                for symbol, payload in symbol_payloads.items()
            },
            "obvious_low_quality_chase_behavior": {
                "verdict": "no_obvious_broad_chase_signature_in_tiny_sample",
                "note": (
                    "The branch stays limited to the first three completed London-open bars, and MGC already self-filters the third bar when "
                    "breakout expansion falls below the minimum band. The GC third-bar pass is the main chase-risk candidate to monitor."
                ),
            },
            "gc_0315_filter_recommendation": (
                {
                    "signal_timestamp": gc_0315["signal_timestamp"],
                    "current_status": "keep_admitted_for_now_watch_closely",
                    "reason": (
                        "GC 03:15 is near the upper end of the intended slope band, but it still satisfies the sibling-branch structure cleanly. "
                        "Keep it in research for now and watch whether third-bar continuation entries degrade on a larger sample before hard-filtering it."
                    ),
                }
                if gc_0315 is not None and gc_0315["sibling_branch_passed"]
                else {
                    "current_status": "not_applicable",
                    "reason": "GC 03:15 was not admitted by the sibling branch.",
                }
            ),
        },
        "recommendation": {
            "path": "create_sibling_branch",
            "rationale": (
                "The inspected move is not a near-miss of the original asiaEarlyNormalBreakoutRetestHoldTurn family. "
                "It behaves like a London-open acceptance continuation with steeper breakout slope, lighter normalized expansion, "
                "and no full retest-hold."
            ),
        },
    }

    json_path = resolved_output_dir / "gc_mgc_london_open_acceptance_continuation_long.json"
    markdown_path = resolved_output_dir / "gc_mgc_london_open_acceptance_continuation_long.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _load_stored_contexts(*, database_path: Path, symbol: str) -> list[StoredBarContext]:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              b.bar_id,
              b.symbol,
              b.timeframe,
              b.start_ts,
              b.end_ts,
              b.open,
              b.high,
              b.low,
              b.close,
              b.volume,
              f.payload_json
            from bars b
            join features f on f.bar_id = b.bar_id
            where b.symbol = ?
            order by b.end_ts asc
            """,
            (symbol,),
        ).fetchall()
    finally:
        connection.close()

    contexts: list[StoredBarContext] = []
    for row in rows:
        end_ts = datetime.fromisoformat(row["end_ts"])
        start_ts = datetime.fromisoformat(row["start_ts"])
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        bar = Bar(
            bar_id=row["bar_id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            start_ts=start_ts,
            end_ts=end_ts,
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            is_final=True,
            session_asia=label_session_phase(end_ts).startswith("ASIA"),
            session_london=label_session_phase(end_ts).startswith("LONDON"),
            session_us=label_session_phase(end_ts).startswith("US"),
            session_allowed=True,
        )
        contexts.append(
            StoredBarContext(
                bar=bar,
                feature=StoredFeature(
                    atr=_payload_decimal(payload.get("atr")),
                    velocity=_payload_decimal(payload.get("velocity")),
                    bar_range=_payload_decimal(payload.get("bar_range")),
                ),
            )
        )
    return contexts


def _evaluate_symbol(*, contexts: list[StoredBarContext], settings: StrategySettings) -> list[BranchEvaluation]:
    rows: list[BranchEvaluation] = []
    for signal_index in range(2, len(contexts)):
        signal = contexts[signal_index]
        local_signal_dt = signal.bar.end_ts.astimezone(settings.timezone_info)
        session_label = label_session_phase(signal.bar.end_ts)
        if session_label != "LONDON_OPEN":
            continue
        if local_signal_dt.time() not in FIRST_THREE_LONDON_OPEN_BARS:
            continue

        prior = contexts[signal_index - 2]
        breakout = contexts[signal_index - 1]
        breakout_atr = max(breakout.feature.atr, settings.risk_floor)
        breakout_normalized_slope = breakout.feature.velocity / breakout_atr
        breakout_range_expansion_ratio = breakout.feature.bar_range / breakout_atr if breakout_atr > 0 else Decimal("0")
        breakout_level = breakout.bar.high

        baseline_detail = {
            "window_allowed": True,
            "breakout_bar_slope_is_flat": (
                abs(breakout_normalized_slope) <= settings.asia_early_breakout_retest_hold_breakout_abs_slope_max
            ),
            "breakout_bar_expansion_is_normal": (
                breakout_range_expansion_ratio > settings.asia_early_breakout_retest_hold_breakout_min_range_expansion_ratio
                and breakout_range_expansion_ratio < settings.asia_early_breakout_retest_hold_breakout_max_range_expansion_ratio
            ),
            "breakout_breaks_prior_1_high": breakout.bar.high > prior.bar.high and breakout.bar.close >= prior.bar.close,
            "signal_retests_and_holds_breakout_level": signal.bar.low <= breakout_level and signal.bar.close >= breakout_level,
        }
        baseline_current_family_passed = all(
            bool(baseline_detail[key])
            for key in (
                "window_allowed",
                "breakout_bar_slope_is_flat",
                "breakout_bar_expansion_is_normal",
                "breakout_breaks_prior_1_high",
                "signal_retests_and_holds_breakout_level",
            )
        )

        sibling_detail = {
            "window_allowed": True,
            "breakout_breaks_prior_1_high": breakout.bar.high > prior.bar.high,
            "breakout_bar_close_greater_equal_prior_bar_close": breakout.bar.close >= prior.bar.close,
            "breakout_normalized_slope_in_range": Decimal("0.25") <= breakout_normalized_slope <= Decimal("1.10"),
            "breakout_range_expansion_ratio_in_range": Decimal("0.10") <= breakout_range_expansion_ratio <= Decimal("0.50"),
            "signal_bar_low_greater_equal_breakout_high": signal.bar.low >= breakout_level,
            "signal_bar_close_greater_equal_breakout_high_plus_0_35_atr": (
                signal.bar.close >= breakout_level + (Decimal("0.35") * breakout.feature.atr)
            ),
        }
        sibling_branch_passed = all(bool(value) for value in sibling_detail.values())

        rows.append(
            BranchEvaluation(
                signal_timestamp=signal.bar.end_ts.isoformat(),
                symbol=signal.bar.symbol,
                session_label=session_label,
                prior_high=_decimal_text(prior.bar.high),
                breakout_high=_decimal_text(breakout.bar.high),
                breakout_close=_decimal_text(breakout.bar.close),
                signal_low=_decimal_text(signal.bar.low),
                signal_close=_decimal_text(signal.bar.close),
                breakout_normalized_slope=_decimal_text(breakout_normalized_slope),
                breakout_range_expansion_ratio=_decimal_text(breakout_range_expansion_ratio),
                baseline_current_family_passed=baseline_current_family_passed,
                baseline_detail={key: _json_ready(value) for key, value in baseline_detail.items()},
                sibling_branch_passed=sibling_branch_passed,
                sibling_detail={key: _json_ready(value) for key, value in sibling_detail.items()},
            )
        )
    return rows


def _is_first_three_london_open(signal_timestamp: str, settings: StrategySettings) -> bool:
    local_dt = datetime.fromisoformat(signal_timestamp).astimezone(settings.timezone_info)
    return label_session_phase(local_dt) == "LONDON_OPEN" and local_dt.time() in FIRST_THREE_LONDON_OPEN_BARS


def _payload_decimal(value: Any) -> Decimal:
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(str(value["value"]))
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    return value


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC London-Open Acceptance Continuation Long",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Research branch id: `{report['research_branch_id']}`",
        f"- Status: `{report['status']}`",
        "",
        "## Definition",
        "",
    ]
    for key, value in report["definition"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Inspected Move", ""])
    for symbol in ("GC", "MGC"):
        lines.append(f"### {symbol}")
        rows = report["before_after_on_inspected_move"].get(symbol, [])
        if not rows:
            lines.append("- No first-three London-open rows were available.")
            lines.append("")
            continue
        for row in rows:
            lines.append(
                f"- `{row['signal_timestamp']}`: current_family={row['baseline_current_family_passed']} sibling_branch={row['sibling_branch_passed']}"
            )
        lines.append("")
    lines.extend(["## Sample Summary", ""])
    for symbol, payload in report["sample_summary"].items():
        lines.append(
            f"- `{symbol}`: loaded_bars={payload['loaded_bar_count']} current_family_candidates={payload['candidate_count_current_family']} sibling_candidates={payload['candidate_count_sibling_branch']} overlap={payload['direct_overlap_with_existing_gc_mgc_family_candidates']}"
        )
    lines.extend(
        [
            "",
            "## Overlap And Chase Review",
            "",
            f"- Chase verdict: {report['overlap_and_chase_review']['obvious_low_quality_chase_behavior']['verdict']}",
            f"- Chase note: {report['overlap_and_chase_review']['obvious_low_quality_chase_behavior']['note']}",
            f"- GC 03:15 recommendation: {report['overlap_and_chase_review']['gc_0315_filter_recommendation']['current_status']}",
            "",
            "## Recommendation",
            "",
            f"- Path: `{report['recommendation']['path']}`",
            f"- Rationale: {report['recommendation']['rationale']}",
        ]
    )
    return "\n".join(lines)
