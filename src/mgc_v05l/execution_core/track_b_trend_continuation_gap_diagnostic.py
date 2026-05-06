"""Research-only Track B trend-continuation gap diagnostic.

This module starts the Trend Continuation Overlay research lane without
registering an executable strategy. It reads the same-day postmortem and emits a
bounded diagnostic artifact describing candidate Track 1 research, the retained
missed-rally fixture, and a non-paper-eligible research scaffold.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable


DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_DIAGNOSTIC_PATH = DEFAULT_OUTPUT_ROOT / "latest_track_b_trend_continuation_gap_diagnostic.json"
DEFAULT_POSTMORTEM_PATH = DEFAULT_OUTPUT_ROOT / "latest_track_b_same_day_postmortem.json"
OVERLAY_ID = "TREND_CONTINUATION_OVERLAY_RESEARCH_V1"
MAX_JSON_BYTES = 3 * 1024 * 1024


@dataclass(frozen=True)
class TrackBTrendContinuationGapDiagnosticResult:
    report_json: Path
    report: dict[str, Any]


def build_track_b_trend_continuation_gap_diagnostic(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    postmortem_path: Path = DEFAULT_POSTMORTEM_PATH,
    now: datetime | None = None,
    write: bool = True,
) -> TrackBTrendContinuationGapDiagnosticResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    output_root = Path(output_root)
    resolved_postmortem = _resolve_repo_path(root, postmortem_path)
    postmortem = _load_json(resolved_postmortem)

    fixture = _build_failure_fixture(postmortem)
    candidates = _candidate_survey(root)
    overlay = _research_overlay_scaffold(postmortem)
    classifications = _classify(postmortem=postmortem, candidates=candidates, overlay=overlay)
    report = {
        "schema_version": "track_b_trend_continuation_gap_diagnostic_v1",
        "generated_at": actual_now.isoformat(),
        "source_tag": "RESEARCH_DIAGNOSTIC_ONLY",
        "postmortem_path": str(resolved_postmortem),
        "candidate_strategy_search": candidates,
        "missed_rally_failure_fixture": fixture,
        "research_overlay_scaffold": overlay,
        "trend_continuation_gap_assessment": _trend_gap_assessment(postmortem, overlay),
        "classifications": classifications,
        "recommended_next_action": _recommended_next_action(candidates, classifications),
        "safety": {
            "broker_commands_invoked": False,
            "paper_proof_cli_invoked": False,
            "submit_cancel_place_order_invoked": False,
            "broker_state_mutated": False,
            "existing_strategy_thresholds_changed": False,
            "existing_track_b_strategies_changed": False,
            "overlay_registered_for_execution": False,
            "overlay_paper_eligible": False,
            "overlay_live_money_eligible": False,
            "live_money_readiness": False,
        },
    }
    path = output_root / DEFAULT_DIAGNOSTIC_PATH.name
    if write:
        output_root.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return TrackBTrendContinuationGapDiagnosticResult(report_json=path, report=report)


def _build_failure_fixture(postmortem: Mapping[str, Any]) -> dict[str, Any]:
    missed = postmortem.get("missed_rally_window_review") if isinstance(postmortem, Mapping) else {}
    trade = _first_mapping(postmortem.get("trade_lifecycle_reconstruction"))
    return {
        "fixture_id": "TRACK_B_MISSED_RALLY_2026_05_06_MGC_MNQ",
        "source": "latest_track_b_same_day_postmortem",
        "expected_classification": "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION",
        "actual_track_b_meaningful_managed_trades": 0,
        "proof_style_lifecycle_trades": 1 if _is_proof_style_trade(trade) else 0,
        "proof_style_trade_excluded_from_meaningful_participation": _is_proof_style_trade(trade),
        "track_b_realized_pnl": _nested(postmortem, "simple_baseline_comparison", "track_b_realized_pnl_today"),
        "missed_opportunity_benchmark": _nested(
            postmortem,
            "simple_baseline_comparison",
            "missed_opportunity_magnitude",
        ),
        "rally_windows": {
            "MGC": _nested(missed, "MGC", "main_rally_window"),
            "MNQ": _nested(missed, "MNQ", "main_rally_window"),
        },
        "near_miss_counts": {
            "MGC": _nested(missed, "MGC", "near_misses"),
            "MNQ": _nested(missed, "MNQ", "near_misses"),
        },
        "use": (
            "Future trend-continuation overlay research should identify this as a major long "
            "trend-participation regime, while still remaining diagnostic-only until validated."
        ),
    }


def _candidate_survey(repo_root: Path) -> list[dict[str, Any]]:
    known = [
        {
            "candidate_name": "ATP_COMPANION_V1_ASIA_US / ATP v1 pullback continuation",
            "source_files": [
                "docs/TREND_PARTICIPATION_ENGINE.md",
                "src/mgc_v05l/research/trend_participation/phase2_continuation.py",
            ],
            "instrument_scope": "MES/MNQ initial research universe",
            "session_scope": "ASIA + US executable benchmark; London diagnostic-only",
            "side": "LONG and SHORT variants",
            "timeframe": "5m structural context plus 1m timing detail",
            "research_evidence": "Documented active trend participation engine with continuation families and canary packaging.",
            "predicate_cleanliness": "Medium: structured research models exist, but Track B would need explicit envelope/state adaptation.",
            "migration_status": "research_only_not_track_b_paper_ready",
            "matches_today_regime": "Partial for MNQ long trend participation; not directly wired to MGC and not execution-envelope safe yet.",
            "reason_not_paper_ready_now": "Uses research engine/canary flow, 1m timing and position assumptions; needs Track B-safe envelope and lifecycle design.",
        },
        {
            "candidate_name": "NDX UP opening-drive same-day long-bias signal extraction",
            "source_files": [
                "docs/us_open_ndx_signal_extraction_note.md",
                "src/mgc_v05l/research/us_open_follow_through/probabilistic_pass5.py",
            ],
            "instrument_scope": "NQ/MNQ",
            "session_scope": "US open, decision around 10:00 ET",
            "side": "LONG bias on UP opening-drive days",
            "timeframe": "Opening-drive observation with same-day outcome profiling",
            "research_evidence": "Holdout note reports UP_OPEN avg close +13.374, win rate 61.05%, Q5_UP_OPEN avg close +24.772.",
            "predicate_cleanliness": "Low-to-medium: signal extraction exists, but docs explicitly say not a trading rule.",
            "migration_status": "research_only_retained_signal_candidate",
            "matches_today_regime": "Strong conceptual match for MNQ/NQ long rally day, but not execution-ready.",
            "reason_not_paper_ready_now": "No execution design, risk management, or Track B envelope semantics yet.",
        },
        {
            "candidate_name": "ES/MES opening-drive continuation research",
            "source_files": ["src/mgc_v05l/app/es_mes_opening_drive_continuation_research.py"],
            "instrument_scope": "ES/MES",
            "session_scope": "US cash open",
            "side": "LONG",
            "timeframe": "1m source, 5m decision",
            "research_evidence": "Research app with v1/v2/v3 variants, setup/entry/exit assumptions and cost modeling.",
            "predicate_cleanliness": "Medium for ES/MES; not directly MNQ/MGC.",
            "migration_status": "research_only_wrong_instrument_for_today_primary_gap",
            "matches_today_regime": "Indirect index-rally match; not directly MNQ/NQ or MGC.",
            "reason_not_paper_ready_now": "Instrument scope does not cover MNQ/MGC and would need separate validation.",
        },
        {
            "candidate_name": "MGC impulse burst continuation research",
            "source_files": ["src/mgc_v05l/app/mgc_impulse_burst_continuation_research.py"],
            "instrument_scope": "MGC",
            "session_scope": "Full overlap of available 1m and 5m MGC history",
            "side": "LONG/SHORT direction from impulse continuation variants",
            "timeframe": "1m detection, 5m context",
            "research_evidence": "Research-only first pass tests direct impulse and shallow-pullback continuation variants.",
            "predicate_cleanliness": "Medium: explicit candidate specs exist, but still broad research-only path.",
            "migration_status": "research_only_not_track_b_paper_ready",
            "matches_today_regime": "Potentially relevant to MGC impulse/rally behavior.",
            "reason_not_paper_ready_now": "Not promoted; needs replay evidence review and Track B-safe envelope semantics.",
        },
        {
            "candidate_name": "GC/MGC London-open acceptance continuation long",
            "source_files": ["src/mgc_v05l/app/gc_mgc_london_open_acceptance_continuation_research.py"],
            "instrument_scope": "GC/MGC",
            "session_scope": "First three completed London-open 5m bars",
            "side": "LONG",
            "timeframe": "5m",
            "research_evidence": "Research scaffold for sibling branch with explicit acceptance-continuation predicates.",
            "predicate_cleanliness": "Medium-high for narrow London-open branch.",
            "migration_status": "research_scaffold_only",
            "matches_today_regime": "Partial for early MGC rally, but window is narrow and not full-day trend continuation.",
            "reason_not_paper_ready_now": "Research scaffold only; not validated as Track B PAPER strategy.",
        },
    ]
    surveyed: list[dict[str, Any]] = []
    for candidate in known:
        existing = [path for path in candidate["source_files"] if (repo_root / path).exists()]
        payload = dict(candidate)
        payload["source_files_found"] = existing
        payload["source_files_missing"] = [path for path in candidate["source_files"] if path not in existing]
        payload["track_b_adaptation_feasibility"] = "possible_after_replay_evidence" if existing else "not_available"
        payload["paper_migration_ready"] = False
        payload["paper_eligible"] = False
        payload["live_money_eligible"] = False
        surveyed.append(payload)
    return surveyed


def _research_overlay_scaffold(postmortem: Mapping[str, Any]) -> dict[str, Any]:
    missed = postmortem.get("missed_rally_window_review") if isinstance(postmortem, Mapping) else {}
    instruments: list[dict[str, Any]] = []
    for instrument in ("MGC", "MNQ"):
        review = missed.get(instrument) if isinstance(missed, Mapping) else {}
        window = review.get("main_rally_window") if isinstance(review, Mapping) else {}
        point_change = _decimal_or_none(_nested(window, "point_change"))
        detected = point_change is not None and point_change > (Decimal("20") if instrument == "MGC" else Decimal("100"))
        instruments.append(
            {
                "instrument": instrument,
                "trend_regime_detected": bool(detected),
                "direction": "LONG" if detected else None,
                "session": _session_label_from_window(window),
                "window": window,
                "expansion_magnitude": str(point_change) if point_change is not None else None,
                "slope_ema_vwap_alignment": "UNAVAILABLE_IN_POSTMORTEM",
                "pullback_depth_or_continuation_structure": "UNAVAILABLE_IN_POSTMORTEM",
                "latest_decision_bar_source": "DIAGNOSTIC_POSTMORTEM_REPLAY_NOT_EXECUTION_LIVE",
                "current_enabled_strategies_covered_opportunity": bool(
                    review.get("long_side_trend_continuation_coverage_exists")
                )
                if isinstance(review, Mapping)
                else False,
                "future_research_candidate": bool(detected),
            }
        )
    return {
        "strategy_id": OVERLAY_ID,
        "research_only": True,
        "paper_eligible": False,
        "live_money_eligible": False,
        "registered_in_track_b_strategy_registry": False,
        "can_submit": False,
        "can_handoff_to_paper_lifecycle": False,
        "purpose": (
            "Detect major directional trend-participation regimes in MGC and MNQ without "
            "interfering with existing Track B strategies."
        ),
        "diagnostic_outputs": instruments,
    }


def _trend_gap_assessment(postmortem: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    instruments = [
        item["instrument"]
        for item in overlay.get("diagnostic_outputs", [])
        if item.get("trend_regime_detected") and not item.get("current_enabled_strategies_covered_opportunity")
    ]
    return {
        "today_contained_long_trend_continuation_regime": bool(instruments),
        "instruments": instruments,
        "current_track_b_strategies_directionally_aligned": False if instruments else None,
        "existing_strategy_family_attempted_coverage": _existing_family_attempts(postmortem),
        "failure_reason": (
            "no_trend_continuation_strategy_enabled"
            if instruments
            else "diagnostic_inconclusive_or_no_major_regime_detected"
        ),
    }


def _existing_family_attempts(postmortem: Mapping[str, Any]) -> dict[str, Any]:
    missed = postmortem.get("missed_rally_window_review") if isinstance(postmortem, Mapping) else {}
    attempts: dict[str, Any] = {}
    for instrument, row in (missed or {}).items():
        if not isinstance(row, Mapping):
            continue
        attempts[instrument] = {
            "near_misses": row.get("near_misses"),
            "top_failed_predicates": row.get("top_failed_predicates", [])[:8],
            "dominant_blocker": row.get("dominant_blocker"),
            "long_side_trend_continuation_coverage_exists": row.get("long_side_trend_continuation_coverage_exists"),
        }
    return attempts


def _classify(
    *,
    postmortem: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    overlay: Mapping[str, Any],
) -> list[str]:
    classifications: list[str] = []
    outputs = overlay.get("diagnostic_outputs", [])
    if any(item.get("trend_regime_detected") and not item.get("current_enabled_strategies_covered_opportunity") for item in outputs):
        classifications.append("TREND_CONTINUATION_COVERAGE_GAP")
    near_misses = [
        _nested(row, "near_misses", "one_predicate_away") or 0
        for row in (postmortem.get("missed_rally_window_review") or {}).values()
        if isinstance(row, Mapping)
    ]
    near_misses += [
        _nested(row, "near_misses", "two_predicates_away") or 0
        for row in (postmortem.get("missed_rally_window_review") or {}).values()
        if isinstance(row, Mapping)
    ]
    if any(int(item) > 0 for item in near_misses):
        classifications.append("EXISTING_STRATEGY_NEAR_MISS")
    if any(candidate.get("source_files_found") for candidate in candidates):
        classifications.append("RESEARCH_CANDIDATE_FOUND_IN_TRACK_1")
    if "OPERATIONAL_READINESS_MISSED_RALLY" in (postmortem.get("classifications") or []):
        classifications.append("OPERATIONAL_READINESS_MISSED_REGIME")
    if not classifications:
        classifications.append("DIAGNOSTIC_INCONCLUSIVE")
    return classifications


def _recommended_next_action(candidates: Sequence[Mapping[str, Any]], classifications: Sequence[str]) -> list[str]:
    actions = []
    if "RESEARCH_CANDIDATE_FOUND_IN_TRACK_1" in classifications:
        actions.append("Review existing ATP/NDX-open/MGC-continuation research before inventing a new executable strategy.")
    if "TREND_CONTINUATION_COVERAGE_GAP" in classifications:
        actions.append("Use the 2026-05-06 fixture as a mandatory failure case for any future overlay replay.")
    actions.append("Keep overlay research_only=true and paper_eligible=false until replay evidence and Track B envelope semantics exist.")
    return actions


def _is_proof_style_trade(trade: Mapping[str, Any]) -> bool:
    return bool(
        trade
        and trade.get("closed_by_guarded_paper_proof_lifecycle") is True
        and trade.get("closed_by_normal_strategy_logic") is False
    )


def _session_label_from_window(window: Mapping[str, Any]) -> str | None:
    start = str(window.get("start_timestamp") or "")
    if "T13:" in start or "T14:" in start or "T15:" in start:
        return "US"
    if "T07:" in start or "T08:" in start or "T09:" in start or "T10:" in start:
        return "LONDON_EUROPE"
    if start:
        return "GLOBEX_OR_MULTI_SESSION"
    return None


def _load_json(path: Path) -> Any:
    if not path.exists() or path.stat().st_size > MAX_JSON_BYTES:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _resolve_repo_path(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _first_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, list) and value and isinstance(value[0], Mapping):
        return value[0]
    return {}


def _nested(payload: Any, *keys: str) -> Any:
    current = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
