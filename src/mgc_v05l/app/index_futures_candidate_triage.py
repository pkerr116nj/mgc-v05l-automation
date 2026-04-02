"""Compact index-futures executable candidate triage for MNQ and MES."""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path("/Users/patrick/Documents/MGC-v05l-automation")
REPORT_DIR = ROOT / "outputs/reports/approved_branch_research"
PORTABILITY_AUDIT_MD = REPORT_DIR / "approved_branch_futures_portability_audit.md"
IMPULSE_SAME_BAR_PATH = REPORT_DIR / "mgc_impulse_same_bar_causalization.json"
IMPULSE_DELAYED_PATH = REPORT_DIR / "mgc_impulse_delayed_confirmation_revalidation.json"

TARGETS: dict[str, list[str]] = {
    "MNQ": ["usDerivativeBearTurn", "firstBearSnapTurn", "firstBullSnapTurn"],
    "MES": ["firstBullSnapTurn", "firstBearSnapTurn", "usLatePauseResumeLongTurn"],
}

DIRECT_INDEX_ARTIFACTS: dict[tuple[str, str], dict[str, Path]] = {
    (
        "MNQ",
        "usLatePauseResumeLongTurn",
    ): {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_futures_approved_mnq_20260319_121156.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_futures_approved_mnq_20260319_121156.trade_ledger.csv",
    },
    (
        "MES",
        "usLatePauseResumeLongTurn",
    ): {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_futures_approved_mes_20260319_120816.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_futures_approved_mes_20260319_120816.trade_ledger.csv",
    },
}

MGC_REFERENCES: dict[str, dict[str, Path]] = {
    "usLatePauseResumeLongTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
    },
    "usDerivativeBearTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.trade_ledger.csv",
    },
    "firstBullSnapTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
    },
    "firstBearSnapTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
    },
}

FAMILY_NOTES: dict[str, dict[str, Any]] = {
    "usDerivativeBearTurn": {
        "bias": "SHORT_BIASED",
        "future_confirmation": False,
        "fit_regular_us_hours": True,
        "observable_live_window_note": "Focused in US_PREOPEN_OPENING, US_CASH_OPEN_IMPULSE, and US_OPEN_LATE; some replay artifacts also show later follow-on trades.",
        "likely_cadence": "LOW_TO_MEDIUM",
        "structural_note": "Compact derivative short-turn family with explicit slope/curvature/VWAP/EMA gating and no lookahead dependency.",
    },
    "firstBearSnapTurn": {
        "bias": "SHORT_BIASED",
        "future_confirmation": False,
        "fit_regular_us_hours": True,
        "observable_live_window_note": "Observable in US windows, but broad and mixed with Asia traffic rather than a dedicated index-hours lane.",
        "likely_cadence": "MEDIUM",
        "structural_note": "Causal snap-reversal family, but broader and noisier than the derivative-bear stack.",
    },
    "firstBullSnapTurn": {
        "bias": "LONG_BIASED",
        "future_confirmation": False,
        "fit_regular_us_hours": False,
        "observable_live_window_note": "Mostly Asia-driven in existing MGC evidence; weak fit for regular US-hours observation.",
        "likely_cadence": "MEDIUM",
        "structural_note": "Causal baseline snap family, but not a clean US-hours index expansion target.",
    },
    "usLatePauseResumeLongTurn": {
        "bias": "LONG_BIASED",
        "future_confirmation": False,
        "fit_regular_us_hours": True,
        "observable_live_window_note": "Direct US_LATE family with clear live observation window and existing portability evidence.",
        "likely_cadence": "MEDIUM",
        "structural_note": "Causal pause-pullback-resume long family with explicit US_LATE fit; good operational shape but index portability may degrade.",
    },
}


@dataclass(frozen=True)
class PairMetrics:
    sample_start: str | None
    sample_end: str | None
    trades: int | None
    realized_pnl: float | None
    avg_trade: float | None
    median_trade: float | None
    profit_factor: float | None
    max_drawdown: float | None
    win_rate: float | None
    top_1_contribution: float | None
    top_3_contribution: float | None
    survives_without_top_1: bool | None
    survives_without_top_3: bool | None


def build_and_write_index_futures_candidate_triage() -> dict[str, str]:
    impulse_reference = _load_impulse_reference()
    portability_notes = _load_portability_audit_notes()
    mgc_reference_metrics = {family: _load_metrics_from_artifact(*_artifact_pair(family)) for family in MGC_REFERENCES}

    pair_rows: list[dict[str, Any]] = []
    for instrument, families in TARGETS.items():
        for family in families:
            pair_rows.append(
                _build_pair_row(
                    instrument=instrument,
                    family=family,
                    impulse_reference=impulse_reference,
                    portability_notes=portability_notes,
                    mgc_reference_metrics=mgc_reference_metrics,
                )
            )

    ranking = _build_ranked_recommendation(pair_rows)
    payload = {
        "thread_scope": "thread_1_only",
        "research_scope": "research_only",
        "instrument_family_pairs_reviewed": [(instrument, family) for instrument, families in TARGETS.items() for family in families],
        "pair_reviews": pair_rows,
        "comparisons": _build_comparisons(pair_rows),
        "ranked_recommendation": ranking,
        "impulse_reference": impulse_reference,
    }

    json_path = REPORT_DIR / "index_futures_candidate_triage.json"
    md_path = REPORT_DIR / "index_futures_candidate_triage.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "index_futures_candidate_triage_json": str(json_path),
        "index_futures_candidate_triage_md": str(md_path),
    }


def _build_pair_row(
    *,
    instrument: str,
    family: str,
    impulse_reference: dict[str, Any],
    portability_notes: dict[tuple[str, str], dict[str, Any]],
    mgc_reference_metrics: dict[str, PairMetrics],
) -> dict[str, Any]:
    artifact = DIRECT_INDEX_ARTIFACTS.get((instrument, family))
    direct_metrics = _load_metrics_from_artifact(artifact["summary"], artifact["ledger"], family) if artifact else PairMetrics(
        sample_start=None,
        sample_end=None,
        trades=None,
        realized_pnl=None,
        avg_trade=None,
        median_trade=None,
        profit_factor=None,
        max_drawdown=None,
        win_rate=None,
        top_1_contribution=None,
        top_3_contribution=None,
        survives_without_top_1=None,
        survives_without_top_3=None,
    )
    session_counts = _load_session_counts(artifact["ledger"], family) if artifact else {}
    mgc_reference = mgc_reference_metrics[family]
    note = FAMILY_NOTES[family]
    portability = portability_notes.get((instrument, family))
    evidence_status = "DIRECT_INDEX_REPLAY_AVAILABLE" if artifact else "NO_DIRECT_INDEX_REPLAY_ARTIFACT"

    cleaner_on_instrument = _cleaner_on_instrument(direct_metrics, mgc_reference)
    more_promising_than_impulse = _more_promising_than_impulse(direct_metrics, note, impulse_reference, evidence_status)

    return {
        "instrument": instrument,
        "family": family,
        "evidence_status": evidence_status,
        "structural_execution_fit": {
            "naturally_causal_executable": True,
            "relies_on_future_confirmation": note["future_confirmation"],
            "fits_regular_us_hours_behavior": note["fit_regular_us_hours"],
            "likely_observable_activity_in_live_windows": note["likely_cadence"] != "LOW",
        },
        "replay_research_quality": asdict(direct_metrics),
        "operational_suitability": {
            "primary_session_windows_et": _session_windows(session_counts, family),
            "likely_cadence_or_sparsity": note["likely_cadence"] if not artifact else _cadence_label(direct_metrics.trades),
            "bias": note["bias"],
            "cleaner_on_this_instrument_than_on_mgc": cleaner_on_instrument,
            "more_promising_than_parked_impulse_branch": more_promising_than_impulse,
            "structural_note": note["structural_note"],
            "observable_live_window_note": note["observable_live_window_note"],
        },
        "verdict_bucket": _verdict_bucket(
            instrument=instrument,
            family=family,
            evidence_status=evidence_status,
            metrics=direct_metrics,
            cleaner_on_instrument=cleaner_on_instrument,
        ),
        "artifact_sources": {
            "direct_summary": str(artifact["summary"]) if artifact else None,
            "direct_trade_ledger": str(artifact["ledger"]) if artifact else None,
            "mgc_reference_summary": str(MGC_REFERENCES[family]["summary"]),
            "mgc_reference_trade_ledger": str(MGC_REFERENCES[family]["ledger"]),
            "portability_audit_note": portability,
        },
    }


def _artifact_pair(family: str) -> tuple[Path, Path, str]:
    return MGC_REFERENCES[family]["summary"], MGC_REFERENCES[family]["ledger"], family


def _load_metrics_from_artifact(summary_path: Path, ledger_path: Path, family: str) -> PairMetrics:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    with ledger_path.open(encoding="utf-8", newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row["setup_family"] == family]
    pnls = [float(row["net_pnl"]) for row in rows]
    total = sum(pnls)
    wins = [value for value in pnls if value > 0]
    losses = [-value for value in pnls if value < 0]
    profit_factor = None if not losses else sum(wins) / sum(losses)
    top = sorted(pnls, reverse=True)
    return PairMetrics(
        sample_start=summary.get("slice_start_ts") or summary.get("source_first_bar_ts"),
        sample_end=summary.get("slice_end_ts") or summary.get("source_last_bar_ts"),
        trades=len(rows),
        realized_pnl=round(total, 4),
        avg_trade=round(total / len(rows), 4) if rows else None,
        median_trade=round(float(median(pnls)), 4) if rows else None,
        profit_factor=round(profit_factor, 4) if profit_factor is not None else None,
        max_drawdown=round(_max_drawdown(pnls), 4),
        win_rate=round(sum(1 for value in pnls if value > 0) / len(rows), 4) if rows else None,
        top_1_contribution=round((top[0] / total) * 100, 4) if total else None,
        top_3_contribution=round((sum(top[:3]) / total) * 100, 4) if total else None,
        survives_without_top_1=((total - top[0]) > 0) if top else None,
        survives_without_top_3=((total - sum(top[:3])) > 0) if top else None,
    )


def _load_session_counts(ledger_path: Path, family: str) -> dict[str, int]:
    with ledger_path.open(encoding="utf-8", newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row["setup_family"] == family]
    counter = Counter()
    for row in rows:
        phase = row.get("entry_session_phase") or "UNCLASSIFIED"
        counter[phase] += 1
    return dict(counter)


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _session_windows(session_counts: dict[str, int], family: str) -> list[str]:
    if session_counts:
        windows = {
            "US_PREOPEN_OPENING": "09:00-09:30 ET",
            "US_CASH_OPEN_IMPULSE": "09:30-10:00 ET",
            "US_OPEN_LATE": "10:00-10:30 ET",
            "US_MIDDAY": "10:30-14:00 ET",
            "US_LATE": "14:00-17:00 ET",
            "ASIA_EARLY": "18:00-20:30 ET",
            "ASIA_LATE": "20:30-23:00 ET",
            "UNCLASSIFIED": "Outside labeled research pockets",
            "SESSION_RESET_1800": "18:00 ET reset bar",
        }
        return [f"{phase}: {windows.get(phase, phase)}" for phase, _ in Counter(session_counts).most_common()]
    if family == "usDerivativeBearTurn":
        return ["US_PREOPEN_OPENING: 09:00-09:30 ET", "US_CASH_OPEN_IMPULSE: 09:30-10:00 ET", "US_OPEN_LATE: 10:00-10:30 ET"]
    if family == "usLatePauseResumeLongTurn":
        return ["US_LATE: 14:00-17:00 ET"]
    if family == "firstBearSnapTurn":
        return ["Mixed ASIA and US pockets; not a dedicated US-hours lane"]
    return ["Mostly ASIA_EARLY / ASIA_LATE in home-lane evidence"]


def _cadence_label(trades: int | None) -> str:
    if trades is None:
        return "UNKNOWN"
    if trades >= 20:
        return "MEDIUM"
    if trades >= 8:
        return "LOW_TO_MEDIUM"
    return "LOW"


def _cleaner_on_instrument(direct: PairMetrics, mgc_reference: PairMetrics) -> str:
    if direct.trades is None:
        return "UNKNOWN_NO_DIRECT_INDEX_ARTIFACT"
    if not direct.profit_factor or not mgc_reference.profit_factor:
        return "NO"
    if (
        direct.profit_factor > mgc_reference.profit_factor
        and (direct.median_trade or 0) >= (mgc_reference.median_trade or 0)
        and (direct.top_3_contribution or 10_000) <= (mgc_reference.top_3_contribution or 10_000)
    ):
        return "YES"
    return "NO"


def _more_promising_than_impulse(
    direct: PairMetrics,
    note: dict[str, Any],
    impulse_reference: dict[str, Any],
    evidence_status: str,
) -> str:
    if evidence_status == "NO_DIRECT_INDEX_REPLAY_ARTIFACT":
        if not note["fit_regular_us_hours"]:
            return "NO"
        return "STRUCTURALLY_MORE_EXECUTABLE_BUT_UNPROVEN"
    impulse = impulse_reference["failed_same_bar_metrics"]
    if (
        direct.profit_factor
        and direct.profit_factor > impulse["profit_factor"]
        and (direct.median_trade or -10_000) > impulse["median_trade"]
    ):
        return "YES"
    if note["fit_regular_us_hours"]:
        return "MORE_EXECUTABLE_BUT_ECONOMICALLY_UNPROVEN"
    return "NO"


def _verdict_bucket(
    *,
    instrument: str,
    family: str,
    evidence_status: str,
    metrics: PairMetrics,
    cleaner_on_instrument: str,
) -> str:
    if evidence_status == "DIRECT_INDEX_REPLAY_AVAILABLE":
        if metrics.realized_pnl is not None and metrics.realized_pnl > 0 and (metrics.profit_factor or 0) > 1.2:
            return "SERIOUS_NEXT_CANDIDATE"
        if family == "usLatePauseResumeLongTurn":
            return "DEPRIORITIZE"
        return "LATER_REVIEW"
    if instrument == "MNQ" and family == "usDerivativeBearTurn":
        return "SERIOUS_NEXT_CANDIDATE"
    if instrument == "MES" and family == "firstBearSnapTurn":
        return "LATER_REVIEW"
    if family == "firstBullSnapTurn":
        return "DEPRIORITIZE"
    return "LATER_REVIEW"


def _load_impulse_reference() -> dict[str, Any]:
    same_bar = json.loads(IMPULSE_SAME_BAR_PATH.read_text(encoding="utf-8"))
    delayed = json.loads(IMPULSE_DELAYED_PATH.read_text(encoding="utf-8"))
    return {
        "family": "impulse_burst_continuation",
        "failed_same_bar_bucket": same_bar["causalization_conclusion"]["best_bucket"],
        "failed_same_bar_metrics": same_bar["raw_control_metrics"],
        "failed_delayed_bucket": delayed["causal_revalidation_conclusion"]["decision_bucket"],
    }


def _load_portability_audit_notes() -> dict[tuple[str, str], dict[str, Any]]:
    notes: dict[tuple[str, str], dict[str, Any]] = {}
    current_symbol: str | None = None
    for raw_line in PORTABILITY_AUDIT_MD.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("### "):
            current_symbol = line.removeprefix("### ").strip()
            continue
        if current_symbol in {"MNQ", "MES"} and line.startswith("- "):
            family, rest = line[2:].split(":", 1)
            notes[(current_symbol, family.strip())] = {"summary_line": line}
    return notes


def _build_comparisons(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_pair = {(row["instrument"], row["family"]): row for row in rows}
    return {
        "MNQ": {
            "ranking": ["usDerivativeBearTurn", "firstBearSnapTurn", "firstBullSnapTurn"],
            "note": (
                "MNQ usDerivativeBearTurn is the strongest structural candidate even without a dedicated index replay artifact; "
                "the snap families remain broader and less US-hours-specific."
            ),
            "pairs": [by_pair[("MNQ", family)]["verdict_bucket"] for family in TARGETS["MNQ"]],
        },
        "MES": {
            "ranking": ["firstBearSnapTurn", "usLatePauseResumeLongTurn", "firstBullSnapTurn"],
            "note": (
                "MES usLatePauseResumeLongTurn has real portability evidence and it is weak; "
                "if MES gets any further attention, firstBearSnapTurn is the only listed pair worth keeping alive as a baseline research lead."
            ),
            "pairs": [by_pair[("MES", family)]["verdict_bucket"] for family in TARGETS["MES"]],
        },
        "cross_note": (
            "MNQ looks like the better first index-futures expansion lane overall, but the evidence is still weak because the best-looking MNQ branch "
            "does not yet have a dedicated replay packet on this instrument."
        ),
    }


def _build_ranked_recommendation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "mnq_next_active_research_focus": "usDerivativeBearTurn",
        "mes_next_active_research_focus": "firstBearSnapTurn",
        "single_strongest_overall_index_candidate": "MNQ / usDerivativeBearTurn",
        "most_likely_next_paper_candidate_after_current_mgc_pl_gc_work": "MNQ / usDerivativeBearTurn",
        "do_not_spend_time_now": ["MNQ / firstBullSnapTurn", "MES / firstBullSnapTurn", "MES / usLatePauseResumeLongTurn"],
        "better_first_index_futures_expansion_lane": "MNQ",
        "lane_note": (
            "MNQ is the better first index-futures lane because MES already shows weak direct portability on the listed admitted family, "
            "while MNQ still has one structurally strong unexplored short branch worth a dedicated replay before expansion is dismissed."
        ),
        "evidence_strength_note": (
            "Evidence is still thin overall. The top MNQ pick is structurally strong but not yet backed by a dedicated instrument-specific replay artifact."
        ),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Index Futures Candidate Triage",
        "",
        "## Ranked Recommendation",
        f"- MNQ next active focus: `{payload['ranked_recommendation']['mnq_next_active_research_focus']}`",
        f"- MES next active focus: `{payload['ranked_recommendation']['mes_next_active_research_focus']}`",
        f"- Strongest overall index-futures candidate: `{payload['ranked_recommendation']['single_strongest_overall_index_candidate']}`",
        f"- Most likely next paper candidate after current MGC/PL/GC work: `{payload['ranked_recommendation']['most_likely_next_paper_candidate_after_current_mgc_pl_gc_work']}`",
        f"- Do not spend time on right now: {', '.join(f'`{item}`' for item in payload['ranked_recommendation']['do_not_spend_time_now'])}",
        f"- Better first index-futures lane: `{payload['ranked_recommendation']['better_first_index_futures_expansion_lane']}`",
        f"- Evidence note: {payload['ranked_recommendation']['evidence_strength_note']}",
        "",
        "## Pair Reviews",
    ]
    for row in payload["pair_reviews"]:
        metrics = row["replay_research_quality"]
        ops = row["operational_suitability"]
        fit = row["structural_execution_fit"]
        lines.extend(
            [
                f"### {row['instrument']} / {row['family']}",
                f"- Verdict: `{row['verdict_bucket']}`",
                f"- Evidence status: `{row['evidence_status']}`",
                f"- Sample: `{metrics['sample_start']}` -> `{metrics['sample_end']}`",
                (
                    f"- Trades `{metrics['trades']}`, realized P/L `{metrics['realized_pnl']}`, avg trade `{metrics['avg_trade']}`, "
                    f"median trade `{metrics['median_trade']}`, PF `{metrics['profit_factor']}`, max DD `{metrics['max_drawdown']}`, "
                    f"win rate `{metrics['win_rate']}`"
                ),
                (
                    f"- Concentration: top-1 `{metrics['top_1_contribution']}`, top-3 `{metrics['top_3_contribution']}`, "
                    f"survives ex top-1 `{metrics['survives_without_top_1']}`, survives ex top-3 `{metrics['survives_without_top_3']}`"
                ),
                f"- Structural fit: causal `{fit['naturally_causal_executable']}`, future confirmation `{fit['relies_on_future_confirmation']}`, regular US-hours fit `{fit['fits_regular_us_hours_behavior']}`",
                f"- Sessions: {', '.join(ops['primary_session_windows_et'])}",
                f"- Cadence: `{ops['likely_cadence_or_sparsity']}`",
                f"- Bias: `{ops['bias']}`",
                f"- Cleaner than MGC: `{ops['cleaner_on_this_instrument_than_on_mgc']}`",
                f"- More promising than parked impulse branch: `{ops['more_promising_than_parked_impulse_branch']}`",
                f"- Note: {ops['structural_note']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Cross-Instrument Note",
            payload["comparisons"]["cross_note"],
            "",
            "## Impulse Reference",
            (
                f"- Parked impulse branch remains: same-bar `{payload['impulse_reference']['failed_same_bar_bucket']}`, "
                f"delayed `{payload['impulse_reference']['failed_delayed_bucket']}`"
            ),
        ]
    )
    return "\n".join(lines) + "\n"
