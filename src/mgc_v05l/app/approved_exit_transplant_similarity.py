"""Rank approved non-ATP lanes by structural similarity for exit-only transplant review."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path.cwd()
DEFAULT_APPROVED_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_approved_models_snapshot.json"
DEFAULT_HISTORICAL_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_exit_transplant_similarity"


def _camel_to_snake(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", text).lower()


def _is_atp_row(row: dict[str, Any]) -> bool:
    joined = " ".join(
        str(row.get(key) or "")
        for key in ("branch", "lane_id", "source_family", "paper_strategy_class")
    ).lower()
    return "atp_companion" in joined or "active_trend_participation" in joined


def _branch_to_probable_study_id(branch: str) -> str:
    text = str(branch or "").strip()
    if not text or " / " not in text:
        return ""
    left, right = [part.strip() for part in text.split(" / ", 1)]
    if "_" in left and " " not in left:
        family = left
        instrument = right.upper()
        return f"{family}__{instrument}"
    instrument = left.upper()
    family = _camel_to_snake(right)
    if not family:
        return ""
    return f"{instrument.lower()}_{family}__{instrument}"


def _load_historical_trade_counts(historical_playback_dir: Path) -> dict[str, int]:
    trade_counts: dict[str, int] = {}
    for path in historical_playback_dir.glob("historical_playback_*.strategy_study.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        strategy_id = str(payload.get("standalone_strategy_id") or "").strip()
        if not strategy_id:
            continue
        trade_counts[strategy_id] = len((payload.get("summary") or {}).get("closed_trade_breakdown") or [])
    return trade_counts


def _similarity_score(target: dict[str, Any], row: dict[str, Any]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    if str(row.get("source_family") or "") == str(target.get("source_family") or ""):
        score += 5
        reasons.append("same source family")
    if str(row.get("participation_policy") or "") == str(target.get("participation_policy") or ""):
        score += 2
        reasons.append("same participation policy")
    if str(row.get("execution_timeframe") or "") == str(target.get("execution_timeframe") or ""):
        score += 1
        reasons.append("same execution timeframe")
    if tuple(row.get("context_timeframes") or []) == tuple(target.get("context_timeframes") or []):
        score += 1
        reasons.append("same context timeframes")
    if str(row.get("side") or "") == str(target.get("side") or ""):
        score += 1
        reasons.append("same side")
    if str(row.get("session_restriction") or "") == str(target.get("session_restriction") or ""):
        score += 1
        reasons.append("same session restriction")
    return score, reasons


def _review_bucket(*, row: dict[str, Any], score: int, historical_trade_count: int | None) -> str:
    if str(row.get("source_family") or "") == "asiaEarlyNormalBreakoutRetestHoldTurn":
        return "near_clone"
    if historical_trade_count == 0:
        return "wait_for_history"
    if score >= 5 and "breakout" in str(row.get("source_family") or "").lower():
        return "second_tier_breakout"
    return "different_family"


def run_approved_exit_transplant_similarity(
    *,
    target_branch: str,
    approved_snapshot_path: str | Path = DEFAULT_APPROVED_SNAPSHOT_PATH,
    historical_playback_dir: str | Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    report_dir: str | Path = DEFAULT_REPORT_DIR,
) -> dict[str, Path]:
    approved_snapshot = json.loads(Path(approved_snapshot_path).read_text(encoding="utf-8"))
    rows = [
        dict(row)
        for row in list(approved_snapshot.get("rows") or [])
        if not _is_atp_row(dict(row))
    ]
    target = next((row for row in rows if str(row.get("branch") or "") == str(target_branch)), None)
    if target is None:
        raise ValueError(f"Target approved lane not found: {target_branch}")

    trade_counts = _load_historical_trade_counts(Path(historical_playback_dir))
    ranked_rows: list[dict[str, Any]] = []
    for row in rows:
        branch = str(row.get("branch") or "")
        if branch == str(target_branch):
            continue
        probable_study_id = _branch_to_probable_study_id(branch)
        historical_trade_count = trade_counts.get(probable_study_id)
        score, reasons = _similarity_score(target, row)
        ranked_rows.append(
            {
                "score": score,
                "review_bucket": _review_bucket(row=row, score=score, historical_trade_count=historical_trade_count),
                "branch": branch,
                "lane_id": row.get("lane_id"),
                "instrument": row.get("instrument"),
                "source_family": row.get("source_family"),
                "side": row.get("side"),
                "session_restriction": row.get("session_restriction"),
                "participation_policy": row.get("participation_policy"),
                "execution_timeframe": row.get("execution_timeframe"),
                "context_timeframes": row.get("context_timeframes"),
                "probable_study_id": probable_study_id or None,
                "historical_trade_count": historical_trade_count,
                "similarity_reasons": reasons,
            }
        )

    ranked_rows.sort(
        key=lambda row: (
            {"near_clone": 0, "second_tier_breakout": 1, "wait_for_history": 2, "different_family": 3}.get(str(row["review_bucket"]), 9),
            -int(row["score"]),
            str(row["branch"]),
        )
    )
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "target_branch": str(target_branch),
        "target_lane": {
            "branch": target.get("branch"),
            "lane_id": target.get("lane_id"),
            "instrument": target.get("instrument"),
            "source_family": target.get("source_family"),
            "side": target.get("side"),
            "session_restriction": target.get("session_restriction"),
            "participation_policy": target.get("participation_policy"),
            "execution_timeframe": target.get("execution_timeframe"),
            "context_timeframes": target.get("context_timeframes"),
        },
        "ranked_candidates": ranked_rows,
    }

    resolved_report_dir = Path(report_dir)
    resolved_report_dir.mkdir(parents=True, exist_ok=True)
    slug = _branch_to_probable_study_id(target_branch) or _camel_to_snake(target_branch.replace(" / ", "_"))
    json_path = resolved_report_dir / f"{slug}.json"
    markdown_path = resolved_report_dir / f"{slug}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines = [
        "# Approved Exit-Transplant Similarity",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        f"Target: `{target_branch}`",
        "",
        "## Ranked Candidates",
        "",
    ]
    for row in ranked_rows:
        lines.extend(
            [
                f"### `{row['branch']}`",
                "",
                f"- Bucket: `{row['review_bucket']}`",
                f"- Score: `{row['score']}`",
                f"- Instrument: `{row['instrument']}`",
                f"- Family: `{row['source_family']}`",
                f"- Session: `{row['session_restriction']}`",
                f"- Participation: `{row['participation_policy']}`",
                f"- Probable study id: `{row['probable_study_id'] or 'UNKNOWN'}`",
                f"- Historical trade count: `{row['historical_trade_count'] if row['historical_trade_count'] is not None else 'UNKNOWN'}`",
                f"- Similarity reasons: `{', '.join(row['similarity_reasons']) if row['similarity_reasons'] else 'none'}`",
                "",
            ]
        )
    markdown_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
    }
