"""Explain the winning diagnostic-only GRE shadow gate policy."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_historical_backfill import BACKFILL_ROWS_JSONL
from mgc_v05l.execution_core.track_b_research_feature_dataset import RESEARCH_FEATURE_DATASET_DIR, RESEARCH_FEATURE_DATASET_JSONL


SCHEMA_VERSION = "track_b_gre_shadow_gate_explanation_v1"
WINNING_POLICY_ID = "avwap_confirmation_globex_session_open_18et"
WINNING_ANCHOR = "globex_session_open_18et"
EXPLANATION_JSON = "gre_shadow_gate_explanation.json"
EXPLANATION_MD = "gre_shadow_gate_explanation.md"
ARCHETYPES_MD = "gre_shadow_gate_archetypes.md"
ACCEPTED_REJECTED_MD = "gre_shadow_gate_accepted_vs_rejected.md"
MISSED_AVOIDED_MD = "gre_shadow_gate_missed_winners_avoided_losers.md"
ROBUSTNESS_MD = "gre_shadow_gate_robustness_notes.md"
HORIZONS = ("5m", "15m", "30m", "60m")
CONFIDENCE_BANDS = (("0-20", 0, 20), ("20-40", 20, 40), ("40-60", 40, 60), ("60-80", 60, 80), ("80-100", 80, 101))
AVWAP_ANCHORS = ("globex_session_open_18et", "london_open", "us_rth_open")
LOW_SAMPLE_THRESHOLD = 30


@dataclass(frozen=True)
class ShadowGateExplanationResult:
    report: dict[str, Any]
    json_path: Path
    markdown_path: Path
    archetypes_path: Path
    accepted_vs_rejected_path: Path
    missed_avoided_path: Path
    robustness_path: Path


def run_gre_shadow_gate_explainer(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    rows_path: Path | None = None,
    crfd_rows_path: Path | None = None,
    max_snapshot_bytes: int | None = None,
) -> ShadowGateExplanationResult:
    generated_at = _coerce_now(now)
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    source_rows = rows_path or gold_dir / BACKFILL_ROWS_JSONL
    source_crfd = crfd_rows_path or output_root / RESEARCH_FEATURE_DATASET_DIR / RESEARCH_FEATURE_DATASET_JSONL
    report = build_gre_shadow_gate_explanation(
        _read_jsonl(source_rows),
        crfd_rows=_read_jsonl(source_crfd),
        generated_at=generated_at,
        rows_path=source_rows,
        crfd_rows_path=source_crfd,
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = gold_dir / EXPLANATION_JSON
    write_bounded_snapshot_json(json_path, report, config=config)
    markdown_path = gold_dir / EXPLANATION_MD
    markdown_path.write_text(render_explanation_markdown(report), encoding="utf-8")
    archetypes_path = gold_dir / ARCHETYPES_MD
    archetypes_path.write_text(render_archetypes_markdown(report), encoding="utf-8")
    accepted_path = gold_dir / ACCEPTED_REJECTED_MD
    accepted_path.write_text(render_accepted_rejected_markdown(report), encoding="utf-8")
    missed_path = gold_dir / MISSED_AVOIDED_MD
    missed_path.write_text(render_missed_avoided_markdown(report), encoding="utf-8")
    robustness_path = gold_dir / ROBUSTNESS_MD
    robustness_path.write_text(render_robustness_markdown(report), encoding="utf-8")
    return ShadowGateExplanationResult(report, json_path, markdown_path, archetypes_path, accepted_path, missed_path, robustness_path)


def build_gre_shadow_gate_explanation(
    rows: Sequence[Mapping[str, Any]],
    *,
    crfd_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    rows_path: Path | str,
    crfd_rows_path: Path | str,
) -> dict[str, Any]:
    enriched = [_enrich_row(row, _CrfdIndex(crfd_rows)) for row in rows]
    accepted = [row for row in enriched if winning_policy_accepts(row)]
    rejected = [row for row in enriched if not winning_policy_accepts(row)]
    accepted_winners = [row for row in accepted if (_signed_outcome(row) or 0.0) > 0]
    accepted_losers = [row for row in accepted if (_signed_outcome(row) or 0.0) < 0]
    avoided_losers = [row for row in rejected if (_signed_outcome(row) or 0.0) < 0]
    missed_winners = [row for row in rejected if (_signed_outcome(row) or 0.0) > 0]
    accepted_vs_rejected = {
        "accepted": _metrics(accepted),
        "rejected": _metrics(rejected),
        "accepted_long": _metrics([row for row in accepted if row.get("regime_label") == "LONG"]),
        "rejected_long": _metrics([row for row in rejected if row.get("regime_label") == "LONG"]),
        "accepted_short": _metrics([row for row in accepted if row.get("regime_label") == "SHORT"]),
        "rejected_short": _metrics([row for row in rejected if row.get("regime_label") == "SHORT"]),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "source_rows_path": str(rows_path),
        "crfd_rows_path": str(crfd_rows_path),
        "policy": {
            "policy_id": WINNING_POLICY_ID,
            "anchor": WINNING_ANCHOR,
            "definition": "Accept GRE LONG above Globex-session AVWAP or GRE SHORT below Globex-session AVWAP.",
            "production_gate_recommended": False,
        },
        "overall": {
            "opportunities": len(enriched),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "accepted_winners": len(accepted_winners),
            "accepted_losers": len(accepted_losers),
            "avoided_losers": len(avoided_losers),
            "missed_winners": len(missed_winners),
            "acceptance_rate": round(len(accepted) / len(enriched), 4) if enriched else None,
            "crfd_joined": sum(1 for row in enriched if row.get("crfd_joined") is True),
        },
        "accepted_vs_rejected": accepted_vs_rejected,
        "breakdowns": {
            "accepted": _breakdowns(accepted),
            "rejected": _breakdowns(rejected),
            "avoided_losers": _breakdowns(avoided_losers),
            "missed_winners": _breakdowns(missed_winners),
            "accepted_winners": _breakdowns(accepted_winners),
            "accepted_losers": _breakdowns(accepted_losers),
        },
        "archetypes": _archetypes(accepted=accepted, rejected=rejected, avoided_losers=avoided_losers, missed_winners=missed_winners),
        "case_reviews": {
            "largest_accepted_winners": _case_rows(sorted(accepted_winners, key=lambda row: _signed_outcome(row) or 0.0, reverse=True)[:8]),
            "largest_accepted_losers": _case_rows(sorted(accepted_losers, key=lambda row: _signed_outcome(row) or 0.0)[:8]),
            "largest_avoided_losers": _case_rows(sorted(avoided_losers, key=lambda row: _signed_outcome(row) or 0.0)[:8]),
            "largest_missed_winners": _case_rows(sorted(missed_winners, key=lambda row: _signed_outcome(row) or 0.0, reverse=True)[:8]),
        },
        "robustness": _robustness_notes(enriched, accepted, rejected),
        "recommendations": _recommendations(accepted, avoided_losers, missed_winners),
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
        "production_behavior_changed": False,
        "gre_scoring_changed": False,
    }
    return report


def winning_policy_accepts(row: Mapping[str, Any]) -> bool:
    relation = row.get(f"avwap_relation_{WINNING_ANCHOR}")
    regime = row.get("regime_label")
    return (regime == "LONG" and relation == "above_avwap") or (regime == "SHORT" and relation == "below_avwap")


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
        rows = self._rows.get(str(contract or "").upper(), ())
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate


def _enrich_row(row: Mapping[str, Any], index: _CrfdIndex) -> dict[str, Any]:
    item = dict(row)
    crfd = index.latest_at_or_before(contract=str(row.get("contract") or "GC"), timestamp=_parse_datetime(row.get("gre_generated_at")))
    item["crfd_joined"] = crfd is not None
    item["joined_session"] = (crfd or {}).get("session") or row.get("session") or "UNKNOWN"
    item["joined_vwap_relation"] = (crfd or {}).get("vwap_relation") or "unavailable"
    item["time_bucket_utc"] = _time_bucket(_parse_datetime(row.get("gre_generated_at")))
    for anchor in AVWAP_ANCHORS:
        item[f"avwap_relation_{anchor}"] = (crfd or {}).get(f"avwap_relation_{anchor}") or "unavailable"
        item[f"avwap_distance_{anchor}"] = _number((crfd or {}).get(f"distance_from_avwap_{anchor}_points"))
        item[f"avwap_distance_band_{anchor}"] = _distance_band(item[f"avwap_distance_{anchor}"])
    return item


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    signed = [value for value in (_signed_outcome(row) for row in rows) if value is not None]
    return {
        "count": len(rows),
        "expectancy_proxy": _average(signed),
        "average_forward_return": {horizon: _average(_forward_values(rows, horizon)) for horizon in HORIZONS},
        "average_mfe": _average([value for value in (_number(row.get("mfe")) for row in rows) if value is not None]),
        "average_mae": _average([value for value in (_number(row.get("mae")) for row in rows) if value is not None]),
        "direction_correctness": _direction_correctness(rows),
        "positive_rate_signed": round(sum(1 for value in signed if value > 0) / len(signed), 4) if signed else None,
        "tail_loss_count_signed_le_minus_10": sum(1 for value in signed if value <= -10),
    }


def _breakdowns(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "regime": _count_by(rows, "regime_label"),
        "side": _count_by(rows, "directional_bias"),
        "session": _count_by(rows, "joined_session"),
        "contract": _count_by(rows, "contract"),
        "confidence_band": _count_by_callable(rows, lambda row: _confidence_band(row.get("confidence"))),
        "vwap_relation": _count_by(rows, "joined_vwap_relation"),
        "globex_avwap_relation": _count_by(rows, f"avwap_relation_{WINNING_ANCHOR}"),
        "london_avwap_relation": _count_by(rows, "avwap_relation_london_open"),
        "us_rth_avwap_relation": _count_by(rows, "avwap_relation_us_rth_open"),
        "globex_avwap_distance_band": _count_by(rows, f"avwap_distance_band_{WINNING_ANCHOR}"),
        "time_bucket_utc": _count_by(rows, "time_bucket_utc"),
    }


def _archetypes(
    *,
    accepted: Sequence[Mapping[str, Any]],
    rejected: Sequence[Mapping[str, Any]],
    avoided_losers: Sequence[Mapping[str, Any]],
    missed_winners: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "edge_driver": _edge_driver(accepted),
        "accepted_archetype": _dominant_archetype(accepted),
        "avoided_loser_archetype": _dominant_archetype(avoided_losers),
        "missed_winner_archetype": _dominant_archetype(missed_winners),
        "rejection_character": "Rejected set remains mixed; policy improvement is from quality/selectivity, not eliminating every winner.",
        "tail_loss_reduction": {
            "accepted_tail_losses": _metrics(accepted)["tail_loss_count_signed_le_minus_10"],
            "rejected_tail_losses": _metrics(rejected)["tail_loss_count_signed_le_minus_10"],
        },
    }


def _robustness_notes(
    rows: Sequence[Mapping[str, Any]],
    accepted: Sequence[Mapping[str, Any]],
    rejected: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    sessions = _count_by(accepted, "joined_session")
    contracts = _count_by(accepted, "contract")
    time_buckets = _count_by(accepted, "time_bucket_utc")
    return {
        "selectivity_warning": len(accepted) < 75,
        "sample_size_warning": len(accepted) < 50,
        "accepted_sample": len(accepted),
        "total_sample": len(rows),
        "concentration": {
            "top_session": _top_item(sessions),
            "top_contract": _top_item(contracts),
            "top_time_bucket": _top_item(time_buckets),
        },
        "is_narrow_sample": _is_concentrated(sessions, len(accepted)) or _is_concentrated(time_buckets, len(accepted)),
        "rejected_expectancy_proxy": _metrics(rejected).get("expectancy_proxy"),
        "notes": [
            "Research-only: not a production gate.",
            "Single bounded historical slice; needs out-of-sample weeks.",
            "Policy is selective; live shadow observation should record every accepted/rejected decision without affecting trades.",
        ],
    }


def _recommendations(
    accepted: Sequence[Mapping[str, Any]],
    avoided_losers: Sequence[Mapping[str, Any]],
    missed_winners: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "worth_live_runtime_shadow_observation_later": len(accepted) >= 30,
        "recommended_live_shadow_artifact": "gre_shadow_gate_observations.jsonl",
        "artifact_fields": [
            "generated_at",
            "strategy_id",
            "symbol",
            "intended_side",
            "gre_label",
            "confidence",
            "globex_avwap_relation",
            "policy_decision",
            "diagnostic_only",
        ],
        "production_gating_recommended": False,
        "additional_evidence_required": "Out-of-sample historical weeks, live shadow observations, and trade-aligned replay before any production gate.",
        "next_analysis": "Investigate a less selective variant that keeps Globex AVWAP confirmation but adds conflict-warning mode for London/RTH anchors.",
        "investigate_less_selective_variant": len(missed_winners) > len(avoided_losers) * 0.75,
        "reason": "Policy quality is promising, but missed winners are numerous and selectivity is high.",
    }


def render_explanation_markdown(report: Mapping[str, Any]) -> str:
    overall = report.get("overall", {})
    rec = report.get("recommendations", {})
    return (
        "# GRE Shadow Gate Explanation\n\n"
        f"Policy: `{report.get('policy', {}).get('policy_id')}`\n\n"
        f"Accepted: `{overall.get('accepted')}` / `{overall.get('opportunities')}`\n\n"
        f"Avoided losers: `{overall.get('avoided_losers')}`; missed winners: `{overall.get('missed_winners')}`\n\n"
        f"Worth live shadow observation later: `{rec.get('worth_live_runtime_shadow_observation_later')}`\n\n"
        f"Production gating recommended: `{rec.get('production_gating_recommended')}`\n\n"
        "Diagnostic only: `true`\n"
    )


def render_archetypes_markdown(report: Mapping[str, Any]) -> str:
    return "# GRE Shadow Gate Archetypes\n\n" + json.dumps(report.get("archetypes", {}), indent=2, sort_keys=True)


def render_accepted_rejected_markdown(report: Mapping[str, Any]) -> str:
    return "# GRE Shadow Gate Accepted vs Rejected\n\n" + json.dumps(report.get("accepted_vs_rejected", {}), indent=2, sort_keys=True)


def render_missed_avoided_markdown(report: Mapping[str, Any]) -> str:
    cases = report.get("case_reviews", {})
    text = "# GRE Shadow Gate Missed Winners / Avoided Losers\n\n"
    for name, rows in cases.items():
        text += f"## {name}\n\n"
        for row in rows:
            text += f"- `{row.get('gre_generated_at')}` `{row.get('regime_label')}` `{row.get('contract')}` signed `{row.get('signed_outcome')}` session `{row.get('session')}`\n"
        if not rows:
            text += "- None\n"
        text += "\n"
    return text


def render_robustness_markdown(report: Mapping[str, Any]) -> str:
    return "# GRE Shadow Gate Robustness Notes\n\n" + json.dumps(report.get("robustness", {}), indent=2, sort_keys=True)


def _case_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        forward = row.get("forward_returns") if isinstance(row.get("forward_returns"), Mapping) else {}
        result.append(
            {
                "gre_generated_at": row.get("gre_generated_at"),
                "contract": row.get("contract"),
                "regime_label": row.get("regime_label"),
                "confidence": row.get("confidence"),
                "session": row.get("joined_session"),
                "signed_outcome": _signed_outcome(row),
                "forward_60m": forward.get("60m"),
                "mfe": row.get("mfe"),
                "mae": row.get("mae"),
                "globex_avwap_relation": row.get(f"avwap_relation_{WINNING_ANCHOR}"),
                "globex_distance_band": row.get(f"avwap_distance_band_{WINNING_ANCHOR}"),
            }
        )
    return result


def _signed_outcome(row: Mapping[str, Any]) -> float | None:
    forward = row.get("forward_returns")
    if not isinstance(forward, Mapping):
        return None
    raw = _number(forward.get("60m") if forward.get("60m") is not None else forward.get("30m"))
    if raw is None:
        return None
    if row.get("regime_label") == "SHORT":
        return -raw
    return raw


def _forward_values(rows: Sequence[Mapping[str, Any]], horizon: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        forward = row.get("forward_returns")
        if isinstance(forward, Mapping):
            value = _number(forward.get(horizon))
            if value is not None:
                values.append(value)
    return values


def _direction_correctness(rows: Sequence[Mapping[str, Any]]) -> float | None:
    directional = [row for row in rows if row.get("direction_correctness") in {True, False}]
    if not directional:
        return None
    return round(sum(1 for row in directional if row.get("direction_correctness") is True) / len(directional), 4)


def _dominant_archetype(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(rows),
        "regime": _top_item(_count_by(rows, "regime_label")),
        "session": _top_item(_count_by(rows, "joined_session")),
        "contract": _top_item(_count_by(rows, "contract")),
        "confidence_band": _top_item(_count_by_callable(rows, lambda row: _confidence_band(row.get("confidence")))),
        "globex_distance_band": _top_item(_count_by(rows, f"avwap_distance_band_{WINNING_ANCHOR}")),
    }


def _edge_driver(rows: Sequence[Mapping[str, Any]]) -> str:
    long_metrics = _metrics([row for row in rows if row.get("regime_label") == "LONG"])
    short_metrics = _metrics([row for row in rows if row.get("regime_label") == "SHORT"])
    long_exp = _number(long_metrics.get("expectancy_proxy")) or 0.0
    short_exp = _number(short_metrics.get("expectancy_proxy")) or 0.0
    if long_metrics["count"] and short_metrics["count"] and long_exp > 0 and short_exp > 0:
        return "both_long_and_short"
    if long_exp > short_exp:
        return "mostly_long"
    if short_exp > long_exp:
        return "mostly_short"
    return "inconclusive"


def _count_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    return _count_by_callable(rows, lambda row: str(row.get(key) or "UNKNOWN"))


def _count_by_callable(rows: Sequence[Mapping[str, Any]], fn: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(fn(row) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _confidence_band(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "UNKNOWN"
    for name, low, high in CONFIDENCE_BANDS:
        if low <= number < high:
            return name
    return "UNKNOWN"


def _distance_band(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "unavailable"
    absolute = abs(number)
    if absolute <= 7.5:
        return "near"
    if absolute <= 15.0:
        return "modest_extension"
    return "large_extension"


def _time_bucket(value: datetime | None) -> str:
    if value is None:
        return "UNKNOWN"
    hour = value.astimezone(UTC).hour
    return f"{hour:02d}:00-{hour:02d}:59Z"


def _top_item(counts: Mapping[str, int]) -> dict[str, Any] | None:
    if not counts:
        return None
    key = max(counts, key=lambda item: counts[item])
    return {"value": key, "count": counts[key]}


def _is_concentrated(counts: Mapping[str, int], total: int) -> bool:
    if not counts or total <= 0:
        return False
    return max(counts.values()) / total >= 0.65


def _average(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, Mapping):
                rows.append(dict(payload))
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = _parse_datetime(value)
    return parsed if parsed else datetime.now(UTC)
