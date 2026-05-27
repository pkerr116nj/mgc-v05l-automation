"""Read-only Hold-State / Exit-Selection shadow engine for Track B PAPER.

The engine consumes PositionIntent metadata and available position/evidence
artifacts to recommend what an exit policy *would* do. It never submits,
cancels, closes, modifies, flattens, or mutates lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_attribution_policy_v2 import (
    ALPHA_EXIT,
    BUG_FIX_EXIT,
)
from mgc_v05l.execution_core.track_b_position_intent_contract import (
    DEFAULT_OUTPUT_PATH as DEFAULT_POSITION_INTENT_AUDIT_PATH,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LATEST_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_hold_exit_shadow_recommendations.json"
)
DEFAULT_EVENTS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "hold_exit_shadow_recommendations.jsonl"
)
DEFAULT_LIVE_POSITION_STATUS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_trade_ledger"
    / "latest_track_b_broker_reconciled_live_position_status.json"
)
DEFAULT_FALLBACK_LIVE_POSITION_STATUS_PATH = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"
)
DEFAULT_EXIT_ATTRIBUTION_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_exit_attribution_review.json"
)

HOLD_THESIS_INTACT = "HOLD_THESIS_INTACT"
HOLD_EXTEND_PARTICIPATION_STRONG = "HOLD_EXTEND_PARTICIPATION_STRONG"
HARVEST_PROFIT_AVAILABLE = "HARVEST_PROFIT_AVAILABLE"
EXIT_DECAY_STATE = "EXIT_DECAY"
EXIT_THESIS_FAILURE_STATE = "EXIT_THESIS_FAILURE"
TIMEBOX_EXIT_DUE_STATE = "TIMEBOX_EXIT_DUE"
INSUFFICIENT_EVIDENCE_STATE = "INSUFFICIENT_EVIDENCE"

HOLD = "HOLD"
EXTEND_HOLD = "EXTEND_HOLD"
HARVEST = "HARVEST"
EXIT_DECAY = "EXIT_DECAY"
EXIT_THESIS_FAILURE = "EXIT_THESIS_FAILURE"
TIMEBOX_EXIT = "TIMEBOX_EXIT"
NO_RECOMMENDATION = "NO_RECOMMENDATION"

HOLD_EXIT_SHADOW_READY = "HOLD_EXIT_SHADOW_READY"
HOLD_EXIT_SHADOW_NO_POSITIONS_OR_CLEAN_ALPHA_EXITS = "HOLD_EXIT_SHADOW_NO_POSITIONS_OR_CLEAN_ALPHA_EXITS"

NO_AUTHORITY_FLAGS = {
    "shadow_only": True,
    "read_only": True,
    "submit_authority": False,
    "submit_allowed": False,
    "broker_mutation_allowed": False,
    "lifecycle_authority": False,
    "not_order_authority": True,
    "not_lifecycle_authority": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "dashboard_projection_consumed": False,
}


@dataclass(frozen=True)
class HoldExitShadowEngineConfig:
    repo_root: Path = REPO_ROOT
    position_intent_audit_path: Path = DEFAULT_POSITION_INTENT_AUDIT_PATH
    live_position_status_path: Path = DEFAULT_LIVE_POSITION_STATUS_PATH
    fallback_live_position_status_path: Path = DEFAULT_FALLBACK_LIVE_POSITION_STATUS_PATH
    exit_attribution_path: Path = DEFAULT_EXIT_ATTRIBUTION_PATH
    latest_output_path: Path = DEFAULT_LATEST_OUTPUT_PATH
    events_path: Path = DEFAULT_EVENTS_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def evaluate_hold_exit_shadow(
    *,
    position_intent: Mapping[str, Any],
    evidence: Mapping[str, Any],
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    actual_now = generated_at or datetime.now(UTC)
    basis: list[str] = []
    missing = _missing_evidence(position_intent=position_intent, evidence=evidence)
    if missing:
        return _recommendation(
            generated_at=actual_now,
            position_intent=position_intent,
            evidence=evidence,
            hold_state=INSUFFICIENT_EVIDENCE_STATE,
            recommendation=NO_RECOMMENDATION,
            basis=[f"missing_evidence:{item}" for item in missing],
        )

    completed_bars = _int(evidence.get("completed_5m_bars_since_entry"))
    max_bars = _int(_nested(position_intent, "hold_policy", "expected_hold_bars_5m")) or _max_bars_from_policy(
        _nested(position_intent, "hold_policy", "max_hold_policy")
    )
    current_net = _decimal(evidence.get("current_net_points"))
    mfe = _decimal(evidence.get("mfe_points"))
    giveback = _decimal(evidence.get("giveback_from_mfe_points"))
    participation_state = str(evidence.get("participation_state") or "").upper()
    microtrend_state = str(evidence.get("microtrend_state") or "").upper()
    thesis_invalidated = evidence.get("thesis_invalidated") is True or _contains_any(
        (participation_state, microtrend_state, str(evidence.get("regime_label") or "").upper()),
        ("THESIS_FAILED", "INVALIDATED", "REVERSAL_AGAINST_POSITION"),
    )
    participation_strong = _contains_any(
        (participation_state, microtrend_state),
        ("STRONG", "ALIGNED", "FOLLOW_THROUGH", "TREND_PARTICIPATION"),
    )
    participation_decay = _contains_any(
        (participation_state, microtrend_state),
        ("DECAY", "WEAK", "STAGNANT", "DIVERGENCE"),
    )
    favorable = current_net is not None and current_net > Decimal("0")
    material_mfe = mfe is not None and mfe >= _profit_threshold(str(position_intent.get("instrument_family") or ""))
    high_giveback = bool(material_mfe and giveback is not None and giveback >= mfe * Decimal("0.40"))

    if thesis_invalidated:
        return _recommendation(
            generated_at=actual_now,
            position_intent=position_intent,
            evidence=evidence,
            hold_state=EXIT_THESIS_FAILURE_STATE,
            recommendation=EXIT_THESIS_FAILURE,
            basis=["thesis_invalidation_evidence_present"],
        )
    if max_bars and completed_bars >= max_bars and not participation_strong:
        return _recommendation(
            generated_at=actual_now,
            position_intent=position_intent,
            evidence=evidence,
            hold_state=TIMEBOX_EXIT_DUE_STATE,
            recommendation=TIMEBOX_EXIT,
            basis=[f"completed_5m_bars_since_entry={completed_bars} >= max_hold_bars={max_bars}"],
        )
    if participation_decay and high_giveback:
        basis.extend(["participation_decay", "material_mfe_with_high_giveback"])
        return _recommendation(
            generated_at=actual_now,
            position_intent=position_intent,
            evidence=evidence,
            hold_state=HARVEST_PROFIT_AVAILABLE,
            recommendation=HARVEST,
            basis=basis,
        )
    if participation_decay:
        return _recommendation(
            generated_at=actual_now,
            position_intent=position_intent,
            evidence=evidence,
            hold_state=EXIT_DECAY_STATE,
            recommendation=EXIT_DECAY,
            basis=["participation_or_microtrend_decay"],
        )
    if participation_strong and favorable:
        return _recommendation(
            generated_at=actual_now,
            position_intent=position_intent,
            evidence=evidence,
            hold_state=HOLD_EXTEND_PARTICIPATION_STRONG,
            recommendation=EXTEND_HOLD,
            basis=["participation_strong", "current_net_favorable"],
        )
    if favorable:
        return _recommendation(
            generated_at=actual_now,
            position_intent=position_intent,
            evidence=evidence,
            hold_state=HOLD_THESIS_INTACT,
            recommendation=HOLD,
            basis=["current_net_favorable_without_exit_signal"],
        )
    return _recommendation(
        generated_at=actual_now,
        position_intent=position_intent,
        evidence=evidence,
        hold_state=INSUFFICIENT_EVIDENCE_STATE,
        recommendation=NO_RECOMMENDATION,
        basis=["no_positive_hold_or_exit_selection_evidence"],
    )


def run_hold_exit_shadow_engine(
    *,
    config: HoldExitShadowEngineConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    intent_audit = _read_json(config.resolve(config.position_intent_audit_path))
    intents_by_strategy = _intents_by_strategy(intent_audit)
    live_status_path, live_status = _live_status(config)
    open_positions = _open_position_records(live_status)
    open_recommendations = [
        evaluate_hold_exit_shadow(
            position_intent=_intent_for_position(position, intents_by_strategy),
            evidence=_evidence_for_open_position(position),
            generated_at=actual_now,
        )
        for position in open_positions
    ]
    attribution = _read_json(config.resolve(config.exit_attribution_path))
    closed_recommendations, skipped_closed = _closed_alpha_shadow_recommendations(
        attribution=attribution,
        intents_by_strategy=intents_by_strategy,
        generated_at=actual_now,
    )
    all_recommendations = [*open_recommendations, *closed_recommendations]
    classification = (
        HOLD_EXIT_SHADOW_READY if all_recommendations else HOLD_EXIT_SHADOW_NO_POSITIONS_OR_CLEAN_ALPHA_EXITS
    )
    payload = {
        "schema_version": "track_b_hold_exit_shadow_recommendations_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        **NO_AUTHORITY_FLAGS,
        "summary": {
            "open_position_count": len(open_positions),
            "open_recommendation_count": len(open_recommendations),
            "closed_alpha_recommendation_count": len(closed_recommendations),
            "skipped_bug_fix_exit_count": skipped_closed["BUG_FIX_EXIT"],
            "skipped_unknown_or_ineligible_exit_count": skipped_closed["OTHER"],
            "hold_state_counts": dict(Counter(str(item.get("hold_state")) for item in all_recommendations)),
            "recommendation_counts": dict(Counter(str(item.get("recommendation")) for item in all_recommendations)),
            "source_live_position_status": str(live_status_path),
            "source_position_intent_audit": str(config.resolve(config.position_intent_audit_path)),
            "source_exit_attribution": str(config.resolve(config.exit_attribution_path)),
        },
        "open_position_recommendations": open_recommendations,
        "closed_alpha_exit_recommendations": closed_recommendations,
        "evidence_gaps": _evidence_gaps(open_recommendations, closed_recommendations, intent_audit),
        "artifact_paths": {
            "latest": str(config.resolve(config.latest_output_path)),
            "events": str(config.resolve(config.events_path)),
            "docs": str(config.repo_root / "docs/track_b_hold_exit_shadow_engine.md"),
        },
    }
    write_json_atomic(config.resolve(config.latest_output_path), payload)
    _append_jsonl(config.resolve(config.events_path), payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run read-only Track B Hold-State / Exit-Selection shadow engine.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = HoldExitShadowEngineConfig(repo_root=Path(args.repo_root).expanduser().resolve())
    payload = run_hold_exit_shadow_engine(config=config)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"{payload['classification']}: "
            f"{payload['summary']['open_recommendation_count']} open, "
            f"{payload['summary']['closed_alpha_recommendation_count']} closed-alpha recommendations"
        )
    return 0


def _recommendation(
    *,
    generated_at: datetime,
    position_intent: Mapping[str, Any],
    evidence: Mapping[str, Any],
    hold_state: str,
    recommendation: str,
    basis: Sequence[str],
) -> dict[str, Any]:
    return {
        "generated_at": generated_at.isoformat(),
        "strategy_id": position_intent.get("strategy_id") or evidence.get("strategy_id"),
        "lane_id": position_intent.get("lane_id") or evidence.get("lane_id"),
        "source_type": evidence.get("source_type"),
        "lifecycle_id": evidence.get("lifecycle_id"),
        "trade_id": evidence.get("trade_id"),
        "instrument_family": position_intent.get("instrument_family") or evidence.get("instrument_family"),
        "side": position_intent.get("side") or evidence.get("side"),
        "quantity": evidence.get("quantity") or position_intent.get("quantity"),
        "thesis_type": _nested(position_intent, "trade_thesis", "thesis_type"),
        "expected_hold_type": _nested(position_intent, "hold_policy", "expected_hold_type"),
        "actual_exit_policy": evidence.get("actual_exit_policy")
        or _nested(position_intent, "exit_policy", "managed_exit_policy_id"),
        "hold_state": hold_state,
        "recommendation": recommendation,
        "basis": list(basis),
        "completed_5m_bars_since_entry": evidence.get("completed_5m_bars_since_entry"),
        "mfe_points": _string(_decimal(evidence.get("mfe_points"))),
        "mae_points": _string(_decimal(evidence.get("mae_points"))),
        "current_net_points": _string(_decimal(evidence.get("current_net_points"))),
        "giveback_from_mfe_points": _string(_decimal(evidence.get("giveback_from_mfe_points"))),
        "participation_state": evidence.get("participation_state"),
        "microtrend_state": evidence.get("microtrend_state"),
        "session_label": evidence.get("session_label"),
        "regime_label": evidence.get("regime_label"),
        "recommendation_only": True,
        **NO_AUTHORITY_FLAGS,
    }


def _missing_evidence(*, position_intent: Mapping[str, Any], evidence: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    if not position_intent:
        missing.append("position_intent")
    if not position_intent.get("strategy_id"):
        missing.append("position_intent.strategy_id")
    if not _nested(position_intent, "trade_thesis", "thesis_type"):
        missing.append("trade_thesis.thesis_type")
    if evidence.get("completed_5m_bars_since_entry") is None:
        missing.append("completed_5m_bars_since_entry")
    if evidence.get("current_net_points") is None and evidence.get("mfe_points") is None:
        missing.append("mfe_or_current_net")
    return missing


def _intents_by_strategy(audit: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in audit.get("strategies") or []:
        if not isinstance(row, Mapping):
            continue
        intent = row.get("position_intent")
        if not isinstance(intent, Mapping):
            continue
        key = (str(intent.get("strategy_id") or ""), str(intent.get("lane_id") or ""))
        result[key] = dict(intent)
    return result


def _intent_for_position(
    position: Mapping[str, Any],
    intents_by_strategy: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    strategy_id = str(position.get("strategy_id") or "")
    lane_id = str(position.get("lane_id") or "")
    if (strategy_id, lane_id) in intents_by_strategy:
        return dict(intents_by_strategy[(strategy_id, lane_id)])
    for (known_strategy, _known_lane), intent in intents_by_strategy.items():
        if known_strategy == strategy_id:
            return dict(intent)
    return {
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "instrument_family": position.get("instrument_family") or position.get("instrument") or position.get("symbol"),
        "side": _position_side(position),
        "quantity": position.get("quantity") or position.get("qty") or position.get("position"),
    }


def _evidence_for_open_position(position: Mapping[str, Any]) -> dict[str, Any]:
    mfe = _first(position, "mfe", "mfe_points", "max_favorable_excursion")
    current_net = _first(position, "current_net_points", "unrealized_points", "unrealized_pnl_points")
    return {
        "strategy_id": position.get("strategy_id"),
        "lane_id": position.get("lane_id"),
        "source_type": "OPEN_POSITION",
        "lifecycle_id": position.get("lifecycle_id"),
        "instrument_family": position.get("instrument_family") or position.get("instrument") or position.get("symbol"),
        "side": _position_side(position),
        "quantity": position.get("quantity") or position.get("qty") or position.get("position"),
        "completed_5m_bars_since_entry": _first(
            position,
            "completed_5m_bars_since_entry",
            "bars_since_entry",
            "completed_5m_bars_since_signal",
        ),
        "mfe_points": mfe,
        "mae_points": _first(position, "mae", "mae_points", "max_adverse_excursion"),
        "current_net_points": current_net,
        "giveback_from_mfe_points": _giveback(mfe=mfe, current_net=current_net),
        "participation_state": _first(position, "participation_state", "continuation_participation_state"),
        "microtrend_state": _first(position, "microtrend_state", "continuation_microtrend_state"),
        "session_label": position.get("session_label") or position.get("session"),
        "regime_label": position.get("regime_label") or position.get("regime"),
        "actual_exit_policy": position.get("managed_exit_policy_id") or position.get("exit_policy_id"),
    }


def _closed_alpha_shadow_recommendations(
    *,
    attribution: Mapping[str, Any],
    intents_by_strategy: Mapping[tuple[str, str], Mapping[str, Any]],
    generated_at: datetime,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    recommendations: list[dict[str, Any]] = []
    skipped: Counter[str] = Counter()
    for trade in attribution.get("trades") or []:
        if not isinstance(trade, Mapping):
            continue
        if trade.get("exit_intent_category") == BUG_FIX_EXIT:
            skipped["BUG_FIX_EXIT"] += 1
            continue
        if trade.get("exit_intent_category") != ALPHA_EXIT or trade.get("eligible_for_alpha_exit_analysis") is not True:
            skipped["OTHER"] += 1
            continue
        strategy_id = str(trade.get("strategy_id") or "")
        lane_id = str(trade.get("lane_id") or "")
        intent = _intent_for_position(trade, intents_by_strategy)
        if not intent and (strategy_id, lane_id) in intents_by_strategy:
            intent = dict(intents_by_strategy[(strategy_id, lane_id)])
        recommendations.append(
            evaluate_hold_exit_shadow(
                position_intent=intent,
                evidence={
                    "strategy_id": strategy_id,
                    "lane_id": lane_id,
                    "source_type": "CLOSED_ALPHA_EXIT",
                    "trade_id": trade.get("trade_id"),
                    "lifecycle_id": trade.get("lifecycle_id"),
                    "instrument_family": trade.get("instrument") or trade.get("instrument_family"),
                    "side": trade.get("side"),
                    "quantity": trade.get("quantity"),
                    "completed_5m_bars_since_entry": trade.get("bars_held") or trade.get("completed_5m_bars_held"),
                    "mfe_points": trade.get("mfe_points"),
                    "mae_points": trade.get("mae_points"),
                    "current_net_points": trade.get("realized_points") or trade.get("realized_pnl_points"),
                    "giveback_from_mfe_points": trade.get("giveback_from_mfe_points"),
                    "actual_exit_policy": trade.get("actual_exit_profile") or trade.get("exit_policy_v2_taxonomy"),
                    "participation_state": trade.get("participation_state"),
                    "microtrend_state": trade.get("microtrend_state"),
                    "session_label": trade.get("session_label"),
                    "regime_label": trade.get("regime_label"),
                },
                generated_at=generated_at,
            )
        )
    return recommendations, skipped


def _evidence_gaps(
    open_recommendations: Sequence[Mapping[str, Any]],
    closed_recommendations: Sequence[Mapping[str, Any]],
    intent_audit: Mapping[str, Any],
) -> list[str]:
    gaps: list[str] = []
    if not open_recommendations:
        gaps.append("no_current_open_positions")
    if not closed_recommendations:
        gaps.append("no_recent_clean_alpha_exit_trades")
    if intent_audit.get("classification") != "POSITION_INTENT_CONTRACT_READY":
        gaps.append("position_intent_contract_not_ready")
    return gaps


def _live_status(config: HoldExitShadowEngineConfig) -> tuple[Path, dict[str, Any]]:
    primary = config.resolve(config.live_position_status_path)
    payload = _read_json(primary)
    if payload:
        return primary, payload
    fallback = config.resolve(config.fallback_live_position_status_path)
    return fallback, _read_json(fallback)


def _open_position_records(live_status: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("broker_track_b_positions", "open_positions", "positions", "position_records"):
        value = live_status.get(key)
        if isinstance(value, list):
            rows.extend(dict(item) for item in value if isinstance(item, Mapping))
    if rows:
        return rows
    by_instrument = live_status.get("positions_by_instrument")
    if isinstance(by_instrument, Mapping):
        for instrument, value in by_instrument.items():
            if isinstance(value, Mapping):
                row = dict(value)
                row.setdefault("instrument_family", instrument)
                rows.append(row)
    return rows


def _position_side(position: Mapping[str, Any]) -> str:
    side = str(position.get("side") or position.get("direction") or "").upper()
    if side in {"LONG", "SHORT"}:
        return side
    qty = _decimal(position.get("quantity") or position.get("qty") or position.get("position"))
    if qty is None:
        return ""
    return "LONG" if qty > 0 else "SHORT" if qty < 0 else ""


def _max_bars_from_policy(policy: Any) -> int | None:
    text = str(policy or "")
    if text.startswith("3_"):
        return 3
    return None


def _profit_threshold(instrument_family: str) -> Decimal:
    instrument = str(instrument_family or "").upper()
    if instrument in {"MGC", "GC"}:
        return Decimal("2")
    if instrument in {"MNQ", "NQ", "MES", "ES", "YM", "MYM"}:
        return Decimal("10")
    return Decimal("1")


def _contains_any(values: Sequence[str], needles: Sequence[str]) -> bool:
    combined = " ".join(values)
    return any(needle in combined for needle in needles)


def _nested(payload: Mapping[str, Any], parent: str, field: str) -> Any:
    nested = payload.get(parent)
    if isinstance(nested, Mapping):
        return nested.get(field)
    return None


def _first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in {None, ""}:
            return value
    return None


def _giveback(*, mfe: Any, current_net: Any) -> str | None:
    mfe_dec = _decimal(mfe)
    net_dec = _decimal(current_net)
    if mfe_dec is None or net_dec is None:
        return None
    return _string(max(Decimal("0"), mfe_dec - net_dec))


def _decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def _string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(payload), sort_keys=True) + "\n")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
