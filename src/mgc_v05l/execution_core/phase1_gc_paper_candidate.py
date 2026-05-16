"""GC-only Phase-1 PAPER candidate inventory and no-submit evaluation."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from mgc_v05l.domain.models import Bar
from mgc_v05l.execution_core.phase1_gc_paper_candidate_registry import (
    CHOSEN_GC_STRATEGY_ID,
    PHASE1_GC_GUARDED_PAPER_ELIGIBLE_STRATEGY_IDS,
    is_phase1_gc_guarded_paper_eligible_strategy,
)
from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    DEFAULT_RUNTIME_CANDLE_ROOT,
    DEFAULT_RUNTIME_FEATURE_ROOT,
    Phase1RuntimeDataReadinessConfig,
    build_phase1_runtime_data_readiness,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "phase1_gc_paper_candidate"
DEFAULT_CONFIG_PATHS = (
    Path("config") / "base.yaml",
    Path("config") / "live.yaml",
    Path("config") / "probationary_pattern_engine.yaml",
)
ASIA_LONDON_PARTICIPATION_RUNTIME_KIND = "asia_london_participation_candidate_runtime"
ASIA_LONDON_PARTICIPATION_FAMILY = "asia_london_participation_core_v1"
GC_ASIA_LONDON_LONG_V5_SOURCE = "gcAsiaLondonLongV5"
GC_ASIA_LONDON_SHORT_V2_SOURCE = "gcAsiaLondonShortV2"
GC_MGC_FORCED_SESSION_RUNTIME_KIND = "gc_mgc_forced_session_candidate_runtime"
GC_MGC_FORCED_SESSION_FAMILY = "gold_forced_session_baseline_v2"
ASIA_EARLY_SHORT_SOURCE = "asiaEarlyShortV2"
ASIA_EARLY_LONG_SOURCE = "asiaEarlyLongV5"
LONDON_EARLY_LONG_SOURCE = "londonEarlyLongV5"
NY_EARLY_SHORT_SOURCE = "nyEarlyShortV2"
NY_LATE_SHORT_SOURCE = "nyLateShortV2"


@dataclass(frozen=True)
class GcCandidateLane:
    strategy_id: str
    family: str
    approval_status: str
    baseline_status: str
    runtime_kind: str
    source: str
    side: str
    structural_timeframe: str
    execution_timeframe: str
    required_features: tuple[str, ...]
    input_artifacts: tuple[str, ...]
    external_feature_artifacts_required: bool
    required_runtime_feature_artifacts: tuple[str, ...]
    feature_contract_status: str


@dataclass(frozen=True)
class Phase1GcCandidateConfig:
    repo_root: Path = REPO_ROOT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT
    runtime_feature_root: Path = DEFAULT_RUNTIME_FEATURE_ROOT
    config_paths: tuple[Path, ...] = DEFAULT_CONFIG_PATHS
    strategy_id: str = CHOSEN_GC_STRATEGY_ID
    max_bars: int = 1440
    write_report: bool = True
    guarded_route_authorized: bool = False
    now: datetime | None = None


@dataclass(frozen=True)
class Phase1GcCandidateArtifacts:
    report: dict[str, Any]
    inventory_rows: list[dict[str, Any]]
    evaluation: dict[str, Any]


def build_phase1_gc_paper_candidate(
    *,
    config: Phase1GcCandidateConfig,
) -> Phase1GcCandidateArtifacts:
    repo_root = Path(config.repo_root)
    inventory = inventory_gc_paper_candidate_lanes()
    selected = next((row for row in inventory if row.strategy_id == config.strategy_id), None)
    readiness = build_phase1_runtime_data_readiness(
        config=Phase1RuntimeDataReadinessConfig(
            repo_root=repo_root,
            runtime_candle_root=config.runtime_candle_root,
            runtime_feature_root=config.runtime_feature_root,
            now=config.now,
        )
    )
    gc_readiness = next(row for row in readiness.rows if row["symbol"] == "GC")
    evaluation = _evaluate_selected_candidate(
        config=config,
        selected=selected,
        gc_readiness=gc_readiness,
    )
    inventory_rows = [_inventory_row(lane) for lane in inventory]
    report = {
        "schema_version": "phase1_gc_paper_candidate_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "instrument_scope": "GC_ONLY",
        "chosen_strategy_id": config.strategy_id,
        "chosen_strategy": evaluation.get("strategy_id"),
        "chosen_strategy_family": evaluation.get("family"),
        "recommendation": "PROMOTE_TO_TRACK_B_PAPER_CANDIDATE_EVALUATION",
        "recommendation_reason": (
            "Chosen because the GC Asia-London long v5 lane already has a runtime adapter, "
            "a Phase-1 GC bridge adapter, 3m structural / 1m execution semantics, and direct bridge tests."
        ),
        "inventory_rows": inventory_rows,
        "evaluation": evaluation,
        "approved_strategy_ids": sorted(PHASE1_GC_GUARDED_PAPER_ELIGIBLE_STRATEGY_IDS),
        "paper_candidate_approved": is_phase1_gc_guarded_paper_eligible_strategy(
            strategy_id=str(evaluation.get("strategy_id") or config.strategy_id),
            instrument="GC",
        ),
        "guarded_route_authorized": bool(config.guarded_route_authorized),
        "non_gc_strategy_promoted": False,
        "realtime_feed_confirmed": bool(gc_readiness.get("realtime_feed_confirmed")),
        "historical_seed_ready": bool(gc_readiness.get("historical_seed_ready")),
        "runtime_candles_ready": bool(gc_readiness.get("runtime_candles_ready")),
        "derived_features_ready": bool(gc_readiness.get("derived_features_ready")),
        "paper_watch_ready": bool(evaluation.get("paper_watch_ready")),
        "paper_watch_block_reason": evaluation.get("paper_watch_block_reason"),
        "can_submit": False,
        "live_money_eligible": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
    }
    if config.write_report:
        _write_report(config=config, report=report)
    return Phase1GcCandidateArtifacts(report=report, inventory_rows=inventory_rows, evaluation=evaluation)


def inventory_gc_paper_candidate_lanes() -> list[GcCandidateLane]:
    return [
        GcCandidateLane(
            strategy_id=CHOSEN_GC_STRATEGY_ID,
            family=ASIA_LONDON_PARTICIPATION_FAMILY,
            approval_status="PAPER_CANDIDATE_BASELINE_READY",
            baseline_status="PROMOTED_ASIA_LONDON_PARTICIPATION_CORE_V1",
            runtime_kind=ASIA_LONDON_PARTICIPATION_RUNTIME_KIND,
            source=GC_ASIA_LONDON_LONG_V5_SOURCE,
            side="LONG",
            structural_timeframe="3m",
            execution_timeframe="1m",
            required_features=("completed_ohlcv_1m", "in_engine_asia_london_segment_context"),
            input_artifacts=("phase1_runtime_market_data/GC/1m/latest_runtime_candles.json",),
            external_feature_artifacts_required=False,
            required_runtime_feature_artifacts=(),
            feature_contract_status="IN_ENGINE_BAR_DERIVED_CONTEXT",
        ),
        GcCandidateLane(
            strategy_id="gc_1x_asia_london_participation__asia_london_short_v2",
            family=ASIA_LONDON_PARTICIPATION_FAMILY,
            approval_status="PAPER_CANDIDATE_BASELINE_READY",
            baseline_status="PROMOTED_ASIA_LONDON_PARTICIPATION_CORE_V1",
            runtime_kind=ASIA_LONDON_PARTICIPATION_RUNTIME_KIND,
            source=GC_ASIA_LONDON_SHORT_V2_SOURCE,
            side="SHORT",
            structural_timeframe="3m",
            execution_timeframe="1m",
            required_features=("completed_ohlcv_1m", "in_engine_asia_london_segment_context"),
            input_artifacts=("phase1_runtime_market_data/GC/1m/latest_runtime_candles.json",),
            external_feature_artifacts_required=False,
            required_runtime_feature_artifacts=(),
            feature_contract_status="IN_ENGINE_BAR_DERIVED_CONTEXT",
        ),
        *[
            GcCandidateLane(
                strategy_id=f"gc_1x_all_lanes__{suffix}",
                family=GC_MGC_FORCED_SESSION_FAMILY,
                approval_status="PAPER_CANDIDATE_BASELINE_READY",
                baseline_status="PROMOTED_GOLD_FORCED_SESSION_BASELINE_V2",
                runtime_kind=GC_MGC_FORCED_SESSION_RUNTIME_KIND,
                source=source,
                side=side,
                structural_timeframe="3m",
                execution_timeframe="1m",
                required_features=("completed_ohlcv_1m", "in_engine_forced_session_context"),
                input_artifacts=("phase1_runtime_market_data/GC/1m/latest_runtime_candles.json",),
                external_feature_artifacts_required=False,
                required_runtime_feature_artifacts=(),
                feature_contract_status="IN_ENGINE_BAR_DERIVED_CONTEXT",
            )
            for suffix, source, side in (
                ("asia_early_long", ASIA_EARLY_LONG_SOURCE, "LONG"),
                ("asia_early_short", ASIA_EARLY_SHORT_SOURCE, "SHORT"),
                ("london_early_long", LONDON_EARLY_LONG_SOURCE, "LONG"),
                ("us_early_short", NY_EARLY_SHORT_SOURCE, "SHORT"),
                ("us_midday_short", NY_LATE_SHORT_SOURCE, "SHORT"),
            )
        ],
    ]


def _evaluate_selected_candidate(
    *,
    config: Phase1GcCandidateConfig,
    selected: GcCandidateLane | None,
    gc_readiness: dict[str, Any],
) -> dict[str, Any]:
    if selected is None:
        return _blocked_evaluation("STRATEGY_NOT_IN_GC_INVENTORY", strategy_id=config.strategy_id)
    bridge_adapter_present = _phase1_gc_bridge_adapter_present(strategy_id=selected.strategy_id)
    if not bridge_adapter_present:
        return _blocked_evaluation("LANE_ADAPTER_MISSING", strategy_id=selected.strategy_id, family=selected.family)
    if not bool(gc_readiness.get("historical_seed_ready") or gc_readiness.get("runtime_candles_ready")):
        return _blocked_evaluation("RUNTIME_CANDLES_MISSING", strategy_id=selected.strategy_id, family=selected.family)
    candle_path = _runtime_candle_path(config=config, symbol="GC", timeframe="1m")
    bars, candle_error = _load_runtime_bars(candle_path, max_bars=config.max_bars)
    if candle_error:
        return _blocked_evaluation(candle_error, strategy_id=selected.strategy_id, family=selected.family, candle_path=str(candle_path))
    external_features_ready = _external_feature_artifacts_ready(
        gc_readiness=gc_readiness,
        external_feature_artifacts_required=selected.external_feature_artifacts_required,
    )
    paper_watch_ready = bool(
        gc_readiness.get("runtime_candles_ready")
        and external_features_ready
        and gc_readiness.get("realtime_feed_confirmed")
        and config.guarded_route_authorized
    )
    return {
        "strategy_id": selected.strategy_id,
        "family": selected.family,
        "approval_status": selected.approval_status,
        "runtime_kind": selected.runtime_kind,
        "source": selected.source,
        "side": selected.side,
        "bridge_adapter_present": bridge_adapter_present,
        "candidate_evaluation_ready": True,
        "candidate_evaluation_mode": "TRACK_B_PAPER_CANDIDATE_ARTIFACT_NO_SUBMIT",
        "bars_evaluated": len(bars),
        "signal_packets_evaluated": 0,
        "shadow_order_intents": 0,
        "historical_seed_ready": bool(gc_readiness.get("historical_seed_ready")),
        "realtime_feed_confirmed": bool(gc_readiness.get("realtime_feed_confirmed")),
        "runtime_candles_ready": bool(gc_readiness.get("runtime_candles_ready")),
        "derived_features_ready": bool(gc_readiness.get("derived_features_ready")),
        "external_feature_artifacts_required": bool(selected.external_feature_artifacts_required),
        "external_feature_artifacts_ready": external_features_ready,
        "required_runtime_feature_artifacts": list(selected.required_runtime_feature_artifacts),
        "feature_contract_status": selected.feature_contract_status,
        "paper_candidate_approved": is_phase1_gc_guarded_paper_eligible_strategy(
            strategy_id=selected.strategy_id,
            instrument="GC",
        ),
        "guarded_route_authorized": bool(config.guarded_route_authorized),
        "paper_watch_ready": paper_watch_ready,
        "paper_watch_block_reason": "READY" if paper_watch_ready else _paper_watch_block_reason(
            gc_readiness=gc_readiness,
            guarded_route_authorized=config.guarded_route_authorized,
            external_feature_artifacts_required=selected.external_feature_artifacts_required,
        ),
        "can_submit": False,
        "submit_attempted": False,
        "live_money_eligible": False,
        "broker_mutation_path_touched": False,
        "paper_proof_invoked": False,
        "non_gc_strategy_promoted": False,
    }


def _load_runtime_bars(path: Path, *, max_bars: int) -> tuple[list[Bar], str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [], "RUNTIME_CANDLES_MISSING"
    if not isinstance(payload, dict):
        return [], "RUNTIME_CANDLES_INVALID"
    if bool(payload.get("research_artifact_used")) or bool(payload.get("archive_artifact_used")):
        return [], "RUNTIME_CANDLES_UNSAFE_PROVENANCE"
    if not str(payload.get("source_id") or payload.get("source") or "").strip():
        return [], "RUNTIME_CANDLES_SOURCE_MISSING"
    if str(payload.get("symbol") or "").strip().upper() not in {"", "GC"}:
        return [], "RUNTIME_CANDLES_SYMBOL_MISMATCH"
    if str(payload.get("timeframe") or "").strip() not in {"", "1m"}:
        return [], "RUNTIME_CANDLES_TIMEFRAME_MISMATCH"
    if payload.get("completed_candles_only") is not True:
        return [], "RUNTIME_CANDLES_NOT_COMPLETED_ONLY"
    raw_bars = list(payload.get("bars") or [])
    bars = [_bar_from_payload(row, symbol="GC", timeframe="1m") for row in raw_bars[-max(int(max_bars), 1) :]]
    bars = [bar for bar in bars if bar is not None]
    return bars, None if bars else "RUNTIME_CANDLES_MISSING"


def _bar_from_payload(row: Any, *, symbol: str, timeframe: str) -> Bar | None:
    if not isinstance(row, dict):
        return None
    start_ts = _parse_datetime(row.get("bar_start"))
    end_ts = _parse_datetime(row.get("bar_end"))
    if start_ts is None or end_ts is None:
        return None
    try:
        return Bar(
            bar_id=f"{symbol}|{timeframe}|{end_ts.isoformat()}",
            symbol=symbol,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(float(row.get("volume") or 0)),
            is_final=True,
            session_asia=True,
            session_london=True,
            session_us=True,
            session_allowed=True,
        )
    except Exception:
        return None


def _runtime_candle_path(*, config: Phase1GcCandidateConfig, symbol: str, timeframe: str) -> Path:
    root = Path(config.runtime_candle_root)
    if not root.is_absolute():
        root = Path(config.repo_root) / root
    return root / symbol / timeframe / "latest_runtime_candles.json"


def _inventory_row(lane: GcCandidateLane) -> dict[str, Any]:
    adapter_present = _phase1_gc_bridge_adapter_present(strategy_id=lane.strategy_id)
    return {
        "strategy_id": lane.strategy_id,
        "instrument": "GC",
        "family": lane.family,
        "approval_status": lane.approval_status,
        "baseline_status": lane.baseline_status,
        "runtime_kind": lane.runtime_kind,
        "source": lane.source,
        "side": lane.side,
        "required_input_timeframe": lane.execution_timeframe,
        "required_context_timeframe": lane.structural_timeframe,
        "required_features": list(lane.required_features),
        "external_feature_artifacts_required": bool(lane.external_feature_artifacts_required),
        "required_runtime_feature_artifacts": list(lane.required_runtime_feature_artifacts),
        "feature_contract_status": lane.feature_contract_status,
        "rule_runner_status": "EXISTING_RUNTIME_ENGINE",
        "bridge_adapter_present": adapter_present,
        "can_consume_runtime_seed": True,
        "paper_candidate_evaluation_blockers": [],
        "can_submit": False,
        "live_money_eligible": False,
    }


def _phase1_gc_bridge_adapter_present(*, strategy_id: str) -> bool:
    return is_phase1_gc_guarded_paper_eligible_strategy(strategy_id=strategy_id, instrument="GC")


def _blocked_evaluation(reason: str, **extra: Any) -> dict[str, Any]:
    return {
        **extra,
        "candidate_evaluation_ready": False,
        "block_reason": reason,
        "paper_watch_ready": False,
        "paper_watch_block_reason": reason,
        "can_submit": False,
        "submit_attempted": False,
        "live_money_eligible": False,
        "broker_mutation_path_touched": False,
        "paper_proof_invoked": False,
        "non_gc_strategy_promoted": False,
    }


def _external_feature_artifacts_ready(
    *,
    gc_readiness: dict[str, Any],
    external_feature_artifacts_required: bool,
) -> bool:
    if not external_feature_artifacts_required:
        return True
    return bool(gc_readiness.get("derived_features_ready"))


def _paper_watch_block_reason(
    *,
    gc_readiness: dict[str, Any],
    guarded_route_authorized: bool,
    external_feature_artifacts_required: bool,
) -> str:
    if not bool(gc_readiness.get("realtime_feed_confirmed")):
        return "REALTIME_FEED_NOT_CONFIRMED"
    if not bool(gc_readiness.get("runtime_candles_ready")):
        return str(gc_readiness.get("runtime_candles_block_reason") or "RUNTIME_CANDLES_NOT_READY")
    if external_feature_artifacts_required and not bool(gc_readiness.get("derived_features_ready")):
        return str(gc_readiness.get("derived_features_block_reason") or "FEATURES_NOT_READY")
    if not guarded_route_authorized:
        return "GUARDED_ROUTE_NOT_AUTHORIZED"
    return "NOT_READY"


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _write_report(*, config: Phase1GcCandidateConfig, report: dict[str, Any]) -> None:
    output_dir = Path(config.output_dir)
    if not output_dir.is_absolute():
        output_dir = Path(config.repo_root) / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_phase1_gc_paper_candidate.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inventory and evaluate one GC Phase-1 PAPER candidate in no-submit mode.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--strategy-id", default=CHOSEN_GC_STRATEGY_ID)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-bars", type=int, default=1440)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifacts = build_phase1_gc_paper_candidate(
        config=Phase1GcCandidateConfig(
            repo_root=Path(args.repo_root),
            output_dir=Path(args.output_dir),
            strategy_id=str(args.strategy_id),
            max_bars=int(args.max_bars),
            write_report=not bool(args.no_write),
            guarded_route_authorized=False,
        )
    )
    print(
        json.dumps(
            {
                "chosen_strategy": artifacts.report["chosen_strategy"],
                "candidate_evaluation_ready": artifacts.evaluation.get("candidate_evaluation_ready"),
                "bars_evaluated": artifacts.evaluation.get("bars_evaluated", 0),
                "paper_watch_ready": artifacts.report["paper_watch_ready"],
                "can_submit": artifacts.report["can_submit"],
                "live_money_eligible": artifacts.report["live_money_eligible"],
            },
            sort_keys=True,
        )
    )
    return 0 if artifacts.evaluation.get("candidate_evaluation_ready") else 2


if __name__ == "__main__":
    raise SystemExit(main())
