"""GC-only Phase-1 PAPER candidate inventory and no-submit evaluation."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from mgc_v05l.app.asia_london_participation_runtime import (
    ASIA_LONDON_PARTICIPATION_FAMILY,
    ASIA_LONDON_PARTICIPATION_RUNTIME_KIND,
    GC_ASIA_LONDON_LONG_V5_SOURCE,
    GC_ASIA_LONDON_SHORT_V2_SOURCE,
    AsiaLondonParticipationStrategyEngine,
)
from mgc_v05l.app.gc_mgc_forced_session_runtime import (
    ASIA_EARLY_LONG_SOURCE,
    ASIA_EARLY_SHORT_SOURCE,
    GC_MGC_FORCED_SESSION_FAMILY,
    GC_MGC_FORCED_SESSION_RUNTIME_KIND,
    LONDON_EARLY_LONG_SOURCE,
    NY_EARLY_SHORT_SOURCE,
    NY_LATE_SHORT_SOURCE,
)
from mgc_v05l.app.probationary_runtime import ProbationaryPaperLaneSpec, _build_probationary_paper_lane_settings
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.events import OrderIntentCreatedEvent
from mgc_v05l.domain.models import Bar
from mgc_v05l.execution.execution_engine import ExecutionEngine
from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter
from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    DEFAULT_RUNTIME_CANDLE_ROOT,
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
CHOSEN_GC_STRATEGY_ID = "gc_1x_asia_london_participation__asia_london_long_v5"


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


@dataclass(frozen=True)
class Phase1GcCandidateConfig:
    repo_root: Path = REPO_ROOT
    output_dir: Path = DEFAULT_OUTPUT_DIR
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT
    config_paths: tuple[Path, ...] = DEFAULT_CONFIG_PATHS
    strategy_id: str = CHOSEN_GC_STRATEGY_ID
    max_bars: int = 1440
    write_report: bool = True


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
        "non_gc_strategy_promoted": False,
        "realtime_feed_confirmed": bool(gc_readiness.get("realtime_feed_confirmed")),
        "historical_seed_ready": bool(gc_readiness.get("historical_seed_ready")),
        "runtime_candles_ready": bool(gc_readiness.get("runtime_candles_ready")),
        "paper_watch_ready": False,
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
            required_features=("ohlcv_1m", "derived_3m_context"),
            input_artifacts=("phase1_runtime_market_data/GC/1m/latest_runtime_candles.json",),
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
            required_features=("ohlcv_1m", "derived_3m_context"),
            input_artifacts=("phase1_runtime_market_data/GC/1m/latest_runtime_candles.json",),
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
                required_features=("ohlcv_1m", "derived_3m_context"),
                input_artifacts=("phase1_runtime_market_data/GC/1m/latest_runtime_candles.json",),
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
    adapter = lane_submit_bridge_adapter(lane_id=selected.strategy_id)
    if adapter is None:
        return _blocked_evaluation("LANE_ADAPTER_MISSING", strategy_id=selected.strategy_id, family=selected.family)
    if not bool(gc_readiness.get("historical_seed_ready")):
        return _blocked_evaluation("HISTORICAL_SEED_MISSING", strategy_id=selected.strategy_id, family=selected.family)
    candle_path = _runtime_candle_path(config=config, symbol="GC", timeframe="1m")
    bars, candle_error = _load_runtime_bars(candle_path, max_bars=config.max_bars)
    if candle_error:
        return _blocked_evaluation(candle_error, strategy_id=selected.strategy_id, family=selected.family, candle_path=str(candle_path))
    engine = _build_gc_candidate_engine(config=config, lane=selected)
    signal_events = 0
    order_intents = 0
    for bar in bars:
        events = engine.process_bar(bar)
        for event in events:
            if isinstance(event, OrderIntentCreatedEvent):
                order_intents += 1
        signal_events += 1 if getattr(engine, "_last_signal_packet", None) is not None else 0  # noqa: SLF001
    return {
        "strategy_id": selected.strategy_id,
        "family": selected.family,
        "approval_status": selected.approval_status,
        "runtime_kind": selected.runtime_kind,
        "source": selected.source,
        "side": selected.side,
        "bridge_adapter_present": True,
        "candidate_evaluation_ready": True,
        "candidate_evaluation_mode": "TRACK_B_PAPER_CANDIDATE_NO_SUBMIT",
        "bars_evaluated": len(bars),
        "signal_packets_evaluated": signal_events,
        "shadow_order_intents": order_intents,
        "historical_seed_ready": bool(gc_readiness.get("historical_seed_ready")),
        "realtime_feed_confirmed": bool(gc_readiness.get("realtime_feed_confirmed")),
        "runtime_candles_ready": bool(gc_readiness.get("runtime_candles_ready")),
        "paper_watch_ready": False,
        "paper_watch_block_reason": "REALTIME_FEED_NOT_CONFIRMED",
        "can_submit": False,
        "submit_attempted": False,
        "live_money_eligible": False,
        "broker_mutation_path_touched": False,
        "paper_proof_invoked": False,
        "non_gc_strategy_promoted": False,
    }


def _build_gc_candidate_engine(*, config: Phase1GcCandidateConfig, lane: GcCandidateLane) -> AsiaLondonParticipationStrategyEngine:
    base_settings = load_settings_from_files([Path(config.repo_root) / path for path in config.config_paths])
    spec = ProbationaryPaperLaneSpec(
        lane_id=lane.strategy_id,
        display_name="GC / ASIA_LONDON_LONG_V5 / x1",
        symbol="GC",
        standalone_strategy_id=lane.strategy_id,
        long_sources=(lane.source,) if lane.side == "LONG" else (),
        short_sources=(lane.source,) if lane.side == "SHORT" else (),
        session_restriction="ASIA_EARLY/ASIA_LATE/LONDON_EARLY/LONDON_LATE",
        allowed_sessions=("ASIA_EARLY", "ASIA_LATE", "LONDON_EARLY", "LONDON_LATE"),
        point_value=Decimal("100"),
        trade_size=1,
        max_position_quantity=1,
        strategy_family=lane.family,
        runtime_kind=lane.runtime_kind,
        structural_signal_timeframe=lane.structural_timeframe,
        execution_timeframe=lane.execution_timeframe,
        artifact_timeframe=lane.structural_timeframe,
        context_timeframes=(lane.structural_timeframe,),
        live_poll_lookback_minutes=1440,
        observed_instruments=("GC",),
        experimental_status="paper_candidate",
        paper_only=True,
        non_approved=True,
    )
    settings = _build_probationary_paper_lane_settings(base_settings, spec)
    return AsiaLondonParticipationStrategyEngine(
        lane_spec=spec,
        settings=settings,
        repositories=None,
        execution_engine=ExecutionEngine(),
        runtime_identity={
            "standalone_strategy_id": lane.strategy_id,
            "strategy_family": lane.family,
            "instrument": "GC",
            "lane_id": lane.strategy_id,
        },
        shadow_mode_no_submit=True,
    )


def _load_runtime_bars(path: Path, *, max_bars: int) -> tuple[list[Bar], str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [], "RUNTIME_CANDLES_MISSING"
    if not isinstance(payload, dict):
        return [], "RUNTIME_CANDLES_INVALID"
    if bool(payload.get("research_artifact_used")) or bool(payload.get("archive_artifact_used")):
        return [], "RUNTIME_CANDLES_UNSAFE_PROVENANCE"
    if payload.get("source") != "DATABENTO_HISTORICAL_SEED":
        return [], "RUNTIME_CANDLES_SOURCE_NOT_SEED"
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
    adapter = lane_submit_bridge_adapter(lane_id=lane.strategy_id)
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
        "rule_runner_status": "EXISTING_RUNTIME_ENGINE",
        "bridge_adapter_present": adapter is not None,
        "can_consume_runtime_seed": True,
        "paper_candidate_evaluation_blockers": [],
        "can_submit": False,
        "live_money_eligible": False,
    }


def _blocked_evaluation(reason: str, **extra: Any) -> dict[str, Any]:
    return {
        **extra,
        "candidate_evaluation_ready": False,
        "block_reason": reason,
        "paper_watch_ready": False,
        "can_submit": False,
        "submit_attempted": False,
        "live_money_eligible": False,
        "broker_mutation_path_touched": False,
        "paper_proof_invoked": False,
        "non_gc_strategy_promoted": False,
    }


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
