"""Track B rule-runner adapter for guarded PAPER probationary lanes.

This adapter gives a promoted Track B strategy an explicit bridge into the
existing PAPER StrategyEngine lifecycle without changing live rules or bypassing
the normal broker/lifecycle gates. It reads a configured Track B state artifact,
evaluates the configured rule runner, and emits a standard StrategyEngine
SignalPacket only when the rule emits a timestamp-coherent PAPER signal.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ..domain.enums import LongEntryFamily, ShortEntryFamily
from ..domain.models import FeaturePacket, SignalPacket
from ..strategy.strategy_engine import StrategyEngine, _empty_signal_packet_payload
from ..execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)


TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND = "track_b_rule_runner_paper_strategy_engine"
DEFAULT_TRACK_B_RULE_RUNNER_SIGNAL_FRESHNESS_SECONDS = 360.0


class TrackBRuleRunnerPaperStrategyEngine(StrategyEngine):
    """Run a promoted Track B rule inside the standard PAPER StrategyEngine shell."""

    def __init__(self, *, lane_spec: Any, repo_root: Path | None = None, **kwargs: Any) -> None:
        self._track_b_lane_spec = lane_spec
        self._track_b_repo_root = (repo_root or Path(__file__).resolve().parents[3]).resolve()
        self._latest_track_b_rule_report: dict[str, Any] = {}
        super().__init__(**kwargs)

    def latest_track_b_rule_runner_report(self) -> dict[str, Any]:
        return dict(self._latest_track_b_rule_report)

    def _evaluate_signals(self, feature_packet: FeaturePacket, feature_history: list[FeaturePacket]) -> SignalPacket:
        del feature_history
        payload = _empty_signal_packet_payload(feature_packet.bar_id)
        config = self._track_b_rule_runner_config()
        event_path = self._resolve_repo_path(config.get("input_event_path") or config.get("state_artifact_path"))
        event_payload = _read_json(event_path)
        current_bar = self._bar_history[-1] if self._bar_history else None
        freshness_blocker = _state_freshness_blocker(event_payload, current_bar_end=getattr(current_bar, "end_ts", None), config=config)
        if freshness_blocker is not None:
            self._latest_track_b_rule_report = {
                "classification": "TRACK_B_RULE_RUNNER_PAPER_NO_SIGNAL",
                "primary_blocker": freshness_blocker,
                "input_event_path": str(event_path),
                "submit_allowed": False,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
            return SignalPacket(**payload)

        result = run_track_b_strategy_rule(
            input_event_payload=event_payload,
            input_event_path=event_path,
            inbox_dir=self._resolve_repo_path(config.get("inbox_dir") or "outputs/track_b_execution_core/listener_inbox"),
            expected_account_id=str(config.get("expected_account_id") or "DUM882026"),
            source_id=str(config.get("source_id") or self._track_b_lane_spec.lane_id),
            strategy_id=str(config.get("strategy_id") or self._track_b_lane_spec.standalone_strategy_id or self._track_b_lane_spec.lane_id),
            lane_id=str(config.get("lane_id") or self._track_b_lane_spec.lane_id),
            rule_id=str(config.get("rule_id") or self._track_b_lane_spec.strategy_family),
            rule_mode=str(config.get("rule_mode") or self._track_b_lane_spec.strategy_family),
            emit_signal=False,
            allow_fixture_input=False,
            output_root=self._resolve_repo_path(config.get("output_root") or "outputs/track_b_execution_core/track_b_strategy_rule_runner"),
            runner_id=f"{self._track_b_lane_spec.lane_id}_{feature_packet.bar_id}",
        )
        report = dict(result.report)
        self._latest_track_b_rule_report = {
            **report,
            "paper_runtime_adapter": TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
            "submit_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        if result.verdict != TrackBStrategyRuleRunnerVerdict.NO_SIGNAL:
            return SignalPacket(**payload)
        if report.get("decision_reason") != "Rule conditions passed, but --emit-signal was not supplied.":
            return SignalPacket(**payload)
        rule_evaluation = dict(report.get("rule_evaluation") or {})
        direction = str(report.get("signal_direction") or rule_evaluation.get("decision") or report.get("decision") or "").upper()
        source = str(config.get("entry_source") or config.get("rule_mode") or self._track_b_lane_spec.strategy_family)
        if direction == "LONG":
            payload.update(
                {
                    "long_entry_raw": True,
                    "recent_long_setup": True,
                    "long_entry": True,
                    "long_entry_source": source,
                }
            )
        elif direction == "SHORT":
            payload.update(
                {
                    "short_entry_raw": True,
                    "recent_short_setup": True,
                    "short_entry": True,
                    "short_entry_source": source,
                }
            )
        return SignalPacket(**payload)

    def _resolve_long_entry_family(self, signal_packet: SignalPacket) -> LongEntryFamily:
        if signal_packet.long_entry_source in self._promoted_sources():
            return LongEntryFamily.K
        return super()._resolve_long_entry_family(signal_packet)

    def _resolve_short_entry_family(self, signal_packet: SignalPacket) -> ShortEntryFamily:
        if signal_packet.short_entry_source in self._promoted_sources():
            return ShortEntryFamily.BEAR_SNAP
        return super()._resolve_short_entry_family(signal_packet)

    def _track_b_rule_runner_config(self) -> dict[str, Any]:
        return dict(getattr(self._track_b_lane_spec, "runtime_overlay_params", None) or {})

    def _promoted_sources(self) -> set[str]:
        config = self._track_b_rule_runner_config()
        return {
            str(item)
            for item in (
                config.get("entry_source"),
                config.get("rule_mode"),
                config.get("rule_id"),
                self._track_b_lane_spec.strategy_family,
            )
            if item
        }

    def _resolve_repo_path(self, value: object) -> Path:
        path = Path(str(value))
        if path.is_absolute():
            return path
        return self._track_b_repo_root / path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _state_freshness_blocker(
    payload: Mapping[str, Any],
    *,
    current_bar_end: datetime | None,
    config: Mapping[str, Any],
) -> str | None:
    if not payload:
        return "track_b_rule_runner_input_event_missing"
    if bool(config.get("require_timestamp_coherence", True)) is False:
        return None
    if current_bar_end is None:
        return "track_b_rule_runner_current_bar_missing"
    candidate_ts = _parse_ts(
        payload.get("candle_timestamp")
        or payload.get("bar_end_ts")
        or payload.get("timestamp")
        or payload.get("observed_at")
    )
    if candidate_ts is None:
        return "track_b_rule_runner_input_event_timestamp_missing"
    tolerance = float(config.get("timestamp_tolerance_seconds") or DEFAULT_TRACK_B_RULE_RUNNER_SIGNAL_FRESHNESS_SECONDS)
    if abs((current_bar_end.astimezone(UTC) - candidate_ts.astimezone(UTC)).total_seconds()) > tolerance:
        return "track_b_rule_runner_input_event_not_timestamp_coherent"
    return None


def _parse_ts(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
