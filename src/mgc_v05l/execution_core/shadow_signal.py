"""Track B-native no-submit shadow signal input boundary."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_SHADOW_SIGNAL_OUTPUT_ROOT = Path("outputs/track_b_execution_core/shadow_signals")


class SignalDecisionStyle(str, Enum):
    BINARY = "BINARY"
    SCORED_STATIC = "SCORED_STATIC"
    SCORED_DYNAMIC = "SCORED_DYNAMIC"
    HUMAN_REVIEW = "HUMAN_REVIEW"


class SignalDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"
    NONE = "NONE"


class ScoreScale(str, Enum):
    ZERO_TO_ONE = "ZERO_TO_ONE"
    POINTS = "POINTS"
    RANK = "RANK"
    CUSTOM = "CUSTOM"


class ConfidenceLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    UNCALIBRATED = "UNCALIBRATED"


class ScoringAuthority(str, Enum):
    INFORMATIONAL_ONLY = "INFORMATIONAL_ONLY"


class ShadowSignalValidationVerdict(str, Enum):
    VALID_FOR_REVIEW = "SHADOW_SIGNAL_VALID_FOR_REVIEW"
    BLOCKED_LIVE_MODE = "SHADOW_SIGNAL_BLOCKED_LIVE_MODE"
    BLOCKED_MISSING_STRATEGY = "SHADOW_SIGNAL_BLOCKED_MISSING_STRATEGY"
    BLOCKED_MISSING_LANE = "SHADOW_SIGNAL_BLOCKED_MISSING_LANE"
    BLOCKED_MISSING_DIRECTION = "SHADOW_SIGNAL_BLOCKED_MISSING_DIRECTION"
    BLOCKED_MISSING_CONTRACT = "SHADOW_SIGNAL_BLOCKED_MISSING_CONTRACT"
    BLOCKED_MISSING_TIMESTAMP = "SHADOW_SIGNAL_BLOCKED_MISSING_TIMESTAMP"
    BLOCKED_SCORING_REQUIRED = "SHADOW_SIGNAL_BLOCKED_SCORING_REQUIRED"
    BLOCKED_INVALID_SCORE_RANGE = "SHADOW_SIGNAL_BLOCKED_INVALID_SCORE_RANGE"
    BLOCKED_SCHEMA_ERROR = "SHADOW_SIGNAL_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class ShadowSignalScoring:
    signal_score: Any = None
    score_scale: str | None = None
    probability_win: Any = None
    expected_value_r: Any = None
    expected_drawdown_r: Any = None
    confidence_level: str | None = None
    model_version: str | None = None
    calibration_window: str | None = None
    sample_size: Any = None
    regime_score: Any = None
    risk_quality: Any = None
    scoring_authority: str = ScoringAuthority.INFORMATIONAL_ONLY.value

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ShadowSignalScoring":
        score_scale = _optional_enum(ScoreScale, payload.get("score_scale"), "score_scale")
        confidence = _optional_enum(ConfidenceLevel, payload.get("confidence_level"), "confidence_level")
        return cls(
            signal_score=payload.get("signal_score"),
            score_scale=score_scale,
            probability_win=payload.get("probability_win"),
            expected_value_r=payload.get("expected_value_r"),
            expected_drawdown_r=payload.get("expected_drawdown_r"),
            confidence_level=confidence,
            model_version=_optional_str(payload.get("model_version")),
            calibration_window=_optional_str(payload.get("calibration_window")),
            sample_size=payload.get("sample_size"),
            regime_score=payload.get("regime_score"),
            risk_quality=payload.get("risk_quality"),
            scoring_authority=ScoringAuthority.INFORMATIONAL_ONLY.value,
        )

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "signal_score": self.signal_score,
            "score_scale": self.score_scale,
            "probability_win": self.probability_win,
            "expected_value_r": self.expected_value_r,
            "expected_drawdown_r": self.expected_drawdown_r,
            "confidence_level": self.confidence_level,
            "model_version": self.model_version,
            "calibration_window": self.calibration_window,
            "sample_size": self.sample_size,
            "regime_score": self.regime_score,
            "risk_quality": self.risk_quality,
            "scoring_authority": ScoringAuthority.INFORMATIONAL_ONLY.value,
        }


@dataclass(frozen=True)
class ShadowSignal:
    signal_id: str | None
    strategy_id: str
    lane_id: str
    signal_type: str
    signal_direction: str
    decision_style: str
    mode: str
    account_id: str
    local_execution_contract_key: str
    instrument_family: str | None
    signal_timestamp: datetime | None
    observed_at: datetime | None
    source: str
    reason: str
    scoring: ShadowSignalScoring | None
    metadata: Mapping[str, Any]
    submit_requested: bool = False
    live_money_readiness: bool = False

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ShadowSignal":
        scoring_payload = payload.get("scoring")
        if scoring_payload is not None and not isinstance(scoring_payload, Mapping):
            raise ShadowSignalSchemaError("scoring must be an object when provided.")
        metadata_payload = payload.get("metadata") or {}
        if not isinstance(metadata_payload, Mapping):
            raise ShadowSignalSchemaError("metadata must be an object when provided.")
        try:
            return cls(
                signal_id=_optional_str(payload.get("signal_id")),
                strategy_id=str(payload.get("strategy_id") or "").strip(),
                lane_id=str(payload.get("lane_id") or "").strip(),
                signal_type=str(payload.get("signal_type") or "").strip(),
                signal_direction=_optional_enum(SignalDirection, payload.get("signal_direction"), "signal_direction"),
                decision_style=_enum_value(SignalDecisionStyle, payload.get("decision_style") or SignalDecisionStyle.BINARY.value, "decision_style"),
                mode=str(payload.get("mode") or "").strip().upper(),
                account_id=str(payload.get("account_id") or "").strip(),
                local_execution_contract_key=str(payload.get("local_execution_contract_key") or payload.get("contract_key") or "").strip(),
                instrument_family=_optional_str(payload.get("instrument_family") or payload.get("symbol")),
                signal_timestamp=_parse_optional_timestamp(payload.get("signal_timestamp"), "signal_timestamp"),
                observed_at=_parse_optional_timestamp(payload.get("observed_at") or payload.get("decision_timestamp"), "observed_at"),
                source=str(payload.get("source") or "").strip(),
                reason=str(payload.get("reason") or "").strip(),
                scoring=ShadowSignalScoring.from_mapping(scoring_payload) if isinstance(scoring_payload, Mapping) else None,
                metadata=dict(metadata_payload),
                submit_requested=bool(payload.get("submit_requested", False)),
                live_money_readiness=bool(payload.get("live_money_readiness", False)),
            )
        except (ValueError, TypeError) as exc:
            raise ShadowSignalSchemaError(str(exc)) from exc


class ShadowSignalSchemaError(ValueError):
    """Raised when a shadow signal payload cannot be interpreted."""


@dataclass(frozen=True)
class ShadowSignalValidationResult:
    verdict: ShadowSignalValidationVerdict
    report_json: Path
    report: dict[str, Any]
    signal: ShadowSignal | None


@dataclass(frozen=True)
class ShadowSignalValidationConfig:
    expected_account_id: str | None = None
    output_root: Path = DEFAULT_SHADOW_SIGNAL_OUTPUT_ROOT


def validate_shadow_signal(
    *,
    payload: Mapping[str, Any],
    config: ShadowSignalValidationConfig | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> ShadowSignalValidationResult:
    actual_config = config or ShadowSignalValidationConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or f"shadow_signal_{uuid.uuid4().hex}"
    report_json = Path(actual_config.output_root) / actual_run_id / "shadow_signal_report.json"
    try:
        signal = ShadowSignal.from_mapping(payload)
    except ShadowSignalSchemaError as exc:
        return _write_report(
            report_json=report_json,
            verdict=ShadowSignalValidationVerdict.BLOCKED_SCHEMA_ERROR,
            signal=None,
            primary_blocker=str(exc),
            required_next_action="Fix shadow signal schema before Track B review.",
            now=actual_now,
        )
    verdict, blocker, action, secondary = _classify_signal(signal, actual_config)
    return _write_report(
        report_json=report_json,
        verdict=verdict,
        signal=signal,
        primary_blocker=blocker,
        required_next_action=action,
        now=actual_now,
        secondary_blockers=secondary,
    )


def _classify_signal(
    signal: ShadowSignal,
    config: ShadowSignalValidationConfig,
) -> tuple[ShadowSignalValidationVerdict, str | None, str, tuple[str, ...]]:
    secondary: list[str] = []
    if signal.mode != "PAPER":
        return (
            ShadowSignalValidationVerdict.BLOCKED_LIVE_MODE,
            "Shadow signal boundary accepts PAPER mode only.",
            "Use PAPER mode. Live-money shadow signals are not supported.",
            tuple(secondary),
        )
    if not signal.strategy_id:
        return (
            ShadowSignalValidationVerdict.BLOCKED_MISSING_STRATEGY,
            "strategy_id is required.",
            "Add an explicit Track B strategy_id before review.",
            tuple(secondary),
        )
    if not signal.lane_id:
        return (
            ShadowSignalValidationVerdict.BLOCKED_MISSING_LANE,
            "lane_id is required.",
            "Add an explicit Track B lane_id before review.",
            tuple(secondary),
        )
    if not signal.signal_direction:
        return (
            ShadowSignalValidationVerdict.BLOCKED_MISSING_DIRECTION,
            "signal_direction is required.",
            "Emit LONG, SHORT, FLAT, or NONE before review.",
            tuple(secondary),
        )
    if not signal.local_execution_contract_key:
        return (
            ShadowSignalValidationVerdict.BLOCKED_MISSING_CONTRACT,
            "local_execution_contract_key is required.",
            "Use an explicit Track B execution contract key.",
            tuple(secondary),
        )
    if signal.signal_timestamp is None:
        return (
            ShadowSignalValidationVerdict.BLOCKED_MISSING_TIMESTAMP,
            "signal_timestamp is required.",
            "Emit a timezone-aware signal_timestamp before review.",
            tuple(secondary),
        )
    if signal.decision_style in {SignalDecisionStyle.SCORED_STATIC.value, SignalDecisionStyle.SCORED_DYNAMIC.value} and signal.scoring is None:
        return (
            ShadowSignalValidationVerdict.BLOCKED_SCORING_REQUIRED,
            f"{signal.decision_style} signals require an informational scoring block.",
            "Add scoring metadata or use BINARY/HUMAN_REVIEW decision_style.",
            tuple(secondary),
        )
    score_error = _score_error(signal.scoring)
    if score_error is not None:
        return (
            ShadowSignalValidationVerdict.BLOCKED_INVALID_SCORE_RANGE,
            score_error,
            "Fix scoring ranges before Track B signal review.",
            tuple(secondary),
        )
    if signal.decision_style == SignalDecisionStyle.SCORED_DYNAMIC.value:
        secondary.append("SCORED_DYNAMIC schema is accepted, but dynamic scoring logic is not implemented.")
    if signal.submit_requested:
        secondary.append("Signal claimed submit_requested; Track B shadow signal reports force submit_allowed=false.")
    if signal.live_money_readiness:
        secondary.append("Signal claimed live_money_readiness; Track B shadow signal reports force this to false.")
    if config.expected_account_id and signal.account_id and signal.account_id != config.expected_account_id:
        secondary.append("Signal account_id differs from expected account; downstream intent/lane gates must enforce account policy.")
    return (
        ShadowSignalValidationVerdict.VALID_FOR_REVIEW,
        None,
        "Shadow signal is valid for no-submit review. Intent creation, lane authorization, order planning, readiness, and proof gates remain separate.",
        tuple(secondary),
    )


def _write_report(
    *,
    report_json: Path,
    verdict: ShadowSignalValidationVerdict,
    signal: ShadowSignal | None,
    primary_blocker: str | None,
    required_next_action: str,
    now: datetime,
    secondary_blockers: tuple[str, ...] = (),
) -> ShadowSignalValidationResult:
    allowed = verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    scoring = signal.scoring if signal is not None else None
    report = {
        "schema_version": "track_b_shadow_signal_validation_v1",
        "generated_at": now.isoformat(),
        "shadow_signal_validation_verdict": verdict.value,
        "signal_allowed_for_review": allowed,
        "decision_style": None if signal is None else signal.decision_style,
        "scoring_present": scoring is not None,
        "scoring_authority": ScoringAuthority.INFORMATIONAL_ONLY.value,
        "dynamic_scoring_implemented": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": list(secondary_blockers),
        "required_next_action": required_next_action,
        "signal_id": None if signal is None else signal.signal_id or _signal_id(signal),
        "strategy_id": None if signal is None else signal.strategy_id,
        "lane_id": None if signal is None else signal.lane_id,
        "signal_type": None if signal is None else signal.signal_type,
        "signal_direction": None if signal is None else signal.signal_direction,
        "mode": None if signal is None else signal.mode,
        "account_id": None if signal is None else signal.account_id,
        "local_execution_contract_key": None if signal is None else signal.local_execution_contract_key,
        "instrument_family": None if signal is None else signal.instrument_family,
        "signal_timestamp": None if signal is None or signal.signal_timestamp is None else signal.signal_timestamp.isoformat(),
        "observed_at": None if signal is None or signal.observed_at is None else signal.observed_at.isoformat(),
        "source": None if signal is None else signal.source,
        "reason": None if signal is None else signal.reason,
        "scoring": None if scoring is None else scoring.to_report_dict(),
        "metadata": {} if signal is None else dict(signal.metadata),
        "submit_requested": False if signal is None else signal.submit_requested,
        "live_money_readiness": False,
        "signal_is_intent": False,
        "signal_authorizes_lane": False,
        "signal_creates_order_plan": False,
        "order_plan_created": False,
        "manifest_registry_readiness_gates_bypassed": False,
        "local_execution_contract_key_is_execution_authority": True,
        "metadata_is_authoritative": False,
        "scoring_is_execution_authority": False,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return ShadowSignalValidationResult(verdict=verdict, report_json=report_json, report=report, signal=signal)


def _score_error(scoring: ShadowSignalScoring | None) -> str | None:
    if scoring is None:
        return None
    if scoring.signal_score is not None and scoring.score_scale == ScoreScale.ZERO_TO_ONE.value:
        score = _decimal(scoring.signal_score, "signal_score")
        if score < 0 or score > 1:
            return "signal_score must be between 0 and 1 when score_scale is ZERO_TO_ONE."
    if scoring.probability_win is not None:
        probability = _decimal(scoring.probability_win, "probability_win")
        if probability < 0 or probability > 1:
            return "probability_win must be between 0 and 1."
    if scoring.sample_size is not None:
        sample_size = _decimal(scoring.sample_size, "sample_size")
        if sample_size < 0 or sample_size != sample_size.to_integral_value():
            return "sample_size must be a non-negative integer when provided."
    return None


def _enum_value(enum_type: type[Enum], value: object, field_name: str) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        raise ShadowSignalSchemaError(f"{field_name} is required.")
    try:
        return enum_type(raw).value  # type: ignore[call-arg, return-value]
    except ValueError as exc:
        allowed = ", ".join(str(item.value) for item in enum_type)
        raise ShadowSignalSchemaError(f"{field_name} must be one of: {allowed}.") from exc


def _optional_enum(enum_type: type[Enum], value: object, field_name: str) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    try:
        return enum_type(raw).value  # type: ignore[call-arg, return-value]
    except ValueError as exc:
        allowed = ", ".join(str(item.value) for item in enum_type)
        raise ShadowSignalSchemaError(f"{field_name} must be one of: {allowed}.") from exc


def _optional_str(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _parse_optional_timestamp(value: object, field_name: str) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        require_aware_datetime(value, field_name)
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShadowSignalSchemaError(f"{field_name} must be an ISO timestamp.") from exc
    require_aware_datetime(parsed, field_name)
    return parsed


def _decimal(value: object, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ShadowSignalSchemaError(f"{field_name} must be decimal-compatible.") from exc


def _signal_id(signal: ShadowSignal) -> str:
    raw = "|".join(
        str(value or "")
        for value in (
            signal.strategy_id,
            signal.lane_id,
            signal.signal_type,
            signal.signal_direction,
            signal.local_execution_contract_key,
            None if signal.signal_timestamp is None else signal.signal_timestamp.isoformat(),
            signal.source,
        )
    )
    return "shadow_signal_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
