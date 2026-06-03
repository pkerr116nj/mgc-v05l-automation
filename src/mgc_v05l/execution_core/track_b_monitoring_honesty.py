"""Monitoring honesty contract for Track B operator reports.

The contract is intentionally small and mechanical: any report that claims a
live observation interval must carry enough timing metadata to prove what was
actually run.  Short checks are allowed, but they must be labeled as such.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping


class MonitoringMode(StrEnum):
    LIVE_CONTINUOUS_MONITOR = "LIVE_CONTINUOUS_MONITOR"
    TIMED_SAMPLING_MONITOR = "TIMED_SAMPLING_MONITOR"
    ONE_SHOT_STATUS_CHECK = "ONE_SHOT_STATUS_CHECK"


FORBIDDEN_UNPROVEN_PHRASES = (
    "observed for 15 minutes",
    "observed for fifteen minutes",
)


@dataclass(frozen=True)
class MonitoringHonestyContract:
    requested_duration_seconds: float
    actual_start_timestamp: datetime
    actual_end_timestamp: datetime
    sample_cadence_seconds: float | None
    sample_count: int
    command_or_process: str
    mode: MonitoringMode
    artifact_path: str | None

    @property
    def actual_elapsed_seconds(self) -> float:
        return max((self.actual_end_timestamp - self.actual_start_timestamp).total_seconds(), 0.0)

    @property
    def duration_fulfilled(self) -> bool:
        return self.actual_elapsed_seconds >= max(float(self.requested_duration_seconds), 0.0)

    @property
    def truthful_summary_label(self) -> str:
        if self.mode is MonitoringMode.ONE_SHOT_STATUS_CHECK:
            return "checked current status"
        if not self.duration_fulfilled:
            return "sampled briefly"
        return "monitored for requested interval"

    @property
    def duration_shortfall_seconds(self) -> float:
        return max(float(self.requested_duration_seconds) - self.actual_elapsed_seconds, 0.0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_duration_seconds": self.requested_duration_seconds,
            "actual_start_timestamp": _ensure_utc(self.actual_start_timestamp).isoformat(),
            "actual_end_timestamp": _ensure_utc(self.actual_end_timestamp).isoformat(),
            "actual_elapsed_seconds": self.actual_elapsed_seconds,
            "sample_cadence_seconds": self.sample_cadence_seconds,
            "sample_count": self.sample_count,
            "command_or_process": self.command_or_process,
            "monitoring_mode": self.mode.value,
            "continuous_sampled_or_one_shot": _mode_style(self.mode),
            "artifact_path": self.artifact_path,
            "duration_fulfilled": self.duration_fulfilled,
            "duration_shortfall_seconds": self.duration_shortfall_seconds,
            "truthful_summary_label": self.truthful_summary_label,
            "claim_guardrails": {
                "observed_for_15_minutes_allowed": self.actual_elapsed_seconds >= 900.0,
                "forbidden_unproven_phrases": list(FORBIDDEN_UNPROVEN_PHRASES),
            },
        }


def one_shot_monitoring_contract(
    *,
    started_at: datetime,
    ended_at: datetime,
    command_or_process: str,
    artifact_path: str | Path | None,
) -> MonitoringHonestyContract:
    return MonitoringHonestyContract(
        requested_duration_seconds=0.0,
        actual_start_timestamp=_ensure_utc(started_at),
        actual_end_timestamp=_ensure_utc(ended_at),
        sample_cadence_seconds=None,
        sample_count=1,
        command_or_process=command_or_process,
        mode=MonitoringMode.ONE_SHOT_STATUS_CHECK,
        artifact_path=str(artifact_path) if artifact_path is not None else None,
    )


def timed_sampling_monitoring_contract(
    *,
    requested_duration_seconds: float,
    actual_start_timestamp: datetime,
    actual_end_timestamp: datetime,
    sample_cadence_seconds: float,
    sample_count: int,
    command_or_process: str,
    artifact_path: str | Path | None,
    continuous: bool = False,
) -> MonitoringHonestyContract:
    return MonitoringHonestyContract(
        requested_duration_seconds=float(requested_duration_seconds),
        actual_start_timestamp=_ensure_utc(actual_start_timestamp),
        actual_end_timestamp=_ensure_utc(actual_end_timestamp),
        sample_cadence_seconds=float(sample_cadence_seconds),
        sample_count=int(sample_count),
        command_or_process=command_or_process,
        mode=MonitoringMode.LIVE_CONTINUOUS_MONITOR if continuous else MonitoringMode.TIMED_SAMPLING_MONITOR,
        artifact_path=str(artifact_path) if artifact_path is not None else None,
    )


def validate_monitoring_claim(*, claim_text: str, contract: Mapping[str, Any]) -> list[str]:
    """Return violations for operator-facing monitoring prose.

    This is deliberately phrase-based for the high-risk claim we want to ban.
    More nuanced prose remains allowed as long as the report includes the
    contract block and uses truthful labels such as "checked current status" or
    "sampled briefly".
    """

    lowered = claim_text.lower()
    elapsed = _float_or_zero(contract.get("actual_elapsed_seconds"))
    violations: list[str] = []
    if any(phrase in lowered for phrase in FORBIDDEN_UNPROVEN_PHRASES) and elapsed < 900.0:
        violations.append("OBSERVED_FOR_15_MINUTES_REQUIRES_ELAPSED_SECONDS_AT_LEAST_900")
    if "live watch" in lowered and contract.get("monitoring_mode") == MonitoringMode.ONE_SHOT_STATUS_CHECK.value:
        violations.append("ONE_SHOT_STATUS_CHECK_CANNOT_BE_DESCRIBED_AS_LIVE_WATCH")
    requested = _float_or_zero(contract.get("requested_duration_seconds"))
    if requested > 0 and elapsed < requested and "observed" in lowered:
        violations.append("UNDER_DURATION_MONITOR_MUST_REPORT_SHORTFALL_PLAINLY")
    return violations


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _float_or_zero(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _mode_style(mode: MonitoringMode) -> str:
    if mode is MonitoringMode.LIVE_CONTINUOUS_MONITOR:
        return "continuous"
    if mode is MonitoringMode.TIMED_SAMPLING_MONITOR:
        return "sampled"
    return "one-shot"
