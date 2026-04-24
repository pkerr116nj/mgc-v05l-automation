"""Session and regime bucketing helpers."""

from __future__ import annotations

from datetime import datetime, time

from ..config.schemas import SessionFilter, ValidationConfig


def infer_session_label(timestamp: datetime, session_filters: tuple[SessionFilter, ...]) -> str:
    probe = timestamp.timetz().replace(tzinfo=None)
    for window in session_filters:
        start = time(window.start_hour, window.start_minute)
        end = time(window.end_hour, window.end_minute)
        if start <= end:
            if start <= probe <= end:
                return window.name
        elif probe >= start or probe <= end:
            return window.name
    return "unknown"


def build_session_labels(timestamps: tuple[datetime, ...], config: ValidationConfig) -> tuple[str, ...]:
    return tuple(infer_session_label(timestamp, config.session_filters) for timestamp in timestamps)


def build_regime_labels(length: int, config: ValidationConfig) -> tuple[str, ...]:
    if length <= 0:
        return ()
    labels = config.regime_config.labels
    bucket_count = max(1, min(config.regime_config.bucket_count, len(labels)))
    regimes: list[str] = []
    for index in range(length):
        bucket = min(bucket_count - 1, int(index * bucket_count / length))
        regimes.append(labels[bucket])
    return tuple(regimes)
