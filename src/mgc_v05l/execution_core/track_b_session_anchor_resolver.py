"""Read-only Track B session anchor producer/resolver.

Session anchors are durable market-data context, not broker authority. This
module never connects to a broker and never mutates runtime or strategy state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


REPO_ROOT = Path(__file__).resolve().parents[3]
NEW_YORK = ZoneInfo("America/New_York")
SCHEMA_VERSION = "track_b_session_anchor_v1"
DEFAULT_SESSION_ANCHOR_ROOT = Path("outputs/track_b_execution_core/session_anchors")
DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT = Path("outputs/track_b_execution_core/phase1_runtime_market_data")
DEFAULT_PHASE1_INTRADAY_BACKFILL_ROOT = Path("outputs/track_b_execution_core/phase1_runtime_market_data_intraday_backfill")
DEFAULT_PHASE1_GAP_BACKFILL_ROOT = Path("outputs/track_b_execution_core/phase1_runtime_market_data_gap_backfill")
DEFAULT_HISTORICAL_BACKFILL_ROOT = Path("outputs/track_b_execution_core/session_anchor_historical_backfill")


class SessionAnchorType(str, Enum):
    US_0930_OPEN = "US_0930_OPEN"
    GLOBEX_1800_REOPEN = "GLOBEX_1800_REOPEN"
    LONDON_0300_OPEN = "LONDON_0300_OPEN"
    LONDON_LATE_0530_REFERENCE = "LONDON_LATE_0530_REFERENCE"
    ASIA_1800_OPEN = "ASIA_1800_OPEN"
    CHANGEOVER_REFERENCE = "CHANGEOVER_REFERENCE"
    ANCHORED_VWAP_START = "ANCHORED_VWAP_START"


class SessionAnchorStatus(str, Enum):
    READY = "READY"
    NOT_READY = "NOT_READY"
    STALE = "STALE"
    AMBIGUOUS = "AMBIGUOUS"


class SessionAnchorReasonCode(str, Enum):
    ANCHOR_READY = "ANCHOR_READY"
    ANCHOR_BAR_NOT_FOUND = "ANCHOR_BAR_NOT_FOUND"
    ANCHOR_SOURCE_STALE = "ANCHOR_SOURCE_STALE"
    ANCHOR_SESSION_DATE_MISMATCH = "ANCHOR_SESSION_DATE_MISMATCH"
    ANCHOR_AMBIGUOUS = "ANCHOR_AMBIGUOUS"
    ANCHOR_RECOVERED_FROM_HISTORICAL = "ANCHOR_RECOVERED_FROM_HISTORICAL"
    ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL = "ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL"
    ANCHOR_READY_FROM_RUNTIME = "ANCHOR_READY_FROM_RUNTIME"
    ANCHOR_READY_FROM_CANONICAL_ARTIFACT = "ANCHOR_READY_FROM_CANONICAL_ARTIFACT"
    NOT_READY_INVALID_TIMESTAMP = "NOT_READY_INVALID_TIMESTAMP"


@dataclass(frozen=True)
class TrackBSessionAnchorConfig:
    repo_root: Path = REPO_ROOT
    anchor_root: Path = DEFAULT_SESSION_ANCHOR_ROOT
    runtime_market_data_root: Path = DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT
    intraday_backfill_root: Path = DEFAULT_PHASE1_INTRADAY_BACKFILL_ROOT
    gap_backfill_root: Path = DEFAULT_PHASE1_GAP_BACKFILL_ROOT
    historical_backfill_root: Path = DEFAULT_HISTORICAL_BACKFILL_ROOT
    max_canonical_age_seconds: int = 7 * 24 * 60 * 60

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


@dataclass(frozen=True)
class SessionAnchorRequest:
    symbol: str
    anchor_type: SessionAnchorType
    as_of: datetime
    timeframe: str = "1m"


@dataclass(frozen=True)
class SessionAnchorResult:
    status: SessionAnchorStatus
    reason_code: SessionAnchorReasonCode
    symbol: str
    anchor_type: SessionAnchorType
    session_date_et: str | None
    anchor_time_et: str | None
    anchor_time_utc: datetime | None
    timeframe: str
    reference_price: str | None
    reference_price_field: str | None
    bar: Mapping[str, Any] | None
    source: str | None
    source_artifact_path: str | None
    source_generated_at: str | None
    searched_source_paths: tuple[str, ...]
    current_session_valid: bool
    source_stale: bool
    not_ready_reason: str | None = None

    @property
    def ready(self) -> bool:
        return self.status == SessionAnchorStatus.READY

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": self.status.value,
            "reason_code": self.reason_code.value,
            "symbol": self.symbol,
            "anchor_type": self.anchor_type.value,
            "session_date_et": self.session_date_et,
            "anchor_time_et": self.anchor_time_et,
            "anchor_time_utc": None if self.anchor_time_utc is None else self.anchor_time_utc.astimezone(UTC).isoformat(),
            "timeframe": self.timeframe,
            "reference_price": self.reference_price,
            "reference_price_field": self.reference_price_field,
            "bar": None if self.bar is None else dict(self.bar),
            "source": self.source,
            "source_artifact_path": self.source_artifact_path,
            "source_generated_at": self.source_generated_at,
            "freshness": {
                "current_session_valid": self.current_session_valid,
                "source_stale": self.source_stale,
            },
            "not_ready_reason": self.not_ready_reason,
            "searched_source_paths": list(self.searched_source_paths),
            "broker_independent": True,
            "read_only": True,
            "broker_mutation_allowed": False,
            "runtime_restart_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }


def resolve_session_anchor(
    symbol: str,
    anchor_type: SessionAnchorType | str,
    as_of: datetime,
    timeframe: str = "1m",
    *,
    config: TrackBSessionAnchorConfig | None = None,
) -> SessionAnchorResult:
    """Resolve a session anchor from authoritative read-only artifacts."""

    actual_config = config or TrackBSessionAnchorConfig()
    normalized_symbol = _normalize_symbol(symbol)
    normalized_anchor_type = _normalize_anchor_type(anchor_type)
    if _is_naive(as_of):
        return _invalid_timestamp_result(
            symbol=normalized_symbol,
            anchor_type=normalized_anchor_type,
            timeframe=timeframe,
            searched=(),
        )
    request = SessionAnchorRequest(
        symbol=normalized_symbol,
        anchor_type=normalized_anchor_type,
        as_of=as_of.astimezone(UTC),
        timeframe=str(timeframe or "1m"),
    )
    expected = _expected_anchor(request)
    searched: list[Path] = []

    ready_results: list[SessionAnchorResult] = []
    canonical = _read_canonical_anchor(config=actual_config, request=request, expected=expected, searched=searched)
    if canonical.status == SessionAnchorStatus.READY:
        ready_results.append(canonical)
    elif canonical.status == SessionAnchorStatus.STALE:
        return canonical

    for source_path, source_kind in _source_paths(actual_config, request):
        searched.append(source_path)
        match = _resolve_from_candle_artifact(
            path=source_path,
            source_kind=source_kind,
            request=request,
            expected=expected,
            searched=tuple(str(path) for path in searched),
        )
        if match.status == SessionAnchorStatus.READY:
            ready_results.append(match)

    return _select_ready_or_not_ready(request=request, expected=expected, ready_results=ready_results, searched=searched)


def produce_session_anchor(
    symbol: str,
    anchor_type: SessionAnchorType | str,
    as_of: datetime,
    timeframe: str = "1m",
    *,
    config: TrackBSessionAnchorConfig | None = None,
) -> SessionAnchorResult:
    """Resolve and write a canonical read-only anchor artifact."""

    actual_config = config or TrackBSessionAnchorConfig()
    result = resolve_session_anchor(symbol, anchor_type, as_of, timeframe, config=actual_config)
    request_anchor_type = _normalize_anchor_type(anchor_type)
    session_date_et = result.session_date_et
    if session_date_et is None and not _is_naive(as_of):
        session_date_et = _expected_anchor(
            SessionAnchorRequest(
                symbol=_normalize_symbol(symbol),
                anchor_type=request_anchor_type,
                as_of=as_of.astimezone(UTC),
                timeframe=str(timeframe or "1m"),
            )
        ).session_date_et.isoformat()
    output_path = _canonical_anchor_path(
        config=actual_config,
        symbol=_normalize_symbol(symbol),
        session_date_et=session_date_et or "unknown_session_date",
        anchor_type=request_anchor_type,
    )
    payload = {
        **result.to_dict(),
        "generated_at": datetime.now(UTC).isoformat(),
        "artifact_path": str(output_path),
    }
    write_json_atomic(output_path, payload)
    return result


def produce_session_anchors_for_symbols(
    *,
    symbols: Sequence[str],
    anchor_types: Sequence[SessionAnchorType | str],
    as_of: datetime,
    timeframe: str = "1m",
    config: TrackBSessionAnchorConfig | None = None,
) -> dict[str, Any]:
    actual_config = config or TrackBSessionAnchorConfig()
    results = []
    for symbol in symbols:
        for anchor_type in anchor_types:
            result = produce_session_anchor(symbol, anchor_type, as_of, timeframe, config=actual_config)
            results.append(result.to_dict())
    return {
        "schema_version": "track_b_session_anchor_producer_report_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "read_only": True,
        "broker_mutation_allowed": False,
        "runtime_restart_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "result_count": len(results),
        "results": results,
    }


@dataclass(frozen=True)
class _ExpectedAnchor:
    session_date_et: date
    anchor_time_et: time
    anchor_time_utc: datetime


def _expected_anchor(request: SessionAnchorRequest) -> _ExpectedAnchor:
    local = request.as_of.astimezone(NEW_YORK)
    anchor_time = _anchor_time(request.anchor_type)
    session_date = local.date()
    if request.anchor_type in {SessionAnchorType.GLOBEX_1800_REOPEN, SessionAnchorType.ASIA_1800_OPEN}:
        if local.timetz().replace(tzinfo=None) < anchor_time:
            session_date = session_date - timedelta(days=1)
    elif request.anchor_type in {SessionAnchorType.CHANGEOVER_REFERENCE, SessionAnchorType.ANCHORED_VWAP_START}:
        if local.timetz().replace(tzinfo=None) < anchor_time:
            session_date = session_date - timedelta(days=1)
    anchor_et = datetime.combine(session_date, anchor_time, tzinfo=NEW_YORK)
    return _ExpectedAnchor(session_date_et=session_date, anchor_time_et=anchor_time, anchor_time_utc=anchor_et.astimezone(UTC))


def _anchor_time(anchor_type: SessionAnchorType) -> time:
    if anchor_type == SessionAnchorType.US_0930_OPEN:
        return time(9, 30)
    if anchor_type in {SessionAnchorType.GLOBEX_1800_REOPEN, SessionAnchorType.ASIA_1800_OPEN}:
        return time(18, 0)
    if anchor_type == SessionAnchorType.LONDON_0300_OPEN:
        return time(3, 0)
    if anchor_type == SessionAnchorType.LONDON_LATE_0530_REFERENCE:
        return time(5, 30)
    if anchor_type == SessionAnchorType.CHANGEOVER_REFERENCE:
        return time(3, 0)
    if anchor_type == SessionAnchorType.ANCHORED_VWAP_START:
        return time(18, 0)
    raise ValueError(f"Unsupported anchor type: {anchor_type}")


def _read_canonical_anchor(
    *,
    config: TrackBSessionAnchorConfig,
    request: SessionAnchorRequest,
    expected: _ExpectedAnchor,
    searched: list[Path],
) -> SessionAnchorResult:
    path = _canonical_anchor_path(
        config=config,
        symbol=request.symbol,
        session_date_et=expected.session_date_et.isoformat(),
        anchor_type=request.anchor_type,
    )
    searched.append(path)
    payload = _read_json(path)
    if not payload:
        return _not_ready_result(
            request=request,
            expected=expected,
            reason_code=SessionAnchorReasonCode.ANCHOR_BAR_NOT_FOUND,
            not_ready_reason="CANONICAL_ANCHOR_ARTIFACT_MISSING",
            searched=tuple(str(item) for item in searched),
        )
    artifact_session_date = str(payload.get("session_date_et") or "")
    if artifact_session_date != expected.session_date_et.isoformat():
        return _not_ready_result(
            request=request,
            expected=expected,
            status=SessionAnchorStatus.STALE,
            reason_code=SessionAnchorReasonCode.ANCHOR_SESSION_DATE_MISMATCH,
            not_ready_reason="CANONICAL_ANCHOR_SESSION_DATE_MISMATCH",
            searched=tuple(str(item) for item in searched),
        )
    if str(payload.get("status") or "").upper() != SessionAnchorStatus.READY.value:
        return _not_ready_result(
            request=request,
            expected=expected,
            reason_code=SessionAnchorReasonCode.ANCHOR_BAR_NOT_FOUND,
            not_ready_reason=str(payload.get("not_ready_reason") or "CANONICAL_ANCHOR_NOT_READY"),
            searched=tuple(str(item) for item in searched),
        )
    anchor_time = _parse_ts(payload.get("anchor_time_utc"))
    if anchor_time is None or anchor_time.astimezone(UTC) != expected.anchor_time_utc:
        return _not_ready_result(
            request=request,
            expected=expected,
            status=SessionAnchorStatus.STALE,
            reason_code=SessionAnchorReasonCode.ANCHOR_SESSION_DATE_MISMATCH,
            not_ready_reason="CANONICAL_ANCHOR_TIME_MISMATCH",
            searched=tuple(str(item) for item in searched),
        )
    bar = payload.get("bar")
    if not isinstance(bar, Mapping):
        return _not_ready_result(
            request=request,
            expected=expected,
            reason_code=SessionAnchorReasonCode.ANCHOR_BAR_NOT_FOUND,
            not_ready_reason="CANONICAL_ANCHOR_BAR_MISSING",
            searched=tuple(str(item) for item in searched),
        )
    return SessionAnchorResult(
        status=SessionAnchorStatus.READY,
        reason_code=SessionAnchorReasonCode.ANCHOR_READY_FROM_CANONICAL_ARTIFACT,
        symbol=request.symbol,
        anchor_type=request.anchor_type,
        session_date_et=expected.session_date_et.isoformat(),
        anchor_time_et=expected.anchor_time_et.isoformat(),
        anchor_time_utc=expected.anchor_time_utc,
        timeframe=str(payload.get("timeframe") or request.timeframe),
        reference_price=_optional_text(payload.get("reference_price")),
        reference_price_field=_optional_text(payload.get("reference_price_field")) or "open",
        bar=dict(bar),
        source=_optional_text(payload.get("source")) or "CANONICAL_SESSION_ANCHOR",
        source_artifact_path=_optional_text(payload.get("source_artifact_path")) or str(path),
        source_generated_at=_optional_text(payload.get("source_generated_at") or payload.get("generated_at")),
        searched_source_paths=tuple(str(item) for item in searched),
        current_session_valid=True,
        source_stale=False,
    )


def _resolve_from_candle_artifact(
    *,
    path: Path,
    source_kind: str,
    request: SessionAnchorRequest,
    expected: _ExpectedAnchor,
    searched: tuple[str, ...],
) -> SessionAnchorResult:
    payload = _read_json(path)
    bars = payload.get("bars") or payload.get("candles") or []
    if not isinstance(bars, list):
        bars = []
    primary_matches = []
    legacy_matches = []
    for row in bars:
        if not isinstance(row, Mapping):
            continue
        bar = _bar_from_row(row)
        if bar is None:
            continue
        if _bar_contains_anchor_primary(bar, expected.anchor_time_utc):
            primary_matches.append(bar)
        elif _bar_contains_anchor_legacy(bar, expected.anchor_time_utc):
            legacy_matches.append(bar)
    matches = primary_matches or legacy_matches
    if len(matches) > 1:
        return _not_ready_result(
            request=request,
            expected=expected,
            status=SessionAnchorStatus.AMBIGUOUS,
            reason_code=SessionAnchorReasonCode.ANCHOR_AMBIGUOUS,
            not_ready_reason="MULTIPLE_ANCHOR_BARS_MATCH",
            searched=searched,
        )
    if not matches:
        return _not_ready_result(
            request=request,
            expected=expected,
            reason_code=SessionAnchorReasonCode.ANCHOR_BAR_NOT_FOUND,
            not_ready_reason="ANCHOR_BAR_NOT_FOUND",
            searched=searched,
        )
    bar = matches[0]
    reason = _ready_reason_for_source(source_kind)
    return SessionAnchorResult(
        status=SessionAnchorStatus.READY,
        reason_code=reason,
        symbol=request.symbol,
        anchor_type=request.anchor_type,
        session_date_et=expected.session_date_et.isoformat(),
        anchor_time_et=expected.anchor_time_et.isoformat(),
        anchor_time_utc=expected.anchor_time_utc,
        timeframe=request.timeframe,
        reference_price=None if bar.open is None else str(bar.open),
        reference_price_field="open",
        bar=_bar_to_payload(bar, request.timeframe),
        source=source_kind,
        source_artifact_path=str(path),
        source_generated_at=_optional_text(payload.get("generated_at")),
        searched_source_paths=searched,
        current_session_valid=True,
        source_stale=False,
    )


def _select_ready_or_not_ready(
    *,
    request: SessionAnchorRequest,
    expected: _ExpectedAnchor,
    ready_results: Sequence[SessionAnchorResult],
    searched: Sequence[Path],
) -> SessionAnchorResult:
    if not ready_results:
        return _not_ready_result(
            request=request,
            expected=expected,
            reason_code=SessionAnchorReasonCode.ANCHOR_BAR_NOT_FOUND,
            not_ready_reason="ANCHOR_BAR_NOT_FOUND",
            searched=tuple(str(item) for item in searched),
        )
    unique = {
        (
            result.reference_price,
            None if result.bar is None else str(result.bar.get("bar_start")),
            None if result.bar is None else str(result.bar.get("bar_end")),
        )
        for result in ready_results
    }
    if len(unique) > 1:
        return _not_ready_result(
            request=request,
            expected=expected,
            status=SessionAnchorStatus.AMBIGUOUS,
            reason_code=SessionAnchorReasonCode.ANCHOR_AMBIGUOUS,
            not_ready_reason="CONFLICTING_ANCHOR_SOURCES",
            searched=tuple(str(item) for item in searched),
        )
    priority = {
        SessionAnchorReasonCode.ANCHOR_READY_FROM_RUNTIME: 0,
        SessionAnchorReasonCode.ANCHOR_READY_FROM_CANONICAL_ARTIFACT: 1,
        SessionAnchorReasonCode.ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL: 2,
        SessionAnchorReasonCode.ANCHOR_RECOVERED_FROM_HISTORICAL: 3,
    }
    return min(ready_results, key=lambda item: priority.get(item.reason_code, 99))


def _source_paths(config: TrackBSessionAnchorConfig, request: SessionAnchorRequest) -> list[tuple[Path, str]]:
    runtime = config.resolve(config.runtime_market_data_root) / request.symbol / request.timeframe / "latest_runtime_candles.json"
    intraday = (
        config.resolve(config.intraday_backfill_root)
        / request.symbol
        / request.timeframe
        / "latest_runtime_candles.json"
    )
    intraday_anchor = (
        config.resolve(config.intraday_backfill_root)
        / _gap_backfill_subdir(request.anchor_type)
        / request.symbol
        / request.timeframe
        / "latest_runtime_candles.json"
    )
    gap = (
        config.resolve(config.gap_backfill_root)
        / _gap_backfill_subdir(request.anchor_type)
        / request.symbol
        / request.timeframe
        / "latest_runtime_candles.json"
    )
    historical = (
        config.resolve(config.historical_backfill_root)
        / request.symbol
        / request.timeframe
        / "latest_runtime_candles.json"
    )
    return [
        (runtime, "LIVE_PHASE1"),
        (intraday, "RECOVERED_PHASE1_1M"),
        (intraday_anchor, "RECOVERED_PHASE1_1M"),
        (gap, "RECOVERED_PHASE1_1M"),
        (historical, "HISTORICAL_BACKFILL"),
    ]


def _gap_backfill_subdir(anchor_type: SessionAnchorType) -> str:
    if anchor_type == SessionAnchorType.US_0930_OPEN:
        return "us_session_reference"
    if anchor_type in {SessionAnchorType.GLOBEX_1800_REOPEN, SessionAnchorType.ASIA_1800_OPEN, SessionAnchorType.ANCHORED_VWAP_START}:
        return "globex_session_reference"
    if anchor_type in {
        SessionAnchorType.LONDON_0300_OPEN,
        SessionAnchorType.LONDON_LATE_0530_REFERENCE,
        SessionAnchorType.CHANGEOVER_REFERENCE,
    }:
        return "london_session_reference"
    return str(anchor_type.value).lower()


def _canonical_anchor_path(
    *,
    config: TrackBSessionAnchorConfig,
    symbol: str,
    session_date_et: str,
    anchor_type: SessionAnchorType,
) -> Path:
    return config.resolve(config.anchor_root) / symbol / session_date_et / f"{anchor_type.value}.json"


def _not_ready_result(
    *,
    request: SessionAnchorRequest,
    expected: _ExpectedAnchor,
    reason_code: SessionAnchorReasonCode,
    not_ready_reason: str,
    searched: tuple[str, ...],
    status: SessionAnchorStatus = SessionAnchorStatus.NOT_READY,
) -> SessionAnchorResult:
    return SessionAnchorResult(
        status=status,
        reason_code=reason_code,
        symbol=request.symbol,
        anchor_type=request.anchor_type,
        session_date_et=expected.session_date_et.isoformat(),
        anchor_time_et=expected.anchor_time_et.isoformat(),
        anchor_time_utc=expected.anchor_time_utc,
        timeframe=request.timeframe,
        reference_price=None,
        reference_price_field=None,
        bar=None,
        source=None,
        source_artifact_path=None,
        source_generated_at=None,
        searched_source_paths=searched,
        current_session_valid=False,
        source_stale=status == SessionAnchorStatus.STALE,
        not_ready_reason=not_ready_reason,
    )


def _invalid_timestamp_result(
    *,
    symbol: str,
    anchor_type: SessionAnchorType,
    timeframe: str,
    searched: tuple[str, ...],
) -> SessionAnchorResult:
    return SessionAnchorResult(
        status=SessionAnchorStatus.NOT_READY,
        reason_code=SessionAnchorReasonCode.NOT_READY_INVALID_TIMESTAMP,
        symbol=symbol,
        anchor_type=anchor_type,
        session_date_et=None,
        anchor_time_et=None,
        anchor_time_utc=None,
        timeframe=timeframe,
        reference_price=None,
        reference_price_field=None,
        bar=None,
        source=None,
        source_artifact_path=None,
        source_generated_at=None,
        searched_source_paths=searched,
        current_session_valid=False,
        source_stale=False,
        not_ready_reason="AS_OF_TIMESTAMP_MUST_BE_TIMEZONE_AWARE",
    )


def _ready_reason_for_source(source_kind: str) -> SessionAnchorReasonCode:
    if source_kind == "LIVE_PHASE1":
        return SessionAnchorReasonCode.ANCHOR_READY_FROM_RUNTIME
    if source_kind == "RECOVERED_PHASE1_1M":
        return SessionAnchorReasonCode.ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL
    if source_kind == "HISTORICAL_BACKFILL":
        return SessionAnchorReasonCode.ANCHOR_RECOVERED_FROM_HISTORICAL
    return SessionAnchorReasonCode.ANCHOR_READY


def _bar_contains_anchor(bar: Any, anchor_time_utc: datetime) -> bool:
    return _bar_contains_anchor_primary(bar, anchor_time_utc) or _bar_contains_anchor_legacy(bar, anchor_time_utc)


def _bar_contains_anchor_primary(bar: Any, anchor_time_utc: datetime) -> bool:
    start = bar.start_ts.astimezone(UTC)
    end = bar.end_ts.astimezone(UTC)
    anchor = anchor_time_utc.astimezone(UTC)
    return start <= anchor < end


def _bar_contains_anchor_legacy(bar: Any, anchor_time_utc: datetime) -> bool:
    end = bar.end_ts.astimezone(UTC)
    anchor = anchor_time_utc.astimezone(UTC)
    return end == anchor


def _bar_from_row(row: Mapping[str, Any]) -> Any | None:
    start = _parse_ts(row.get("bar_start") or row.get("start_ts") or row.get("start"))
    end = _parse_ts(row.get("bar_end") or row.get("end_ts") or row.get("timestamp") or row.get("end"))
    if start is None or end is None:
        return None
    return SimpleNamespace(
        start_ts=start,
        end_ts=end,
        open=row.get("open"),
        high=row.get("high"),
        low=row.get("low"),
        close=row.get("close"),
        volume=row.get("volume"),
    )


def _bar_to_payload(bar: Any, timeframe: str) -> dict[str, Any]:
    return {
        "bar_start": bar.start_ts.astimezone(UTC).isoformat(),
        "bar_end": bar.end_ts.astimezone(UTC).isoformat(),
        "open": None if bar.open is None else str(bar.open),
        "high": None if bar.high is None else str(bar.high),
        "low": None if bar.low is None else str(bar.low),
        "close": None if bar.close is None else str(bar.close),
        "volume": None if bar.volume is None else str(bar.volume),
        "timeframe": timeframe,
    }


def _normalize_anchor_type(value: SessionAnchorType | str) -> SessionAnchorType:
    return value if isinstance(value, SessionAnchorType) else SessionAnchorType(str(value).strip().upper())


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _is_naive(value: datetime) -> bool:
    return not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None


def _parse_ts(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce read-only Track B session anchor artifacts.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--symbols", nargs="+", default=["MNQ", "MES"])
    parser.add_argument(
        "--anchor-types",
        nargs="+",
        default=[
            SessionAnchorType.US_0930_OPEN.value,
            SessionAnchorType.GLOBEX_1800_REOPEN.value,
            SessionAnchorType.LONDON_0300_OPEN.value,
            SessionAnchorType.LONDON_LATE_0530_REFERENCE.value,
        ],
    )
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--output", type=Path, default=Path("outputs/track_b_execution_core/session_anchors/latest_session_anchor_report.json"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    as_of = _parse_ts(args.as_of) if args.as_of else datetime.now(UTC)
    if as_of is None:
        raise SystemExit("invalid --as-of timestamp")
    config = TrackBSessionAnchorConfig(repo_root=args.repo_root)
    report = produce_session_anchors_for_symbols(
        symbols=args.symbols,
        anchor_types=args.anchor_types,
        as_of=as_of,
        timeframe=args.timeframe,
        config=config,
    )
    output_path = config.resolve(args.output)
    write_json_atomic(output_path, report)
    print(json.dumps({"output_path": str(output_path), "result_count": report["result_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
