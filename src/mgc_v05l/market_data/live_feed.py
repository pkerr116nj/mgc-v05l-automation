"""Live ingestion scaffolding for completed-bar market data."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Optional

from ..domain.models import Bar
from .bar_models import build_bar_id
from ..persistence.repositories import RepositorySet
from .databento_provider import DatabentoHttpError, DatabentoMarketDataProvider
from .canonical_maintenance import CanonicalMarketDataMaintenanceService
from .phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS, classify_phase1_futures_market_session
from .provider_models import HistoricalBarsRequest
from .schwab_adapter import SchwabMarketDataAdapter
from .schwab_models import (
    SchwabHistoricalClient,
    SchwabHistoricalRequest,
    SchwabLivePollRequest,
    SchwabLivePollingClient,
    SchwabLiveStreamClient,
)
from .timeframes import normalize_timeframe_label, timeframe_minutes

_DATABENTO_LIVE_POLL_SAFETY_DELAY_SECONDS = 10
_DATABENTO_LIVE_GATEWAY_PORT = 13000
_DATABENTO_LIVE_SESSION_WAIT_SECONDS = 8.0
_DATABENTO_LIVE_REPLAY_LOOKBACK_CAP_MINUTES = 10
_MARKET_DATA_STALE_GRACE_SECONDS = 10.0
_MARKET_DATA_RECOVERY_COOLDOWN_SECONDS = 45.0
_MARKET_DATA_RECOVERY_WINDOW_SECONDS = 300.0
_MARKET_DATA_RECOVERY_MAX_ATTEMPTS_PER_WINDOW = 3
_PHASE1_RUNTIME_ARTIFACT_SOURCE = "DATABENTO_REALTIME_PHASE1"
_PHASE1_RUNTIME_ARTIFACT_FRESHNESS_DEFAULT_SECONDS = 180.0
_PHASE1_RUNTIME_ARTIFACT_DEFAULT_ROOT = (
    Path(__file__).resolve().parents[3] / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data"
)


def databento_live_effective_end(
    now: datetime,
    internal_timeframe: str,
    *,
    safety_delay_seconds: int = _DATABENTO_LIVE_POLL_SAFETY_DELAY_SECONDS,
) -> datetime:
    delayed_now = now - timedelta(seconds=max(safety_delay_seconds, 0))
    return _latest_completed_bar_end(delayed_now, internal_timeframe)


class HistoricalPollingLiveClient:
    """Uses recent Schwab price-history bars as a completed-bar live polling source."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter,
        historical_client: SchwabHistoricalClient,
        lookback_minutes: int = 180,
    ) -> None:
        self._adapter = adapter
        self._historical_client = historical_client
        self._lookback_minutes = lookback_minutes

    def poll_live_bars(
        self,
        external_symbol: str,
        external_timeframe: str,
        request: SchwabLivePollRequest,
    ) -> list[dict]:
        now = datetime.now(self._adapter._settings.timezone_info)  # noqa: SLF001 - adapter already owns runtime tz
        timeframe_duration = timedelta(minutes=timeframe_minutes(external_timeframe))
        recovery_floor = now - timedelta(minutes=self._lookback_minutes)
        start_dt = recovery_floor
        if request.since is not None:
            start_dt = max(request.since - timeframe_duration, recovery_floor)
        payload = self._historical_client.fetch_price_history(
            external_symbol,
            SchwabHistoricalRequest(
                internal_symbol=request.internal_symbol,
                period_type="day",
                frequency_type=self._adapter.map_timeframe(external_timeframe).frequency_type,
                frequency=self._adapter.map_timeframe(external_timeframe).frequency,
                start_date_ms=int(start_dt.timestamp() * 1000),
                end_date_ms=int(now.timestamp() * 1000),
                need_extended_hours_data=True,
                need_previous_close=False,
            ),
            default_frequency=self._adapter.map_timeframe(external_timeframe),
        )
        records = payload.get("candles", [])
        if not isinstance(records, list):
            raise ValueError("Schwab live polling payload must expose a candle list.")
        return list(records)


class DatabentoHistoricalPollingClient:
    """Uses recent Databento 1m bars as the completed-bar live polling source."""

    def __init__(
        self,
        *,
        provider: DatabentoMarketDataProvider,
        timezone_info,
        lookback_minutes: int = 180,
    ) -> None:
        self._provider = provider
        self._timezone_info = timezone_info
        self._lookback_minutes = lookback_minutes

    def poll_live_bars(
        self,
        _external_symbol: str | None,
        external_timeframe: str,
        request: SchwabLivePollRequest,
    ) -> list[Bar]:
        now = datetime.now(self._timezone_info)
        timeframe_duration = timedelta(minutes=timeframe_minutes(external_timeframe))
        effective_end = databento_live_effective_end(now, external_timeframe)
        recovery_floor = effective_end - timedelta(minutes=self._lookback_minutes)
        start_dt = recovery_floor
        if request.since is not None:
            start_dt = max(request.since - timeframe_duration, recovery_floor)
        if start_dt >= effective_end:
            start_dt = effective_end - timeframe_duration
        start_utc = start_dt.astimezone(UTC)
        end_utc = effective_end.astimezone(UTC)
        try:
            result = self._provider.fetch_historical_bars(
                HistoricalBarsRequest(
                    internal_symbol=request.internal_symbol,
                    timeframe=external_timeframe,
                    start=start_utc,
                    end=end_utc,
                )
            )
            return list(result.bars)
        except DatabentoHttpError as exc:
            retry_bounds = _databento_retry_bounds_from_error(
                exc,
                start=start_utc,
                end=end_utc,
                timeframe_duration=timeframe_duration,
                recovery_floor=recovery_floor.astimezone(UTC),
            )
            if retry_bounds is None:
                raise
            retry_start, retry_end = retry_bounds
            result = self._provider.fetch_historical_bars(
                HistoricalBarsRequest(
                    internal_symbol=request.internal_symbol,
                    timeframe=external_timeframe,
                    start=retry_start,
                    end=retry_end,
                )
            )
            return list(result.bars)


def databento_live_gateway_host(dataset: str) -> str:
    normalized = str(dataset or "").strip().lower().replace(".", "-")
    if not normalized:
        raise ValueError("Databento live gateway host requires a dataset.")
    return f"{normalized}.lsg.databento.com"


def databento_live_auth_response(*, cram: str, api_key: str) -> str:
    material = f"{cram}|{api_key}".encode("utf-8")
    digest = hashlib.sha256(material).hexdigest()
    bucket_id = str(api_key)[-5:]
    return f"{digest}-{bucket_id}"


def databento_live_format_timestamp(value: datetime) -> str:
    utc_value = value.astimezone(UTC)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%S")


def _parse_control_message(line: str) -> dict[str, str]:
    payload: dict[str, str] = {}
    for part in str(line).strip().split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        payload[key.strip()] = value.strip()
    return payload


def _parse_databento_timestamp(raw_value: Any) -> datetime:
    if isinstance(raw_value, (int, float)):
        return datetime.fromtimestamp(float(raw_value) / 1_000_000_000, tz=UTC)
    value = str(raw_value or "").strip()
    if not value:
        raise ValueError("Databento live timestamp is empty.")
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_optional_datetime(raw_value: Any) -> datetime | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _ensure_datetime_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _float_value(raw_value: Any, default: float) -> float:
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return default


def _phase1_artifact_forbidden_provenance(payload: Mapping[str, Any]) -> bool:
    forbidden_flag_names = {
        "historical_seed_ready",
        "research_artifact_used",
        "archive_artifact_used",
        "databento_live_api_replay",
        "replay_artifact_used",
    }
    if any(payload.get(name) is True for name in forbidden_flag_names):
        return True
    source_text = " ".join(
        str(payload.get(name) or "")
        for name in ("source", "source_id", "provenance", "artifact_kind", "artifact_role")
    ).lower()
    return any(token in source_text for token in ("historical", "seed", "replay", "research", "archive"))


def _databento_retry_bounds_from_error(
    error: Exception,
    *,
    start: datetime,
    end: datetime,
    timeframe_duration: timedelta,
    recovery_floor: datetime,
) -> tuple[datetime, datetime] | None:
    detail = _parse_databento_error_detail(error)
    if not detail:
        return None
    case = str(detail.get("case") or "").strip().lower()
    payload = detail.get("payload")
    if not isinstance(payload, dict):
        return None
    available_end = _parse_optional_datetime(payload.get("available_end"))
    if available_end is None:
        return None
    if case not in {"data_end_after_available_end", "data_start_after_available_end"}:
        return None
    retry_end = min(end.astimezone(UTC), available_end.astimezone(UTC))
    retry_floor = max(recovery_floor.astimezone(UTC), retry_end - timeframe_duration)
    retry_start = start.astimezone(UTC)
    if retry_start >= retry_end:
        retry_start = retry_floor
    else:
        retry_start = max(retry_start, recovery_floor.astimezone(UTC))
    if retry_start >= retry_end:
        return None
    return retry_start, retry_end


def _parse_databento_error_detail(error: Exception) -> dict[str, Any] | None:
    message = str(error or "")
    brace_index = message.find("{")
    if brace_index < 0:
        return None
    try:
        payload = json.loads(message[brace_index:])
    except json.JSONDecodeError:
        return None
    detail = payload.get("detail")
    if isinstance(detail, dict):
        return detail
    return None


def _parse_databento_price(raw_value: Any) -> Decimal:
    if isinstance(raw_value, int):
        return Decimal(raw_value) / Decimal("1000000000")
    if isinstance(raw_value, float):
        return Decimal(str(raw_value))
    return Decimal(str(raw_value))


def _decimal_from_artifact(raw_value: Any, *, field_name: str) -> Decimal:
    if raw_value is None:
        raise Phase1RuntimeArtifactError(f"Phase-1 runtime candle artifact row is missing {field_name}.")
    return Decimal(str(raw_value))


class Phase1RuntimeArtifactError(RuntimeError):
    """Phase-1 runtime candle artifact cannot safely provide live PAPER bars."""


class Phase1RuntimeArtifactRecoverableError(Phase1RuntimeArtifactError):
    """Phase-1 artifact outage that can recover without restarting PAPER runtime."""


class Phase1RuntimeArtifactMissingError(Phase1RuntimeArtifactRecoverableError):
    """Phase-1 runtime candle artifact is missing or lacks completed bars."""


class Phase1RuntimeArtifactStaleError(Phase1RuntimeArtifactRecoverableError):
    """Phase-1 runtime candle artifact exists but is too stale to route from."""


class Phase1RuntimeArtifactMarketClosedError(Phase1RuntimeArtifactStaleError):
    """Phase-1 artifact is stale because the futures market is closed."""


class _DatabentoRawLiveSession:
    def __init__(
        self,
        *,
        api_key: str,
        dataset: str,
        request_symbol: str,
        stype_in: str,
        schema_name: str,
        internal_symbol: str,
        internal_timeframe: str,
        lookback_minutes: int,
    ) -> None:
        self._api_key = api_key
        self._dataset = dataset
        self._request_symbol = request_symbol
        self._stype_in = stype_in
        self._schema_name = schema_name
        self._internal_symbol = internal_symbol
        self._internal_timeframe = internal_timeframe
        self._lookback_minutes = lookback_minutes
        self._bars: dict[datetime, Bar] = {}
        self._condition = threading.Condition()
        self._last_error: str | None = None
        self._last_stream_end: datetime | None = None
        self._started = False
        self._thread: threading.Thread | None = None
        self._active_socket: socket.socket | None = None

    def ensure_started(self) -> None:
        with self._condition:
            if self._started:
                return
            self._started = True
            self._thread = threading.Thread(
                target=self._run_forever,
                name=f"databento-live-{self._internal_symbol}-{self._internal_timeframe}",
                daemon=True,
            )
            self._thread.start()

    def snapshot_bars(self, *, since: datetime | None) -> list[Bar]:
        self.ensure_started()
        deadline = time.monotonic() + _DATABENTO_LIVE_SESSION_WAIT_SECONDS
        with self._condition:
            while not self._bars and self._last_error is None and time.monotonic() < deadline:
                self._condition.wait(timeout=0.5)
            if self._last_error is not None and not self._bars:
                raise RuntimeError(self._last_error)
            rows = sorted(self._bars.values(), key=lambda row: row.end_ts)
        if since is None:
            return rows
        return [row for row in rows if row.end_ts > since]

    def _run_forever(self) -> None:
        while True:
            try:
                self._run_session()
            except Exception as exc:  # pragma: no cover - exercised through runtime integration
                with self._condition:
                    self._last_error = f"Databento live session failed for {self._request_symbol}: {exc}"
                    self._condition.notify_all()
                time.sleep(1.0)

    def _run_session(self) -> None:
        host = databento_live_gateway_host(self._dataset)
        with socket.create_connection((host, _DATABENTO_LIVE_GATEWAY_PORT), timeout=10.0) as sock:
            with self._condition:
                self._active_socket = sock
            sock.settimeout(10.0)
            reader = sock.makefile("r", encoding="utf-8", newline="\n")
            writer = sock.makefile("w", encoding="utf-8", newline="\n")
            greeting = reader.readline().strip()
            challenge = reader.readline().strip()
            challenge_fields = _parse_control_message(challenge)
            cram = challenge_fields.get("cram")
            if not greeting or not cram:
                raise RuntimeError(f"Databento live gateway handshake failed: {greeting!r} / {challenge!r}")
            auth_line = (
                f"auth={databento_live_auth_response(cram=cram, api_key=self._api_key)}"
                f"|dataset={self._dataset}|encoding=json|pretty_px=1|pretty_ts=1|heartbeat_interval_s=5"
            )
            writer.write(auth_line + "\n")
            writer.flush()
            auth_response = _parse_control_message(reader.readline())
            if auth_response.get("success") != "1":
                raise RuntimeError(f"Databento live authentication failed: {auth_response}")

            timeframe_duration = timedelta(minutes=timeframe_minutes(self._internal_timeframe))
            replay_start = self._next_replay_start(
                now_utc=datetime.now(UTC),
                timeframe_duration=timeframe_duration,
            )
            subscribe_line = (
                f"schema={self._schema_name}|stype_in={self._stype_in}|symbols={self._request_symbol}"
                f"|start={databento_live_format_timestamp(replay_start)}"
            )
            writer.write(subscribe_line + "\n")
            writer.write("start_session=0\n")
            writer.flush()

            with self._condition:
                self._last_error = None
                self._condition.notify_all()

            while True:
                raw_line = reader.readline()
                if not raw_line:
                    raise RuntimeError("Databento live gateway closed the socket.")
                line = raw_line.strip()
                if not line or not line.startswith("{"):
                    continue
                record = json.loads(line)
                bar = self._bar_from_live_record(record)
                if bar is None:
                    continue
                with self._condition:
                    self._bars[bar.end_ts] = bar
                    if len(self._bars) > 2048:
                        oldest_end_ts = min(self._bars)
                        del self._bars[oldest_end_ts]
                    self._last_stream_end = bar.end_ts
                    self._condition.notify_all()
        with self._condition:
            self._active_socket = None

    def request_reconnect(self, *, reason: str) -> None:
        del reason
        with self._condition:
            active_socket = self._active_socket
        if active_socket is None:
            return
        try:
            active_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            active_socket.close()
        except OSError:
            pass

    def _bar_from_live_record(self, record: dict[str, Any]) -> Bar | None:
        required_fields = {"open", "high", "low", "close", "volume"}
        if not required_fields.issubset(record):
            return None
        header = record.get("hd") if isinstance(record.get("hd"), dict) else {}
        ts_event = record.get("ts_event") or header.get("ts_event")
        if not ts_event:
            return None
        start_ts = _parse_databento_timestamp(ts_event)
        duration = timedelta(minutes=timeframe_minutes(self._internal_timeframe))
        end_ts = start_ts + duration
        return Bar(
            bar_id=build_bar_id(self._internal_symbol, self._internal_timeframe, end_ts),
            symbol=self._internal_symbol,
            timeframe=self._internal_timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            open=_parse_databento_price(record["open"]),
            high=_parse_databento_price(record["high"]),
            low=_parse_databento_price(record["low"]),
            close=_parse_databento_price(record["close"]),
            volume=int(record["volume"]),
            is_final=True,
            session_asia=False,
            session_london=False,
            session_us=True,
            session_allowed=True,
        )

    def _next_replay_start(self, *, now_utc: datetime, timeframe_duration: timedelta) -> datetime:
        bounded_replay_floor = now_utc.astimezone(UTC) - timedelta(
            minutes=max(1, min(int(self._lookback_minutes), _DATABENTO_LIVE_REPLAY_LOOKBACK_CAP_MINUTES))
        )
        replay_start = (
            self._last_stream_end - timeframe_duration
            if self._last_stream_end is not None
            else bounded_replay_floor
        )
        return max(replay_start.astimezone(UTC), bounded_replay_floor)


class DatabentoRawLivePollingClient:
    _sessions: dict[tuple[str, str, str, str], _DatabentoRawLiveSession] = {}
    _sessions_lock = threading.Lock()

    def __init__(
        self,
        *,
        provider: DatabentoMarketDataProvider,
        lookback_minutes: int = 180,
    ) -> None:
        self._provider = provider
        self._lookback_minutes = lookback_minutes

    def poll_live_bars(
        self,
        _external_symbol: str | None,
        external_timeframe: str,
        request: SchwabLivePollRequest,
    ) -> list[Bar]:
        symbol_description = self._provider.describe_symbol(request.internal_symbol)
        dataset = str(symbol_description.get("dataset") or "GLBX.MDP3")
        request_symbol = str(symbol_description.get("request_symbol") or request.internal_symbol)
        stype_in = str(symbol_description.get("stype_in") or "continuous")
        schema_name = str((symbol_description.get("schema_by_timeframe") or {}).get(external_timeframe) or "ohlcv-1m")
        api_key = str(getattr(self._provider, "_api_key", "") or os.environ.get(getattr(self._provider._config, "api_key_env", "DATABENTO_API_KEY"), "")).strip()  # noqa: SLF001
        if not api_key:
            raise RuntimeError("Databento live polling requires DATABENTO_API_KEY to be set.")
        session_key = (dataset, request_symbol, stype_in, schema_name)
        with self._sessions_lock:
            session = self._sessions.get(session_key)
            if session is None:
                session = _DatabentoRawLiveSession(
                    api_key=api_key,
                    dataset=dataset,
                    request_symbol=request_symbol,
                    stype_in=stype_in,
                    schema_name=schema_name,
                    internal_symbol=request.internal_symbol,
                    internal_timeframe=external_timeframe,
                    lookback_minutes=self._lookback_minutes,
                )
                self._sessions[session_key] = session
        return session.snapshot_bars(since=request.since)

    def recover_live_bars(
        self,
        *,
        internal_symbol: str,
        internal_timeframe: str,
        reason: str,
    ) -> dict[str, Any]:
        matched = 0
        with self._sessions_lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            if (
                getattr(session, "_internal_symbol", None) == internal_symbol
                and getattr(session, "_internal_timeframe", None) == internal_timeframe
            ):
                matched += 1
                session.request_reconnect(reason=reason)
        return {
            "action": "provider_resubscribe",
            "matched_session_count": matched,
            "ok": matched > 0,
            "detail": (
                f"Requested Databento reconnect for {matched} live session(s)."
                if matched > 0
                else "No active Databento live session matched the stale symbol/timeframe."
            ),
        }


class Phase1RuntimeArtifactPollingClient:
    """Reads canonical Phase-1 runtime candle artifacts as a PAPER live polling source."""

    def __init__(
        self,
        *,
        artifact_root: str | Path | None = None,
        required_source: str = _PHASE1_RUNTIME_ARTIFACT_SOURCE,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._artifact_root = Path(artifact_root) if artifact_root is not None else _PHASE1_RUNTIME_ARTIFACT_DEFAULT_ROOT
        self._required_source = str(required_source)
        self._now_fn = now_fn

    def artifact_path(self, *, internal_symbol: str, internal_timeframe: str) -> Path:
        symbol = str(internal_symbol or "").strip().upper()
        timeframe = normalize_timeframe_label(internal_timeframe)
        return self._artifact_root / symbol / timeframe / "latest_runtime_candles.json"

    def poll_live_bars(
        self,
        _external_symbol: str | None,
        external_timeframe: str,
        request: SchwabLivePollRequest,
    ) -> list[Bar]:
        internal_symbol = str(request.internal_symbol or "").strip().upper()
        internal_timeframe = normalize_timeframe_label(external_timeframe)
        path = self.artifact_path(internal_symbol=internal_symbol, internal_timeframe=internal_timeframe)
        payload = self._read_payload(path)
        self._validate_payload(payload, internal_symbol=internal_symbol, internal_timeframe=internal_timeframe, path=path)
        bars = self._bars_from_payload(payload, internal_symbol=internal_symbol, internal_timeframe=internal_timeframe)
        self._validate_freshness(payload, bars=bars, path=path)
        if request.since is not None:
            since = request.since.astimezone(UTC)
            bars = [bar for bar in bars if bar.end_ts.astimezone(UTC) > since]
        deduped: dict[datetime, Bar] = {}
        for bar in bars:
            deduped[bar.end_ts.astimezone(UTC)] = bar
        return [deduped[key] for key in sorted(deduped)]

    def recover_live_bars(
        self,
        *,
        internal_symbol: str,
        internal_timeframe: str,
        reason: str,
    ) -> dict[str, Any]:
        path = self.artifact_path(internal_symbol=internal_symbol, internal_timeframe=internal_timeframe)
        return {
            "action": "phase1_runtime_artifact_refresh_wait",
            "ok": path.exists(),
            "detail": f"Phase-1 artifact source is file-backed; waiting for listener artifact refresh after {reason}.",
            "artifact_path": str(path),
        }

    @staticmethod
    def _read_payload(path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise Phase1RuntimeArtifactMissingError(f"Phase-1 runtime candle artifact is missing: {path}") from exc
        except json.JSONDecodeError as exc:
            raise Phase1RuntimeArtifactError(f"Phase-1 runtime candle artifact is not valid JSON: {path}") from exc
        if not isinstance(payload, dict):
            raise Phase1RuntimeArtifactError(f"Phase-1 runtime candle artifact must contain a JSON object: {path}")
        return payload

    def _validate_payload(
        self,
        payload: Mapping[str, Any],
        *,
        internal_symbol: str,
        internal_timeframe: str,
        path: Path,
    ) -> None:
        source = str(payload.get("source") or payload.get("provenance") or payload.get("source_id") or "").strip()
        if source != self._required_source:
            raise Phase1RuntimeArtifactError(
                f"Phase-1 runtime candle artifact has invalid provenance {source!r}; expected {self._required_source}: {path}"
            )
        if _phase1_artifact_forbidden_provenance(payload):
            raise Phase1RuntimeArtifactError(f"Phase-1 runtime candle artifact uses historical/replay/research/archive evidence: {path}")
        payload_symbol = str(payload.get("symbol") or payload.get("instrument") or payload.get("root") or "").strip().upper()
        if payload_symbol != internal_symbol:
            raise Phase1RuntimeArtifactError(
                f"Phase-1 runtime candle artifact symbol mismatch: expected {internal_symbol}, found {payload_symbol or '<missing>'}: {path}"
            )
        payload_timeframe = normalize_timeframe_label(str(payload.get("timeframe") or ""))
        if payload_timeframe != internal_timeframe:
            raise Phase1RuntimeArtifactError(
                f"Phase-1 runtime candle artifact timeframe mismatch: expected {internal_timeframe}, found {payload_timeframe}: {path}"
            )

    def _validate_freshness(self, payload: Mapping[str, Any], *, bars: Sequence[Bar], path: Path) -> None:
        if not bars:
            raise Phase1RuntimeArtifactMissingError(f"Phase-1 runtime candle artifact contains no completed bars: {path}")
        now = self._now()
        latest_bar_end = max(bar.end_ts for bar in bars).astimezone(UTC)
        threshold_seconds = _float_value(
            payload.get("freshness_seconds"),
            _PHASE1_RUNTIME_ARTIFACT_FRESHNESS_DEFAULT_SECONDS,
        )
        latest_age_seconds = max((now - latest_bar_end).total_seconds(), 0.0)
        if latest_age_seconds > threshold_seconds:
            session = classify_phase1_futures_market_session(now)
            if session["classification"] == MARKET_CLOSED_NO_FRESH_BARS:
                raise Phase1RuntimeArtifactMarketClosedError(
                    "Phase-1 runtime candle artifact has no fresh bars because the market is closed: "
                    f"classification={MARKET_CLOSED_NO_FRESH_BARS} "
                    f"session_reason={session.get('reason')} latest_bar={latest_bar_end.isoformat()} "
                    f"age_seconds={latest_age_seconds:.3f} threshold_seconds={threshold_seconds:.3f} path={path}"
                )
            raise Phase1RuntimeArtifactStaleError(
                "Phase-1 runtime candle artifact is stale: "
                f"latest_bar={latest_bar_end.isoformat()} age_seconds={latest_age_seconds:.3f} "
                f"threshold_seconds={threshold_seconds:.3f} path={path}"
            )
        generated_at = _parse_optional_datetime(payload.get("generated_at"))
        if generated_at is not None:
            generated_age_seconds = max((now - generated_at.astimezone(UTC)).total_seconds(), 0.0)
            if generated_age_seconds > threshold_seconds:
                session = classify_phase1_futures_market_session(now)
                if session["classification"] == MARKET_CLOSED_NO_FRESH_BARS:
                    raise Phase1RuntimeArtifactMarketClosedError(
                        "Phase-1 runtime candle artifact metadata has no fresh bars because the market is closed: "
                        f"classification={MARKET_CLOSED_NO_FRESH_BARS} "
                        f"session_reason={session.get('reason')} generated_at={generated_at.isoformat()} "
                        f"age_seconds={generated_age_seconds:.3f} threshold_seconds={threshold_seconds:.3f} path={path}"
                    )
                raise Phase1RuntimeArtifactStaleError(
                    "Phase-1 runtime candle artifact metadata is stale: "
                    f"generated_at={generated_at.isoformat()} age_seconds={generated_age_seconds:.3f} "
                    f"threshold_seconds={threshold_seconds:.3f} path={path}"
                )

    def _bars_from_payload(
        self,
        payload: Mapping[str, Any],
        *,
        internal_symbol: str,
        internal_timeframe: str,
    ) -> list[Bar]:
        raw_bars = payload.get("bars")
        if not isinstance(raw_bars, list):
            raise Phase1RuntimeArtifactError("Phase-1 runtime candle artifact must expose a bars list.")
        bars: list[Bar] = []
        duration = timedelta(minutes=timeframe_minutes(internal_timeframe))
        for raw_row in raw_bars:
            if not isinstance(raw_row, Mapping):
                continue
            if raw_row.get("completed") is not True and raw_row.get("is_final") is not True:
                continue
            row_symbol = str(raw_row.get("symbol") or raw_row.get("instrument") or internal_symbol).strip().upper()
            if row_symbol != internal_symbol:
                raise Phase1RuntimeArtifactError(
                    f"Phase-1 runtime candle artifact row symbol mismatch: expected {internal_symbol}, found {row_symbol}."
                )
            end_ts = _parse_optional_datetime(raw_row.get("bar_end") or raw_row.get("end_ts") or raw_row.get("timestamp"))
            if end_ts is None:
                raise Phase1RuntimeArtifactError("Phase-1 runtime candle artifact row is missing bar_end/end_ts.")
            start_ts = _parse_optional_datetime(raw_row.get("bar_start") or raw_row.get("start_ts"))
            if start_ts is None:
                start_ts = end_ts - duration
            bars.append(
                Bar(
                    bar_id=build_bar_id(internal_symbol, internal_timeframe, end_ts),
                    symbol=internal_symbol,
                    timeframe=internal_timeframe,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    open=_decimal_from_artifact(raw_row.get("open"), field_name="open"),
                    high=_decimal_from_artifact(raw_row.get("high"), field_name="high"),
                    low=_decimal_from_artifact(raw_row.get("low"), field_name="low"),
                    close=_decimal_from_artifact(raw_row.get("close"), field_name="close"),
                    volume=max(0, int(Decimal(str(raw_row.get("volume") or 0)))),
                    is_final=True,
                    session_asia=raw_row.get("session_asia") is True,
                    session_london=raw_row.get("session_london") is True,
                    session_us=raw_row.get("session_us") is True or not any(
                        raw_row.get(key) is True for key in ("session_asia", "session_london")
                    ),
                    session_allowed=raw_row.get("session_allowed") is not False,
                )
            )
        return sorted(bars, key=lambda row: row.end_ts)

    def _now(self) -> datetime:
        value = self._now_fn() if self._now_fn is not None else datetime.now(UTC)
        return _ensure_datetime_utc(value)


class LivePollingService:
    """Polls live bar data and normalizes it into the shared internal bar model."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter | None,
        client: Optional[SchwabLivePollingClient] = None,
        repositories: Optional[RepositorySet] = None,
        canonical_maintenance: CanonicalMarketDataMaintenanceService | None = None,
        data_source: str = "schwab_live_poll",
        provider: str = "schwab_market_data",
        provenance_tag: str = "schwab_market_data_live_poll",
        dataset: str | None = "schwab_pricehistory_live_poll",
        schema_name: str | None = "ohlcv-1m",
    ) -> None:
        self._adapter = adapter
        self._client = client
        self._repositories = repositories
        self._canonical_maintenance = canonical_maintenance
        self._data_source = data_source
        self._provider = provider
        self._provenance_tag = provenance_tag
        self._dataset = dataset
        self._schema_name = schema_name
        self._market_data_recovery: dict[tuple[str, str], dict[str, Any]] = {}
        self._market_data_recovery_event_logger: Callable[[dict[str, Any]], Any] | None = None

    @property
    def data_source(self) -> str:
        return self._data_source

    def set_recovery_event_logger(self, callback: Callable[[dict[str, Any]], Any] | None) -> None:
        self._market_data_recovery_event_logger = callback

    def market_data_recovery_snapshot(
        self,
        *,
        internal_symbol: str,
        internal_timeframe: str,
        latest_feature_bar_timestamp: str | None = None,
        latest_processed_bar_timestamp: str | None = None,
        latest_processed_signal_timestamp: str | None = None,
        lane_id: str | None = None,
    ) -> dict[str, Any]:
        key = self._market_data_recovery_key(internal_symbol, internal_timeframe)
        payload = deepcopy(self._market_data_recovery.get(key) or self._initial_market_data_recovery_state(internal_symbol, internal_timeframe))
        payload["latest_feature_bar_timestamp"] = latest_feature_bar_timestamp
        payload["latest_processed_bar_timestamp"] = latest_processed_bar_timestamp
        payload["latest_processed_signal_timestamp"] = latest_processed_signal_timestamp
        payload["affected_symbols"] = [str(internal_symbol).upper()]
        payload["affected_lanes"] = [str(lane_id)] if str(lane_id or "").strip() else []
        return payload

    def poll_bars(
        self,
        request: SchwabLivePollRequest,
        internal_timeframe: str,
        default_is_final: bool = True,
    ) -> list[Bar]:
        if self._client is None:
            raise NotImplementedError(
                "Schwab live polling integration is pending official API confirmation. "
                "Fill in the SchwabLivePollingClient once docs are confirmed."
            )

        raw_records = self._poll_provider_once(request=request, internal_timeframe=internal_timeframe)
        bars = self._normalize_records(
            raw_records,
            internal_symbol=request.internal_symbol,
            internal_timeframe=internal_timeframe,
            default_is_final=default_is_final,
        )
        bars = self._filter_completed_bars(bars, request=request, internal_timeframe=internal_timeframe)
        self._persist_bars(bars)
        self._record_latest_observed_bar(
            internal_symbol=request.internal_symbol,
            internal_timeframe=internal_timeframe,
            bars=bars,
            after_recovery=False,
        )
        recovery_state = self._evaluate_market_data_recovery_state(
            request=request,
            internal_timeframe=internal_timeframe,
            latest_bars=bars,
        )
        if bool(recovery_state.get("stale")) and self._should_attempt_market_data_recovery(recovery_state):
            recovery_state, recovered_bars = self._attempt_market_data_recovery(
                request=request,
                internal_timeframe=internal_timeframe,
                default_is_final=default_is_final,
                prior_state=recovery_state,
            )
            bars = self._merge_bar_rows(bars, recovered_bars)
        self._store_market_data_recovery_state(
            internal_symbol=request.internal_symbol,
            internal_timeframe=internal_timeframe,
            payload=recovery_state,
        )
        return bars

    def _poll_provider_once(
        self,
        *,
        request: SchwabLivePollRequest,
        internal_timeframe: str,
    ) -> Sequence[Bar] | Sequence[dict[str, Any]]:
        external_symbol = self._adapter.map_historical_symbol(request.internal_symbol) if self._adapter is not None else None
        return self._client.poll_live_bars(external_symbol, internal_timeframe, request)

    def _normalize_records(
        self,
        raw_records: Sequence[Bar] | Sequence[dict[str, Any]],
        *,
        internal_symbol: str,
        internal_timeframe: str,
        default_is_final: bool,
    ) -> list[Bar]:
        if not raw_records:
            return []
        first_record = raw_records[0]
        if isinstance(first_record, Bar):
            return list(raw_records)  # type: ignore[arg-type]
        if self._adapter is None:
            raise ValueError("LivePollingService requires an adapter when the client emits raw provider records.")
        return self._adapter.normalize_live_records(
            raw_records,  # type: ignore[arg-type]
            internal_symbol,
            internal_timeframe,
            default_is_final=default_is_final,
        )

    def _filter_completed_bars(
        self,
        bars: list[Bar],
        request: SchwabLivePollRequest,
        internal_timeframe: str,
    ) -> list[Bar]:
        if not bars:
            return []
        latest_completed_end = _latest_completed_bar_end(datetime.now(bars[-1].end_ts.tzinfo), internal_timeframe)
        filtered = [bar for bar in bars if bar.is_final and bar.end_ts <= latest_completed_end]
        if request.since is not None:
            filtered = [bar for bar in filtered if bar.end_ts > request.since]
        return filtered

    def _persist_bars(self, bars: list[Bar]) -> None:
        if self._repositories is None:
            return
        for bar in bars:
            self._repositories.bars.save(bar, data_source=self._data_source)
        if self._canonical_maintenance is not None:
            self._canonical_maintenance.persist_completed_1m_bars(
                bars=bars,
                raw_data_source=self._data_source,
                provider=self._provider,
                provenance_tag=self._provenance_tag,
                dataset=self._dataset,
                schema_name=self._schema_name,
                provider_metadata={"ingest_mode": "completed_live_poll"},
            )

    def _market_data_recovery_key(self, internal_symbol: str, internal_timeframe: str) -> tuple[str, str]:
        return (str(internal_symbol).upper(), str(internal_timeframe).lower())

    def _initial_market_data_recovery_state(self, internal_symbol: str, internal_timeframe: str) -> dict[str, Any]:
        return {
            "market_data_recovery_state": "IDLE",
            "last_recovery_attempt_at": None,
            "recovery_attempt_count": 0,
            "recovery_action": None,
            "recovery_result": None,
            "recovery_root_cause": None,
            "affected_symbols": [str(internal_symbol).upper()],
            "affected_lanes": [],
            "latest_observed_raw_bar_timestamp": None,
            "latest_observed_bar_after_recovery": None,
            "latest_feature_bar_timestamp": None,
            "latest_processed_bar_timestamp": None,
            "latest_processed_signal_timestamp": None,
            "current_wall_clock_timestamp": None,
            "market_data_lag_seconds": None,
            "source_used_for_freshness": self._data_source,
            "data_provider": self._provider,
            "stale_scope": "ONE_SYMBOL_GROUP",
            "recovered": False,
            "stale": False,
            "cooldown_until": None,
            "attempt_history": [],
            "last_recovery_detail": None,
        }

    def _record_latest_observed_bar(
        self,
        *,
        internal_symbol: str,
        internal_timeframe: str,
        bars: Sequence[Bar],
        after_recovery: bool,
    ) -> None:
        if not bars:
            return
        key = self._market_data_recovery_key(internal_symbol, internal_timeframe)
        payload = deepcopy(self._market_data_recovery.get(key) or self._initial_market_data_recovery_state(internal_symbol, internal_timeframe))
        latest_bar = max(bars, key=lambda row: row.end_ts)
        payload["latest_observed_raw_bar_timestamp"] = latest_bar.end_ts.isoformat()
        if after_recovery:
            payload["latest_observed_bar_after_recovery"] = latest_bar.end_ts.isoformat()
        self._market_data_recovery[key] = payload

    def _evaluate_market_data_recovery_state(
        self,
        *,
        request: SchwabLivePollRequest,
        internal_timeframe: str,
        latest_bars: Sequence[Bar],
    ) -> dict[str, Any]:
        key = self._market_data_recovery_key(request.internal_symbol, internal_timeframe)
        payload = deepcopy(self._market_data_recovery.get(key) or self._initial_market_data_recovery_state(request.internal_symbol, internal_timeframe))
        now = datetime.now(UTC)
        latest_observed_ts = None
        if latest_bars:
            latest_observed_ts = max(row.end_ts for row in latest_bars).astimezone(UTC)
        else:
            existing = payload.get("latest_observed_raw_bar_timestamp")
            latest_observed_ts = _parse_optional_datetime(existing) if existing else None
        expected_completed_end = _latest_completed_bar_end(now, internal_timeframe).astimezone(UTC)
        grace_deadline = expected_completed_end + timedelta(seconds=_MARKET_DATA_STALE_GRACE_SECONDS)
        market_data_lag_seconds = None
        if latest_observed_ts is not None:
            market_data_lag_seconds = max(
                (expected_completed_end - latest_observed_ts).total_seconds(),
                0.0,
            )
        else:
            market_data_lag_seconds = max((now - expected_completed_end).total_seconds(), 0.0)
        if now <= grace_deadline:
            root_cause = "NORMAL_PUBLICATION_DELAY_WITHIN_GRACE"
            stale = False
            state = "GRACE"
            recovered = False
        elif latest_observed_ts is None or latest_observed_ts < expected_completed_end:
            stale = True
            recovered = False
            if self._data_source.startswith("databento"):
                root_cause = "SUBSCRIPTION_DROPPED"
            else:
                root_cause = "DATA_PROVIDER_LAG"
            state = "STALE_DETECTED"
        else:
            stale = False
            recovered = bool(payload.get("recovered"))
            root_cause = None
            state = "RECOVERED" if recovered else "HEALTHY"
        payload.update(
            {
                "market_data_recovery_state": state,
                "recovery_root_cause": root_cause,
                "current_wall_clock_timestamp": now.isoformat(),
                "market_data_lag_seconds": market_data_lag_seconds,
                "source_used_for_freshness": self._data_source,
                "data_provider": self._provider,
                "stale": stale,
                "recovered": recovered,
            }
        )
        return payload

    def _should_attempt_market_data_recovery(self, payload: dict[str, Any]) -> bool:
        if not bool(payload.get("stale")):
            return False
        now = _parse_optional_datetime(payload.get("current_wall_clock_timestamp")) or datetime.now(UTC)
        cooldown_until = _parse_optional_datetime(payload.get("cooldown_until"))
        if cooldown_until is not None and now < cooldown_until.astimezone(UTC):
            payload["market_data_recovery_state"] = "COOLDOWN_ACTIVE"
            payload["recovery_result"] = "RATE_LIMITED"
            return False
        attempt_history = [
            _parse_optional_datetime(value)
            for value in list(payload.get("attempt_history") or [])
            if _parse_optional_datetime(value) is not None
        ]
        window_floor = now - timedelta(seconds=_MARKET_DATA_RECOVERY_WINDOW_SECONDS)
        recent_attempts = [value for value in attempt_history if value is not None and value.astimezone(UTC) >= window_floor]
        payload["attempt_history"] = [value.isoformat() for value in recent_attempts]
        if len(recent_attempts) >= _MARKET_DATA_RECOVERY_MAX_ATTEMPTS_PER_WINDOW:
            payload["market_data_recovery_state"] = "COOLDOWN_ACTIVE"
            payload["recovery_result"] = "RATE_LIMITED"
            payload["cooldown_until"] = (now + timedelta(seconds=_MARKET_DATA_RECOVERY_COOLDOWN_SECONDS)).isoformat()
            return False
        return True

    def _attempt_market_data_recovery(
        self,
        *,
        request: SchwabLivePollRequest,
        internal_timeframe: str,
        default_is_final: bool,
        prior_state: dict[str, Any],
    ) -> tuple[dict[str, Any], list[Bar]]:
        now = datetime.now(UTC)
        payload = deepcopy(prior_state)
        attempt_history = list(payload.get("attempt_history") or [])
        attempt_history.append(now.isoformat())
        payload["attempt_history"] = attempt_history
        payload["recovery_attempt_count"] = len(attempt_history)
        payload["last_recovery_attempt_at"] = now.isoformat()
        payload["market_data_recovery_state"] = "RECOVERY_IN_PROGRESS"
        recovery_result: dict[str, Any] = {
            "action": "provider_resubscribe_unsupported",
            "ok": False,
            "detail": "The active market-data client does not support scoped recovery.",
        }
        recover_hook = getattr(self._client, "recover_live_bars", None)
        if callable(recover_hook):
            recovery_result = dict(
                recover_hook(
                    internal_symbol=request.internal_symbol,
                    internal_timeframe=internal_timeframe,
                    reason=str(payload.get("recovery_root_cause") or "market_data_stale"),
                )
                or {}
            )
        payload["recovery_action"] = recovery_result.get("action")
        payload["last_recovery_detail"] = recovery_result.get("detail")
        recovered_bars: list[Bar] = []
        if bool(recovery_result.get("ok")):
            raw_records = self._poll_provider_once(request=request, internal_timeframe=internal_timeframe)
            recovered_bars = self._normalize_records(
                raw_records,
                internal_symbol=request.internal_symbol,
                internal_timeframe=internal_timeframe,
                default_is_final=default_is_final,
            )
            recovered_bars = self._filter_completed_bars(
                recovered_bars,
                request=request,
                internal_timeframe=internal_timeframe,
            )
            self._persist_bars(recovered_bars)
            self._record_latest_observed_bar(
                internal_symbol=request.internal_symbol,
                internal_timeframe=internal_timeframe,
                bars=recovered_bars,
                after_recovery=True,
            )
        post_state = self._evaluate_market_data_recovery_state(
            request=request,
            internal_timeframe=internal_timeframe,
            latest_bars=recovered_bars,
        )
        post_state["attempt_history"] = list(payload.get("attempt_history") or [])
        post_state["recovery_attempt_count"] = payload["recovery_attempt_count"]
        post_state["last_recovery_attempt_at"] = payload["last_recovery_attempt_at"]
        post_state["recovery_action"] = payload["recovery_action"]
        post_state["last_recovery_detail"] = payload["last_recovery_detail"]
        if bool(post_state.get("stale")):
            post_state["market_data_recovery_state"] = "FAILED"
            post_state["recovery_result"] = "NO_FRESH_BAR_AFTER_RECOVERY"
            post_state["recovered"] = False
            post_state["cooldown_until"] = (now + timedelta(seconds=_MARKET_DATA_RECOVERY_COOLDOWN_SECONDS)).isoformat()
        else:
            post_state["market_data_recovery_state"] = "RECOVERED"
            post_state["recovery_result"] = "FRESH_BAR_OBSERVED"
            post_state["recovered"] = True
            post_state["cooldown_until"] = None
        self._emit_market_data_recovery_event(request=request, internal_timeframe=internal_timeframe, payload=post_state)
        return post_state, recovered_bars

    def _store_market_data_recovery_state(
        self,
        *,
        internal_symbol: str,
        internal_timeframe: str,
        payload: dict[str, Any],
    ) -> None:
        key = self._market_data_recovery_key(internal_symbol, internal_timeframe)
        self._market_data_recovery[key] = deepcopy(payload)

    def _emit_market_data_recovery_event(
        self,
        *,
        request: SchwabLivePollRequest,
        internal_timeframe: str,
        payload: dict[str, Any],
    ) -> None:
        if self._market_data_recovery_event_logger is None:
            return
        event = {
            "event_type": "market_data_recovery_attempt",
            "symbol": str(request.internal_symbol).upper(),
            "timeframe": str(internal_timeframe),
            **deepcopy(payload),
        }
        try:
            self._market_data_recovery_event_logger(event)
        except Exception:
            return

    @staticmethod
    def _merge_bar_rows(existing: Sequence[Bar], recovered: Sequence[Bar]) -> list[Bar]:
        merged: dict[tuple[str, datetime], Bar] = {}
        for row in [*existing, *recovered]:
            merged[(row.symbol, row.end_ts)] = row
        return sorted(merged.values(), key=lambda row: row.end_ts)


class LiveStreamService:
    """Placeholder live-stream service targeting the same internal Bar model."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter,
        client: Optional[SchwabLiveStreamClient] = None,
    ) -> None:
        self._adapter = adapter
        self._client = client

    def subscribe_bars(self, internal_symbol: str, internal_timeframe: str) -> Iterable[Bar]:
        if self._client is None:
            raise NotImplementedError(
                "Schwab live streaming integration is pending official API confirmation. "
                "Fill in the SchwabLiveStreamClient once docs are confirmed."
            )

        external_symbol = self._adapter.map_historical_symbol(internal_symbol)
        raw_records = self._client.subscribe_live_bars(external_symbol, internal_timeframe)
        return self._adapter.normalize_live_records(raw_records, internal_symbol, internal_timeframe, default_is_final=False)


def _latest_completed_bar_end(now: datetime, internal_timeframe: str) -> datetime:
    minutes = timeframe_minutes(internal_timeframe)
    floored_minute = now.minute - (now.minute % minutes)
    return now.replace(minute=floored_minute, second=0, microsecond=0)
