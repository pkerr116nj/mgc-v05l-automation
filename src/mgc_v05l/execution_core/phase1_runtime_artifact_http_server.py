"""Read-only HTTP surface for Phase-1 runtime candle artifacts."""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlparse

from mgc_v05l.market_data.timeframes import normalize_timeframe_label

DEFAULT_BIND_HOST = "127.0.0.1"
DEFAULT_BIND_PORT = 8766
DEFAULT_MAX_RESPONSE_BYTES = 1_048_576
DEFAULT_ARTIFACT_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_LISTENER_STATUS_PATH = (
    Path("outputs")
    / "reports"
    / "phase1_databento_live_runtime_candles"
    / "latest_phase1_databento_live_listener_status.json"
)
DEFAULT_HEALTH_STALE_SECONDS = 180.0
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,16}$")
_SUPPORTED_TIMEFRAMES = {"1m", "3m", "5m"}


@dataclass(frozen=True)
class Phase1RuntimeArtifactHttpServerConfig:
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT
    host: str = DEFAULT_BIND_HOST
    port: int = DEFAULT_BIND_PORT
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
    listener_status_path: Path | None = DEFAULT_LISTENER_STATUS_PATH
    health_stale_seconds: float = DEFAULT_HEALTH_STALE_SECONDS


class Phase1RuntimeArtifactHttpError(ValueError):
    def __init__(self, message: str, *, status: HTTPStatus, classification: str) -> None:
        super().__init__(message)
        self.status = status
        self.classification = classification


def build_phase1_runtime_artifact_http_handler(
    *,
    artifact_root: Path,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    listener_status_path: Path | None = DEFAULT_LISTENER_STATUS_PATH,
    health_stale_seconds: float = DEFAULT_HEALTH_STALE_SECONDS,
) -> type[BaseHTTPRequestHandler]:
    root = Path(artifact_root)
    byte_limit = max(1, int(max_response_bytes))
    status_path = Path(listener_status_path) if listener_status_path is not None else None
    started_at = datetime.now(timezone.utc)
    generation_id = f"phase1_runtime_artifact_http_{uuid.uuid4().hex}"
    stale_seconds = max(1.0, float(health_stale_seconds))

    class Phase1RuntimeArtifactHttpHandler(BaseHTTPRequestHandler):
        server_version = "Phase1RuntimeArtifactHTTP/1.0"

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler hook
            if urlparse(self.path).path == "/health":
                self._write_json(
                    HTTPStatus.OK,
                    _health_payload(
                        root=root,
                        listener_status_path=status_path,
                        started_at=started_at,
                        generation_id=generation_id,
                        stale_seconds=stale_seconds,
                    ),
                )
                return
            try:
                symbol, timeframe = _route(self.path)
                payload, raw_bytes = _load_payload(
                    root=root,
                    symbol=symbol,
                    timeframe=timeframe,
                    max_response_bytes=byte_limit,
                )
                validate_phase1_runtime_artifact_payload(payload, expected_symbol=symbol, expected_timeframe=timeframe)
            except Phase1RuntimeArtifactHttpError as exc:
                self._write_error(exc.status, exc.classification, str(exc))
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw_bytes)))
            self.end_headers()
            self.wfile.write(raw_bytes)

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _write_json(self, status: HTTPStatus, payload: Mapping[str, Any]) -> None:
            body = json.dumps(dict(payload), sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_error(self, status: HTTPStatus, classification: str, message: str) -> None:
            body = json.dumps(
                {
                    "schema_version": "phase1_runtime_artifact_http_error_v1",
                    "classification": classification,
                    "error": message,
                    "can_submit": False,
                    "live_money_eligible": False,
                },
                sort_keys=True,
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Phase1RuntimeArtifactHttpHandler


def run_phase1_runtime_artifact_http_server(config: Phase1RuntimeArtifactHttpServerConfig) -> None:
    handler = build_phase1_runtime_artifact_http_handler(
        artifact_root=config.artifact_root,
        max_response_bytes=config.max_response_bytes,
        listener_status_path=config.listener_status_path,
        health_stale_seconds=config.health_stale_seconds,
    )
    server = ThreadingHTTPServer((config.host, int(config.port)), handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def validate_phase1_runtime_artifact_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
    expected_timeframe: str,
) -> None:
    symbol = str(payload.get("symbol") or payload.get("instrument") or payload.get("root") or "").strip().upper()
    timeframe = normalize_timeframe_label(str(payload.get("timeframe") or ""))
    if symbol != expected_symbol:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime artifact symbol mismatch: expected {expected_symbol}, found {symbol or '<missing>'}.",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_WRONG_SYMBOL",
        )
    if timeframe != expected_timeframe:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime artifact timeframe mismatch: expected {expected_timeframe}, found {timeframe}.",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_WRONG_TIMEFRAME",
        )
    schema = str(payload.get("schema") or "").strip()
    if schema != "ohlcv-1m":
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime artifact schema mismatch: expected ohlcv-1m, found {schema or '<missing>'}.",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_SCHEMA_MISMATCH",
        )
    _parse_required_timestamp(payload.get("generated_at"), "generated_at")
    bars = payload.get("bars")
    if not isinstance(bars, list):
        raise Phase1RuntimeArtifactHttpError(
            "Phase-1 runtime artifact must expose a bars list.",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_MALFORMED",
        )
    for index, row in enumerate(bars):
        if not isinstance(row, Mapping):
            raise Phase1RuntimeArtifactHttpError(
                f"Phase-1 runtime artifact bar {index} must be an object.",
                status=HTTPStatus.BAD_GATEWAY,
                classification="PHASE1_RUNTIME_ARTIFACT_MALFORMED",
            )
        _parse_required_timestamp(row.get("bar_end") or row.get("end_ts") or row.get("timestamp"), f"bars[{index}].bar_end")
        if row.get("bar_start") or row.get("start_ts"):
            _parse_required_timestamp(row.get("bar_start") or row.get("start_ts"), f"bars[{index}].bar_start")


def _route(raw_path: str) -> tuple[str, str]:
    path = urlparse(raw_path).path
    parts = [unquote(part) for part in path.split("/") if part]
    if len(parts) != 5 or parts[:2] != ["phase1", "runtime-market-data"] or parts[4] != "latest":
        raise Phase1RuntimeArtifactHttpError(
            "Unknown Phase-1 runtime artifact endpoint.",
            status=HTTPStatus.NOT_FOUND,
            classification="PHASE1_RUNTIME_ARTIFACT_ENDPOINT_NOT_FOUND",
        )
    symbol = parts[2].strip().upper()
    if _SYMBOL_RE.fullmatch(symbol) is None:
        raise Phase1RuntimeArtifactHttpError(
            "Phase-1 runtime artifact symbol path segment is invalid.",
            status=HTTPStatus.BAD_REQUEST,
            classification="PHASE1_RUNTIME_ARTIFACT_INVALID_SYMBOL",
        )
    try:
        timeframe = normalize_timeframe_label(parts[3])
    except Exception as exc:  # noqa: BLE001
        raise Phase1RuntimeArtifactHttpError(
            "Phase-1 runtime artifact timeframe path segment is invalid.",
            status=HTTPStatus.BAD_REQUEST,
            classification="PHASE1_RUNTIME_ARTIFACT_INVALID_TIMEFRAME",
        ) from exc
    if timeframe not in _SUPPORTED_TIMEFRAMES:
        raise Phase1RuntimeArtifactHttpError(
            f"Unsupported Phase-1 runtime artifact timeframe: {timeframe}.",
            status=HTTPStatus.BAD_REQUEST,
            classification="PHASE1_RUNTIME_ARTIFACT_INVALID_TIMEFRAME",
        )
    return symbol, timeframe


def _load_payload(
    *,
    root: Path,
    symbol: str,
    timeframe: str,
    max_response_bytes: int,
) -> tuple[dict[str, Any], bytes]:
    path = root / symbol / timeframe / "latest_runtime_candles.json"
    try:
        with path.open("rb") as handle:
            raw = handle.read(max_response_bytes + 1)
    except FileNotFoundError as exc:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime candle artifact is missing: {path}",
            status=HTTPStatus.NOT_FOUND,
            classification="PHASE1_RUNTIME_ARTIFACT_MISSING",
        ) from exc
    except OSError as exc:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime candle artifact could not be read: {path}",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_READ_FAILED",
        ) from exc
    if len(raw) > max_response_bytes:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime candle artifact exceeds response byte limit: {path}",
            status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            classification="PHASE1_RUNTIME_ARTIFACT_TOO_LARGE",
        )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime candle artifact is malformed JSON: {path}",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_MALFORMED",
        ) from exc
    if not isinstance(payload, dict):
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime candle artifact must contain a JSON object: {path}",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_MALFORMED",
        )
    return payload, raw


def _health_payload(
    *,
    root: Path,
    listener_status_path: Path | None,
    started_at: datetime,
    generation_id: str,
    stale_seconds: float,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    root_readable = root.exists() and root.is_dir() and os.access(root, os.R_OK | os.X_OK)
    latest_artifact = _latest_artifact_snapshot(root) if root_readable else None
    listener_status = _listener_status_snapshot(listener_status_path)
    latest_artifact_age = _age_seconds(now, latest_artifact.get("generated_at") if latest_artifact else None)
    listener_status_age = _age_seconds(now, listener_status.get("timestamp") if listener_status else None)
    blockers: list[str] = []
    if not root_readable:
        blockers.append("artifact_root_not_readable")
    if latest_artifact is None:
        blockers.append("latest_artifact_missing")
    elif latest_artifact_age is None or latest_artifact_age > stale_seconds:
        blockers.append("latest_artifact_stale")
    if listener_status_path is not None:
        if listener_status is None:
            blockers.append("listener_status_missing")
        elif listener_status_age is None or listener_status_age > stale_seconds:
            blockers.append("listener_status_stale")
    listener_replay_status = str(listener_status.get("replay_catchup_status") or "") if listener_status else ""
    if listener_status and listener_replay_status in {"STARTING", "REPLAY_CATCHUP", "STALE"}:
        blockers.append(f"listener_replay_{listener_replay_status.lower()}")
    if blockers:
        classification = listener_replay_status if listener_replay_status in {"STARTING", "REPLAY_CATCHUP", "STALE"} else "STALE_OR_BLOCKED"
    elif listener_replay_status in {"CURRENT", "DEGRADED"}:
        classification = listener_replay_status
    else:
        classification = "HEALTHY"
    return {
        "schema_version": "phase1_runtime_artifact_http_health_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "blockers": blockers,
        "process": {
            "generation_id": generation_id,
            "started_at": started_at.isoformat(),
            "uptime_seconds": round(max((now - started_at).total_seconds(), 0.0), 3),
        },
        "artifact_root": {
            "path": str(root),
            "readable": root_readable,
        },
        "latest_artifact": latest_artifact,
        "latest_artifact_age_seconds": latest_artifact_age,
        "listener_status": listener_status,
        "listener_status_age_seconds": listener_status_age,
        "stale_threshold_seconds": stale_seconds,
        "can_submit": False,
        "live_money_eligible": False,
    }


def _latest_artifact_snapshot(root: Path) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for path in root.glob("*/*/latest_runtime_candles.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, Mapping):
            continue
        generated_at = _optional_timestamp(payload.get("generated_at"))
        if generated_at is None:
            continue
        candidate = {
            "path": str(path),
            "symbol": str(payload.get("symbol") or path.parents[1].name),
            "timeframe": str(payload.get("timeframe") or path.parent.name),
            "generated_at": generated_at,
            "last_completed_bar_ts": str(payload.get("last_completed_bar_ts") or ""),
            "bar_count": payload.get("bar_count"),
        }
        if latest is None or generated_at > latest["generated_at"]:
            latest = candidate
    if latest is None:
        return None
    latest["generated_at"] = latest["generated_at"].isoformat()
    return latest


def _listener_status_snapshot(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    timestamp = _optional_timestamp(payload.get("latest_record_at")) or _optional_timestamp(payload.get("generated_at"))
    if timestamp is None:
        return None
    return {
        "path": str(path),
        "timestamp": timestamp.isoformat(),
        "provider_status": payload.get("provider_status"),
        "listener_alive": payload.get("listener_alive"),
        "realtime_feed_confirmed_count": payload.get("realtime_feed_confirmed_count"),
        "readiness_required_confirmed_count": payload.get("readiness_required_confirmed_count"),
        "replay_catchup_status": payload.get("replay_catchup_status"),
        "selected_replay_anchor": payload.get("selected_replay_anchor"),
        "replay_anchor_source": payload.get("replay_anchor_source"),
        "current_lag_seconds": payload.get("current_lag_seconds"),
        "latest_durable_completed_bar_ts": payload.get("latest_durable_completed_bar_ts"),
        "current_readiness_blocked_reason": payload.get("current_readiness_blocked_reason"),
    }


def _age_seconds(now: datetime, timestamp: Any) -> float | None:
    parsed = _optional_timestamp(timestamp)
    if parsed is None:
        return None
    return round(max((now - parsed).total_seconds(), 0.0), 3)


def _optional_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_required_timestamp(value: Any, field_name: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime artifact is missing timestamp field {field_name}.",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_TIMESTAMP_MISSING",
        )
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Phase1RuntimeArtifactHttpError(
            f"Phase-1 runtime artifact timestamp field {field_name} is invalid.",
            status=HTTPStatus.BAD_GATEWAY,
            classification="PHASE1_RUNTIME_ARTIFACT_TIMESTAMP_INVALID",
        ) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve Phase-1 runtime candle artifacts over read-only HTTP.")
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--host", default=DEFAULT_BIND_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_BIND_PORT)
    parser.add_argument("--max-response-bytes", type=int, default=DEFAULT_MAX_RESPONSE_BYTES)
    parser.add_argument("--listener-status-path", type=Path, default=DEFAULT_LISTENER_STATUS_PATH)
    parser.add_argument("--health-stale-seconds", type=float, default=DEFAULT_HEALTH_STALE_SECONDS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_phase1_runtime_artifact_http_server(
        Phase1RuntimeArtifactHttpServerConfig(
            artifact_root=args.artifact_root,
            host=args.host,
            port=args.port,
            max_response_bytes=args.max_response_bytes,
            listener_status_path=args.listener_status_path,
            health_stale_seconds=args.health_stale_seconds,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
