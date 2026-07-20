from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

from mgc_v05l.execution_core.phase1_runtime_artifact_http_server import build_phase1_runtime_artifact_http_handler


def _write_artifact(root: Path, *, symbol: str = "MNQ", timeframe: str = "1m", **overrides: object) -> None:
    path = root / symbol / timeframe / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": "2026-05-18T12:00:10+00:00",
        "source": "DATABENTO_REALTIME_PHASE1",
        "symbol": symbol,
        "instrument": symbol,
        "timeframe": timeframe,
        "schema": "ohlcv-1m",
        "bars": [
            {
                "bar_start": "2026-05-18T11:59:00+00:00",
                "bar_end": "2026-05-18T12:00:00+00:00",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
                "completed": True,
            }
        ],
    }
    payload.update(overrides)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


class _Server:
    def __init__(self, root: Path, *, listener_status_path: Path | None = None, health_stale_seconds: float = 180.0) -> None:
        handler = build_phase1_runtime_artifact_http_handler(
            artifact_root=root,
            listener_status_path=listener_status_path,
            health_stale_seconds=health_stale_seconds,
        )
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def test_phase1_runtime_artifact_http_endpoint_serves_existing_payload(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_artifact(root)
    server = _Server(root)
    try:
        with urlopen(f"{server.base_url}/phase1/runtime-market-data/MNQ/1m/latest", timeout=2) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.close()

    assert payload["source"] == "DATABENTO_REALTIME_PHASE1"
    assert payload["symbol"] == "MNQ"
    assert payload["timeframe"] == "1m"
    assert payload["schema"] == "ohlcv-1m"


def test_phase1_runtime_artifact_http_endpoint_reports_missing_and_bad_metadata(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    server = _Server(root)
    try:
        try:
            urlopen(f"{server.base_url}/phase1/runtime-market-data/MNQ/1m/latest", timeout=2)  # noqa: S310
        except HTTPError as exc:
            missing_status = exc.code
            missing_payload = json.loads(exc.read().decode("utf-8"))
        else:  # pragma: no cover
            raise AssertionError("missing artifact should return an HTTP error")

        _write_artifact(root, symbol="MNQ", timeframe="1m", schema="trades")
        try:
            urlopen(f"{server.base_url}/phase1/runtime-market-data/MNQ/1m/latest", timeout=2)  # noqa: S310
        except HTTPError as exc:
            malformed_status = exc.code
            malformed_payload = json.loads(exc.read().decode("utf-8"))
        else:  # pragma: no cover
            raise AssertionError("bad artifact schema should return an HTTP error")
    finally:
        server.close()

    assert missing_status == 404
    assert missing_payload["classification"] == "PHASE1_RUNTIME_ARTIFACT_MISSING"
    assert malformed_status == 502
    assert malformed_payload["classification"] == "PHASE1_RUNTIME_ARTIFACT_SCHEMA_MISMATCH"


def test_phase1_runtime_artifact_http_health_reports_process_and_fresh_inputs(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    status_path = tmp_path / "latest_phase1_databento_live_listener_status.json"
    now = datetime.now(timezone.utc).isoformat()
    _write_artifact(root, generated_at=now)
    status_path.write_text(
        json.dumps(
            {
                "generated_at": now,
                "latest_record_at": now,
                "provider_status": "RUNNING",
                "listener_alive": True,
                "realtime_feed_confirmed_count": 1,
            }
        ),
        encoding="utf-8",
    )
    server = _Server(root, listener_status_path=status_path)
    try:
        with urlopen(f"{server.base_url}/health", timeout=2) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.close()

    assert payload["classification"] == "HEALTHY"
    assert payload["process"]["generation_id"].startswith("phase1_runtime_artifact_http_")
    assert payload["artifact_root"]["readable"] is True
    assert payload["latest_artifact"]["symbol"] == "MNQ"
    assert payload["listener_status"]["listener_alive"] is True
    assert payload["can_submit"] is False
    assert payload["live_money_eligible"] is False


def test_phase1_runtime_artifact_http_health_reports_stale_or_blocked_inputs(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_artifact(root, generated_at="2026-05-18T12:00:10+00:00")
    server = _Server(root, listener_status_path=tmp_path / "missing_status.json", health_stale_seconds=1)
    try:
        with urlopen(f"{server.base_url}/health", timeout=2) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.close()

    assert payload["classification"] == "STALE_OR_BLOCKED"
    assert "latest_artifact_stale" in payload["blockers"]
    assert "listener_status_missing" in payload["blockers"]
