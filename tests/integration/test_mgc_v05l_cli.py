"""CLI integration coverage for replay research utilities."""

import json
from pathlib import Path
from unittest.mock import Mock

import mgc_v05l.app.schwab_token_bootstrap_web as schwab_token_bootstrap_web_module
from mgc_v05l.app.main import main
from mgc_v05l.app.probationary_runtime import ProbationaryRuntimeTransportFailure


def test_research_causal_report_cli_writes_output(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    report_csv = tmp_path / "report.csv"
    replay_db = tmp_path / "cli.sqlite3"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    replay_csv = tmp_path / "replay.csv"
    replay_csv.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-03-13T18:00:00-04:00,100,101,99,100,100\n"
        "2026-03-13T18:05:00-04:00,100,103,99,102,100\n"
        "2026-03-13T18:10:00-04:00,102,104,101,103,100\n",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "research-causal-report",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--csv",
            str(replay_csv),
            "--output",
            str(report_csv),
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)

    assert exit_code == 0
    assert payload["rows"] == 3
    assert payload["research_only"] is True
    assert report_csv.exists()


def test_probationary_paper_soak_cli_invokes_runner(monkeypatch, capsys, tmp_path: Path) -> None:
    summary = {
        "artifacts_dir": tmp_path / "paper_session",
        "processed_bars": 7,
        "reconciliation_clean": True,
        "stop_reason": None,
    }
    runner = Mock()
    runner.run.return_value = summary
    builder = Mock(return_value=runner)
    monkeypatch.setattr("mgc_v05l.app.main.build_probationary_paper_runner", builder)

    exit_code = main(
        [
            "probationary-paper-soak",
            "--schwab-config",
            "config/custom_schwab.json",
            "--poll-once",
            "--max-cycles",
            "5",
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)

    assert exit_code == 0
    builder.assert_called_once_with(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        schwab_config_path="config/custom_schwab.json",
    )
    runner.run.assert_called_once_with(poll_once=True, max_cycles=5)
    assert payload == {
        "artifacts_dir": str(tmp_path / "paper_session"),
        "processed_bars": 7,
        "reconciliation_clean": True,
        "stop_reason": None,
    }


def test_probationary_operator_control_cli_routes_shared_lane_target(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_submit(config_paths, action: str, *, payload=None, shared_strategy_identity=None):
        captured["config_paths"] = list(config_paths)
        captured["action"] = action
        captured["payload"] = payload
        captured["shared_strategy_identity"] = shared_strategy_identity
        return {
            "action": action,
            "control_path": "/tmp/operator_control.json",
            "status": "pending",
            "requested_at": "2026-04-01T00:00:00+00:00",
        }

    monkeypatch.setattr("mgc_v05l.app.main.submit_probationary_operator_control", _fake_submit)

    exit_code = main(
        [
            "probationary-operator-control",
            "--action",
            "resume_entries",
            "--lane-id",
            "mgc_us_late_pause_resume_long",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "config_paths": [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        "action": "resume_entries",
        "payload": {"lane_id": "mgc_us_late_pause_resume_long"},
        "shared_strategy_identity": None,
    }
    assert payload["action"] == "resume_entries"
    assert payload["status"] == "pending"


def test_probationary_operator_control_cli_routes_shared_strategy_identity(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_submit(config_paths, action: str, *, payload=None, shared_strategy_identity=None):
        captured["config_paths"] = list(config_paths)
        captured["action"] = action
        captured["payload"] = payload
        captured["shared_strategy_identity"] = shared_strategy_identity
        return {
            "action": action,
            "control_path": "/tmp/operator_control.json",
            "status": "pending",
            "requested_at": "2026-04-01T00:00:00+00:00",
        }

    monkeypatch.setattr("mgc_v05l.app.main.submit_probationary_operator_control", _fake_submit)

    exit_code = main(
        [
            "probationary-operator-control",
            "--action",
            "resume_entries",
            "--shared-strategy-identity",
            "ATP_COMPANION_V1_ASIA_US",
            "--payload-json",
            '{"source":"cli-test"}',
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "config_paths": [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        "action": "resume_entries",
        "payload": {"source": "cli-test"},
        "shared_strategy_identity": "ATP_COMPANION_V1_ASIA_US",
    }
    assert payload["action"] == "resume_entries"
    assert payload["status"] == "pending"


def test_probationary_market_data_probe_cli_routes_shared_runtime_probe(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_probe(config_paths, schwab_config_path):
        captured["config_paths"] = list(config_paths)
        captured["schwab_config_path"] = schwab_config_path
        return {"status": "ok", "runtime_ready": True, "artifact_path": "/tmp/probe.json"}

    monkeypatch.setattr("mgc_v05l.app.main.run_probationary_market_data_transport_probe", _fake_probe)

    exit_code = main(["probationary-market-data-probe", "--schwab-config", "config/custom.json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "config_paths": [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        "schwab_config_path": "config/custom.json",
    }
    assert payload["runtime_ready"] is True


def test_probationary_paper_soak_cli_returns_structured_transport_failure(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "mgc_v05l.app.main.build_probationary_paper_runner",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ProbationaryRuntimeTransportFailure(
                {
                    "blocker_label": "market_data_transport_failure",
                    "target_host": "api.schwabapi.com",
                    "rendered_url": "https://api.schwabapi.com/marketdata/v1/pricehistory",
                    "exception_text": "dns failed",
                    "runtime_ready": False,
                }
            )
        ),
    )

    exit_code = main(["probationary-paper-soak"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["blocker_label"] == "market_data_transport_failure"
    assert payload["runtime_ready"] is False


def test_schwab_auth_gate_cli_routes_shared_probe(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class _FakeService:
        def __init__(self, *, token_file=None, schwab_config_path=None, probe_symbol="MGC", **_kwargs) -> None:
            captured["token_file"] = token_file
            captured["schwab_config_path"] = schwab_config_path
            captured["probe_symbol"] = probe_symbol

        def check_runtime_ready(self) -> dict[str, object]:
            return {
                "runtime_ready": True,
                "probe_symbol": captured["probe_symbol"],
                "schwab_config_path": str(captured["schwab_config_path"]),
            }

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "SchwabTokenBootstrapService", _FakeService)

    exit_code = main(
        [
            "schwab-auth-gate",
            "--token-file",
            "/tmp/test-tokens.json",
            "--schwab-config",
            "config/schwab.local.json",
            "--internal-symbol",
            "MGC",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "token_file": "/tmp/test-tokens.json",
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
    }
    assert payload["runtime_ready"] is True
    assert payload["probe_symbol"] == "MGC"


def test_schwab_token_web_cli_routes_shared_bootstrap_server(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_run_server(
        *,
        host: str,
        port: int,
        token_file=None,
        open_browser: bool = True,
        info_file=None,
        port_search_limit: int = 25,
        schwab_config_path=None,
        probe_symbol: str = "MGC",
    ) -> dict[str, object]:
        captured.update(
            {
                "host": host,
                "port": port,
                "token_file": token_file,
                "open_browser": open_browser,
                "info_file": info_file,
                "port_search_limit": port_search_limit,
                "schwab_config_path": schwab_config_path,
                "probe_symbol": probe_symbol,
            }
        )
        return {"url": f"http://{host}:{port}/", "port": port}

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "run_schwab_token_bootstrap_server", _fake_run_server)

    exit_code = main(
        [
            "schwab-token-web",
            "--host",
            "127.0.0.1",
            "--port",
            "8765",
            "--token-file",
            "/tmp/test-tokens.json",
            "--info-file",
            "/tmp/bootstrap-info.json",
            "--port-search-limit",
            "7",
            "--schwab-config",
            "config/schwab.local.json",
            "--probe-symbol",
            "MGC",
            "--no-browser",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "host": "127.0.0.1",
        "port": 8765,
        "token_file": "/tmp/test-tokens.json",
        "open_browser": False,
        "info_file": "/tmp/bootstrap-info.json",
        "port_search_limit": 7,
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
    }
    assert payload["url"] == "http://127.0.0.1:8765/"
    assert payload["port"] == 8765


def test_schwab_debug_exchange_refresh_cli_routes_shared_backend_harness(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class _FakeService:
        def __init__(self, *, token_file=None, schwab_config_path=None, probe_symbol="MGC", **_kwargs) -> None:
            captured["token_file"] = token_file
            captured["schwab_config_path"] = schwab_config_path
            captured["probe_symbol"] = probe_symbol

        def debug_exchange_refresh(self, code: str) -> dict[str, object]:
            captured["code"] = code
            return {
                "final_state": "RUNTIME_READY",
                "runtime_ready": True,
                "exchange_diagnostic_path": "/tmp/latest_exchange_result.json",
            }

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "SchwabTokenBootstrapService", _FakeService)

    exit_code = main(
        [
            "schwab-debug-exchange-refresh",
            "--code",
            "abc123",
            "--token-file",
            "/tmp/test-tokens.json",
            "--schwab-config",
            "config/schwab.local.json",
            "--probe-symbol",
            "MGC",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "token_file": "/tmp/test-tokens.json",
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
        "code": "abc123",
    }
    assert payload["final_state"] == "RUNTIME_READY"


def test_schwab_local_authorize_proof_cli_routes_shared_backend_harness(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class _FakeService:
        def __init__(self, *, token_file=None, schwab_config_path=None, probe_symbol="MGC", **_kwargs) -> None:
            captured["token_file"] = token_file
            captured["schwab_config_path"] = schwab_config_path
            captured["probe_symbol"] = probe_symbol

        def local_authorize_proof(self, *, state: str, scope=None, timeout_seconds: int = 180) -> dict[str, object]:
            captured["state"] = state
            captured["scope"] = scope
            captured["timeout_seconds"] = timeout_seconds
            return {
                "final_state": "RUNTIME_READY",
                "runtime_ready": True,
                "exchange_diagnostic_path": "/tmp/latest_exchange_result.json",
                "refresh_result_artifact_path": "/tmp/latest_refresh_result.json",
            }

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "SchwabTokenBootstrapService", _FakeService)

    exit_code = main(
        [
            "schwab-local-authorize-proof",
            "--token-file",
            "/tmp/test-tokens.json",
            "--schwab-config",
            "config/schwab.local.json",
            "--probe-symbol",
            "MGC",
            "--state",
            "abc-state",
            "--timeout-seconds",
            "240",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "token_file": "/tmp/test-tokens.json",
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
        "state": "abc-state",
        "scope": None,
        "timeout_seconds": 240,
    }
    assert payload["final_state"] == "RUNTIME_READY"
