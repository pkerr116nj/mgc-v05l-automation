"""CLI integration coverage for replay research utilities."""

import json
from pathlib import Path

import mgc_v05l.app.schwab_token_bootstrap_web as schwab_token_bootstrap_web_module
from mgc_v05l.app.main import main


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


def test_probationary_operator_control_cli_queues_resume_entries(tmp_path: Path, capsys) -> None:
    control_path = tmp_path / "paper_artifacts" / "operator_control.json"
    override_config = tmp_path / "override.yaml"
    override_config.write_text(
        (
            f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"\n'
            f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"\n'
            "probationary_paper_runtime_exclusive_config: true\n"
            f'probationary_operator_control_path: "{control_path}"\n'
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "probationary-operator-control",
            "--config",
            "config/base.yaml",
            "--config",
            "config/live.yaml",
            "--config",
            "config/probationary_pattern_engine.yaml",
            "--config",
            "config/probationary_pattern_engine_paper.yaml",
            "--config",
            str(override_config),
            "--action",
            "resume_entries",
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)
    control_payload = json.loads(control_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["action"] == "resume_entries"
    assert payload["status"] == "pending"
    assert payload["control_path"] == str(control_path)
    assert control_payload["action"] == "resume_entries"
    assert control_payload["status"] == "pending"


def test_probationary_operator_control_cli_targets_shared_strategy_identity(tmp_path: Path, capsys) -> None:
    control_path = tmp_path / "paper_artifacts" / "runtime" / "operator_control.json"
    override_config = tmp_path / "override.yaml"
    override_config.write_text(
        (
            f'database_url: "sqlite:///{tmp_path / "probationary.paper.sqlite3"}"\n'
            f'probationary_artifacts_dir: "{tmp_path / "paper_artifacts"}"\n'
            "probationary_paper_runtime_exclusive_config: true\n"
            'probationary_paper_lanes_json: \'[{"shared_strategy_identity":"ATP_COMPANION_V1_ASIA_US","lane_id":"atp_companion_v1_asia_us","display_name":"ATP Companion","symbol":"MGC","long_sources":["trend_participation.pullback_continuation.long.conservative"],"short_sources":[],"session_restriction":"ASIA/US","allowed_sessions":["ASIA","US"],"point_value":"10","trade_size":1,"catastrophic_open_loss":"-500","lane_mode":"ATP_COMPANION_BENCHMARK","strategy_family":"active_trend_participation_engine","strategy_identity_root":"ATP_COMPANION_V1_ASIA_US","runtime_kind":"atp_companion_benchmark_paper"}]\'\n'
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "probationary-operator-control",
            "--config",
            "config/base.yaml",
            "--config",
            "config/live.yaml",
            "--config",
            "config/probationary_pattern_engine.yaml",
            "--config",
            "config/probationary_pattern_engine_paper.yaml",
            "--config",
            str(override_config),
            "--action",
            "resume_entries",
            "--shared-strategy-identity",
            "ATP_COMPANION_V1_ASIA_US",
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)
    control_payload = json.loads(control_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["control_path"] == str(control_path)
    assert control_payload["action"] == "resume_entries"
    assert control_payload["control_scope"] == "lane"
    assert control_payload["lane_id"] == "atp_companion_v1_asia_us"
    assert control_payload["shared_strategy_identity"] == "ATP_COMPANION_V1_ASIA_US"


def test_schwab_auth_gate_cli_reports_runtime_readiness(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class _FakeService:
        def __init__(self, *, token_file=None, schwab_config_path=None, probe_symbol="MGC", **_kwargs) -> None:
            captured["token_file"] = token_file
            captured["schwab_config_path"] = schwab_config_path
            captured["probe_symbol"] = probe_symbol

        def check_runtime_ready(self) -> dict[str, object]:
            return {"runtime_ready": True, "probe_symbol": captured["probe_symbol"]}

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
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)

    assert exit_code == 0
    assert captured["token_file"] == "/tmp/test-tokens.json"
    assert str(captured["schwab_config_path"]) == "config/schwab.local.json"
    assert captured["probe_symbol"] == "MGC"
    assert payload["runtime_ready"] is True
    assert payload["probe_symbol"] == "MGC"


def test_schwab_token_web_cli_runs_existing_bootstrap_server(monkeypatch, capsys) -> None:
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
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)

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
