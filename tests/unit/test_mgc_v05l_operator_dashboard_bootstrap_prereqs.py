from __future__ import annotations

from pathlib import Path

from mgc_v05l.app.operator_dashboard import OperatorDashboardService

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_dashboard_bootstrap_prerequisites_report_missing_replay_db_and_auth_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    missing_db = tmp_path / "mgc_v05l.replay.sqlite3"
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_STATUS", "missing")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_PATH", str(missing_db))
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_REPLAY_DB_NEXT_ACTION",
        f"Run `bash scripts/backfill_schwab_1m_history.sh` to create and populate {missing_db}.",
    )
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES",
        "SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL",
    )
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS", "missing")
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_NEXT_ACTION",
        "Export SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL or source .local/schwab_env.sh before running Schwab-backed bootstrap actions.",
    )
    monkeypatch.delenv("SCHWAB_APP_KEY", raising=False)
    monkeypatch.delenv("SCHWAB_APP_SECRET", raising=False)
    monkeypatch.delenv("SCHWAB_CALLBACK_URL", raising=False)

    payload = OperatorDashboardService(tmp_path)._dashboard_bootstrap_prerequisites_payload()  # noqa: SLF001

    assert payload["status"] == "reduced_mode"
    assert payload["reduced_mode"] is True
    assert payload["issue_count"] == 2
    replay_item = next(item for item in payload["items"] if item["key"] == "replay_database")
    auth_item = next(item for item in payload["items"] if item["key"] == "schwab_auth_env")
    assert replay_item["status"] == "missing"
    assert "backfill_schwab_1m_history.sh" in replay_item["next_action"]
    assert auth_item["status"] == "missing"
    assert auth_item["missing_names"] == ["SCHWAB_APP_KEY", "SCHWAB_APP_SECRET", "SCHWAB_CALLBACK_URL"]


def test_dashboard_snapshot_surfaces_bootstrap_prerequisites_without_replay_db(
    tmp_path: Path,
    monkeypatch,
) -> None:
    missing_db = tmp_path / "mgc_v05l.replay.sqlite3"
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_STATUS", "missing")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_PATH", str(missing_db))
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES",
        "SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL",
    )
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS", "missing")
    monkeypatch.delenv("SCHWAB_APP_KEY", raising=False)
    monkeypatch.delenv("SCHWAB_APP_SECRET", raising=False)
    monkeypatch.delenv("SCHWAB_CALLBACK_URL", raising=False)

    snapshot = OperatorDashboardService(REPO_ROOT).snapshot()

    assert snapshot["bootstrap_prerequisites"]["reduced_mode"] is True
    assert snapshot["bootstrap_prerequisites"]["issue_count"] == 2
    assert snapshot["operator_surface"]["runtime_readiness"]["bootstrap_prerequisites"]["reduced_mode"] is True
    assert snapshot["research_capture"]["research_database_present"] is False
    assert "reduced mode" in snapshot["research_capture"]["status_line"].lower()


def test_dashboard_bootstrap_prerequisites_report_ready_when_inputs_exist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    replay_db = tmp_path / "mgc_v05l.replay.sqlite3"
    replay_db.write_text("", encoding="utf-8")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_STATUS", "ready")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_PATH", str(replay_db))
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES", "")
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS", "ready")
    monkeypatch.setenv("SCHWAB_APP_KEY", "key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182/callback")

    payload = OperatorDashboardService(tmp_path)._dashboard_bootstrap_prerequisites_payload()  # noqa: SLF001

    assert payload["status"] == "ready"
    assert payload["reduced_mode"] is False
    assert payload["issue_count"] == 0
