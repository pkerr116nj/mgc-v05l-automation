from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.app import track_b_canonical_readiness as cli


def _payload(state: str) -> dict:
    return {
        "schema_version": "track_b_canonical_readiness_v1",
        "generated_at": "2026-05-18T12:00:00+00:00",
        "paper_only": True,
        "canonical_readiness": state,
        "state": state,
        "readiness_blockers": []
        if not state.startswith("NOT_READY")
        else [{"code": "broker_truth_not_fresh_or_complete"}],
        "readiness_warnings": [{"code": "latest_broker_attempt_failed"}] if state == "DEGRADED_NO_SUBMIT" else [],
        "root_guard_summary": {
            "root_match": state != "NOT_READY_WRONG_ROOT",
            "wrong_root_processes": [],
        },
        "broker_truth": {"fresh": state != "NOT_READY_DEPENDENCY"},
        "phase1_reconciliation": {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
        "runtime": {"eligible_lane_count": 3 if state == "READY_SUBMIT_CAPABLE" else 0},
        "lane_quarantine": {"quarantine_count": 0},
        "live_money_eligible": False,
    }


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_produces_artifact_without_dashboard(tmp_path: Path, monkeypatch, capsys) -> None:
    repo_root = tmp_path
    runtime_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime"
    report_dir = repo_root / "outputs" / "reports"
    lanes_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "mnq"
    runtime_dir.mkdir(parents=True)
    (report_dir / "ibkr_read_only_verification").mkdir(parents=True)
    (report_dir / "track_b_paper_broker_reconciliation").mkdir(parents=True)
    lanes_dir.mkdir(parents=True)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json").write_text(
        """
        {
          "source_runtime_pid": 100,
          "source_runtime_repo_root": "%s",
          "active_lane_ids": ["mnq"],
          "usable_lane_count": 1,
          "entries_enabled": true,
          "operator_halt": false,
          "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
          "health": {"market_data_ok": true}
        }
        """
        % repo_root,
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text('{"lanes": [{"lane_id": "mnq"}]}', encoding="utf-8")
    (runtime_dir / "paper_lane_quarantine_status.json").write_text(
        '{"classification": "PAPER_LANE_QUARANTINE_CLEAR", "quarantine_count": 0, "live_money_eligible": false}',
        encoding="utf-8",
    )
    (runtime_dir / "market_data_transport_probe.json").write_text('{"status": "ok", "runtime_ready": true}', encoding="utf-8")
    (report_dir / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json").write_text(
        """
        {
          "classification": "BROKER_TRUTH_REFRESH_READY",
          "generated_at": "2026-05-18T11:59:10+00:00",
          "last_success": true,
          "positions_complete": true,
          "open_orders_complete": true,
          "live_money_eligible": false
        }
        """,
        encoding="utf-8",
    )
    (
        report_dir
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    ).write_text(
        """
        {
          "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
          "generated_at": "2026-05-18T11:59:20+00:00",
          "broker_reconciled": true,
          "review_required_count": 0,
          "lifecycle_open_position_count": 0,
          "live_money_eligible": false
        }
        """,
        encoding="utf-8",
    )
    (lanes_dir / "live_timing_summary_latest.json").write_text(
        """
        {
          "lane_id": "mnq",
          "broker_truth": {
            "account_health": {
              "status": "HEALTHY",
              "route_destination": "ibkr_paper_bridge_submit_capable"
            }
          }
        }
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 100)
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state.datetime", _FrozenDateTime)

    output = repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    exit_code = cli.main(["--repo-root", str(repo_root), "--expected-root", str(repo_root), "--json"])

    stdout = capsys.readouterr().out
    summary = json.loads(stdout)
    artifact = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert output.exists()
    assert artifact["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert summary["classification"] == "READY_SUBMIT_CAPABLE"
    assert summary["root_match"] is True


def test_main_writes_summary_output_when_requested(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "readiness.json"
    summary_output = tmp_path / "summary.json"
    monkeypatch.setattr(
        cli,
        "write_canonical_readiness_artifact",
        lambda **kwargs: _write_and_return(output, _payload("READY_SUBMIT_CAPABLE")),
    )

    exit_code = cli.main(
        [
            "--repo-root",
            str(tmp_path),
            "--output-path",
            str(output),
            "--summary-output-path",
            str(summary_output),
        ]
    )

    summary = json.loads(summary_output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert summary["classification"] == "READY_SUBMIT_CAPABLE"


def test_wrong_root_exits_2(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "readiness.json"
    monkeypatch.setattr(cli, "write_canonical_readiness_artifact", lambda **kwargs: _write_and_return(output, _payload("NOT_READY_WRONG_ROOT")))

    assert cli.main(["--repo-root", str(tmp_path), "--output-path", str(output)]) == 2


def test_clean_submit_capable_fixture_exits_0(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "readiness.json"
    monkeypatch.setattr(cli, "write_canonical_readiness_artifact", lambda **kwargs: _write_and_return(output, _payload("READY_SUBMIT_CAPABLE")))

    assert cli.main(["--repo-root", str(tmp_path), "--output-path", str(output)]) == 0


def test_degraded_fixture_exits_1(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "readiness.json"
    monkeypatch.setattr(cli, "write_canonical_readiness_artifact", lambda **kwargs: _write_and_return(output, _payload("DEGRADED_NO_SUBMIT")))

    assert cli.main(["--repo-root", str(tmp_path), "--output-path", str(output)]) == 1


def test_not_ready_dependency_exits_2(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "readiness.json"
    monkeypatch.setattr(cli, "write_canonical_readiness_artifact", lambda **kwargs: _write_and_return(output, _payload("NOT_READY_DEPENDENCY")))

    assert cli.main(["--repo-root", str(tmp_path), "--output-path", str(output)]) == 2


def test_json_summary_contains_compact_fields(capsys) -> None:
    cli.print_summary(cli.compact_readiness_summary(_payload("READY_SUBMIT_CAPABLE")), as_json=True)

    summary = json.loads(capsys.readouterr().out)
    assert sorted(summary) == [
        "blockers",
        "broker_truth_fresh",
        "broker_truth_lease_age_seconds",
        "broker_truth_lease_entry_seconds_remaining",
        "broker_truth_lease_state",
        "classification",
        "eligible_lane_count",
        "generated_at",
        "quarantine_count",
        "reconciliation_state",
        "root_match",
        "warnings",
    ]
    assert summary["generated_at"] == "2026-05-18T12:00:00+00:00"


def test_cli_source_has_no_broker_order_api_calls() -> None:
    source = Path(cli.__file__).read_text(encoding="utf-8")
    forbidden = (
        "placeOrder",
        "cancelOrder",
        "reqGlobalCancel",
        "place_order",
        "submit_order",
        "broker.submit",
        "broker.cancel",
        "broker.close",
        "closePosition",
    )
    assert not any(token in source for token in forbidden)


def _write_and_return(output: Path, payload: dict) -> dict:
    _write(output, payload)
    return payload


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        value = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
        return value if tz is None else value.astimezone(tz)
