from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import mgc_v05l.app.weekly_data_maintenance as wdm
from mgc_v05l.app.weekly_data_maintenance import (
    PHASE1_RUNTIME_TICKER_ORDER,
    WeeklyMaintenanceConfig,
    build_weekly_data_maintenance_report,
    main,
    write_weekly_data_maintenance_report,
)
from mgc_v05l.paths import PROJECT_ROOT


def _write(path: Path, text: str = "{}") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_apply_mode_rejected_before_outputs(tmp_path: Path) -> None:
    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, mode="apply")
    )

    assert report["final_verdict"] == "APPLY_DISPOSABLE_BUILD_BLOCKED"
    assert report["review_required"] is True
    assert "CONFIRM_DISPOSABLE_BUILD_CLEANUP_REQUIRED" in report["blocking_reasons"]


def test_default_symbols_follow_phase1_runtime_registry(tmp_path: Path) -> None:
    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, week_ending=date(2026, 5, 8))
    )

    assert tuple(report["symbols"]) == PHASE1_RUNTIME_TICKER_ORDER
    assert report["phase1_symbol_count"] == len(PHASE1_RUNTIME_TICKER_ORDER)
    assert report["week_start"] == "2026-05-02"
    assert report["week_end"] == "2026-05-08"


def test_default_roots_derive_from_canonical_project_root() -> None:
    assert wdm.REPO_ROOT == PROJECT_ROOT
    assert wdm.DEFAULT_ARCHIVE_STAGING_ROOT == PROJECT_ROOT.parent / "_mgc_v05l_archive_staging"
    assert "/Users/patrick/Dev/MGC-v05l-automation" not in str(wdm.DEFAULT_ARCHIVE_STAGING_ROOT)
    assert not any(pattern.startswith("/Users/patrick/Documents") for pattern in wdm.OLD_ROOT_PATTERNS)


def test_active_runtime_files_are_preserved_not_delete_candidates(tmp_path: Path) -> None:
    _write(
        tmp_path / "var" / "runtime_market_data" / "MNQ" / "1m" / "latest_runtime_candles.json",
        '{"symbol":"MNQ"}',
    )
    _write(
        tmp_path / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / "GC" / "1m" / "latest_runtime_candles.json",
        '{"symbol":"GC"}',
    )

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, include_full_paths=True)
    )
    rows = {row["path"]: row for row in report["classifications"]}

    assert rows["var/runtime_market_data/MNQ/1m/latest_runtime_candles.json"]["classification"] == "HOT_DECISION_RUNTIME_DATA"
    assert rows["outputs/track_b_execution_core/phase1_runtime_market_data/GC/1m/latest_runtime_candles.json"]["classification"] == "HOT_DECISION_RUNTIME_DATA"
    assert report["delete_candidates_count"] == 0


def test_broker_review_evidence_is_archive_or_preserve_not_delete(tmp_path: Path) -> None:
    _write(tmp_path / "outputs" / "reports" / "manual_reconciliation_close" / "latest_review_required.json")
    _write(tmp_path / "outputs" / "reports" / "ibkr_bridge" / "broker_order_evidence.json")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, include_full_paths=True)
    )
    rows = {row["path"]: row for row in report["classifications"]}

    assert rows["outputs/reports/manual_reconciliation_close/latest_review_required.json"]["classification"] == "COLD_ARCHIVE_CANDIDATE"
    assert rows["outputs/reports/ibkr_bridge/broker_order_evidence.json"]["classification"] == "COLD_ARCHIVE_CANDIDATE"
    assert report["delete_candidates_count"] == 0


def test_disposable_build_metadata_is_delete_candidate_dry_run_only(tmp_path: Path) -> None:
    _write(tmp_path / "src" / "mgc_v05l_automation.egg-info" / "PKG-INFO", "metadata")
    _write(tmp_path / ".pytest_cache" / "v" / "cache" / "nodeids", "[]")
    _write(tmp_path / "src" / "mgc_v05l" / "app" / "__pycache__" / "x.pyc", "bytecode")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, include_full_paths=True)
    )

    disposable = [
        row for row in report["classifications"] if row["classification"] == "DISPOSABLE_BUILD"
    ]
    assert len(disposable) == 3
    assert report["delete_candidates_count"] == 3
    assert all(row["recommended_action"] == "delete_candidate_dry_run_only" for row in disposable)


def test_research_offline_paths_are_deferred(tmp_path: Path) -> None:
    _write(tmp_path / "docs" / "atp_companion_note.md", "note")
    _write(tmp_path / "docs" / "us_open_ndx_note.md", "note")
    _write(tmp_path / "src" / "mgc_v05l" / "app" / "atp_probe.py", "")
    _write(tmp_path / "src" / "mgc_v05l" / "app" / "us_open_probe.py", "")
    _write(tmp_path / "src" / "mgc_v05l" / "research" / "offline.py", "")
    _write(tmp_path / "examples" / "track_b_shadow_listener" / "inbox" / "signal.json", "{}")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, include_full_paths=True)
    )

    assert report["deferred_research_count"] == 6
    assert {
        row["classification"] for row in report["classifications"]
    } == {"DEFERRED_RESEARCH_OFFLINE"}


def test_old_root_detection_triggers_review_required(tmp_path: Path) -> None:
    _write(
        tmp_path / "scripts" / "bad.sh",
        "/Users/patrick/Documents/MGC-v05l-automation/scripts/run_probationary_paper_soak.sh",
    )

    report = build_weekly_data_maintenance_report(config=WeeklyMaintenanceConfig(repo_root=tmp_path))

    assert report["review_required"] is True
    assert report["final_verdict"] == "DRY_RUN_ONLY_REVIEW_REQUIRED"
    assert report["old_root_hits"] == ["scripts/bad.sh:1"]


def test_report_schema_and_written_artifacts(tmp_path: Path) -> None:
    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, week_ending=date(2026, 5, 8))
    )
    written = write_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, week_ending=date(2026, 5, 8)),
        report=report,
    )

    payload = json.loads(written["json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "weekly_data_maintenance_v1"
    assert payload["policy"]["delete_files"] is False
    assert payload["policy"]["move_files"] is False
    assert written["markdown"].name == "weekly_data_maintenance_2026-05-08.md"
    assert "classifications" not in payload
    assert "summary_by_retention_tier" in payload
    assert "detail_lists" in payload


def test_summary_counts_equal_total_classified_candidates(tmp_path: Path) -> None:
    _write(tmp_path / "var" / "runtime_market_data" / "MNQ" / "1m" / "latest.json")
    _write(tmp_path / "outputs" / "reports" / "ibkr_bridge" / "broker_order_evidence.json")
    _write(tmp_path / "docs" / "atp_note.md")
    _write(tmp_path / ".pytest_cache" / "v" / "cache" / "nodeids")

    report = build_weekly_data_maintenance_report(config=WeeklyMaintenanceConfig(repo_root=tmp_path))

    total = report["total_classified_count"]
    assert sum(report["summary_by_retention_tier"].values()) == total
    assert sum(report["summary_by_top_level_directory"].values()) == total
    assert sum(report["summary_by_age_bucket"].values()) == total
    assert sum(report["summary_by_reason"].values()) == total


def test_detail_lists_are_capped_and_hide_full_paths_by_default(tmp_path: Path) -> None:
    for index in range(5):
        _write(tmp_path / ".pytest_cache" / f"cache_{index}" / "nodeids")
        _write(tmp_path / "outputs" / "reports" / "ibkr_bridge" / f"broker_order_{index}.json")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, detail_limit=2)
    )

    assert "classifications" not in report
    assert len(report["detail_lists"]["top_delete_candidates"]) == 1
    assert len(report["detail_lists"]["top_archive_candidates"]) == 2
    assert all("path" not in row for rows in report["detail_lists"].values() for row in rows)
    assert all("path_hint" in row for rows in report["detail_lists"].values() for row in rows)


def test_include_full_paths_adds_full_classifications_and_detail_paths(tmp_path: Path) -> None:
    _write(tmp_path / "outputs" / "reports" / "ibkr_bridge" / "broker_order.json")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, include_full_paths=True)
    )

    assert "classifications" in report
    assert report["classifications"][0]["path"] == "outputs/reports/ibkr_bridge/broker_order.json"
    assert report["detail_lists"]["top_archive_candidates"][0]["path"] == "outputs/reports/ibkr_bridge/broker_order.json"


def test_category_filter_limits_reported_categories(tmp_path: Path) -> None:
    _write(tmp_path / "var" / "runtime_market_data" / "MNQ" / "1m" / "latest.json")
    _write(tmp_path / "docs" / "atp_note.md")
    _write(tmp_path / ".pytest_cache" / "v" / "cache" / "nodeids")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, category_filter=("DISPOSABLE_BUILD",))
    )

    assert report["total_classified_before_filter"] == 3
    assert report["total_classified_count"] == 1
    assert report["summary_by_retention_tier"] == {"DISPOSABLE_BUILD": 1}
    assert report["delete_candidates_count"] == 1


def test_apply_with_non_disposable_category_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path / ".pytest_cache" / "v" / "cache" / "nodeids")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(
            repo_root=tmp_path,
            mode="apply",
            category_filter=("WARM",),
            confirm_disposable_build_cleanup=True,
        )
    )

    assert report["final_verdict"] == "APPLY_DISPOSABLE_BUILD_BLOCKED"
    assert "CATEGORY_FILTER_MUST_EQUAL_DISPOSABLE_BUILD" in report["blocking_reasons"]
    assert (tmp_path / ".pytest_cache").exists()


def test_apply_refuses_protected_paths_even_if_misclassified(tmp_path: Path, monkeypatch) -> None:
    for path in (
        tmp_path / "outputs" / "reports" / "cache.pyc",
        tmp_path / "docs" / "cache.pyc",
        tmp_path / "examples" / "cache.pyc",
        tmp_path / "src" / "mgc_v05l" / "research" / "cache.pyc",
    ):
        _write(path, "bytecode")

    original = wdm._classify_path

    def misclassify(*, path, repo_root, hot_roots, warm_root, week_start, week_end):
        row = original(
            path=path,
            repo_root=repo_root,
            hot_roots=hot_roots,
            warm_root=warm_root,
            week_start=week_start,
            week_end=week_end,
        )
        row.update(
            {
                "classification": "DISPOSABLE_BUILD",
                "retention_tier": "DISPOSABLE_BUILD",
                "reason_key": "disposable_build_metadata",
            }
        )
        return row

    monkeypatch.setattr(wdm, "_classify_path", misclassify)

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(
            repo_root=tmp_path,
            mode="apply",
            category_filter=("DISPOSABLE_BUILD",),
            confirm_disposable_build_cleanup=True,
            include_full_paths=True,
        )
    )

    assert report["final_verdict"] == "APPLY_DISPOSABLE_BUILD_BLOCKED"
    assert report["review_required"] is True
    assert report["apply_mode"]["deleted_count"] == 0
    assert all(path.exists() for path in tmp_path.rglob("cache.pyc"))


def test_apply_refuses_source_json_and_evidence_like_files_even_if_misclassified(tmp_path: Path, monkeypatch) -> None:
    for path in (
        tmp_path / "src" / "mgc_v05l" / "app" / "tool.py",
        tmp_path / "src" / "mgc_v05l" / "app" / "config.json",
        tmp_path / "src" / "mgc_v05l" / "app" / "ibkr_broker_cache.tmp",
    ):
        _write(path, "unsafe")

    original = wdm._classify_path

    def misclassify(*, path, repo_root, hot_roots, warm_root, week_start, week_end):
        row = original(
            path=path,
            repo_root=repo_root,
            hot_roots=hot_roots,
            warm_root=warm_root,
            week_start=week_start,
            week_end=week_end,
        )
        row.update(
            {
                "classification": "DISPOSABLE_BUILD",
                "retention_tier": "DISPOSABLE_BUILD",
                "reason_key": "disposable_build_metadata",
            }
        )
        return row

    monkeypatch.setattr(wdm, "_classify_path", misclassify)

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(
            repo_root=tmp_path,
            mode="apply",
            category_filter=("DISPOSABLE_BUILD",),
            confirm_disposable_build_cleanup=True,
        )
    )

    assert report["final_verdict"] == "APPLY_DISPOSABLE_BUILD_BLOCKED"
    assert report["apply_mode"]["deleted_count"] == 0
    assert all(path.exists() for path in (tmp_path / "src" / "mgc_v05l" / "app").iterdir())


def test_apply_deletes_only_disposable_build_fixture_paths(tmp_path: Path) -> None:
    _write(tmp_path / "src" / "mgc_v05l_automation.egg-info" / "PKG-INFO", "metadata")
    _write(tmp_path / ".pytest_cache" / "v" / "cache" / "nodeids", "[]")
    _write(tmp_path / "src" / "mgc_v05l" / "app" / "__pycache__" / "x.pyc", "bytecode")

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(
            repo_root=tmp_path,
            mode="apply",
            category_filter=("DISPOSABLE_BUILD",),
            confirm_disposable_build_cleanup=True,
            include_full_paths=True,
        )
    )

    assert report["final_verdict"] == "APPLY_DISPOSABLE_BUILD_COMPLETE"
    assert report["review_required"] is False
    assert report["apply_mode"]["deleted_count"] == 3
    assert not (tmp_path / "src" / "mgc_v05l_automation.egg-info").exists()
    assert not (tmp_path / ".pytest_cache").exists()
    assert not (tmp_path / "src" / "mgc_v05l" / "app" / "__pycache__").exists()


def test_apply_is_atomic_when_one_unsafe_candidate_is_present(tmp_path: Path, monkeypatch) -> None:
    _write(tmp_path / ".pytest_cache" / "v" / "cache" / "nodeids")
    _write(tmp_path / "outputs" / "reports" / "unsafe_cache.pyc")

    original = wdm._classify_path

    def misclassify(*, path, repo_root, hot_roots, warm_root, week_start, week_end):
        row = original(
            path=path,
            repo_root=repo_root,
            hot_roots=hot_roots,
            warm_root=warm_root,
            week_start=week_start,
            week_end=week_end,
        )
        if str(row["path"]).endswith("unsafe_cache.pyc"):
            row.update(
                {
                    "classification": "DISPOSABLE_BUILD",
                    "retention_tier": "DISPOSABLE_BUILD",
                    "reason_key": "disposable_build_metadata",
                }
            )
        return row

    monkeypatch.setattr(wdm, "_classify_path", misclassify)

    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(
            repo_root=tmp_path,
            mode="apply",
            category_filter=("DISPOSABLE_BUILD",),
            confirm_disposable_build_cleanup=True,
        )
    )

    assert report["final_verdict"] == "APPLY_DISPOSABLE_BUILD_BLOCKED"
    assert report["apply_mode"]["deleted_count"] == 0
    assert (tmp_path / ".pytest_cache").exists()
    assert (tmp_path / "outputs" / "reports" / "unsafe_cache.pyc").exists()


def test_cli_dry_run_writes_report_and_apply_without_confirmation_returns_one(tmp_path: Path) -> None:
    output_root = tmp_path / "reports"
    assert (
        main(
            [
                "--mode",
                "dry-run",
                "--repo-root",
                str(tmp_path),
                "--week-ending",
                "2026-05-08",
                "--output-root",
                str(output_root),
            ]
        )
        == 0
    )
    assert (output_root / "latest_weekly_data_maintenance_report.json").exists()
    assert main(["--mode", "apply", "--repo-root", str(tmp_path), "--output-root", str(output_root)]) == 1
