"""Tests for formal data acquisition and storage policy loading."""

from pathlib import Path

from mgc_v05l.config_models import load_data_storage_policy
from mgc_v05l.production_link.config import load_schwab_production_link_config


def test_data_storage_policy_loads_repo_defaults(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "data_storage_policy.json").write_text(
        """
        {
          "version": 1,
          "storage_layout": {
            "runtime_base_database_path": "./runtime.sqlite3",
            "runtime_replay_database_path": "./replay.sqlite3",
            "runtime_paper_database_path": "./paper.sqlite3",
            "broker_monitor_database_path": "outputs/broker/broker.sqlite3",
            "broker_monitor_selected_account_path": "outputs/broker/selected_account.json",
            "broker_monitor_snapshot_path": "outputs/ui/broker_snapshot.json",
            "ui_snapshot_root": "outputs/ui",
            "research_root": "outputs/research",
            "token_file_path": ".local/schwab/tokens.json"
          },
          "domains": {
            "research_history": {
              "owner": "research",
              "purpose": "research",
              "includes": ["bars"],
              "rules": ["append"],
              "sqlite_tables": ["bars"],
              "artifact_paths": ["outputs/research"],
              "acquisition_mode": "manual",
              "retention_class": "long",
              "cleanup_policy": "manual"
            },
            "runtime_strategy_state": {
              "owner": "runtime",
              "purpose": "runtime",
              "includes": ["state"],
              "rules": ["restart-safe"],
              "sqlite_tables": ["strategy_state_snapshots"],
              "artifact_paths": ["outputs/runtime"],
              "acquisition_mode": "event",
              "ui_rebuild_interval_seconds": 2,
              "retention_class": "operational",
              "retention_days": 365,
              "cleanup_policy": "age"
            },
            "broker_monitor_truth": {
              "owner": "production_link",
              "purpose": "broker monitor",
              "includes": ["positions"],
              "rules": ["freshness-scoped"],
              "sqlite_tables": ["broker_positions"],
              "artifact_paths": ["outputs/broker"],
              "acquisition_mode": "polling",
              "refresh_interval_seconds": 5,
              "service_cache_ttl_seconds": 15,
              "stale_after_seconds": 120,
              "retention_class": "short",
              "retention_days": 30,
              "cleanup_policy": "age"
            },
            "derived_ui_snapshot_cache": {
              "owner": "ui",
              "purpose": "cache",
              "includes": ["snapshots"],
              "rules": ["rebuildable"],
              "artifact_paths": ["outputs/ui"],
              "acquisition_mode": "display",
              "ui_rebuild_interval_seconds": 2,
              "retention_class": "short",
              "retention_days": 7,
              "cleanup_policy": "replace"
            }
          },
          "tracked_symbols": {
            "broker_held": {
              "storage_domain": "broker_monitor_truth",
              "refresh_policy": "polling",
              "refresh_interval_seconds": 5,
              "promotion_rule": "auto"
            }
          },
          "truth_hierarchy": {
            "broker_monitor": ["broker_monitor_truth", "derived_ui_snapshot_cache"]
          },
          "cleanup": {
            "ui_snapshot_cache_keep_days": 7,
            "broker_monitor_quote_snapshot_keep_days": 30,
            "broker_monitor_order_event_keep_days": 30,
            "broker_monitor_reconciliation_event_keep_days": 365,
            "runtime_event_keep_days": 365,
            "ad_hoc_symbol_expiry_hours": 24
          }
        }
        """,
        encoding="utf-8",
    )

    policy = load_data_storage_policy(tmp_path)

    assert policy.version == 1
    assert policy.broker_monitor_database_path == (tmp_path / "outputs" / "broker" / "broker.sqlite3")
    assert policy.broker_monitor_snapshot_path == (tmp_path / "outputs" / "ui" / "broker_snapshot.json")
    assert policy.tracked_symbols["broker_held"].refresh_interval_seconds == 5


def test_production_link_config_uses_policy_defaults(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "data_storage_policy.json").write_text(
        """
        {
          "version": 1,
          "storage_layout": {
            "runtime_base_database_path": "./runtime.sqlite3",
            "runtime_replay_database_path": "./replay.sqlite3",
            "runtime_paper_database_path": "./paper.sqlite3",
            "broker_monitor_database_path": "outputs/broker/broker.sqlite3",
            "broker_monitor_selected_account_path": "outputs/broker/selected_account.json",
            "broker_monitor_snapshot_path": "outputs/ui/broker_snapshot.json",
            "ui_snapshot_root": "outputs/ui",
            "research_root": "outputs/research",
            "token_file_path": ".local/schwab/tokens.json"
          },
          "domains": {
            "research_history": {
              "owner": "research",
              "purpose": "research",
              "includes": ["bars"],
              "rules": ["append"],
              "sqlite_tables": ["bars"],
              "artifact_paths": ["outputs/research"],
              "acquisition_mode": "manual",
              "retention_class": "long",
              "cleanup_policy": "manual"
            },
            "runtime_strategy_state": {
              "owner": "runtime",
              "purpose": "runtime",
              "includes": ["state"],
              "rules": ["restart-safe"],
              "sqlite_tables": ["strategy_state_snapshots"],
              "artifact_paths": ["outputs/runtime"],
              "acquisition_mode": "event",
              "ui_rebuild_interval_seconds": 2,
              "retention_class": "operational",
              "retention_days": 365,
              "cleanup_policy": "age"
            },
            "broker_monitor_truth": {
              "owner": "production_link",
              "purpose": "broker monitor",
              "includes": ["positions"],
              "rules": ["freshness-scoped"],
              "sqlite_tables": ["broker_positions"],
              "artifact_paths": ["outputs/broker"],
              "acquisition_mode": "polling",
              "refresh_interval_seconds": 5,
              "service_cache_ttl_seconds": 21,
              "stale_after_seconds": 90,
              "retention_class": "short",
              "retention_days": 30,
              "cleanup_policy": "age"
            },
            "derived_ui_snapshot_cache": {
              "owner": "ui",
              "purpose": "cache",
              "includes": ["snapshots"],
              "rules": ["rebuildable"],
              "artifact_paths": ["outputs/ui"],
              "acquisition_mode": "display",
              "ui_rebuild_interval_seconds": 2,
              "retention_class": "short",
              "retention_days": 7,
              "cleanup_policy": "replace"
            }
          },
          "tracked_symbols": {
            "broker_held": {
              "storage_domain": "broker_monitor_truth",
              "refresh_policy": "polling",
              "refresh_interval_seconds": 5,
              "promotion_rule": "auto"
            }
          },
          "truth_hierarchy": {
            "broker_monitor": ["broker_monitor_truth", "derived_ui_snapshot_cache"]
          },
          "cleanup": {
            "ui_snapshot_cache_keep_days": 7,
            "broker_monitor_quote_snapshot_keep_days": 30,
            "broker_monitor_order_event_keep_days": 30,
            "broker_monitor_reconciliation_event_keep_days": 365,
            "runtime_event_keep_days": 365,
            "ad_hoc_symbol_expiry_hours": 24
          }
        }
        """,
        encoding="utf-8",
    )
    (config_dir / "schwab.local.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.delenv("MGC_PRODUCTION_LINK_DB", raising=False)
    monkeypatch.delenv("MGC_PRODUCTION_LINK_SNAPSHOT_PATH", raising=False)
    monkeypatch.delenv("MGC_PRODUCTION_LINK_SELECTED_ACCOUNT_FILE", raising=False)
    monkeypatch.delenv("MGC_PRODUCTION_LINK_CACHE_TTL_SECONDS", raising=False)
    monkeypatch.delenv("MGC_PRODUCTION_BROKER_FRESHNESS_MAX_AGE_SECONDS", raising=False)
    monkeypatch.delenv("MGC_PRODUCTION_LINK_CONFIG", raising=False)
    monkeypatch.delenv("MGC_PRODUCTION_LINK_MARKET_DATA_CONFIG", raising=False)

    config = load_schwab_production_link_config(tmp_path)

    assert config.database_path == (tmp_path / "outputs" / "broker" / "broker.sqlite3")
    assert config.selected_account_path == (tmp_path / "outputs" / "broker" / "selected_account.json")
    assert config.snapshot_path == (tmp_path / "outputs" / "ui" / "broker_snapshot.json")
    assert config.cache_ttl_seconds == 21
    assert config.features.broker_freshness_max_age_seconds == 90


def test_repo_policy_daily_capture_defaults_include_non_ad_hoc_classes() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    policy = load_data_storage_policy(repo_root)

    assert policy.tracked_symbols["research_universe"].include_in_daily_research_capture is True
    assert policy.tracked_symbols["watched"].include_in_daily_research_capture is True
    assert policy.tracked_symbols["broker_held"].include_in_daily_research_capture is True
    assert policy.tracked_symbols["paper_active"].include_in_daily_research_capture is True
    assert policy.tracked_symbols["ad_hoc"].include_in_daily_research_capture is False
    assert policy.tracked_symbols["broker_held"].symbol_discovery == "broker_monitor_activity"
    assert policy.tracked_symbols["paper_active"].symbol_discovery == "runtime_paper_activity"
