"""Tests for daily research-history capture."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from mgc_v05l.app.research_daily_capture import DailyResearchHistoryCaptureService
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.timeframes import normalize_timeframe_label
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.db import create_schema
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import bars_table, order_intents_table, research_capture_runs_table, research_capture_status_table
from mgc_v05l.production_link.models import BrokerOrderRecord, BrokerPositionSnapshot
from mgc_v05l.production_link.store import ProductionLinkStore


class _FakeHistoricalClient:
    def __init__(self, payloads_by_symbol: dict[str, dict]) -> None:
        self._payloads_by_symbol = payloads_by_symbol
        self.calls: list[dict[str, object]] = []

    def fetch_price_history(self, external_symbol: str, request, default_frequency):
        self.calls.append(
            {
                "external_symbol": external_symbol,
                "internal_symbol": request.internal_symbol,
                "start_date_ms": request.start_date_ms,
                "end_date_ms": request.end_date_ms,
                "period_type": request.period_type,
                "frequency_type": default_frequency.frequency_type if default_frequency else None,
                "frequency": default_frequency.frequency if default_frequency else None,
            }
        )
        return self._payloads_by_symbol[request.internal_symbol]


def test_daily_capture_resolves_policy_backed_targets(tmp_path: Path, monkeypatch) -> None:
    repo_root = _build_repo_layout(
        tmp_path,
        policy_overrides={
            "tracked_symbols": {
                "research_universe": {
                    "storage_domain": "research_history",
                    "refresh_policy": "scheduled_backfill",
                    "promotion_rule": "explicit research manifest or backfill target set",
                    "symbols": ["MGC"],
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 10,
                },
                "watched": {
                    "storage_domain": "broker_monitor_truth",
                    "refresh_policy": "lighter_polling",
                    "promotion_rule": "explicit operator watchlist membership",
                    "symbols": ["MES"],
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                },
                "ad_hoc": {
                    "storage_domain": "derived_ui_snapshot_cache",
                    "refresh_policy": "temporary_lookup",
                    "promotion_rule": "manual promote",
                    "symbols": ["NG"],
                    "include_in_daily_research_capture": False,
                    "refresh_interval_seconds": 30,
                    "expiry_hours": 24,
                },
            }
        },
    )
    _set_schwab_env(monkeypatch, repo_root)

    service = DailyResearchHistoryCaptureService(
        repo_root,
        config_paths=[Path("config/base.yaml"), repo_root / "config" / "replay.yaml"],
        historical_client=_FakeHistoricalClient(payloads_by_symbol={"MGC": _history_payload(), "MES": _history_payload()}),
    )

    targets = service._resolve_targets()

    assert [(item.capture_class, item.symbol, item.timeframe) for item in targets] == [
        ("research_universe", "MGC", "5m"),
        ("watched", "MES", "5m"),
    ]


def test_daily_capture_resolves_dynamic_broker_and_paper_targets(tmp_path: Path, monkeypatch) -> None:
    repo_root = _build_repo_layout(
        tmp_path,
        policy_overrides={
            "tracked_symbols": {
                "research_universe": {
                    "storage_domain": "research_history",
                    "refresh_policy": "scheduled_backfill",
                    "promotion_rule": "explicit research manifest or backfill target set",
                    "symbol_discovery": "explicit_symbols",
                    "symbols": ["MGC"],
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 10,
                },
                "watched": {
                    "storage_domain": "broker_monitor_truth",
                    "refresh_policy": "lighter_polling",
                    "promotion_rule": "explicit operator watchlist membership",
                    "symbol_discovery": "explicit_symbols",
                    "symbols": ["MES"],
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                },
                "broker_held": {
                    "storage_domain": "broker_monitor_truth",
                    "refresh_policy": "near_live_polling",
                    "refresh_interval_seconds": 5,
                    "promotion_rule": "automatic while a live broker position or open broker order exists",
                    "symbol_discovery": "broker_monitor_activity",
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 10,
                },
                "paper_active": {
                    "storage_domain": "runtime_strategy_state",
                    "refresh_policy": "bar_and_event_driven",
                    "refresh_interval_seconds": 2,
                    "promotion_rule": "automatic while a paper lane is active or has open paper exposure",
                    "symbol_discovery": "runtime_paper_activity",
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 10,
                },
                "ad_hoc": {
                    "storage_domain": "derived_ui_snapshot_cache",
                    "refresh_policy": "temporary_lookup",
                    "promotion_rule": "manual promote",
                    "symbol_discovery": "explicit_symbols",
                    "symbols": ["NG"],
                    "include_in_daily_research_capture": False,
                    "refresh_interval_seconds": 30,
                    "expiry_hours": 24,
                },
            }
        },
        symbol_map={"MGC": "/MGC", "MES": "/MES", "ES": "/ES", "NQ": "/NQ"},
    )
    _set_schwab_env(monkeypatch, repo_root)
    _seed_broker_monitor_symbol(repo_root, "ES")
    _seed_paper_runtime_symbol(repo_root, "NQ")

    service = DailyResearchHistoryCaptureService(
        repo_root,
        config_paths=[Path("config/base.yaml"), repo_root / "config" / "replay.yaml"],
        historical_client=_FakeHistoricalClient(
            payloads_by_symbol={
                "MGC": _history_payload(),
                "MES": _history_payload(),
                "ES": _history_payload(),
                "NQ": _history_payload(),
            }
        ),
    )

    targets = service._resolve_targets()

    assert {(item.capture_class, item.symbol, item.timeframe) for item in targets} == {
        ("research_universe", "MGC", "5m"),
        ("watched", "MES", "5m"),
        ("broker_held", "ES", "5m"),
        ("paper_active", "NQ", "5m"),
    }


def test_daily_capture_excludes_transient_option_contracts_from_auto_discovered_targets(tmp_path: Path, monkeypatch) -> None:
    repo_root = _build_repo_layout(
        tmp_path,
        policy_overrides={
            "tracked_symbols": {
                "research_universe": {
                    "storage_domain": "research_history",
                    "refresh_policy": "scheduled_backfill",
                    "promotion_rule": "explicit research manifest or backfill target set",
                    "symbol_discovery": "explicit_symbols",
                    "symbols": ["SPY", "QQQ", "TQQQ", "SQQQ"],
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 10,
                },
                "broker_held": {
                    "storage_domain": "broker_monitor_truth",
                    "refresh_policy": "near_live_polling",
                    "refresh_interval_seconds": 5,
                    "promotion_rule": "automatic while a live broker position or open broker order exists",
                    "symbol_discovery": "broker_monitor_activity",
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 10,
                },
                "paper_active": {
                    "storage_domain": "runtime_strategy_state",
                    "refresh_policy": "bar_and_event_driven",
                    "refresh_interval_seconds": 2,
                    "promotion_rule": "automatic while a paper lane is active or has open paper exposure",
                    "symbol_discovery": "runtime_paper_activity",
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 10,
                },
            }
        },
        symbol_map={"SPY": "SPY", "QQQ": "QQQ", "TQQQ": "TQQQ", "SQQQ": "SQQQ", "ES": "/ES", "NQ": "/NQ"},
    )
    _set_schwab_env(monkeypatch, repo_root)
    _seed_broker_monitor_symbol(repo_root, "ES")
    _seed_broker_monitor_symbol(repo_root, "SPY   260402P00635000")
    _seed_paper_runtime_symbol(repo_root, "NQ")
    _seed_paper_runtime_symbol(repo_root, "TQQQ  260417P00042000")

    service = DailyResearchHistoryCaptureService(
        repo_root,
        config_paths=[Path("config/base.yaml"), repo_root / "config" / "replay.yaml"],
        historical_client=_FakeHistoricalClient(
            payloads_by_symbol={
                "SPY": _history_payload(),
                "QQQ": _history_payload(),
                "TQQQ": _history_payload(),
                "SQQQ": _history_payload(),
                "ES": _history_payload(),
                "NQ": _history_payload(),
            }
        ),
    )

    targets = service._resolve_targets()

    assert {(item.capture_class, item.symbol, item.timeframe) for item in targets} == {
        ("research_universe", "SPY", "5m"),
        ("research_universe", "QQQ", "5m"),
        ("research_universe", "TQQQ", "5m"),
        ("research_universe", "SQQQ", "5m"),
        ("broker_held", "ES", "5m"),
        ("paper_active", "NQ", "5m"),
    }


def test_daily_capture_appends_incrementally_and_ignores_overlap_duplicates(tmp_path: Path, monkeypatch) -> None:
    repo_root = _build_repo_layout(
        tmp_path,
        policy_overrides={
            "tracked_symbols": {
                "research_universe": {
                    "storage_domain": "research_history",
                    "refresh_policy": "scheduled_backfill",
                    "promotion_rule": "explicit research manifest or backfill target set",
                    "symbols": ["MGC"],
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 30,
                }
            }
        },
        symbol_map={"MGC": "/MGC"},
    )
    _set_schwab_env(monkeypatch, repo_root)
    engine = build_engine(f"sqlite:///{repo_root / 'research.sqlite3'}")
    repositories = RepositorySet(engine)
    existing_bar = _bar("MGC", "5m", datetime(2026, 3, 24, 14, 35, tzinfo=timezone.utc), 100)
    repositories.bars.save(existing_bar, data_source="schwab_history")

    fake_client = _FakeHistoricalClient(
        payloads_by_symbol={
            "MGC": _history_payload(
                datetimes=(
                    datetime(2026, 3, 24, 14, 35, tzinfo=timezone.utc),
                    datetime(2026, 3, 24, 14, 40, tzinfo=timezone.utc),
                )
            )
        }
    )
    service = DailyResearchHistoryCaptureService(
        repo_root,
        config_paths=[Path("config/base.yaml"), repo_root / "config" / "replay.yaml"],
        historical_client=fake_client,
        now_fn=lambda: datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
    )

    summary = service.capture()

    assert summary["status"] == "success"
    with engine.begin() as connection:
        bars = connection.execute(
            select(bars_table.c.end_ts)
            .where(bars_table.c.ticker == "MGC")
            .where(bars_table.c.timeframe == "5m")
            .where(bars_table.c.data_source == "schwab_history")
            .order_by(bars_table.c.end_ts.asc())
        ).all()
        status_row = connection.execute(select(research_capture_status_table)).mappings().one()
        run_row = connection.execute(select(research_capture_runs_table)).mappings().one()

    assert len(bars) == 2
    assert status_row["last_status"] == "success"
    assert status_row["last_bar_end_ts"] == "2026-03-24T14:40:00+00:00"
    assert run_row["fetched_bar_count"] == 2
    assert fake_client.calls[0]["start_date_ms"] is not None


def test_daily_capture_reports_failed_symbol_without_aborting_successful_symbols(tmp_path: Path, monkeypatch) -> None:
    repo_root = _build_repo_layout(
        tmp_path,
        policy_overrides={
            "tracked_symbols": {
                "research_universe": {
                    "storage_domain": "research_history",
                    "refresh_policy": "scheduled_backfill",
                    "promotion_rule": "explicit research manifest or backfill target set",
                    "symbols": ["MGC", "MES"],
                    "include_in_daily_research_capture": True,
                    "research_capture_timeframes": ["5m"],
                    "bootstrap_lookback_days": 5,
                }
            }
        },
        symbol_map={"MGC": "/MGC"},
    )
    _set_schwab_env(monkeypatch, repo_root)
    fake_client = _FakeHistoricalClient(payloads_by_symbol={"MGC": _history_payload()})
    service = DailyResearchHistoryCaptureService(
        repo_root,
        config_paths=[Path("config/base.yaml"), repo_root / "config" / "replay.yaml"],
        historical_client=fake_client,
        now_fn=lambda: datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
    )

    summary = service.capture()

    assert summary["status"] == "partial_failure"
    assert summary["success_count"] == 1
    assert summary["failure_count"] == 1
    assert summary["attempted_symbols"] == ["MES", "MGC"]
    assert summary["succeeded_symbols"] == ["MGC"]
    assert summary["failed_symbols"][0]["symbol"] == "MES"
    results_by_symbol = {item["symbol"]: item for item in summary["results"]}
    target_rows_by_symbol = {item["symbol"]: item for item in summary["target_rows"]}
    assert results_by_symbol["MGC"]["status"] == "success"
    assert results_by_symbol["MES"]["status"] == "failure"
    assert target_rows_by_symbol["MGC"]["last_captured_bar_end_ts"] == results_by_symbol["MGC"]["persisted_last_bar_end_ts"]
    assert target_rows_by_symbol["MES"]["failure_code"] == results_by_symbol["MES"]["failure_code"]
    assert "No Schwab historical symbol mapping configured" in results_by_symbol["MES"]["failure_detail"]

    engine = build_engine(f"sqlite:///{repo_root / 'research.sqlite3'}")
    with engine.begin() as connection:
        status_rows = connection.execute(select(research_capture_status_table)).mappings().all()
    assert {row["symbol"]: row["last_status"] for row in status_rows} == {"MGC": "success", "MES": "failure"}


def _build_repo_layout(
    tmp_path: Path,
    *,
    policy_overrides: dict | None = None,
    symbol_map: dict[str, str] | None = None,
) -> Path:
    repo_root = tmp_path
    config_dir = repo_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "replay.yaml").write_text(f'mode: "replay"\ndatabase_url: "sqlite:///{repo_root / "runtime_unused.sqlite3"}"\n', encoding="utf-8")
    policy = json.loads((Path("config/data_storage_policy.json")).read_text(encoding="utf-8"))
    policy["storage_layout"]["runtime_replay_database_path"] = "research.sqlite3"
    if policy_overrides:
        _deep_update(policy, policy_overrides)
    (config_dir / "data_storage_policy.json").write_text(json.dumps(policy, indent=2, sort_keys=True), encoding="utf-8")
    mapping = symbol_map or {"MGC": "/MGC", "MES": "/MES"}
    (config_dir / "schwab.local.json").write_text(
        json.dumps(
            {
                "historical_symbol_map": mapping,
                "quote_symbol_map": mapping,
                "timeframe_map": {"5m": {"frequency_type": "minute", "frequency": 5}},
                "quotes_symbol_query_param": "symbols",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return repo_root


def _set_schwab_env(monkeypatch, repo_root: Path) -> None:
    monkeypatch.setenv("SCHWAB_APP_KEY", "test-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "test-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(repo_root / ".local" / "schwab" / "tokens.json"))


def _history_payload(*, datetimes: tuple[datetime, ...] | None = None) -> dict:
    values = datetimes or (
        datetime(2026, 3, 24, 14, 35, tzinfo=timezone.utc),
        datetime(2026, 3, 24, 14, 40, tzinfo=timezone.utc),
    )
    candles = []
    base = 100.0
    for index, timestamp in enumerate(values):
        candles.append(
            {
                "datetime": int(timestamp.timestamp() * 1000),
                "open": base + index,
                "high": base + index + 1.0,
                "low": base + index - 1.0,
                "close": base + index + 0.5,
                "volume": 100 + index,
            }
        )
    return {"candles": candles}


def _bar(symbol: str, timeframe: str, end_ts: datetime, price: int) -> Bar:
    canonical_timeframe = normalize_timeframe_label(timeframe)
    start_ts = end_ts - timedelta(minutes=5)
    return Bar(
        bar_id=build_bar_id(symbol, canonical_timeframe, end_ts.astimezone(timezone.utc)),
        symbol=symbol,
        timeframe=canonical_timeframe,
        start_ts=start_ts.astimezone(timezone.utc),
        end_ts=end_ts.astimezone(timezone.utc),
        open=Decimal(price),
        high=Decimal(price + 1),
        low=Decimal(price - 1),
        close=Decimal(price),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def _deep_update(target: dict, updates: dict) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)
        else:
            target[key] = value


def _seed_broker_monitor_symbol(repo_root: Path, symbol: str) -> None:
    store = ProductionLinkStore(repo_root / "outputs" / "production_link" / "schwab_production_link.sqlite3")
    store.save_portfolio_snapshot(
        account_hash="acct-1",
        balances=None,
        positions=[
            BrokerPositionSnapshot(
                account_hash="acct-1",
                position_key=f"{symbol}-position",
                symbol=symbol,
                description=f"{symbol} futures",
                asset_class="FUTURE",
                quantity=Decimal("1"),
                side="LONG",
                average_cost=Decimal("100"),
                mark_price=Decimal("101"),
                market_value=Decimal("101"),
                current_day_pnl=Decimal("1"),
                open_pnl=Decimal("1"),
                ytd_pnl=Decimal("1"),
                margin_impact=Decimal("10"),
                broker_position_id=f"{symbol}-broker-position",
                fetched_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
                raw_payload={"symbol": symbol},
            )
        ],
    )
    store.upsert_orders(
        [
            BrokerOrderRecord(
                broker_order_id=f"{symbol}-order",
                account_hash="acct-1",
                client_order_id=f"{symbol}-client-order",
                symbol=symbol,
                description=f"{symbol} opening order",
                asset_class="FUTURE",
                instruction="BUY_TO_OPEN",
                quantity=Decimal("1"),
                filled_quantity=Decimal("0"),
                order_type="LIMIT",
                duration="DAY",
                session="NORMAL",
                status="WORKING",
                entered_at=datetime(2026, 3, 25, 11, 55, tzinfo=timezone.utc),
                closed_at=None,
                updated_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
                limit_price=Decimal("101"),
                stop_price=None,
                source="test",
                raw_payload={"symbol": symbol},
            )
        ],
        event_source="test",
    )


def _seed_paper_runtime_symbol(repo_root: Path, symbol: str) -> None:
    engine = build_engine(f"sqlite:///{repo_root / 'mgc_v05l.paper.sqlite3'}")
    create_schema(engine)
    with engine.begin() as connection:
        connection.execute(
            order_intents_table.insert(),
            {
                "order_intent_id": f"{symbol}-intent",
                "standalone_strategy_id": "strategy-1",
                "strategy_family": "test_family",
                "instrument": symbol,
                "lane_id": "lane-1",
                "bar_id": f"{symbol}-bar",
                "symbol": symbol,
                "intent_type": "BUY_TO_OPEN",
                "quantity": 1,
                "created_at": "2026-03-25T12:00:00+00:00",
                "reason_code": "test",
                "broker_order_id": None,
                "order_status": "OPEN",
            },
        )
