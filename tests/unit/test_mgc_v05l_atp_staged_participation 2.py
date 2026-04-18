from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mgc_v05l.config_models import RuntimeMode, StrategySettings, load_settings_from_files
from mgc_v05l.app.probationary_runtime import _atp_runtime_identity_payload, _load_probationary_paper_lane_specs
from mgc_v05l.domain.enums import LongEntryFamily, OrderStatus, ParticipationPolicy, PositionSide
from mgc_v05l.domain.models import Bar
from mgc_v05l.execution.order_models import FillEvent
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.strategy.strategy_engine import StrategyEngine


def _candidate_lane_payload(config_path: Path) -> dict[str, object]:
    config_text = config_path.read_text(encoding="utf-8")
    for line in config_text.splitlines():
        if line.startswith("probationary_paper_lanes_json:"):
            payload = line.split(":", 1)[1].strip()
            if payload.startswith("'") and payload.endswith("'"):
                payload = payload[1:-1]
            return json.loads(payload)[0]
    raise AssertionError(f"probationary_paper_lanes_json not found in {config_path}")


def _candidate_settings(tmp_path: Path, config_name: str) -> StrategySettings:
    payload = _candidate_lane_payload(Path("config") / config_name)
    base = load_settings_from_files([Path("config/base.yaml")])
    database_path = tmp_path / f"{str(payload['symbol']).lower()}_atp.sqlite3"
    return base.model_copy(
        update={
            "mode": RuntimeMode.PAPER,
            "symbol": payload["symbol"],
            "database_url": f"sqlite:///{database_path}",
            "probationary_artifacts_dir": str(tmp_path / "outputs"),
            "trade_size": int(payload["trade_size"]),
            "participation_policy": ParticipationPolicy(str(payload["participation_policy"])),
            "max_concurrent_entries": int(payload["max_concurrent_entries"]),
            "max_position_quantity": int(payload["max_position_quantity"]),
            "max_adds_after_entry": int(payload["max_adds_after_entry"]),
        }
    )


def _bar(symbol: str, index: int, *, close: str = "100") -> Bar:
    end_ts = datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=5 * index)
    start_ts = end_ts - timedelta(minutes=5)
    price = Decimal(close)
    return Bar(
        bar_id=f"{symbol}|5m|{index}",
        symbol=symbol,
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def _fill_entry(engine: StrategyEngine, *, bar: Bar, price: Decimal, reason_code: str) -> None:
    intent = engine.submit_runtime_entry_intent(
        bar,
        side="LONG",
        signal_source=reason_code,
        reason_code=reason_code,
        long_entry_family=LongEntryFamily.K,
    )
    assert intent is not None
    engine.apply_fill(
        FillEvent(
            order_intent_id=intent.order_intent_id,
            intent_type=intent.intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=bar.end_ts,
            fill_price=price,
            broker_order_id=f"paper-{intent.order_intent_id}",
            quantity=intent.quantity,
        ),
        signal_bar_id=bar.bar_id,
        long_entry_family=LongEntryFamily.K,
    )
    engine._execution_engine.clear_intent(intent.order_intent_id)  # noqa: SLF001


def test_atp_candidate_configs_enable_staged_participation() -> None:
    gc_text = Path("config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml").read_text(encoding="utf-8")
    pl_text = Path("config/probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml").read_text(encoding="utf-8")
    gc = _candidate_lane_payload(Path("config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml"))
    pl = _candidate_lane_payload(Path("config/probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml"))

    assert 'mode: "paper"' in gc_text
    assert 'probationary_paper_runtime_exclusive_config: true' in gc_text
    assert 'mode: "paper"' in pl_text
    assert 'probationary_paper_runtime_exclusive_config: true' in pl_text
    assert gc["participation_policy"] == "STAGED_SAME_DIRECTION"
    assert gc["max_concurrent_entries"] == 2
    assert gc["max_position_quantity"] == 2
    assert gc["max_adds_after_entry"] == 1
    assert pl["participation_policy"] == "STAGED_SAME_DIRECTION"
    assert pl["max_concurrent_entries"] == 2
    assert pl["max_position_quantity"] == 2
    assert pl["max_adds_after_entry"] == 1


def test_atp_candidate_lane_supports_entry_add_partial_exit_and_restore(tmp_path: Path) -> None:
    settings = _candidate_settings(tmp_path, "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml")
    runtime_identity = {"standalone_strategy_id": "atp_companion_v1__paper_gc_asia_us", "instrument": settings.symbol, "lane_id": "atp_gc"}
    repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    engine = StrategyEngine(settings=settings, repositories=repositories, runtime_identity=runtime_identity)

    _fill_entry(engine, bar=_bar(settings.symbol, 1, close="100"), price=Decimal("100"), reason_code="atpEntry1")
    _fill_entry(engine, bar=_bar(settings.symbol, 2, close="101"), price=Decimal("101"), reason_code="atpEntry2")

    assert engine.state.position_side is PositionSide.LONG
    assert engine.state.internal_position_qty == 2
    assert len(engine.state.open_entry_legs) == 2

    assert (
        engine.submit_runtime_entry_intent(
            _bar(settings.symbol, 3, close="102"),
            side="LONG",
            signal_source="atpEntry3",
            reason_code="atpEntry3",
            long_entry_family=LongEntryFamily.K,
        )
        is None
    )

    exit_intent = engine.submit_runtime_exit_intent(
        _bar(settings.symbol, 4, close="103").end_ts,
        quantity=1,
        reason_code="atpPartialExit",
    )
    assert exit_intent is not None
    engine.apply_fill(
        FillEvent(
            order_intent_id=exit_intent.order_intent_id,
            intent_type=exit_intent.intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=_bar(settings.symbol, 4, close="103").end_ts,
            fill_price=Decimal("103"),
            broker_order_id=f"paper-{exit_intent.order_intent_id}",
            quantity=1,
        )
    )
    engine._execution_engine.clear_intent(exit_intent.order_intent_id)  # noqa: SLF001

    assert engine.state.position_side is PositionSide.LONG
    assert engine.state.internal_position_qty == 1
    assert len(engine.state.open_entry_legs) == 1

    restored_repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    restored = StrategyEngine(settings=settings, repositories=restored_repositories, runtime_identity=runtime_identity)

    assert restored.state.position_side is PositionSide.LONG
    assert restored.state.internal_position_qty == 1
    assert len(restored.state.open_entry_legs) == 1


def test_frozen_atp_benchmark_remains_explicit_single_entry_by_config() -> None:
    payload = _candidate_lane_payload(Path("config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml"))

    assert payload["participation_policy"] == "SINGLE_ENTRY_ONLY"
    assert payload["max_concurrent_entries"] == 1
    assert payload["max_adds_after_entry"] == 0


def test_atp_runtime_identity_labels_candidates_separately_from_benchmark() -> None:
    benchmark_settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml"),
        ]
    )
    gc_settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml"),
        ]
    )
    pl_settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml"),
        ]
    )

    benchmark_identity = _atp_runtime_identity_payload(_load_probationary_paper_lane_specs(benchmark_settings)[0])
    gc_identity = _atp_runtime_identity_payload(_load_probationary_paper_lane_specs(gc_settings)[0])
    pl_identity = _atp_runtime_identity_payload(_load_probationary_paper_lane_specs(pl_settings)[0])

    assert benchmark_identity["strategy_status"] == "RUNNING_ATP_COMPANION_BENCHMARK_PAPER"
    assert benchmark_identity["benchmark_designation"] == "CURRENT_ATP_COMPANION_BENCHMARK"
    assert benchmark_identity["tracked_strategy_id"] == "atp_companion_v1_asia_us"

    assert gc_identity["strategy_status"] == "RUNNING_ATP_COMPANION_CANDIDATE_STAGED_PAPER"
    assert gc_identity["benchmark_designation"] is None
    assert gc_identity["tracked_strategy_id"] == "atp_companion_v1__paper_gc_asia_us"
    assert "Candidate" in gc_identity["scope_label"]
    assert "Staged" in gc_identity["scope_label"]

    assert pl_identity["strategy_status"] == "RUNNING_ATP_COMPANION_CANDIDATE_STAGED_PAPER"
    assert pl_identity["benchmark_designation"] is None
    assert pl_identity["tracked_strategy_id"] == "atp_companion_v1__paper_pl_asia_us"
    assert "Candidate" in pl_identity["scope_label"]
    assert "Staged" in pl_identity["scope_label"]
