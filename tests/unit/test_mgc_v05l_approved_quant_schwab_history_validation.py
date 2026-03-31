from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from mgc_v05l.app.approved_quant_lanes.probation import ApprovedQuantProbationArtifacts
from mgc_v05l.app.approved_quant_lanes.specs import approved_quant_lane_scope_payload, approved_quant_lane_specs
from mgc_v05l.app.approved_quant_schwab_history_validation import (
    _compare_lane_behavior,
    run_approved_quant_schwab_history_validation,
)
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.schwab_models import (
    SchwabAuthConfig,
    SchwabBarFieldMap,
    SchwabMarketDataConfig,
    SchwabPriceHistoryFrequency,
    SchwabHistoricalRequest,
    TimestampSemantics,
)


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "validation.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _build_config(tmp_path: Path) -> SchwabMarketDataConfig:
    approved_symbols = sorted({symbol for spec in approved_quant_lane_specs() for symbol in spec.symbols})
    return SchwabMarketDataConfig(
        auth=SchwabAuthConfig(
            app_key="app-key",
            app_secret="app-secret",
            callback_url="http://127.0.0.1:8182/callback",
            token_store_path=tmp_path / "tokens.json",
        ),
        historical_symbol_map={symbol: f"/{symbol}" for symbol in approved_symbols},
        quote_symbol_map={symbol: f"/{symbol}" for symbol in approved_symbols},
        timeframe_map={
            "5m": SchwabPriceHistoryFrequency(frequency_type="minute", frequency=5),
            "60m": SchwabPriceHistoryFrequency(frequency_type="minute", frequency=60),
            "240m": SchwabPriceHistoryFrequency(frequency_type="minute", frequency=240),
            "720m": SchwabPriceHistoryFrequency(frequency_type="minute", frequency=720),
            "1440m": SchwabPriceHistoryFrequency(frequency_type="daily", frequency=1),
        },
        field_map=SchwabBarFieldMap(
            timestamp_field="datetime",
            open_field="open",
            high_field="high",
            low_field="low",
            close_field="close",
            volume_field="volume",
            is_final_field=None,
            timestamp_semantics=TimestampSemantics.END,
        ),
    )


def _bar(symbol: str, index: int) -> Bar:
    end_ts = datetime(2026, 3, 18, 0, 5, tzinfo=UTC) + timedelta(minutes=5 * index)
    open_ = Decimal("100.0") + Decimal(index) * Decimal("0.01")
    close = open_ + Decimal("0.02")
    return Bar(
        bar_id=f"{symbol}-5m-{index}",
        symbol=symbol,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=open_,
        high=close + Decimal("0.03"),
        low=open_ - Decimal("0.03"),
        close=close,
        volume=100 + index,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


class _FakeHistoricalBackfillService:
    def __init__(self, adapter, client, repositories) -> None:
        del adapter, client
        self._repositories = repositories

    def fetch_bars(self, request: SchwabHistoricalRequest, internal_timeframe: str) -> list[Bar]:
        assert internal_timeframe == "5m"
        bars = [_bar(request.internal_symbol, index) for index in range(288)]
        for bar in bars:
            self._repositories.bars.save(bar, data_source="schwab_history")
        return bars


def _write_fake_probation_artifacts(root_dir: Path) -> ApprovedQuantProbationArtifacts:
    root_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for spec in approved_quant_lane_specs():
        lane_dir = root_dir / "lanes" / spec.lane_id
        lane_dir.mkdir(parents=True, exist_ok=True)
        if "breakout" in spec.lane_id:
            trades = [
                {
                    "symbol": "GC",
                    "session_label": "US",
                    "exit_reason": "time_exit",
                }
            ]
        else:
            trades = [
                {
                    "symbol": "CL",
                    "session_label": "LONDON",
                    "exit_reason": "structural_invalidation",
                }
            ]
        (lane_dir / "signals.jsonl").write_text(json.dumps({"lane_id": spec.lane_id}) + "\n", encoding="utf-8")
        (lane_dir / "trades.jsonl").write_text(
            "\n".join(json.dumps(row, sort_keys=True) for row in trades) + "\n",
            encoding="utf-8",
        )
        rows.append(
            {
                "lane_id": spec.lane_id,
                "lane_name": spec.lane_name,
                "approved_scope": approved_quant_lane_scope_payload(spec),
                "probation_status": "normal",
                "promotion_state": "operator_baseline_candidate",
                "post_cost_monitoring_read": {"label": "stable_positive_post_cost"},
                "warning_flags": [],
                "symbol_attribution_summary": ["GC +0.100R (1)"] if "breakout" in spec.lane_id else ["CL +0.100R (1)"],
                "session_attribution_summary": ["US +0.100R (1)"] if "breakout" in spec.lane_id else ["LONDON +0.100R (1)"],
                "artifacts": {
                    "lane_dir": str(lane_dir),
                },
            }
        )

    snapshot = {"rows": rows}
    snapshot_json_path = root_dir / "approved_quant_baselines_snapshot.json"
    snapshot_markdown_path = root_dir / "approved_quant_baselines_snapshot.md"
    current_status_json_path = root_dir / "current_active_baseline_status.json"
    current_status_markdown_path = root_dir / "current_active_baseline_status.md"
    snapshot_json_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    snapshot_markdown_path.write_text("# snapshot\n", encoding="utf-8")
    current_status_json_path.write_text(json.dumps({"rows": rows}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    current_status_markdown_path.write_text("# current status\n", encoding="utf-8")
    return ApprovedQuantProbationArtifacts(
        snapshot_json_path=snapshot_json_path,
        snapshot_markdown_path=snapshot_markdown_path,
        current_status_json_path=current_status_json_path,
        current_status_markdown_path=current_status_markdown_path,
        root_dir=root_dir,
        report=snapshot,
    )


def test_run_approved_quant_schwab_history_validation_builds_fetch_resample_and_probation_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = _build_settings(tmp_path)
    config = _build_config(tmp_path)

    monkeypatch.setattr(
        "mgc_v05l.app.approved_quant_schwab_history_validation.load_schwab_market_data_config",
        lambda _path=None: config,
    )
    monkeypatch.setattr(
        "mgc_v05l.app.approved_quant_schwab_history_validation.load_settings_from_files",
        lambda _paths=None: settings,
    )
    monkeypatch.setattr(
        "mgc_v05l.app.approved_quant_schwab_history_validation.HistoricalBackfillService",
        _FakeHistoricalBackfillService,
    )
    monkeypatch.setattr(
        "mgc_v05l.app.approved_quant_schwab_history_validation._build_oauth_client",
        lambda _auth: object(),
    )

    def _fake_run_probation(*, database_path, execution_timeframe, output_dir):  # type: ignore[no-untyped-def]
        assert Path(database_path).exists()
        assert execution_timeframe == "5m"
        return _write_fake_probation_artifacts(Path(output_dir))

    monkeypatch.setattr(
        "mgc_v05l.app.approved_quant_schwab_history_validation.run_approved_quant_baseline_probation",
        _fake_run_probation,
    )

    artifacts = run_approved_quant_schwab_history_validation(
        config_paths=("config/base.yaml", str(tmp_path / "overlay.yaml")),
        schwab_config_path="config/schwab.local.json",
        execution_timeframe="5m",
        output_dir=tmp_path / "report",
        days_back=5,
    )

    assert artifacts.json_path.exists()
    assert artifacts.markdown_path.exists()
    assert artifacts.report["validation_ready_for_trustworthy_evaluation"] is True
    assert artifacts.report["blockers"] == []
    assert len(artifacts.report["fetch_results"]) == 9
    assert all(row["status"] == "fetched" for row in artifacts.report["fetch_results"])
    assert all(int(row["bar_count"]) == 288 for row in artifacts.report["fetch_results"])
    assert all(row["status"] == "resampled" for row in artifacts.report["resample_results"])
    assert all(
        all(int(timeframe_row["bar_count"]) > 0 for timeframe_row in row["timeframes"])
        for row in artifacts.report["resample_results"]
    )
    assert all(row["matches_expected_behavior"] for row in artifacts.report["lane_behavior_validation"])
    assert "repeatable approved-lane replay harness" in artifacts.report["recommended_next_step"]


def test_compare_lane_behavior_flags_session_and_exit_mismatches(tmp_path: Path) -> None:
    spec = approved_quant_lane_specs()[0]
    lane_dir = tmp_path / spec.lane_id
    lane_dir.mkdir(parents=True)
    (lane_dir / "signals.jsonl").write_text(json.dumps({"lane_id": spec.lane_id}) + "\n", encoding="utf-8")
    (lane_dir / "trades.jsonl").write_text(
        json.dumps({"symbol": "GC", "session_label": "LONDON", "exit_reason": "target"}) + "\n",
        encoding="utf-8",
    )

    rows = _compare_lane_behavior(
        snapshot={
            "rows": [
                {
                    "lane_id": spec.lane_id,
                    "lane_name": spec.lane_name,
                    "approved_scope": approved_quant_lane_scope_payload(spec),
                    "probation_status": "normal",
                    "promotion_state": "operator_baseline_candidate",
                    "post_cost_monitoring_read": {"label": "stable_positive_post_cost"},
                    "warning_flags": [],
                    "symbol_attribution_summary": [],
                    "session_attribution_summary": [],
                    "artifacts": {"lane_dir": str(lane_dir)},
                }
            ]
        }
    )

    assert rows[0]["unexpected_sessions"] == ["LONDON"]
    assert rows[0]["unexpected_exit_reasons"] == ["target"]
    assert rows[0]["matches_expected_behavior"] is False
