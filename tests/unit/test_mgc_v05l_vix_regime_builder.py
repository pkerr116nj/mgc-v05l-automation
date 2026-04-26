from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.research.regime.vix_regime_builder import build_vol_regime_daily, load_vol_regime_rows
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout


def test_build_vol_regime_daily_applies_config_buckets(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    layout = build_layout(warehouse_root)
    materialize_parquet_dataset(
        layout["vix_daily"] / "vix_daily.parquet",
        [
            {
                "vix_trade_date": "2026-01-02",
                "vix_asof_ts": "2026-01-02T21:15:00+00:00",
                "vix_open": 16.0,
                "vix_high": 17.0,
                "vix_low": 15.5,
                "vix_close": 17.0,
                "source": "cboe_official_daily_history",
                "loaded_at": "2026-04-25T12:00:00+00:00",
            },
            {
                "vix_trade_date": "2026-01-05",
                "vix_asof_ts": "2026-01-05T21:15:00+00:00",
                "vix_open": 18.0,
                "vix_high": 21.0,
                "vix_low": 17.5,
                "vix_close": 20.4,
                "source": "cboe_official_daily_history",
                "loaded_at": "2026-04-25T12:00:00+00:00",
            },
        ],
    )
    config_path = tmp_path / "research_regime_buckets.json"
    config_path.write_text(
        json.dumps(
            {
                "vix_daily": {
                    "source_url": "unused",
                    "asof_time_et": "16:15:00",
                    "version": "test-v1",
                    "level_buckets": [
                        {"label": "LOW", "min": None, "max": 18.0},
                        {"label": "MID", "min": 18.0, "max": 25.0},
                        {"label": "HIGH", "min": 25.0, "max": None},
                    ],
                    "change_pct_buckets": [
                        {"label": "DOWN", "min": None, "max": -0.03},
                        {"label": "FLAT", "min": -0.03, "max": 0.03},
                        {"label": "UP", "min": 0.03, "max": None},
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    result = build_vol_regime_daily(
        warehouse_root=warehouse_root,
        bucket_config_path=config_path,
    )

    rows = load_vol_regime_rows(warehouse_root)
    assert result["row_count"] == 2
    assert rows[0]["vix_level_bucket"] == "LOW"
    assert rows[0]["vix_change_bucket"] == "UNKNOWN"
    assert rows[1]["vix_level_bucket"] == "MID"
    assert rows[1]["vix_change_bucket"] == "UP"
    assert rows[1]["vix_combined_bucket"] == "MID_UP"
