"""Reusable shared feature materializers for the warehouse substrate."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from ...app.session_phase_labels import label_session_phase
from ...app.strategy_universe_retest import _build_probationary_shared_signal_context
from ...config_models import load_settings_from_files
from ...market_data.session_clock import classify_sessions
from ...research.trend_participation.phase3_timing import (
    VWAP_CHASE_RISK,
    VWAP_NEUTRAL,
    classify_vwap_price_quality,
)
from ..trend_participation.storage import materialize_parquet_dataset
from ._warehouse_common import (
    file_signature,
    now_utc,
    read_parquet_rows,
    read_stage_cache_manifest,
    row_to_domain_bar,
    stable_cache_key,
    write_stage_cache_manifest,
)
from .layout import build_layout
from .raw_materializer import build_dataset_partition_path, coverage_range

REPO_ROOT = Path.cwd()
PROBATIONARY_CONFIG_PATHS = (REPO_ROOT / "config" / "base.yaml", REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml")
SHARED_FEATURES_5M_CACHE_VERSION = "warehouse_shared_features_5m_v2"
SHARED_FEATURES_1M_TIMING_CACHE_VERSION = "warehouse_shared_features_1m_timing_v2"


def materialize_shared_features_5m_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    derived_5m_partition_path: Path,
    derived_from_version: str,
    config_paths: tuple[Path, ...] = PROBATIONARY_CONFIG_PATHS,
) -> dict[str, Any]:
    layout = build_layout(root_dir.resolve())
    symbol = symbol.upper()
    materialized_ts = now_utc()
    output_path = build_dataset_partition_path(
        dataset_root=layout["shared_features_5m"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="features.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": SHARED_FEATURES_5M_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "derived_from_version": derived_from_version,
            "config_signatures": [file_signature(path) for path in config_paths],
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        rows = read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        return {
            "dataset_name": "shared_features_5m",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "timeframe": "5m",
            "partition_path": output_path,
            "row_count": len(rows),
            "coverage": coverage_range(rows, timestamp_key="decision_ts"),
            "provenance_tag": rows[0]["provenance_tag"] if rows else None,
            "rows": rows,
            "cache": {
                "cache_hit": True,
                "cache_key": cache_key,
            },
            "timing": {
                "cache_hit": True,
                "parquet_read_seconds": round(read_seconds, 6),
                "total_seconds": round(perf_counter() - stage_started, 6),
            },
        }

    read_started = perf_counter()
    derived_rows = read_parquet_rows(derived_5m_partition_path)
    read_seconds = perf_counter() - read_started
    settings_started = perf_counter()
    base_settings = load_settings_from_files(config_paths)
    settings_seconds = perf_counter() - settings_started
    domain_started = perf_counter()
    raw_bars = [row_to_domain_bar(row=row, timeframe="5m") for row in derived_rows]
    domain_seconds = perf_counter() - domain_started
    shared_lanes = [
        {
            "strategy_id": f"{symbol.lower()}__warehouse_{family}",
            "symbol": symbol,
            "long_sources": [family] if family != "asiaEarlyPauseResumeShortTurn" else [],
            "short_sources": [family] if family == "asiaEarlyPauseResumeShortTurn" else [],
            "session_restriction": "",
        }
        for family in [
            "usLatePauseResumeLongTurn",
            "asiaEarlyNormalBreakoutRetestHoldTurn",
            "asiaEarlyPauseResumeShortTurn",
        ]
    ]
    context_started = perf_counter()
    shared_context = _build_probationary_shared_signal_context(
        lanes=shared_lanes,
        base_settings=base_settings,
        structural_bars=raw_bars,
    )
    context_seconds = perf_counter() - context_started
    settings = shared_context["settings"]
    bars = [classify_sessions(bar, settings) for bar in shared_context["structural_bars"]]
    features_by_bar_id = shared_context["features_by_bar_id"]
    signals_by_bar_id = shared_context["signals_by_bar_id"]
    rows: list[dict[str, Any]] = []
    row_build_started = perf_counter()
    for bar in bars:
        feature_packet = features_by_bar_id[bar.bar_id]
        signal_packet = signals_by_bar_id[bar.bar_id]
        rows.append(
            {
                "symbol": symbol,
                "shard_id": shard_id,
                "decision_ts": bar.end_ts,
                "bar_id": bar.bar_id,
                "timeframe": "5m",
                "session_phase": label_session_phase(bar.end_ts),
                "atr": float(feature_packet.atr),
                "bar_range": float(feature_packet.bar_range),
                "body_size": float(feature_packet.body_size),
                "vol_ratio": float(feature_packet.vol_ratio),
                "turn_ema_fast": float(feature_packet.turn_ema_fast),
                "turn_ema_slow": float(feature_packet.turn_ema_slow),
                "velocity": float(feature_packet.velocity),
                "velocity_delta": float(feature_packet.velocity_delta),
                "vwap": float(feature_packet.vwap),
                "vwap_buffer": float(feature_packet.vwap_buffer),
                "downside_stretch": float(feature_packet.downside_stretch),
                "upside_stretch": float(feature_packet.upside_stretch),
                "bull_close_strong": bool(feature_packet.bull_close_strong),
                "bear_close_weak": bool(feature_packet.bear_close_weak),
                "bull_snap_turn_candidate": bool(signal_packet.bull_snap_turn_candidate),
                "bear_snap_turn_candidate": bool(signal_packet.bear_snap_turn_candidate),
                "asia_reclaim_bar_raw": bool(signal_packet.asia_reclaim_bar_raw),
                "asia_vwap_long_signal": bool(signal_packet.asia_vwap_long_signal),
                "us_late_pause_resume_long_turn_candidate": bool(signal_packet.us_late_pause_resume_long_turn_candidate),
                "asia_early_normal_breakout_retest_hold_long_turn_candidate": bool(
                    signal_packet.asia_early_normal_breakout_retest_hold_long_turn_candidate
                ),
                "asia_early_pause_resume_short_turn_candidate": bool(
                    signal_packet.asia_early_pause_resume_short_turn_candidate
                ),
                "derived_from_version": derived_from_version,
                "materialized_ts": materialized_ts,
                "coverage_window_start": bars[0].end_ts if bars else None,
                "coverage_window_end": bars[-1].end_ts if bars else None,
                "provenance_tag": f"shared_features_5m:{symbol}:{shard_id}:{derived_from_version}",
            }
        )
    row_build_seconds = perf_counter() - row_build_started
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name="shared_features_5m",
        cache_key=cache_key,
    )
    return {
        "dataset_name": "shared_features_5m",
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "timeframe": "5m",
        "partition_path": output_path,
        "row_count": len(rows),
        "coverage": coverage_range(rows, timestamp_key="decision_ts"),
        "provenance_tag": rows[0]["provenance_tag"] if rows else None,
        "rows": rows,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "parquet_read_seconds": round(read_seconds, 6),
            "settings_load_seconds": round(settings_seconds, 6),
            "domain_bar_build_seconds": round(domain_seconds, 6),
            "shared_context_seconds": round(context_seconds, 6),
            "row_build_seconds": round(row_build_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def materialize_shared_features_1m_timing_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    raw_1m_partition_path: Path,
    raw_version: str,
) -> dict[str, Any]:
    layout = build_layout(root_dir.resolve())
    symbol = symbol.upper()
    materialized_ts = now_utc()
    output_path = build_dataset_partition_path(
        dataset_root=layout["shared_features_1m_timing"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="timing.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": SHARED_FEATURES_1M_TIMING_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "raw_version": raw_version,
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        rows = read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        return {
            "dataset_name": "shared_features_1m_timing",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "timeframe": "1m",
            "partition_path": output_path,
            "row_count": len(rows),
            "coverage": coverage_range(rows, timestamp_key="timing_ts"),
            "provenance_tag": rows[0]["provenance_tag"] if rows else None,
            "rows": rows,
            "cache": {
                "cache_hit": True,
                "cache_key": cache_key,
            },
            "timing": {
                "cache_hit": True,
                "parquet_read_seconds": round(read_seconds, 6),
                "total_seconds": round(perf_counter() - stage_started, 6),
            },
        }

    read_started = perf_counter()
    raw_rows = read_parquet_rows(raw_1m_partition_path)
    read_seconds = perf_counter() - read_started
    rows: list[dict[str, Any]] = []
    coverage_start = raw_rows[0]["bar_ts"] if raw_rows else None
    coverage_end = raw_rows[-1]["bar_ts"] if raw_rows else None
    row_build_started = perf_counter()
    for raw_row in raw_rows:
        close_price = float(raw_row["close"])
        bar_vwap = (float(raw_row["high"]) + float(raw_row["low"]) + float(raw_row["close"])) / 3.0
        band_reference = max(float(raw_row["high"]) - float(raw_row["low"]), 1e-9)
        long_quality = classify_vwap_price_quality(
            side="LONG",
            entry_price=close_price,
            bar_vwap=bar_vwap,
            band_reference=band_reference,
        )
        short_quality = classify_vwap_price_quality(
            side="SHORT",
            entry_price=close_price,
            bar_vwap=bar_vwap,
            band_reference=band_reference,
        )
        rows.append(
            {
                "symbol": symbol,
                "shard_id": shard_id,
                "timing_ts": raw_row["bar_ts"],
                "bar_id": f"{symbol}:1m:{raw_row['bar_ts'].isoformat()}",
                "timeframe": "1m",
                "close_price": close_price,
                "high_price": float(raw_row["high"]),
                "low_price": float(raw_row["low"]),
                "bar_vwap": bar_vwap,
                "bar_range_points": band_reference,
                "long_close_quality": long_quality,
                "short_close_quality": short_quality,
                "long_neutral_tight_ok": _neutral_tight_allowed(long_quality, close_price, bar_vwap, band_reference),
                "short_neutral_tight_ok": _neutral_tight_allowed(short_quality, close_price, bar_vwap, band_reference),
                "materialized_from_raw_version": raw_version,
                "materialized_ts": materialized_ts,
                "coverage_window_start": coverage_start,
                "coverage_window_end": coverage_end,
                "provenance_tag": f"shared_features_1m_timing:{symbol}:{shard_id}:{raw_version}",
            }
        )
    row_build_seconds = perf_counter() - row_build_started
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name="shared_features_1m_timing",
        cache_key=cache_key,
    )
    return {
        "dataset_name": "shared_features_1m_timing",
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "timeframe": "1m",
        "partition_path": output_path,
        "row_count": len(rows),
        "coverage": coverage_range(rows, timestamp_key="timing_ts"),
        "provenance_tag": rows[0]["provenance_tag"] if rows else None,
        "rows": rows,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "parquet_read_seconds": round(read_seconds, 6),
            "row_build_seconds": round(row_build_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def _neutral_tight_allowed(quality: str, entry_price: float, bar_vwap: float, band_reference: float) -> bool:
    if quality == VWAP_CHASE_RISK:
        return False
    if quality != VWAP_NEUTRAL:
        return True
    neutral_band = max(float(band_reference), 1e-9) * 0.10
    return abs(entry_price - bar_vwap) <= neutral_band * 0.5
