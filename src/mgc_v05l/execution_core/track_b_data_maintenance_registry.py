"""Track B data-maintenance instrument registry.

The registry is intentionally conservative: MGC is the first runtime-enabled
instrument. Broader Track A-style futures and ETF symbols are represented as
disabled planning entries so the data-maintenance shape can grow without
accidentally expanding runtime scope.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .track_b_data_maintenance import DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT


@dataclass(frozen=True)
class TrackBDataMaintenanceInstrument:
    internal_symbol: str
    contract_key: str | None
    provider_symbol: str | None
    continuous_symbol: str | None
    dataset: str | None
    schema: str
    timeframe: str
    asset_class: str
    enabled_for_runtime: bool
    enabled_for_research: bool
    maintenance_priority: int
    latest_good_path: Path


def latest_good_history_path(internal_symbol: str, *, output_root: Path = DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT) -> Path:
    normalized = internal_symbol.strip().lower()
    return Path(output_root) / f"latest_good_{normalized}_1m_history.json"


MGC_DATA_MAINTENANCE_INSTRUMENT = TrackBDataMaintenanceInstrument(
    internal_symbol="MGC",
    contract_key="MGC-202606",
    provider_symbol="MGCM6",
    continuous_symbol="MGC.v.0",
    dataset="GLBX.MDP3",
    schema="ohlcv-1m",
    timeframe="1m",
    asset_class="FUTURES",
    enabled_for_runtime=True,
    enabled_for_research=True,
    maintenance_priority=1,
    latest_good_path=latest_good_history_path("MGC"),
)


_FUTURES_PLACEHOLDERS = (
    "GC",
    "MES",
    "ES",
    "MNQ",
    "NQ",
    "ZT",
    "ZF",
    "ZN",
    "ZB",
    "ZQ",
    "6E",
    "6J",
    "6B",
    "6A",
    "HG",
    "QC",
    "PL",
    "CL",
    "NG",
    "BTC",
    "MBT",
    "YM",
)
_ETF_PLACEHOLDERS = ("SPY", "QQQ", "TQQQ", "SQQQ")


def _placeholder(symbol: str, *, asset_class: str, priority: int) -> TrackBDataMaintenanceInstrument:
    return TrackBDataMaintenanceInstrument(
        internal_symbol=symbol,
        contract_key=None,
        provider_symbol=None,
        continuous_symbol=None,
        dataset="GLBX.MDP3" if asset_class == "FUTURES" else None,
        schema="ohlcv-1m",
        timeframe="1m",
        asset_class=asset_class,
        enabled_for_runtime=False,
        enabled_for_research=False,
        maintenance_priority=priority,
        latest_good_path=latest_good_history_path(symbol),
    )


TRACK_B_DATA_MAINTENANCE_REGISTRY: dict[str, TrackBDataMaintenanceInstrument] = {
    MGC_DATA_MAINTENANCE_INSTRUMENT.internal_symbol: MGC_DATA_MAINTENANCE_INSTRUMENT,
    **{
        symbol: _placeholder(symbol, asset_class="FUTURES", priority=50 + index)
        for index, symbol in enumerate(_FUTURES_PLACEHOLDERS, start=1)
    },
    **{
        symbol: _placeholder(symbol, asset_class="ETF", priority=100 + index)
        for index, symbol in enumerate(_ETF_PLACEHOLDERS, start=1)
    },
}


def get_data_maintenance_instrument(internal_symbol: str) -> TrackBDataMaintenanceInstrument | None:
    return TRACK_B_DATA_MAINTENANCE_REGISTRY.get(internal_symbol.strip().upper())


def require_runtime_data_maintenance_instrument(internal_symbol: str) -> TrackBDataMaintenanceInstrument:
    instrument = get_data_maintenance_instrument(internal_symbol)
    if instrument is None:
        raise ValueError(f"Unknown Track B data-maintenance instrument: {internal_symbol}.")
    if not instrument.enabled_for_runtime:
        raise ValueError(f"Track B data-maintenance instrument {instrument.internal_symbol} is not enabled for runtime maintenance.")
    return instrument
