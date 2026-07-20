"""Shared Track B live runtime market-data symbol namelist.

This module is intentionally pure configuration validation. It does not start
market-data producers, touch broker state, approve strategy behavior, or grant
live-money eligibility.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH = Path("config/track_b_live_market_data_symbols.yaml")
REPO_ROOT = Path(__file__).resolve().parents[3]
REQUIRED_EXECUTION_REFERENCE_PAIRS = {
    "MGC": "GC",
    "MES": "ES",
    "MNQ": "NQ",
}
_REQUIRED_ROW_FIELDS = (
    "symbol",
    "enabled",
    "required_for_readiness",
    "asset_class",
    "execution_symbol",
    "reference_symbol",
    "databento_symbol",
    "dataset",
    "schema",
    "venue",
    "timezone",
    "min_confirmed_bars",
    "freshness_threshold_seconds",
    "notes",
)
_OPTIONAL_ROW_FIELDS = (
    "display_label",
    "session_calendar",
    "market_freshness_policy",
    "latest_bar_freshness_seconds",
)
_SUPPORTED_TOP_LEVEL_FIELDS = {"version", "symbols"}
_SUPPORTED_ROW_FIELDS = set(_REQUIRED_ROW_FIELDS) | set(_OPTIONAL_ROW_FIELDS)
SESSION_CALENDAR_GLOBEX_FUTURES = "globex_futures"
SESSION_CALENDAR_CME_CRYPTO_FUTURES = "cme_crypto_futures"
MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS = "liquid_trade_bar_required"
MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED = "thin_quote_feed_live"
_SUPPORTED_SESSION_CALENDARS = {
    SESSION_CALENDAR_GLOBEX_FUTURES,
    SESSION_CALENDAR_CME_CRYPTO_FUTURES,
}
_SUPPORTED_MARKET_FRESHNESS_POLICIES = {
    MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS,
    MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED,
}


class TrackBLiveMarketDataSymbolConfigError(ValueError):
    """Raised when the Track B live market-data namelist is malformed."""


@dataclass(frozen=True)
class TrackBLiveMarketDataSymbol:
    symbol: str
    enabled: bool
    required_for_readiness: bool
    asset_class: str
    execution_symbol: str
    reference_symbol: str
    databento_symbol: str
    dataset: str
    schema: str
    venue: str
    timezone: str
    min_confirmed_bars: int
    freshness_threshold_seconds: int
    notes: str
    display_label: str = ""
    session_calendar: str = SESSION_CALENDAR_GLOBEX_FUTURES
    market_freshness_policy: str = MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS
    latest_bar_freshness_seconds: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def databento_mapping_key(self) -> tuple[str, str, str]:
        return (self.dataset, self.schema, self.databento_symbol)


@dataclass(frozen=True)
class TrackBLiveMarketDataSymbolNamelist:
    version: int
    symbols: tuple[TrackBLiveMarketDataSymbol, ...]

    def all_symbols(self) -> tuple[TrackBLiveMarketDataSymbol, ...]:
        return self.symbols

    def enabled_symbols(self) -> tuple[TrackBLiveMarketDataSymbol, ...]:
        return tuple(symbol for symbol in self.symbols if symbol.enabled)

    def disabled_symbols(self) -> tuple[TrackBLiveMarketDataSymbol, ...]:
        return tuple(symbol for symbol in self.symbols if not symbol.enabled)

    def required_for_readiness_symbols(self) -> tuple[TrackBLiveMarketDataSymbol, ...]:
        return tuple(symbol for symbol in self.enabled_symbols() if symbol.required_for_readiness)

    def optional_symbols(self) -> tuple[TrackBLiveMarketDataSymbol, ...]:
        return tuple(symbol for symbol in self.enabled_symbols() if not symbol.required_for_readiness)

    def by_symbol(self) -> dict[str, TrackBLiveMarketDataSymbol]:
        return {symbol.symbol: symbol for symbol in self.symbols}

    def enabled_databento_symbols(self) -> tuple[str, ...]:
        return tuple(symbol.databento_symbol for symbol in self.enabled_symbols())

    def active_phase1_runtime_symbols(self) -> tuple[str, ...]:
        return tuple(symbol.symbol for symbol in self.enabled_symbols())

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "symbols": [symbol.as_dict() for symbol in self.symbols],
        }


def load_track_b_live_market_data_symbols(
    path: Path | str = DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
) -> TrackBLiveMarketDataSymbolNamelist:
    config_path = _resolve_config_path(path)
    payload = _parse_yaml_subset(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise TrackBLiveMarketDataSymbolConfigError("Track B live market-data config must be a mapping.")
    return parse_track_b_live_market_data_symbols(payload)


def active_phase1_runtime_symbols(
    path: Path | str = DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
) -> tuple[str, ...]:
    return load_track_b_live_market_data_symbols(path).active_phase1_runtime_symbols()


def required_phase1_runtime_symbols(
    path: Path | str = DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
) -> tuple[str, ...]:
    return tuple(row.symbol for row in load_track_b_live_market_data_symbols(path).required_for_readiness_symbols())


def _resolve_config_path(path: Path | str) -> Path:
    config_path = Path(path)
    if config_path.is_absolute() or config_path.exists():
        return config_path
    repo_path = REPO_ROOT / config_path
    return repo_path if repo_path.exists() else config_path


def parse_track_b_live_market_data_symbols(payload: Mapping[str, Any]) -> TrackBLiveMarketDataSymbolNamelist:
    unknown_top_level = sorted(set(payload) - _SUPPORTED_TOP_LEVEL_FIELDS)
    if unknown_top_level:
        raise TrackBLiveMarketDataSymbolConfigError(
            f"Unsupported Track B live market-data config fields: {', '.join(unknown_top_level)}."
        )
    version = payload.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
        raise TrackBLiveMarketDataSymbolConfigError("Track B live market-data config version must be a positive integer.")
    raw_symbols = payload.get("symbols")
    if not isinstance(raw_symbols, Sequence) or isinstance(raw_symbols, (str, bytes)):
        raise TrackBLiveMarketDataSymbolConfigError("Track B live market-data config symbols must be a list.")
    if not raw_symbols:
        raise TrackBLiveMarketDataSymbolConfigError("Track B live market-data config must define at least one symbol.")

    symbols = tuple(_parse_symbol_row(raw_symbol, index=index) for index, raw_symbol in enumerate(raw_symbols, start=1))
    namelist = TrackBLiveMarketDataSymbolNamelist(version=version, symbols=symbols)
    _validate_namelist(namelist)
    return namelist


def _parse_symbol_row(raw_symbol: Any, *, index: int) -> TrackBLiveMarketDataSymbol:
    if not isinstance(raw_symbol, Mapping):
        raise TrackBLiveMarketDataSymbolConfigError(f"Symbol row {index} must be a mapping.")
    unknown_fields = sorted(set(raw_symbol) - _SUPPORTED_ROW_FIELDS)
    if unknown_fields:
        raise TrackBLiveMarketDataSymbolConfigError(
            f"Symbol row {index} has unsupported fields: {', '.join(unknown_fields)}."
        )
    missing_fields = [field for field in _REQUIRED_ROW_FIELDS if field not in raw_symbol]
    if missing_fields:
        raise TrackBLiveMarketDataSymbolConfigError(
            f"Symbol row {index} is missing fields: {', '.join(missing_fields)}."
        )

    enabled = _required_bool(raw_symbol, "enabled", index=index)
    required_for_readiness = _required_bool(raw_symbol, "required_for_readiness", index=index)
    return TrackBLiveMarketDataSymbol(
        symbol=_required_text(raw_symbol, "symbol", index=index).upper(),
        enabled=enabled,
        required_for_readiness=required_for_readiness,
        asset_class=_required_text(raw_symbol, "asset_class", index=index),
        execution_symbol=_required_text(raw_symbol, "execution_symbol", index=index).upper(),
        reference_symbol=_required_text(raw_symbol, "reference_symbol", index=index).upper(),
        databento_symbol=_optional_text(raw_symbol, "databento_symbol"),
        dataset=_optional_text(raw_symbol, "dataset"),
        schema=_optional_text(raw_symbol, "schema"),
        venue=_optional_text(raw_symbol, "venue"),
        timezone=_optional_text(raw_symbol, "timezone"),
        min_confirmed_bars=_required_positive_int(raw_symbol, "min_confirmed_bars", index=index),
        freshness_threshold_seconds=_required_positive_int(raw_symbol, "freshness_threshold_seconds", index=index),
        notes=_optional_text(raw_symbol, "notes"),
        display_label=_optional_text(raw_symbol, "display_label"),
        session_calendar=_optional_choice(
            raw_symbol,
            "session_calendar",
            choices=_SUPPORTED_SESSION_CALENDARS,
            default=SESSION_CALENDAR_GLOBEX_FUTURES,
            index=index,
        ),
        market_freshness_policy=_optional_choice(
            raw_symbol,
            "market_freshness_policy",
            choices=_SUPPORTED_MARKET_FRESHNESS_POLICIES,
            default=MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS,
            index=index,
        ),
        latest_bar_freshness_seconds=_optional_positive_int(
            raw_symbol,
            "latest_bar_freshness_seconds",
            index=index,
        ),
    )


def _validate_namelist(namelist: TrackBLiveMarketDataSymbolNamelist) -> None:
    _validate_unique_symbols(namelist.symbols)
    _validate_required_readiness(namelist.symbols)
    _validate_enabled_symbols(namelist.enabled_symbols())
    _validate_explicit_execution_reference_pairs(namelist.enabled_symbols())


def _validate_unique_symbols(symbols: Sequence[TrackBLiveMarketDataSymbol]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in symbols:
        if row.symbol in seen:
            duplicates.add(row.symbol)
        seen.add(row.symbol)
    if duplicates:
        raise TrackBLiveMarketDataSymbolConfigError(
            f"Duplicate Track B live market-data symbols: {', '.join(sorted(duplicates))}."
        )


def _validate_enabled_symbols(symbols: Sequence[TrackBLiveMarketDataSymbol]) -> None:
    seen_databento_mappings: set[tuple[str, str, str]] = set()
    duplicate_databento_mappings: set[tuple[str, str, str]] = set()
    for row in symbols:
        missing_mapping_fields = [
            field
            for field in ("databento_symbol", "dataset", "schema", "venue", "timezone")
            if not getattr(row, field)
        ]
        if missing_mapping_fields:
            raise TrackBLiveMarketDataSymbolConfigError(
                f"Enabled symbol {row.symbol} is missing Databento mapping fields: "
                f"{', '.join(missing_mapping_fields)}."
            )
        if row.databento_mapping_key in seen_databento_mappings:
            duplicate_databento_mappings.add(row.databento_mapping_key)
        seen_databento_mappings.add(row.databento_mapping_key)
        if (
            row.market_freshness_policy == MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED
            and row.latest_bar_freshness_seconds is None
        ):
            raise TrackBLiveMarketDataSymbolConfigError(
                f"Thin market symbol {row.symbol} must define latest_bar_freshness_seconds."
            )
    if duplicate_databento_mappings:
        formatted = ", ".join(
            f"{dataset}/{schema}/{databento_symbol}"
            for dataset, schema, databento_symbol in sorted(duplicate_databento_mappings)
        )
        raise TrackBLiveMarketDataSymbolConfigError(f"Duplicate enabled Databento mappings: {formatted}.")


def _validate_required_readiness(symbols: Sequence[TrackBLiveMarketDataSymbol]) -> None:
    disabled_required = [row.symbol for row in symbols if not row.enabled and row.required_for_readiness]
    if disabled_required:
        raise TrackBLiveMarketDataSymbolConfigError(
            f"Disabled symbols cannot be required for readiness: {', '.join(sorted(disabled_required))}."
        )


def _validate_explicit_execution_reference_pairs(symbols: Sequence[TrackBLiveMarketDataSymbol]) -> None:
    by_symbol = {row.symbol: row for row in symbols}
    missing_pairs = sorted(symbol for symbol in REQUIRED_EXECUTION_REFERENCE_PAIRS if symbol not in by_symbol)
    if missing_pairs:
        raise TrackBLiveMarketDataSymbolConfigError(
            "Missing required Track B execution/reference pair rows for: " + ", ".join(missing_pairs) + "."
        )
    invalid_pairs = [
        f"{symbol}/{row.reference_symbol}"
        for symbol, expected_reference in REQUIRED_EXECUTION_REFERENCE_PAIRS.items()
        for row in (by_symbol[symbol],)
        if row.execution_symbol != symbol or row.reference_symbol != expected_reference
    ]
    if invalid_pairs:
        raise TrackBLiveMarketDataSymbolConfigError(
            "Invalid Track B execution/reference pairs: "
            + ", ".join(sorted(invalid_pairs))
            + ". Expected MGC/GC, MES/ES, MNQ/NQ."
        )


def _required_text(raw_symbol: Mapping[str, Any], field: str, *, index: int) -> str:
    value = raw_symbol.get(field)
    if not isinstance(value, str) or not value.strip():
        raise TrackBLiveMarketDataSymbolConfigError(f"Symbol row {index} field {field} must be a non-empty string.")
    return value.strip()


def _optional_text(raw_symbol: Mapping[str, Any], field: str) -> str:
    value = raw_symbol.get(field)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TrackBLiveMarketDataSymbolConfigError(f"Symbol field {field} must be a string when provided.")
    return value.strip()


def _optional_choice(
    raw_symbol: Mapping[str, Any],
    field: str,
    *,
    choices: set[str],
    default: str,
    index: int,
) -> str:
    value = raw_symbol.get(field)
    if value is None:
        return default
    if not isinstance(value, str) or not value.strip():
        raise TrackBLiveMarketDataSymbolConfigError(f"Symbol row {index} field {field} must be a non-empty string.")
    normalized = value.strip()
    if normalized not in choices:
        raise TrackBLiveMarketDataSymbolConfigError(
            f"Symbol row {index} field {field} has unsupported value {normalized!r}."
        )
    return normalized


def _required_bool(raw_symbol: Mapping[str, Any], field: str, *, index: int) -> bool:
    value = raw_symbol.get(field)
    if not isinstance(value, bool):
        raise TrackBLiveMarketDataSymbolConfigError(f"Symbol row {index} field {field} must be boolean.")
    return value


def _required_positive_int(raw_symbol: Mapping[str, Any], field: str, *, index: int) -> int:
    value = raw_symbol.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise TrackBLiveMarketDataSymbolConfigError(f"Symbol row {index} field {field} must be a positive integer.")
    return value


def _optional_positive_int(raw_symbol: Mapping[str, Any], field: str, *, index: int) -> int | None:
    value = raw_symbol.get(field)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise TrackBLiveMarketDataSymbolConfigError(f"Symbol row {index} field {field} must be a positive integer.")
    return value


def _parse_yaml_subset(text: str) -> dict[str, Any]:
    """Parse the small YAML subset used by the symbol namelist.

    The project does not declare PyYAML as a runtime dependency. This parser is
    deliberately narrow: top-level scalar fields plus a top-level list of
    mapping rows with scalar values.
    """

    payload: dict[str, Any] = {}
    current_list_key: str | None = None
    current_row: dict[str, Any] | None = None
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line.startswith(" "):
            if stripped.endswith(":"):
                current_list_key = stripped[:-1]
                payload[current_list_key] = []
                current_row = None
                continue
            key, value = _split_yaml_key_value(stripped, line_number=line_number)
            payload[key] = _parse_yaml_scalar(value)
            current_list_key = None
            current_row = None
            continue
        if current_list_key is None:
            raise TrackBLiveMarketDataSymbolConfigError(f"Unsupported YAML indentation at line {line_number}.")
        if not isinstance(payload.get(current_list_key), list):
            raise TrackBLiveMarketDataSymbolConfigError(f"YAML field {current_list_key} is not a list.")
        if stripped.startswith("- "):
            current_row = {}
            payload[current_list_key].append(current_row)
            remainder = stripped[2:].strip()
            if remainder:
                key, value = _split_yaml_key_value(remainder, line_number=line_number)
                current_row[key] = _parse_yaml_scalar(value)
            continue
        if current_row is None:
            raise TrackBLiveMarketDataSymbolConfigError(f"YAML list item field without row at line {line_number}.")
        key, value = _split_yaml_key_value(stripped, line_number=line_number)
        current_row[key] = _parse_yaml_scalar(value)
    return payload


def _split_yaml_key_value(text: str, *, line_number: int) -> tuple[str, str]:
    if ":" not in text:
        raise TrackBLiveMarketDataSymbolConfigError(f"Expected key/value pair at YAML line {line_number}.")
    key, value = text.split(":", 1)
    key = key.strip()
    if not key:
        raise TrackBLiveMarketDataSymbolConfigError(f"Missing YAML key at line {line_number}.")
    return key, value.strip()


def _parse_yaml_scalar(value: str) -> Any:
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value in {"null", "Null", "~"}:
        return None
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value
