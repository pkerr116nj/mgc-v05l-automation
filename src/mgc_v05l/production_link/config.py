"""Configuration loading for the isolated Schwab production link."""

from __future__ import annotations

import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ..config_models.data_policy import load_data_storage_policy
from .models import ProductionFeatureFlags, SchwabProductionLinkConfig


def load_schwab_production_link_config(repo_root: Path) -> SchwabProductionLinkConfig:
    data_policy = load_data_storage_policy(repo_root)
    config_path = _resolve_optional_path(repo_root, os.environ.get("MGC_PRODUCTION_LINK_CONFIG"))
    payload = _load_optional_json(config_path)
    broker_monitor_policy = data_policy.domains["broker_monitor_truth"]

    features = ProductionFeatureFlags(
        production_connectivity_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_LINK_ENABLED",
            payload,
            "enabled",
            default=False,
        ),
        manual_order_ticket_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED",
            payload,
            "manual_order_ticket_enabled",
            default=False,
        ),
        manual_live_pilot_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED",
            payload,
            "manual_live_pilot_enabled",
            default=False,
        ),
        live_order_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED",
            payload,
            "live_order_submit_enabled",
            default=False,
        ),
        stock_market_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_STOCK_MARKET_LIVE_SUBMIT_ENABLED",
            payload,
            "stock_market_live_submit_enabled",
            default=False,
        ),
        stock_limit_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED",
            payload,
            "stock_limit_live_submit_enabled",
            default=False,
        ),
        stock_stop_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_STOCK_STOP_LIVE_SUBMIT_ENABLED",
            payload,
            "stock_stop_live_submit_enabled",
            default=False,
        ),
        stock_stop_limit_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_STOCK_STOP_LIMIT_LIVE_SUBMIT_ENABLED",
            payload,
            "stock_stop_limit_live_submit_enabled",
            default=False,
        ),
        advanced_tif_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_ADVANCED_TIF_ENABLED",
            payload,
            "advanced_tif_enabled",
            default=False,
        ),
        ext_exto_ticket_support_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_EXT_EXTO_TICKET_SUPPORT_ENABLED",
            payload,
            "ext_exto_ticket_support_enabled",
            default=False,
        ),
        oco_ticket_support_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_OCO_TICKET_SUPPORT_ENABLED",
            payload,
            "oco_ticket_support_enabled",
            default=False,
        ),
        ext_exto_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_EXT_EXTO_LIVE_SUBMIT_ENABLED",
            payload,
            "ext_exto_live_submit_enabled",
            default=False,
        ),
        oco_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_OCO_LIVE_SUBMIT_ENABLED",
            payload,
            "oco_live_submit_enabled",
            default=False,
        ),
        trailing_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_TRAILING_LIVE_SUBMIT_ENABLED",
            payload,
            "trailing_live_submit_enabled",
            default=False,
        ),
        close_order_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_CLOSE_ORDER_LIVE_SUBMIT_ENABLED",
            payload,
            "close_order_live_submit_enabled",
            default=False,
        ),
        futures_live_submit_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_FUTURES_LIVE_SUBMIT_ENABLED",
            payload,
            "futures_live_submit_enabled",
            default=False,
        ),
        portfolio_statement_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_PORTFOLIO_STATEMENT_ENABLED",
            payload,
            "portfolio_statement_enabled",
            default=False,
        ),
        analytics_overlays_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_ANALYTICS_OVERLAYS_ENABLED",
            payload,
            "analytics_overlays_enabled",
            default=False,
        ),
        replace_order_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_REPLACE_ORDER_ENABLED",
            payload,
            "replace_order_enabled",
            default=False,
        ),
        sell_short_enabled=_env_or_config_bool(
            "MGC_PRODUCTION_SELL_SHORT_ENABLED",
            payload,
            "sell_short_enabled",
            default=False,
        ),
        supported_manual_asset_classes=_env_or_config_csv(
            "MGC_PRODUCTION_SUPPORTED_MANUAL_ASSET_CLASSES",
            payload,
            "supported_manual_asset_classes",
            default=("STOCK",),
        ),
        supported_manual_order_types=_env_or_config_csv(
            "MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES",
            payload,
            "supported_manual_order_types",
            default=("LIMIT",),
        ),
        supported_manual_dry_run_order_types=_env_or_config_csv(
            "MGC_PRODUCTION_SUPPORTED_MANUAL_DRY_RUN_ORDER_TYPES",
            payload,
            "supported_manual_dry_run_order_types",
            default=(
                "MARKET",
                "LIMIT",
                "STOP",
                "STOP_LIMIT",
                "TRAIL_STOP",
                "TRAIL_STOP_LIMIT",
                "MARKET_ON_CLOSE",
                "LIMIT_ON_CLOSE",
            ),
        ),
        supported_manual_time_in_force_values=_env_or_config_csv(
            "MGC_PRODUCTION_SUPPORTED_MANUAL_TIF_VALUES",
            payload,
            "supported_manual_time_in_force_values",
            default=("DAY", "GTC"),
        ),
        supported_manual_session_values=_env_or_config_csv(
            "MGC_PRODUCTION_SUPPORTED_MANUAL_SESSION_VALUES",
            payload,
            "supported_manual_session_values",
            default=("NORMAL",),
        ),
        live_verified_order_keys=_env_or_config_csv(
            "MGC_PRODUCTION_LIVE_VERIFIED_ORDER_KEYS",
            payload,
            "live_verified_order_keys",
            default=(),
        ),
        manual_symbol_whitelist=_env_or_config_csv(
            "MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST",
            payload,
            "manual_symbol_whitelist",
            default=(),
        ),
        manual_max_quantity=_env_or_config_decimal(
            "MGC_PRODUCTION_MANUAL_MAX_QUANTITY",
            payload,
            "manual_max_quantity",
            default=Decimal("1"),
        ),
        broker_freshness_max_age_seconds=_env_or_config_int(
            "MGC_PRODUCTION_BROKER_FRESHNESS_MAX_AGE_SECONDS",
            payload,
            "broker_freshness_max_age_seconds",
            default=broker_monitor_policy.stale_after_seconds or 120,
        ),
    )
    enabled = features.production_connectivity_enabled

    output_root = data_policy.broker_monitor_database_path.parent
    output_root.mkdir(parents=True, exist_ok=True)
    database_path = _resolve_path(
        repo_root,
        os.environ.get("MGC_PRODUCTION_LINK_DB"),
        payload.get("database_path"),
        default=data_policy.broker_monitor_database_path,
    )
    selected_account_path = _resolve_path(
        repo_root,
        os.environ.get("MGC_PRODUCTION_LINK_SELECTED_ACCOUNT_FILE"),
        payload.get("selected_account_path"),
        default=data_policy.broker_monitor_selected_account_path,
    )
    snapshot_path = _resolve_path(
        repo_root,
        os.environ.get("MGC_PRODUCTION_LINK_SNAPSHOT_PATH"),
        payload.get("snapshot_path"),
        default=data_policy.broker_monitor_snapshot_path,
    )

    return SchwabProductionLinkConfig(
        repo_root=repo_root,
        enabled=enabled,
        features=features,
        trader_api_base_url=_env_or_config_str(
            "SCHWAB_TRADER_API_BASE_URL",
            payload,
            "trader_api_base_url",
            default="https://api.schwabapi.com/trader/v1",
        ),
        market_data_config_path=_resolve_path(
            repo_root,
            os.environ.get("MGC_PRODUCTION_LINK_MARKET_DATA_CONFIG"),
            payload.get("market_data_config_path"),
            default=repo_root / "config" / "schwab.local.json",
        ),
        request_timeout_seconds=_env_or_config_int(
            "MGC_PRODUCTION_LINK_TIMEOUT_SECONDS",
            payload,
            "request_timeout_seconds",
            default=30,
        ),
        cache_ttl_seconds=_env_or_config_int(
            "MGC_PRODUCTION_LINK_CACHE_TTL_SECONDS",
            payload,
            "cache_ttl_seconds",
            default=broker_monitor_policy.service_cache_ttl_seconds or 15,
        ),
        database_path=database_path,
        snapshot_path=snapshot_path,
        selected_account_path=selected_account_path,
        open_orders_lookback_days=_env_or_config_int(
            "MGC_PRODUCTION_LINK_OPEN_ORDERS_LOOKBACK_DAYS",
            payload,
            "open_orders_lookback_days",
            default=14,
        ),
        recent_fills_lookback_days=_env_or_config_int(
            "MGC_PRODUCTION_LINK_RECENT_FILLS_LOOKBACK_DAYS",
            payload,
            "recent_fills_lookback_days",
            default=7,
        ),
        manual_order_ack_timeout_seconds=_env_or_config_int(
            "MGC_PRODUCTION_MANUAL_ORDER_ACK_TIMEOUT_SECONDS",
            payload,
            "manual_order_ack_timeout_seconds",
            default=30,
        ),
        manual_order_fill_timeout_seconds=_env_or_config_int(
            "MGC_PRODUCTION_MANUAL_ORDER_FILL_TIMEOUT_SECONDS",
            payload,
            "manual_order_fill_timeout_seconds",
            default=180,
        ),
        manual_order_reconcile_grace_seconds=_env_or_config_int(
            "MGC_PRODUCTION_MANUAL_ORDER_RECONCILE_GRACE_SECONDS",
            payload,
            "manual_order_reconcile_grace_seconds",
            default=30,
        ),
        manual_order_post_ack_grace_seconds=_env_or_config_int(
            "MGC_PRODUCTION_MANUAL_ORDER_POST_ACK_GRACE_SECONDS",
            payload,
            "manual_order_post_ack_grace_seconds",
            default=20,
        ),
        default_account_hash=_env_or_config_optional_str(
            "MGC_PRODUCTION_LINK_DEFAULT_ACCOUNT_HASH",
            payload,
            "default_account_hash",
        ),
        default_account_number=_env_or_config_optional_str(
            "MGC_PRODUCTION_LINK_DEFAULT_ACCOUNT_NUMBER",
            payload,
            "default_account_number",
        ),
        config_path=config_path,
    )


def _load_optional_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _resolve_optional_path(repo_root: Path, raw_value: str | None) -> Path | None:
    if not raw_value:
        return None
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve(strict=False)


def _resolve_path(repo_root: Path, env_value: str | None, payload_value: Any, *, default: Path) -> Path:
    raw = env_value if env_value not in (None, "") else payload_value
    if raw in (None, ""):
        return default.resolve(strict=False)
    path = Path(str(raw)).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve(strict=False)


def _env_or_config_bool(env_name: str, payload: dict[str, Any], key: str, *, default: bool) -> bool:
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return env_value.strip().lower() in {"1", "true", "yes", "on"}
    raw = payload.get(key)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return default


def _env_or_config_str(env_name: str, payload: dict[str, Any], key: str, *, default: str) -> str:
    env_value = os.environ.get(env_name)
    if env_value not in (None, ""):
        return str(env_value).strip()
    raw = payload.get(key)
    if raw not in (None, ""):
        return str(raw).strip()
    return default


def _env_or_config_optional_str(env_name: str, payload: dict[str, Any], key: str) -> str | None:
    env_value = os.environ.get(env_name)
    if env_value not in (None, ""):
        return str(env_value).strip()
    raw = payload.get(key)
    if raw not in (None, ""):
        return str(raw).strip()
    return None


def _env_or_config_int(env_name: str, payload: dict[str, Any], key: str, *, default: int) -> int:
    env_value = os.environ.get(env_name)
    raw = env_value if env_value not in (None, "") else payload.get(key)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_or_config_csv(
    env_name: str,
    payload: dict[str, Any],
    key: str,
    *,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    env_value = os.environ.get(env_name)
    raw = env_value if env_value not in (None, "") else payload.get(key)
    if raw in (None, ""):
        return default
    if isinstance(raw, str):
        values = [item.strip().upper() for item in raw.split(",") if item.strip()]
        return tuple(values) or default
    if isinstance(raw, list):
        values = [str(item).strip().upper() for item in raw if str(item).strip()]
        return tuple(values) or default
    return default


def _env_or_config_decimal(env_name: str, payload: dict[str, Any], key: str, *, default: Decimal) -> Decimal:
    env_value = os.environ.get(env_name)
    raw = env_value if env_value not in (None, "") else payload.get(key)
    if raw in (None, ""):
        return default
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        return default
