"""Track B futures contract roll/expiry resolver.

The resolver is intentionally read-only: it evaluates contractDetails-backed
evidence and returns a pre-submit decision for new entries. Existing lifecycle
exits keep using the filled contract chosen when the position was opened.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

CONTRACT_ALLOWED = "CONTRACT_ALLOWED"
CONTRACT_ROLL_BLOCKED = "CONTRACT_ROLL_BLOCKED"
CONTRACT_DETAILS_STALE = "CONTRACT_DETAILS_STALE"
CONTRACT_AMBIGUOUS = "CONTRACT_AMBIGUOUS"
CONTRACT_NEAR_EXPIRY = "CONTRACT_NEAR_EXPIRY"
CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED = "CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED"

_GOLD_SYMBOLS = {"GC", "MGC"}
_INDEX_SYMBOLS = {"MNQ", "MES", "NQ", "ES"}
_GOLD_ROLL_BLOCK_DAYS = 30
_INDEX_ROLL_WARN_DAYS = 21
_INDEX_ROLL_BLOCK_DAYS = 7
_DEFAULT_CONTRACT_DETAILS_MAX_AGE_SECONDS = 24 * 60 * 60
_GOLD_RECOMMENDATION_MONTH = "202608"
_MONTH_CODE_BY_NUMBER = {
    "01": "F",
    "02": "G",
    "03": "H",
    "04": "J",
    "05": "K",
    "06": "M",
    "07": "N",
    "08": "Q",
    "09": "U",
    "10": "V",
    "11": "X",
    "12": "Z",
}


@dataclass(frozen=True)
class FuturesContractResolverInput:
    strategy_id: str
    symbol: str
    contract_month: str
    action: str
    selected_target: Mapping[str, Any]
    qualified_contract_report: Mapping[str, Any]
    intent_type: str | None = None
    recommendation_contract_report: Mapping[str, Any] | None = None
    ibkr_warnings: tuple[str, ...] = ()
    now: datetime | date | None = None
    contract_details_max_age_seconds: float = _DEFAULT_CONTRACT_DETAILS_MAX_AGE_SECONDS


def evaluate_futures_contract_pre_submit(
    request: FuturesContractResolverInput,
) -> dict[str, Any]:
    """Return a fail-closed contract authority decision for a bridge intent."""

    now = _normalize_now(request.now)
    symbol = _normalize_symbol(request.symbol or request.selected_target.get("symbol"))
    intent_type = str(request.intent_type or "").strip().upper()
    action = str(request.action or "").strip().upper()
    is_new_entry = _is_new_entry(action=action, intent_type=intent_type)
    product_family = _product_family(symbol)
    selected_target = dict(request.selected_target or {})
    qualified_report = dict(request.qualified_contract_report or {})
    details = _details_from_report(qualified_report)
    warnings = tuple(str(warning or "").strip() for warning in request.ibkr_warnings if str(warning or "").strip())
    warning_detected = _roll_warning_detected(warnings)

    base: dict[str, Any] = {
        "classification": CONTRACT_ALLOWED,
        "submit_allowed": True,
        "blocking": False,
        "blocker": None,
        "strategy_id": str(request.strategy_id or ""),
        "symbol": symbol,
        "product_family": product_family,
        "contract_month": str(request.contract_month or selected_target.get("contract_month") or "").strip(),
        "action": action,
        "intent_type": intent_type,
        "new_entry": is_new_entry,
        "policy_source": "TRACK_B_FUTURES_CONTRACT_RESOLVER_V1",
        "read_only": True,
        "broker_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "selected_contract": {},
        "recommended_contract": {},
        "warnings": list(warnings),
        "roll_status": "OK",
        "detail": "Contract resolver allows this intent.",
    }
    if not is_new_entry:
        return {
            **base,
            "classification": CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED,
            "detail": "Existing lifecycle exits/management keep the original filled contract; resolver applies only to new entries.",
        }

    if product_family == "UNKNOWN":
        return _blocked(
            base,
            blocker=CONTRACT_AMBIGUOUS,
            detail=f"No futures contract resolver policy exists for symbol {symbol or 'UNKNOWN'}.",
        )

    freshness = _contract_details_freshness(details=details, now=now, max_age_seconds=request.contract_details_max_age_seconds)
    if not details or not bool(freshness.get("fresh")):
        return _blocked(
            {
                **base,
                "contract_details_freshness": freshness,
                "recommended_contract": _gold_recommendation(
                    symbol=symbol,
                    recommendation_report=request.recommendation_contract_report,
                    now=now,
                    max_age_seconds=request.contract_details_max_age_seconds,
                )
                if product_family == "GOLD"
                else {},
            },
            blocker=CONTRACT_DETAILS_STALE,
            detail="Fresh IBKR contractDetails confirmation is required before a new futures entry can submit.",
        )

    selected = _resolve_selected_contract(
        symbol=symbol,
        contract_month=str(request.contract_month or selected_target.get("contract_month") or "").strip(),
        selected_target=selected_target,
        qualified_contract=dict(qualified_report.get("qualified_contract") or {}),
        details=details,
    )
    base = {**base, "contract_details_freshness": freshness, "selected_contract": selected}
    if not bool(selected.get("resolved")):
        return _blocked(
            base,
            blocker=CONTRACT_AMBIGUOUS,
            detail=str(selected.get("detail") or "IBKR contractDetails did not resolve exactly one selected contract."),
        )

    expiry_date = _parse_contract_expiry(selected.get("expiry"))
    if expiry_date is None:
        return _blocked(
            base,
            blocker=CONTRACT_AMBIGUOUS,
            detail="Selected contract does not have an exact IBKR lastTradeDate/expiry date.",
        )
    days_to_expiry = (expiry_date - now.date()).days
    base = {**base, "days_to_expiry": days_to_expiry}

    if product_family == "GOLD":
        recommendation = _gold_recommendation(
            symbol=symbol,
            recommendation_report=request.recommendation_contract_report,
            now=now,
            max_age_seconds=request.contract_details_max_age_seconds,
        )
        base = {**base, "recommended_contract": recommendation}
        if warning_detected:
            return _blocked(
                {**base, "roll_status": "ROLL_BLOCKED_IBKR_WARNING"},
                blocker=CONTRACT_ROLL_BLOCKED,
                detail="IBKR warning/roll-preference evidence blocks new gold entries on the selected contract.",
            )
        if days_to_expiry <= _GOLD_ROLL_BLOCK_DAYS:
            return _blocked(
                {**base, "roll_status": "ROLL_BLOCKED_NEAR_EXPIRY"},
                blocker=CONTRACT_NEAR_EXPIRY,
                detail=(
                    f"Gold contract {selected.get('local_symbol') or selected.get('contract_month')} is "
                    f"{days_to_expiry} days from expiry/termination; new entries fail closed at "
                    f"{_GOLD_ROLL_BLOCK_DAYS} days."
                ),
            )
        if not bool(recommendation.get("confirmed")) and str(selected.get("contract_month") or "") == "202606":
            return _blocked(
                {**base, "roll_status": "ROLL_RECOMMENDATION_UNCONFIRMED"},
                blocker=CONTRACT_DETAILS_STALE,
                detail="Gold resolver could not confirm the August 2026 replacement contract with fresh IBKR contractDetails.",
            )
        return {
            **base,
            "detail": "Gold selected contract is fresh and outside the roll block window.",
        }

    if product_family == "INDEX":
        if warning_detected:
            return _blocked(
                {**base, "roll_status": "ROLL_BLOCKED_IBKR_WARNING"},
                blocker=CONTRACT_ROLL_BLOCKED,
                detail="IBKR warning/roll-preference evidence blocks new index entries on the selected contract.",
            )
        if days_to_expiry <= _INDEX_ROLL_BLOCK_DAYS:
            return _blocked(
                {**base, "roll_status": "ROLL_BLOCKED_NEAR_EXPIRY"},
                blocker=CONTRACT_NEAR_EXPIRY,
                detail=(
                    f"Index contract {selected.get('local_symbol') or selected.get('contract_month')} is "
                    f"{days_to_expiry} days from expiry; new entries fail closed inside "
                    f"{_INDEX_ROLL_BLOCK_DAYS} days."
                ),
            )
        roll_status = "ROLL_WARNING_ONLY" if days_to_expiry <= _INDEX_ROLL_WARN_DAYS else "OK"
        return {
            **base,
            "roll_status": roll_status,
            "warning_only": roll_status == "ROLL_WARNING_ONLY",
            "detail": (
                "Index contractDetails are fresh; current contract is in roll-watch warning-only status."
                if roll_status == "ROLL_WARNING_ONLY"
                else "Index contractDetails are fresh and outside the near-expiry block window."
            ),
        }

    return {
        **base,
        "detail": "ContractDetails are fresh and selected contract passed generic futures resolver policy.",
    }


def recommended_gold_contract_month() -> str:
    return _GOLD_RECOMMENDATION_MONTH


def recommended_gold_local_symbol(symbol: str) -> str:
    return _local_symbol_for_month(_normalize_symbol(symbol), _GOLD_RECOMMENDATION_MONTH)


def _blocked(base: Mapping[str, Any], *, blocker: str, detail: str) -> dict[str, Any]:
    return {
        **dict(base),
        "classification": blocker,
        "submit_allowed": False,
        "blocking": True,
        "blocker": blocker,
        "detail": detail,
    }


def _normalize_now(value: datetime | date | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _product_family(symbol: str) -> str:
    if symbol in _GOLD_SYMBOLS:
        return "GOLD"
    if symbol in _INDEX_SYMBOLS:
        return "INDEX"
    return "UNKNOWN"


def _is_new_entry(*, action: str, intent_type: str) -> bool:
    if intent_type in {"BUY_TO_CLOSE", "SELL_TO_CLOSE", "CLOSE", "EXIT"}:
        return False
    if action == "EXIT":
        return False
    if intent_type in {"BUY_TO_OPEN", "SELL_TO_OPEN"}:
        return True
    return action == "BUY"


def _details_from_report(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    details = report.get("api_contract_details")
    if isinstance(details, list):
        return [dict(row) for row in details if isinstance(row, Mapping)]
    return []


def _contract_details_freshness(
    *,
    details: list[dict[str, Any]],
    now: datetime,
    max_age_seconds: float,
) -> dict[str, Any]:
    if not details:
        return {"fresh": False, "reason": "contract_details_missing"}
    ages: list[float] = []
    missing_updated_at = False
    for row in details:
        updated_at = _parse_datetime(row.get("updated_at"))
        if updated_at is None:
            missing_updated_at = True
            continue
        ages.append(max(0.0, (now - updated_at).total_seconds()))
    if not ages:
        return {"fresh": False, "reason": "contract_details_updated_at_missing", "missing_updated_at": missing_updated_at}
    max_age = max(ages)
    return {
        "fresh": max_age <= max_age_seconds,
        "max_age_seconds": max_age,
        "freshness_window_seconds": max_age_seconds,
        "missing_updated_at": missing_updated_at,
        "reason": "fresh" if max_age <= max_age_seconds else "contract_details_stale",
    }


def _resolve_selected_contract(
    *,
    symbol: str,
    contract_month: str,
    selected_target: Mapping[str, Any],
    qualified_contract: Mapping[str, Any],
    details: list[dict[str, Any]],
) -> dict[str, Any]:
    candidates = [_contract_from_detail(row) for row in details]
    candidates = [row for row in candidates if row.get("symbol") == symbol]
    target_expiry = str(selected_target.get("expiry") or qualified_contract.get("expiry") or "").strip()
    target_local = str(selected_target.get("local_symbol") or qualified_contract.get("local_symbol") or "").strip().upper()
    target_con_id = _int_or_none(selected_target.get("con_id"))
    if target_con_id is None:
        target_con_id = _int_or_none(qualified_contract.get("con_id"))

    matches = []
    for candidate in candidates:
        if target_con_id is not None and _int_or_none(candidate.get("con_id")) != target_con_id:
            continue
        if target_local and str(candidate.get("local_symbol") or "").strip().upper() != target_local:
            continue
        expiry = str(candidate.get("expiry") or "").strip()
        if target_expiry:
            if len(target_expiry) == 6 and not expiry.startswith(target_expiry):
                continue
            if len(target_expiry) != 6 and expiry != target_expiry:
                continue
        elif contract_month and not expiry.startswith(contract_month):
            continue
        matches.append(candidate)
    if not matches and qualified_contract:
        qualified_con_id = _int_or_none(qualified_contract.get("con_id"))
        qualified_local = str(qualified_contract.get("local_symbol") or "").strip().upper()
        if target_con_id is not None and qualified_con_id is not None and target_con_id != qualified_con_id:
            return {
                "resolved": False,
                "match_count": 0,
                "detail": "Selected target conId does not match the fresh IBKR qualified contract.",
            }
        if target_local and qualified_local and target_local != qualified_local:
            return {
                "resolved": False,
                "match_count": 0,
                "detail": "Selected target localSymbol does not match the fresh IBKR qualified contract.",
            }
        fallback = _contract_from_detail(
            {
                "symbol": qualified_contract.get("broker_symbol") or qualified_contract.get("symbol") or qualified_contract.get("internal_symbol"),
                "expiry": qualified_contract.get("expiry"),
                "con_id": qualified_contract.get("con_id"),
                "local_symbol": qualified_contract.get("local_symbol"),
                "exchange": qualified_contract.get("exchange"),
                "currency": qualified_contract.get("currency"),
                "multiplier": qualified_contract.get("multiplier"),
                "trading_class": qualified_contract.get("trading_class"),
            }
        )
        if fallback.get("symbol") == symbol:
            matches = [fallback]
    if len(matches) != 1:
        return {
            "resolved": False,
            "match_count": len(matches),
            "detail": f"Expected exactly one selected contractDetails match for {symbol} {contract_month}; found {len(matches)}.",
        }
    selected = matches[0]
    return {
        **selected,
        "resolved": True,
        "contract_month": _contract_month_from_expiry(selected.get("expiry")) or contract_month,
        "match_count": 1,
    }


def _contract_from_detail(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol": _normalize_symbol(row.get("symbol") or row.get("broker_symbol") or row.get("internal_symbol")),
        "expiry": str(row.get("expiry") or row.get("lastTradeDateOrContractMonth") or "").strip(),
        "con_id": _int_or_none(row.get("con_id") if "con_id" in row else row.get("conId")),
        "local_symbol": str(row.get("local_symbol") or row.get("localSymbol") or "").strip(),
        "exchange": str(row.get("exchange") or "").strip(),
        "currency": str(row.get("currency") or "").strip(),
        "multiplier": str(row.get("multiplier") or "").strip(),
        "trading_class": str(row.get("trading_class") or row.get("tradingClass") or "").strip(),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }


def _gold_recommendation(
    *,
    symbol: str,
    recommendation_report: Mapping[str, Any] | None,
    now: datetime,
    max_age_seconds: float,
) -> dict[str, Any]:
    desired = {
        "symbol": symbol,
        "contract_month": _GOLD_RECOMMENDATION_MONTH,
        "local_symbol": recommended_gold_local_symbol(symbol),
        "confirmed": False,
    }
    if not recommendation_report:
        return {**desired, "detail": "August 2026 recommendation has not been contractDetails-confirmed."}
    details = _details_from_report(recommendation_report)
    freshness = _contract_details_freshness(details=details, now=now, max_age_seconds=max_age_seconds)
    matches = [
        _contract_from_detail(row)
        for row in details
        if _normalize_symbol(row.get("symbol")) == symbol
        and str(row.get("expiry") or "").startswith(_GOLD_RECOMMENDATION_MONTH)
    ]
    if len(matches) != 1 or not bool(freshness.get("fresh")):
        return {
            **desired,
            "contract_details_freshness": freshness,
            "match_count": len(matches),
            "detail": "August 2026 recommendation could not be confirmed uniquely with fresh IBKR contractDetails.",
        }
    match = matches[0]
    return {
        **desired,
        **match,
        "contract_month": _GOLD_RECOMMENDATION_MONTH,
        "confirmed": True,
        "contract_details_freshness": freshness,
        "detail": "August 2026 recommendation confirmed by fresh IBKR contractDetails.",
    }


def _roll_warning_detected(warnings: tuple[str, ...]) -> bool:
    needles = ("roll", "expiry", "expire", "warning", "august", "202608", "gcq", "mgcq")
    return any(any(needle in warning.lower() for needle in needles) for warning in warnings)


def _parse_contract_expiry(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def _contract_month_from_expiry(value: object) -> str | None:
    text = str(value or "").strip()
    if len(text) >= 6 and text[:6].isdigit():
        return text[:6]
    return None


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _int_or_none(value: object) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _local_symbol_for_month(symbol: str, contract_month: str) -> str:
    month = contract_month[4:6]
    year_digit = contract_month[3]
    return f"{symbol}{_MONTH_CODE_BY_NUMBER.get(month, '')}{year_digit}"
