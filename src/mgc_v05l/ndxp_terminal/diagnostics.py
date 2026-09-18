"""Classify terminal, worker, broker, and market-feed health."""

from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


def classify_diagnostics(
    *,
    market: dict[str, Any] | None,
    broker: dict[str, Any] | None,
    market_error: str | None,
    broker_error: str | None,
    worker_gap_ms: float | None,
    client_gap_ms: float | None,
    source_age_ms: float | None,
    market_poll_age_ms: float | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    classifications: list[str] = []
    evidence: list[str] = []
    databento = (market or {}).get("databento") if isinstance((market or {}).get("databento"), dict) else None
    market_source = str((market or {}).get("market_source") or "Schwab market data")
    measured_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    market_session = "REGULAR_OPEN" if ndxp_regular_session_open(measured_at) else "OUTSIDE_REGULAR_HOURS"

    if client_gap_ms is not None and client_gap_ms > 3500:
        classifications.append("CLIENT_OR_UI_STALL")
        evidence.append(f"Browser heartbeat gap reached {client_gap_ms:.0f} ms.")
    market_latency = _number((market or {}).get("latency_ms"))
    broker_latency = _number((broker or {}).get("latency_ms"))
    databento_error = bool(market_error and "databento" in market_error.lower())
    if databento_error:
        classifications.append("DATABENTO_STREAM_ERROR")
        evidence.append(market_error or "Databento stream failed.")
    elif databento and databento.get("last_error"):
        classifications.append("DATABENTO_STREAM_ERROR")
        evidence.append(str(databento["last_error"]))
    elif databento and databento.get("target_symbol_count") and (
        databento.get("selected_quote_count", 0) < databento.get("target_symbol_count", 0) * 0.8
    ):
        classification = "DATABENTO_QUOTES_PENDING" if not databento.get("selected_quote_count") else "DATABENTO_PARTIAL_QUOTES"
        classifications.append(classification)
        evidence.append(
            f"Databento has {databento.get('selected_quote_count', 0)} quotes for "
            f"{databento.get('target_symbol_count')} selected OPRA contracts."
        )
    if (market_error and not databento_error) or broker_error:
        classifications.append("SCHWAB_OR_NETWORK_ERROR")
        evidence.append((market_error if not databento_error else None) or broker_error or "Schwab request failed.")
    elif (market_latency is not None and market_latency > 2500) or (broker_latency is not None and broker_latency > 4000):
        classifications.append("SCHWAB_RESPONSE_DELAY")
        evidence.append(f"Market/broker response latency is {market_latency or 0:.0f}/{broker_latency or 0:.0f} ms.")
    if worker_gap_ms is not None and worker_gap_ms > 2500:
        classifications.append("APPLICATION_WORKER_STALL")
        evidence.append(f"Background poller scheduling gap reached {worker_gap_ms:.0f} ms.")
    if market_poll_age_ms is not None and market_poll_age_ms > 5000:
        classifications.append("MARKET_POLLER_STALE")
        evidence.append(f"No successful market poll for {market_poll_age_ms / 1000:.1f} seconds.")
    if source_age_ms is not None and source_age_ms > 5000:
        if market_session == "REGULAR_OPEN":
            classifications.append("STALE_MARKET_DATA")
            evidence.append(f"Latest {market_source} option quote is {source_age_ms / 1000:.1f} seconds old.")
        else:
            classifications.append("MARKET_CLOSED_LATEST_QUOTES")
            evidence.append(
                f"NDXP is outside regular hours; latest {market_source} quote is "
                f"{source_age_ms / 1000:.1f} seconds old."
            )

    if not classifications:
        classifications.append("HEALTHY")
        evidence.append("UI heartbeat, local poller, market feed, broker responses, and quote timestamps are within thresholds.")
    return {
        "classification": classifications[0],
        "all_classifications": classifications,
        "evidence": evidence,
        "measured_at": measured_at.isoformat(),
        "market_session": market_session,
        "client_gap_ms": client_gap_ms,
        "worker_gap_ms": worker_gap_ms,
        "market_latency_ms": market_latency,
        "broker_latency_ms": broker_latency,
        "market_poll_age_ms": market_poll_age_ms,
        "quote_source_age_ms": source_age_ms,
        "market_source": market_source,
        "databento": databento,
    }


def ndxp_regular_session_open(at: datetime) -> bool:
    eastern = at.astimezone(EASTERN)
    return eastern.weekday() < 5 and time(9, 30) <= eastern.time().replace(tzinfo=None) < time(16, 0)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
