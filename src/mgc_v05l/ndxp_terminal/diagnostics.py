"""Classify terminal, worker, Schwab response, and quote-timestamp health."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


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
) -> dict[str, Any]:
    classifications: list[str] = []
    evidence: list[str] = []

    if client_gap_ms is not None and client_gap_ms > 3500:
        classifications.append("CLIENT_OR_UI_STALL")
        evidence.append(f"Browser heartbeat gap reached {client_gap_ms:.0f} ms.")
    market_latency = _number((market or {}).get("latency_ms"))
    broker_latency = _number((broker or {}).get("latency_ms"))
    if market_error or broker_error:
        classifications.append("SCHWAB_OR_NETWORK_ERROR")
        evidence.append(market_error or broker_error or "Schwab request failed.")
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
        classifications.append("STALE_MARKET_DATA")
        evidence.append(f"Latest Schwab quote timestamp is {source_age_ms / 1000:.1f} seconds old.")

    if not classifications:
        classifications.append("HEALTHY")
        evidence.append("UI heartbeat, local poller, Schwab responses, and quote timestamps are within thresholds.")
    return {
        "classification": classifications[0],
        "all_classifications": classifications,
        "evidence": evidence,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "client_gap_ms": client_gap_ms,
        "worker_gap_ms": worker_gap_ms,
        "market_latency_ms": market_latency,
        "broker_latency_ms": broker_latency,
        "market_poll_age_ms": market_poll_age_ms,
        "quote_source_age_ms": source_age_ms,
    }


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
