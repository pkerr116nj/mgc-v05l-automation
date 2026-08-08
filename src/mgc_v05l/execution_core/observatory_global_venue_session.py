"""Read-only global venue session producer for The Observatory."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars as xcals


SCHEMA_VERSION = "observatory_global_venue_session_v1"
SOURCE_AUTHORITY = "exchange_calendars"
SOURCE_AUTHORITY_VERSION = getattr(xcals, "__version__", "unknown")
DEFAULT_OUTPUT_PATH = Path("desktop/prototypes/active-desktop/venue_session_snapshot.generated.mjs")

GUARDRAILS = {
    "display_only": True,
    "trading_input": False,
    "broker_authority": False,
    "runtime_authority": False,
}


@dataclass(frozen=True)
class ProductSessionSpec:
    product_group: str
    calendar_code: str | None
    monitored: bool
    limitation: str | None = None


@dataclass(frozen=True)
class VenueSpec:
    venue_id: str
    city: str
    display_name: str
    exchange: str
    latitude: float
    longitude: float
    calendar_code: str | None
    coverage: str
    limitation: str | None = None
    product_sessions: tuple[ProductSessionSpec, ...] = ()


VENUES: tuple[VenueSpec, ...] = (
    VenueSpec("nyse", "New York", "NYSE", "New York Stock Exchange", 40.7128, -74.0060, "XNYS", "SUPPORTED"),
    VenueSpec("nasdaq", "New York", "Nasdaq", "Nasdaq", 40.7128, -74.0060, "XNAS", "SUPPORTED_WITH_LIMITATION", "exchange_calendars aliases XNAS to XNYS trading hours."),
    VenueSpec("cboe", "New York", "Cboe", "Cboe", 40.7128, -74.0060, "XCBF", "SUPPORTED_WITH_LIMITATION", "Represented by XCBF Cboe Futures calendar; not all Cboe venues."),
    VenueSpec(
        "cme",
        "Chicago",
        "CME",
        "CME Globex",
        41.8781,
        -87.6298,
        "CMES",
        "SUPPORTED",
        product_sessions=(
            ProductSessionSpec("Crypto", None, False, "No source-authoritative 24/7 crypto maintenance calendar is configured."),
            ProductSessionSpec("Equity Index", "CMES", True),
            ProductSessionSpec("Rates", "CMES", True),
            ProductSessionSpec("Metals", "CMES", True),
            ProductSessionSpec("Energy", "CMES", False),
            ProductSessionSpec("FX", "CMES", False),
            ProductSessionSpec("Single Stock Futures", None, False, "No source-authoritative single-stock-futures calendar is configured."),
        ),
    ),
    VenueSpec("cfe", "Chicago", "CFE", "Cboe Futures Exchange", 41.8781, -87.6298, "XCBF", "SUPPORTED"),
    VenueSpec("tsx", "Toronto", "TSX", "Toronto Stock Exchange", 43.6532, -79.3832, "XTSE", "SUPPORTED"),
    VenueSpec("mx", "Montreal", "MX", "Montreal Exchange", 45.5019, -73.5674, None, "UNSUPPORTED", "No Montreal Exchange calendar found."),
    VenueSpec("bmv", "Mexico City", "BMV", "Bolsa Mexicana de Valores", 19.4326, -99.1332, "XMEX", "SUPPORTED"),
    VenueSpec("b3", "Sao Paulo", "B3", "B3 / Bovespa", -23.5558, -46.6396, "BVMF", "SUPPORTED"),
    VenueSpec("lse", "London", "LSE / ICE", "London Stock Exchange", 51.5072, -0.1276, "XLON", "SUPPORTED"),
    VenueSpec("ice", "London", "ICE", "ICE Futures Europe", 51.5072, -0.1276, None, "UNSUPPORTED", "No ICE Futures Europe calendar found; XICE is not used because it represents Iceland."),
    VenueSpec("eurex", "Frankfurt", "Deutsche Borse / Eurex", "Xetra", 50.1109, 8.6821, "XETR", "SUPPORTED_WITH_LIMITATION", "Represents Xetra/Deutsche Borse cash session, not Eurex futures."),
    VenueSpec("paris", "Paris", "Euronext Paris", "Euronext Paris", 48.8566, 2.3522, "XPAR", "SUPPORTED"),
    VenueSpec("amsterdam", "Amsterdam", "Euronext Amsterdam", "Euronext Amsterdam", 52.3676, 4.9041, "XAMS", "SUPPORTED"),
    VenueSpec("six", "Zurich", "SIX", "SIX Swiss Exchange", 47.3769, 8.5417, "XSWX", "SUPPORTED"),
    VenueSpec("jpx", "Tokyo", "JPX / TSE", "Tokyo Stock Exchange", 35.6762, 139.6503, "XTKS", "SUPPORTED"),
    VenueSpec("osaka", "Osaka", "Osaka", "Osaka Exchange", 34.6937, 135.5023, None, "UNSUPPORTED", "No Osaka Exchange calendar found; OSE alias resolves to Oslo."),
    VenueSpec("hkex", "Hong Kong", "HKEX", "Hong Kong Exchange", 22.3193, 114.1694, "XHKG", "SUPPORTED"),
    VenueSpec("sgx", "Singapore", "SGX", "Singapore Exchange", 1.3521, 103.8198, "XSES", "SUPPORTED"),
    VenueSpec("krx", "Seoul", "KRX", "Korea Exchange", 37.5665, 126.9780, "XKRX", "SUPPORTED"),
    VenueSpec("twse", "Taipei", "TWSE", "Taiwan Stock Exchange", 25.0330, 121.5654, "XTAI", "SUPPORTED"),
    VenueSpec("sse", "Shanghai", "Shanghai", "Shanghai Stock Exchange", 31.2304, 121.4737, "XSHG", "SUPPORTED"),
    VenueSpec("szse", "Shenzhen", "Shenzhen", "Shenzhen Stock Exchange", 22.5431, 114.0579, None, "UNSUPPORTED", "No Shenzhen calendar found."),
    VenueSpec("nse", "Mumbai", "NSE / BSE", "BSE Mumbai", 19.0760, 72.8777, "XBOM", "SUPPORTED_WITH_LIMITATION", "Represents BSE; NSE calendar unavailable."),
    VenueSpec("asx", "Sydney", "ASX", "Australian Securities Exchange", -33.8688, 151.2093, "XASX", "SUPPORTED"),
    VenueSpec("nzx", "Auckland", "NZX", "New Zealand Exchange", -36.8509, 174.7645, "XNZE", "SUPPORTED"),
)


def parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if str(value) == "NaT":
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if str(value) == "NaT":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return None


def _timezone_name(calendar: Any) -> str:
    tz = getattr(calendar, "tz", None)
    return getattr(tz, "key", None) or str(tz)


def _schedule_rows(calendar: Any, now: datetime):
    start = (now - timedelta(days=3)).date().isoformat()
    end = (now + timedelta(days=10)).date().isoformat()
    return calendar.schedule.loc[start:end]


def _mode_close_time(schedule: Any, timezone_name: str) -> Any | None:
    close_times = []
    zone = ZoneInfo(timezone_name)
    for close in schedule["close"].dropna():
        if hasattr(close, "to_pydatetime"):
            close = close.to_pydatetime()
        close_times.append(close.astimezone(zone).time().replace(tzinfo=None))
    if not close_times:
        return None
    return Counter(close_times).most_common(1)[0][0]


def _next_transition(now: datetime, rows: Any) -> str | None:
    candidates: list[datetime] = []
    for _, row in rows.iterrows():
        for column in ("open", "break_start", "break_end", "close"):
            value = row.get(column)
            iso_value = _iso(value)
            if not iso_value:
                continue
            parsed = parse_timestamp(iso_value)
            if parsed > now:
                candidates.append(parsed)
    return min(candidates).isoformat() if candidates else None


def _status_for_supported(spec: VenueSpec, now: datetime) -> dict[str, Any]:
    calendar = xcals.get_calendar(spec.calendar_code)
    timezone_name = _timezone_name(calendar)
    local_now = now.astimezone(ZoneInfo(timezone_name))
    rows = _schedule_rows(calendar, now)
    active_row = None
    current_day_row = None
    for session_label, row in rows.iterrows():
        open_at = parse_timestamp(_iso(row["open"]))
        close_at = parse_timestamp(_iso(row["close"]))
        if open_at <= now < close_at:
            active_row = row
            break_start = parse_timestamp(_iso(row.get("break_start"))) if _iso(row.get("break_start")) else None
            break_end = parse_timestamp(_iso(row.get("break_end"))) if _iso(row.get("break_end")) else None
            if break_start and break_end and break_start <= now < break_end:
                status = "LUNCH_BREAK"
            elif close_at - now <= timedelta(minutes=30):
                status = "CLOSING_SOON"
            else:
                status = "OPEN"
            break
        if str(session_label.date()) == local_now.date().isoformat():
            current_day_row = row

    if active_row is None:
        if current_day_row is not None:
            open_at = parse_timestamp(_iso(current_day_row["open"]))
            close_at = parse_timestamp(_iso(current_day_row["close"]))
            status = "PREOPEN" if now < open_at else "CLOSED"
            active_row = current_day_row
        elif local_now.weekday() < 5:
            status = "HOLIDAY"
        else:
            status = "CLOSED"

    mode_close = _mode_close_time(rows, timezone_name)
    close_time = None
    if active_row is not None:
        close_iso = _iso(active_row["close"])
        close_time = parse_timestamp(close_iso).astimezone(ZoneInfo(timezone_name)).time().replace(tzinfo=None) if close_iso else None
    special_session = "EARLY_CLOSE" if close_time and mode_close and close_time < mode_close else None

    return {
        "timezone": timezone_name,
        "local_time": local_now.isoformat(),
        "calendar_date": local_now.date().isoformat(),
        "session_status": status,
        "next_transition_at": _next_transition(now, rows),
        "holiday": status == "HOLIDAY",
        "special_session": special_session,
        "calendar_source": spec.calendar_code,
    }


def venue_record(spec: VenueSpec, generated_at: datetime) -> dict[str, Any]:
    base = {
        "venue_id": spec.venue_id,
        "city": spec.city,
        "display_name": spec.display_name,
        "exchange": spec.exchange,
        "latitude": spec.latitude,
        "longitude": spec.longitude,
        "generated_at": generated_at.isoformat(),
        "freshness": {
            "generated_at": generated_at.isoformat(),
            "stale_after": (generated_at + timedelta(minutes=5)).isoformat(),
        },
        "source_authority": SOURCE_AUTHORITY,
        "source_authority_version": SOURCE_AUTHORITY_VERSION,
        "coverage": spec.coverage,
        "limitation": spec.limitation,
        "guardrails": dict(GUARDRAILS),
    }
    product_sessions = _product_session_records(spec, generated_at)
    if not spec.calendar_code:
        return {
            **base,
            "timezone": None,
            "local_time": None,
            "calendar_date": None,
            "session_status": "UNKNOWN",
            "next_transition_at": None,
            "holiday": None,
            "special_session": None,
            "calendar_source": None,
            "venue_operational_state": "UNKNOWN",
            "product_session_states": product_sessions,
            "session_aggregation_rule": "UNKNOWN because no venue-level calendar source is configured.",
        }
    try:
        venue_status = _status_for_supported(spec, generated_at)
        if product_sessions:
            venue_status["session_status"] = _aggregate_product_session_status(product_sessions)
            venue_status["session_aggregation_rule"] = (
                "Ambient state is GREEN/OPEN if any monitored, source-supported CME product family is tradable; "
                "RED/CLOSED if all monitored supported families are closed; GRAY/UNKNOWN if no supported product-family truth exists."
            )
        else:
            venue_status["session_aggregation_rule"] = "Venue-level exchange calendar status."
        return {
            **base,
            **venue_status,
            "venue_operational_state": "UNKNOWN",
            "product_session_states": product_sessions,
        }
    except Exception as exc:
        return {
            **base,
            "timezone": None,
            "local_time": None,
            "calendar_date": None,
            "session_status": "UNKNOWN",
            "next_transition_at": None,
            "holiday": None,
            "special_session": None,
            "calendar_source": spec.calendar_code,
            "venue_operational_state": "UNKNOWN",
            "product_session_states": product_sessions,
            "session_aggregation_rule": "UNKNOWN because calendar query failed.",
            "limitation": f"calendar_query_failed: {type(exc).__name__}: {exc}",
        }


def _product_session_records(spec: VenueSpec, generated_at: datetime) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for product in spec.product_sessions:
        if not product.calendar_code:
            records.append(
                {
                    "product_group": product.product_group,
                    "monitored": product.monitored,
                    "product_session_state": "UNKNOWN",
                    "calendar_source": None,
                    "source_limitation": product.limitation or "No source-authoritative product calendar is configured.",
                }
            )
            continue
        try:
            status = _status_for_supported(
                VenueSpec(
                    venue_id=f"{spec.venue_id}.{product.product_group.lower().replace(' ', '_')}",
                    city=spec.city,
                    display_name=f"{spec.display_name} {product.product_group}",
                    exchange=spec.exchange,
                    latitude=spec.latitude,
                    longitude=spec.longitude,
                    calendar_code=product.calendar_code,
                    coverage=spec.coverage,
                ),
                generated_at,
            )
            records.append(
                {
                    "product_group": product.product_group,
                    "monitored": product.monitored,
                    "product_session_state": status["session_status"],
                    "calendar_source": product.calendar_code,
                    "next_transition_at": status["next_transition_at"],
                    "source_limitation": product.limitation,
                }
            )
        except Exception as exc:
            records.append(
                {
                    "product_group": product.product_group,
                    "monitored": product.monitored,
                    "product_session_state": "UNKNOWN",
                    "calendar_source": product.calendar_code,
                    "source_limitation": f"calendar_query_failed: {type(exc).__name__}: {exc}",
                }
            )
    return records


def _aggregate_product_session_status(product_sessions: list[dict[str, Any]]) -> str:
    monitored = [row for row in product_sessions if row.get("monitored") is True and row.get("calendar_source")]
    if not monitored:
        return "UNKNOWN"
    statuses = {str(row.get("product_session_state") or "UNKNOWN") for row in monitored}
    if statuses & {"OPEN", "AUCTION", "CLOSING_SOON"}:
        return "OPEN"
    if statuses <= {"CLOSED", "HOLIDAY", "PREOPEN", "LUNCH_BREAK"}:
        return "CLOSED"
    return "UNKNOWN"


def build_snapshot(generated_at: datetime | None = None) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    records = [venue_record(spec, generated) for spec in VENUES]
    supported = sum(1 for item in records if item["coverage"] == "SUPPORTED")
    limited = sum(1 for item in records if item["coverage"] == "SUPPORTED_WITH_LIMITATION")
    unsupported = sum(1 for item in records if item["coverage"] == "UNSUPPORTED")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "source_authority": SOURCE_AUTHORITY,
        "source_authority_version": SOURCE_AUTHORITY_VERSION,
        "model_status": "VALID_WITH_WARNINGS" if unsupported or limited else "READY",
        "venues": records,
        "coverage_summary": {
            "supported": supported,
            "supported_with_limitation": limited,
            "unsupported": unsupported,
            "total": len(records),
        },
        "guardrails": dict(GUARDRAILS),
    }


def write_snapshot(snapshot: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix == ".json":
        output_path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
        return
    output_path.write_text(
        f"export const venueSessionSnapshot = {json.dumps(snapshot, indent=2)};\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Observatory global venue/session display snapshot.")
    parser.add_argument("--generated-at", default=None)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    args = parser.parse_args(argv)
    snapshot = build_snapshot(parse_timestamp(args.generated_at))
    write_snapshot(snapshot, Path(args.output))
    print(json.dumps({
        "ok": True,
        "output": args.output,
        "venues": len(snapshot["venues"]),
        "coverage_summary": snapshot["coverage_summary"],
    }, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
