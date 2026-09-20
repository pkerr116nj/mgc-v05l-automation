"""Naive 0DTE NDXP put-credit-spread hold-to-settlement research.

The module intentionally separates market-data normalization from strategy
evaluation.  It consumes timestamped NBBO rows plus official XQC settlement
values and never substitutes an NDX print or futures price for settlement.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_OFFSETS = (-50.0, -25.0, 0.0, 25.0, 50.0)
DEFAULT_CREDIT_TARGETS = (2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)


@dataclass(frozen=True)
class OptionQuote:
    quote_time: datetime
    expiration: date
    option_type: str
    strike: float
    bid: float
    ask: float
    symbol: str = ""
    spot: float | None = None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(frozen=True)
class SpreadCandidate:
    session_date: date
    entry_time: datetime
    expiration: date
    short_strike: float
    long_strike: float
    width: float
    reference_level: float
    short_offset: float
    short_bid: float
    short_ask: float
    long_bid: float
    long_ask: float
    mid_credit: float
    natural_credit: float
    quote_skew_seconds: float


@dataclass(frozen=True)
class TradeOutcome:
    session_date: date
    selector: str
    selector_value: float
    fill_haircut: float
    entry_time: datetime
    reference_level: float
    short_strike: float
    long_strike: float
    short_offset: float
    entry_credit: float
    settlement: float
    expiration_debit: float
    max_profit: float
    max_loss: float
    net_pnl: float
    return_on_max_risk: float
    full_loss: bool


def _number(value: object, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {name}: {value!r}") from exc
    if not math.isfinite(result):
        raise ValueError(f"invalid {name}: {value!r}")
    return result


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=NEW_YORK)
    return parsed.astimezone(NEW_YORK)


def load_quotes_csv(path: Path) -> list[OptionQuote]:
    """Load normalized quote events.

    Required columns: quote_time, expiration, option_type, strike, bid, ask.
    Optional columns: symbol, spot.  Times without offsets are interpreted as ET.
    """

    quotes: list[OptionQuote] = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                bid = _number(row.get("bid"), "bid")
                ask = _number(row.get("ask"), "ask")
                if bid < 0 or ask <= 0 or bid > ask:
                    continue
                option_type = str(row.get("option_type", "")).strip().upper()
                if option_type not in {"C", "P", "CALL", "PUT"}:
                    raise ValueError(f"invalid option_type: {option_type!r}")
                spot_raw = row.get("spot")
                quotes.append(
                    OptionQuote(
                        quote_time=_parse_time(str(row["quote_time"])),
                        expiration=date.fromisoformat(str(row["expiration"])[:10]),
                        option_type=option_type[0],
                        strike=_number(row.get("strike"), "strike"),
                        bid=bid,
                        ask=ask,
                        symbol=str(row.get("symbol", "")).strip(),
                        spot=_number(spot_raw, "spot") if spot_raw not in (None, "") else None,
                    )
                )
            except Exception as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc
    return quotes


def load_settlements_csv(path: Path) -> dict[date, float]:
    """Load official XQC values from columns date and settlement."""

    values: dict[date, float] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                session = date.fromisoformat(str(row["date"])[:10])
                settlement = _number(row.get("settlement"), "settlement")
            except Exception as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc
            if session in values and values[session] != settlement:
                raise ValueError(f"{path}:{line_number}: conflicting settlement for {session}")
            values[session] = settlement
    return values


def _opening_reference(
    quotes: Iterable[OptionQuote],
    *,
    as_of: datetime,
    max_age_seconds: float,
) -> float | None:
    recent = [
        quote
        for quote in quotes
        if 0 <= (as_of - quote.quote_time).total_seconds() <= max_age_seconds
    ]
    spots = [quote.spot for quote in recent if quote.spot is not None and quote.spot > 0]
    if spots:
        return statistics.median(spots)

    by_contract: dict[tuple[str, float], OptionQuote] = {}
    for quote in recent:
        key = (quote.option_type, quote.strike)
        current = by_contract.get(key)
        if current is None or quote.quote_time > current.quote_time:
            by_contract[key] = quote
    parity_levels = []
    strikes = {strike for option_type, strike in by_contract if option_type == "P"}
    for strike in strikes:
        call = by_contract.get(("C", strike))
        put = by_contract.get(("P", strike))
        if call is None or put is None:
            continue
        if abs((call.quote_time - put.quote_time).total_seconds()) > 2.0:
            continue
        # With less than one trading day remaining, the forward/spot financing
        # difference is immaterial for the requested 25-point analysis buckets.
        parity_levels.append(strike + call.mid - put.mid)
    # A single wide or stale call-put pair is too fragile to classify the
    # entire opening chain.  Three distinct strikes give a minimal robust
    # median while preserving the first-seconds entry intent.
    return statistics.median(parity_levels) if len(parity_levels) >= 3 else None


def build_opening_candidates(
    quotes: Iterable[OptionQuote],
    *,
    width: float = 10.0,
    window_start: time = time(9, 30),
    window_seconds: int = 60,
    max_leg_skew_seconds: float = 2.0,
) -> tuple[list[SpreadCandidate], dict[str, str]]:
    """Build the first valid two-leg quote for every 0DTE put spread."""

    grouped: dict[date, list[OptionQuote]] = defaultdict(list)
    for quote in quotes:
        session = quote.quote_time.date()
        if quote.expiration != session:
            continue
        start = datetime.combine(session, window_start, NEW_YORK)
        if start <= quote.quote_time < start + timedelta(seconds=window_seconds):
            grouped[session].append(quote)

    candidates: list[SpreadCandidate] = []
    exclusions: dict[str, str] = {}
    for session, session_quotes in sorted(grouped.items()):
        events = sorted(session_quotes, key=lambda quote: quote.quote_time)
        latest: dict[tuple[str, float], OptionQuote] = {}
        recorded: set[float] = set()
        reference_seen = False
        for quote in events:
            latest[(quote.option_type, quote.strike)] = quote
            reference = _opening_reference(
                latest.values(),
                as_of=quote.quote_time,
                max_age_seconds=max_leg_skew_seconds,
            )
            if reference is None:
                continue
            reference_seen = True
            put_strikes = {strike for option_type, strike in latest if option_type == "P"}
            # Reconsider every outstanding pair when a call update first makes
            # the causal parity reference available.
            for short_strike in sorted(put_strikes):
                long_strike = short_strike - width
                short_key = ("P", short_strike)
                long_key = ("P", long_strike)
                if short_strike in recorded or long_key not in latest:
                    continue
                short = latest[short_key]
                long = latest[long_key]
                skew = abs((short.quote_time - long.quote_time).total_seconds())
                oldest_age = (quote.quote_time - min(short.quote_time, long.quote_time)).total_seconds()
                if skew > max_leg_skew_seconds or oldest_age > max_leg_skew_seconds:
                    continue
                mid_credit = short.mid - long.mid
                natural_credit = short.bid - long.ask
                if not (0 < mid_credit < width) or natural_credit >= width:
                    continue
                recorded.add(short_strike)
                candidates.append(
                    SpreadCandidate(
                        session_date=session,
                        entry_time=max(short.quote_time, long.quote_time),
                        expiration=session,
                        short_strike=short_strike,
                        long_strike=long_strike,
                        width=width,
                        reference_level=reference,
                        short_offset=short_strike - reference,
                        short_bid=short.bid,
                        short_ask=short.ask,
                        long_bid=long.bid,
                        long_ask=long.ask,
                        mid_credit=mid_credit,
                        natural_credit=natural_credit,
                        quote_skew_seconds=skew,
                    )
                )
        if not any(candidate.session_date == session for candidate in candidates):
            exclusions[session.isoformat()] = (
                "no synchronized valid 10-point put spread"
                if reference_seen
                else "no causal spot or three-strike call-put parity reference"
            )
    return candidates, exclusions


def _choose(candidates: Sequence[SpreadCandidate], attribute: str, target: float) -> SpreadCandidate:
    return min(candidates, key=lambda candidate: (abs(getattr(candidate, attribute) - target), candidate.entry_time))


def evaluate_baselines(
    candidates: Iterable[SpreadCandidate],
    settlements: dict[date, float],
    *,
    offsets: Sequence[float] = DEFAULT_OFFSETS,
    credit_targets: Sequence[float] = DEFAULT_CREDIT_TARGETS,
    fill_haircuts: Sequence[float] = (0.0, 0.25, 0.50),
    fee_per_spread: float = 1.324,
) -> tuple[list[TradeOutcome], dict[str, str]]:
    grouped: dict[date, list[SpreadCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.session_date].append(candidate)
    outcomes: list[TradeOutcome] = []
    exclusions: dict[str, str] = {}
    for session, session_candidates in sorted(grouped.items()):
        settlement = settlements.get(session)
        if settlement is None:
            exclusions[session.isoformat()] = "missing official XQC settlement"
            continue
        selections: list[tuple[str, float, SpreadCandidate]] = []
        selections.extend(("offset", target, _choose(session_candidates, "short_offset", target)) for target in offsets)
        selections.extend(("credit", target, _choose(session_candidates, "mid_credit", target)) for target in credit_targets)
        for selector, value, candidate in selections:
            for haircut in fill_haircuts:
                credit = candidate.mid_credit - haircut
                if credit <= 0 or credit >= candidate.width:
                    continue
                expiration_debit = min(candidate.width, max(candidate.short_strike - settlement, 0.0))
                max_profit = credit * 100.0 - fee_per_spread
                max_loss = (candidate.width - credit) * 100.0 + fee_per_spread
                pnl = (credit - expiration_debit) * 100.0 - fee_per_spread
                outcomes.append(
                    TradeOutcome(
                        session_date=session,
                        selector=selector,
                        selector_value=value,
                        fill_haircut=haircut,
                        entry_time=candidate.entry_time,
                        reference_level=candidate.reference_level,
                        short_strike=candidate.short_strike,
                        long_strike=candidate.long_strike,
                        short_offset=candidate.short_offset,
                        entry_credit=credit,
                        settlement=settlement,
                        expiration_debit=expiration_debit,
                        max_profit=max_profit,
                        max_loss=max_loss,
                        net_pnl=pnl,
                        return_on_max_risk=pnl / max_loss,
                        full_loss=expiration_debit >= candidate.width - 1e-9,
                    )
                )
    return outcomes, exclusions


def _max_drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def _longest_losing_streak(values: Sequence[float]) -> int:
    longest = current = 0
    for value in values:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def summarize(outcomes: Iterable[TradeOutcome], *, period: str = "ALL") -> list[dict[str, object]]:
    grouped: dict[tuple[str, float, float], list[TradeOutcome]] = defaultdict(list)
    for outcome in outcomes:
        grouped[(outcome.selector, outcome.selector_value, outcome.fill_haircut)].append(outcome)
    rows: list[dict[str, object]] = []
    for key, trades in sorted(grouped.items()):
        trades.sort(key=lambda trade: trade.session_date)
        pnls = [trade.net_pnl for trade in trades]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]
        tail_count = max(1, math.ceil(len(pnls) * 0.05))
        rows.append(
            {
                "period": period,
                "selector": key[0],
                "selector_value": key[1],
                "fill_haircut": key[2],
                "trades": len(trades),
                "win_rate": len(wins) / len(trades),
                "average_pnl": statistics.fmean(pnls),
                "median_pnl": statistics.median(pnls),
                "average_win": statistics.fmean(wins) if wins else None,
                "average_loss": statistics.fmean(losses) if losses else None,
                "profit_factor": sum(wins) / abs(sum(losses)) if losses else None,
                "total_pnl": sum(pnls),
                "max_drawdown": _max_drawdown(pnls),
                "longest_losing_streak": _longest_losing_streak(pnls),
                "full_loss_rate": sum(trade.full_loss for trade in trades) / len(trades),
                "worst_5pct_average": statistics.fmean(sorted(pnls)[:tail_count]),
                "average_entry_credit": statistics.fmean(trade.entry_credit for trade in trades),
                "average_short_offset": statistics.fmean(trade.short_offset for trade in trades),
                "average_max_loss": statistics.fmean(trade.max_loss for trade in trades),
                "average_return_on_max_risk": statistics.fmean(trade.return_on_max_risk for trade in trades),
                "average_pnl_per_1000_max_risk": statistics.fmean(trade.return_on_max_risk for trade in trades) * 1000.0,
            }
        )
    return rows


def summarize_with_years(outcomes: Sequence[TradeOutcome]) -> list[dict[str, object]]:
    rows = summarize(outcomes)
    years = sorted({outcome.session_date.year for outcome in outcomes})
    for year in years:
        rows.extend(summarize([outcome for outcome in outcomes if outcome.session_date.year == year], period=str(year)))
    return rows


def _write_csv(path: Path, rows: Sequence[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def run_analysis(quotes_path: Path, settlements_path: Path, output_dir: Path) -> dict[str, object]:
    quotes = load_quotes_csv(quotes_path)
    settlements = load_settlements_csv(settlements_path)
    candidates, quote_exclusions = build_opening_candidates(quotes)
    outcomes, settlement_exclusions = evaluate_baselines(candidates, settlements)
    summary = summarize_with_years(outcomes)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "candidates.csv", [asdict(row) for row in candidates])
    _write_csv(output_dir / "outcomes.csv", [asdict(row) for row in outcomes])
    _write_csv(output_dir / "summary.csv", summary)
    report = {
        "method": {
            "entry_window": "09:30:00-09:31:00 America/New_York",
            "spread_width": 10.0,
            "exit": "official XQC settlement",
            "profit_target": None,
            "stop_loss": None,
            "quantity": 1,
            "offsets": list(DEFAULT_OFFSETS),
            "credit_targets": list(DEFAULT_CREDIT_TARGETS),
            "fill_haircuts": [0.0, 0.25, 0.50],
            "fee_per_spread": 1.324,
        },
        "counts": {"quotes": len(quotes), "candidates": len(candidates), "outcomes": len(outcomes)},
        "exclusions": {**quote_exclusions, **settlement_exclusions},
        "summary": summary,
    }
    (output_dir / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quotes", type=Path, required=True, help="Normalized opening NBBO CSV")
    parser.add_argument("--settlements", type=Path, required=True, help="Official XQC settlement CSV")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    report = run_analysis(args.quotes, args.settlements, args.output_dir)
    print(json.dumps(report["counts"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
