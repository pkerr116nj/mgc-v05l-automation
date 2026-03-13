"""VWAP engine contract."""

from collections.abc import Sequence
from decimal import Decimal

from ..config_models import StrategySettings
from ..domain.models import Bar


def compute_session_vwap(history: Sequence[Bar], settings: StrategySettings) -> Decimal:
    """Compute the session-reset VWAP for the latest completed bar.

    For v1 this resets on the local session date in `SESSION_TIMEZONE`, which is
    the explicit basis supplied for the locked `SESSION_RESET` policy.
    """
    if not history:
        raise ValueError("history must include at least one finalized bar.")

    current_bar = history[-1]
    current_session_date = current_bar.end_ts.astimezone(settings.timezone_info).date()
    session_bars = [
        bar for bar in history if bar.end_ts.astimezone(settings.timezone_info).date() == current_session_date
    ]

    cumulative_volume = Decimal("0")
    cumulative_price_volume = Decimal("0")

    for bar in session_bars:
        typical_price = (bar.high + bar.low + bar.close) / Decimal("3")
        volume = Decimal(bar.volume)
        cumulative_price_volume += typical_price * volume
        cumulative_volume += volume

    if cumulative_volume == 0:
        return current_bar.close
    return cumulative_price_volume / cumulative_volume
