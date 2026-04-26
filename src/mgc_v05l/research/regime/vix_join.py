"""As-of VIX regime joins for futures decision timestamps."""

from __future__ import annotations

from bisect import bisect_right
from datetime import datetime
from typing import Any, Sequence


def build_vix_asof_lookup(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: _coerce_ts(row["vix_asof_ts"]))
    return {
        "timestamps": [_coerce_ts(row["vix_asof_ts"]) for row in ordered],
        "rows": ordered,
    }


def lookup_vix_asof(lookup: dict[str, Any], decision_ts: datetime) -> dict[str, Any] | None:
    timestamps = lookup["timestamps"]
    index = bisect_right(timestamps, decision_ts) - 1
    if index < 0:
        return None
    return dict(lookup["rows"][index])


def attach_vix_asof(
    rows: Sequence[dict[str, Any]],
    *,
    vix_rows: Sequence[dict[str, Any]],
    decision_ts_key: str = "decision_ts",
) -> list[dict[str, Any]]:
    lookup = build_vix_asof_lookup(vix_rows)
    payload: list[dict[str, Any]] = []
    for row in rows:
        matched = lookup_vix_asof(lookup, _coerce_ts(row[decision_ts_key]))
        if matched is None:
            payload.append(
                {
                    **row,
                    "vix_trade_date": None,
                    "vix_asof_ts": None,
                    "vix_close": None,
                    "vix_change_abs": None,
                    "vix_change_pct": None,
                    "vix_level_bucket": "UNKNOWN",
                    "vix_change_bucket": "UNKNOWN",
                    "vix_combined_bucket": "UNKNOWN_UNKNOWN",
                }
            )
            continue
        payload.append({**row, **matched})
    return payload


def _coerce_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

