from mgc_v05l.research.ndxp_expansion_fast_cost_estimate import (
    MAX_CALLS, build_requests, quote_requests, representative_sessions, summarize,
)


def test_representative_sessions_deterministic():
    a = representative_sessions("2023-03-28", "2024-12-31", 4)
    assert a == representative_sessions("2023-03-28", "2024-12-31", 4)
    assert len(a) == len(set(a)) == 4


def test_request_plan_is_twelve_short_calls_max():
    reqs = build_requests(representative_sessions("2023-03-28", "2024-12-31", 4))
    assert len(reqs) == 12
    assert len(reqs) <= MAX_CALLS
    assert {r.window for r in reqs} == {"opening_2m", "midday_2m", "closing_2m"}
    assert all(r.params()["symbols"] == ["NDXP.OPT"] for r in reqs)
    assert all((__import__("pandas").Timestamp(r.end) - __import__("pandas").Timestamp(r.start)).total_seconds() <= 120 for r in reqs)


def test_quote_one_call_per_request():
    reqs = build_requests(representative_sessions("2023-03-28", "2024-12-31", 2))
    seen = []
    def fake(method, params):
        seen.append((method, params))
        return 0.10
    rows = quote_requests(reqs, fake, workers=2)
    assert len(seen) == len(reqs) == len(rows)
    assert all(method == "get_cost" for method, _ in seen)


def test_summary_projects_without_downloads():
    rows = [
        {"cost_usd": .10}, {"cost_usd": .20}, {"cost_usd": .30}, {"cost_usd": .40},
    ]
    out = summarize(rows, "2024-01-02", "2024-02-29")
    assert out["download_calls"] == 0
    assert out["metadata_calls"] == 4
    assert out["per_minute_parent_cost_usd"]["median"] == .125
    assert out["projected"]["full_regular_session_total_median_usd"] > 0
