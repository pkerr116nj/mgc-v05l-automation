from mgc_v05l.research.ndxp_expansion_fast_cost_estimate import (
    MAX_CALLS,
    build_requests,
    quote_requests,
    representative_sessions,
    summarize,
)


def test_representative_sessions_deterministic():
    a = representative_sessions("2023-03-28", "2024-12-31", 4)
    b = representative_sessions("2023-03-28", "2024-12-31", 4)
    assert a == b
    assert len(a) == 4
    assert len(set(a)) == 4


def test_request_plan_is_eight_calls_max():
    dates = representative_sessions("2023-03-28", "2024-12-31", 4)
    reqs = build_requests(dates)
    assert len(reqs) == 8
    assert len(reqs) <= MAX_CALLS
    assert {r.window for r in reqs} == {"opening_30m", "full_regular_session"}
    assert all(r.params()["stype_in"] == "parent" for r in reqs)
    assert all(r.params()["symbols"] == ["NDXP.OPT"] for r in reqs)


def test_quote_uses_one_call_per_request_only():
    dates = representative_sessions("2023-03-28", "2024-12-31", 2)
    reqs = build_requests(dates)
    seen = []

    def fake(method, params):
        seen.append((method, params))
        return 1.25 if params["end"] != params["start"] else 0.0

    rows = quote_requests(reqs, fake, workers=2)
    assert len(seen) == len(reqs)
    assert len(rows) == len(reqs)
    assert all(m == "get_cost" for m, _ in seen)


def test_summary_projects_without_downloads():
    rows = [
        {"session": "2024-01-02", "window": "opening_30m", "cost_usd": 1.0},
        {"session": "2024-02-01", "window": "opening_30m", "cost_usd": 3.0},
        {"session": "2024-01-02", "window": "full_regular_session", "cost_usd": 5.0},
        {"session": "2024-02-01", "window": "full_regular_session", "cost_usd": 9.0},
    ]
    out = summarize(rows, "2024-01-02", "2024-02-29")
    assert out["download_calls"] == 0
    assert out["metadata_calls"] == 4
    assert out["windows"]["opening_30m"]["per_session_median_usd"] == 2.0
    assert out["windows"]["full_regular_session"]["per_session_median_usd"] == 7.0
    assert out["derived"]["projected_rest_of_day_median_usd"] >= 0
