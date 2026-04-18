from __future__ import annotations

from mgc_v05l.production_link.client import _http_error_detail


def test_http_error_detail_prefers_nested_validation_messages() -> None:
    payload = {
        "message": "Invalid request data",
        "errors": [
            {"field": "orderLegCollection[0].instrument.symbol", "message": "Unsupported futures symbol."},
            {"field": "session", "detail": "NORMAL is not permitted for this endpoint."},
        ],
    }

    detail = _http_error_detail(payload)

    assert detail == (
        "Invalid request data: Unsupported futures symbol.; NORMAL is not permitted for this endpoint."
    )


def test_http_error_detail_falls_back_to_direct_message() -> None:
    payload = {"error": "Invalid request data"}

    assert _http_error_detail(payload) == "Invalid request data"
