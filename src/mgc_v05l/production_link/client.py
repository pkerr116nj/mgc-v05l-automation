"""Broker HTTP client for production-link account and order truth.

The current implementation is Schwab-specific, but neutral aliases are exposed
at the bottom of the module so higher layers can migrate away from explicit
Schwab naming incrementally.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..market_data.schwab_auth import SchwabOAuthClient


class SchwabBrokerHttpError(RuntimeError):
    """Raised when the Schwab trader HTTP layer fails."""


@dataclass(frozen=True)
class SchwabBrokerHttpResponse:
    status_code: int
    headers: dict[str, str]
    body: Any | None
    text: str


class SchwabBrokerHttpClient:
    """Small trader client focused on account truth and manual-order flows."""

    def __init__(self, *, oauth_client: SchwabOAuthClient, base_url: str, timeout_seconds: int = 30) -> None:
        self._oauth_client = oauth_client
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds

    def list_account_numbers(self) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/accounts/accountNumbers")
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        raise SchwabBrokerHttpError("Expected list payload from Schwab account-number endpoint.")

    def list_accounts(self, *, fields: list[str] | None = None) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/accounts", query=self._fields_query(fields))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        raise SchwabBrokerHttpError("Expected list payload from Schwab accounts endpoint.")

    def get_account(self, account_hash: str, *, fields: list[str] | None = None) -> dict[str, Any]:
        payload = self._request_json("GET", f"/accounts/{account_hash}", query=self._fields_query(fields))
        if isinstance(payload, dict):
            return payload
        raise SchwabBrokerHttpError("Expected object payload from Schwab account endpoint.")

    def get_orders(
        self,
        account_hash: str,
        *,
        from_entered_time: str | None = None,
        to_entered_time: str | None = None,
        status: str | None = None,
        max_results: int | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if from_entered_time:
            query["fromEnteredTime"] = from_entered_time
        if to_entered_time:
            query["toEnteredTime"] = to_entered_time
        if status:
            query["status"] = status
        if max_results:
            query["maxResults"] = max_results
        payload = self._request_json("GET", f"/accounts/{account_hash}/orders", query=query or None)
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        raise SchwabBrokerHttpError("Expected list payload from Schwab orders endpoint.")

    def get_order_status(self, account_hash: str, broker_order_id: str) -> dict[str, Any]:
        payload = self._request_json("GET", f"/accounts/{account_hash}/orders/{broker_order_id}")
        if isinstance(payload, dict):
            return payload
        raise SchwabBrokerHttpError("Expected object payload from Schwab order-status endpoint.")

    def submit_order(self, account_hash: str, order_payload: dict[str, Any]) -> dict[str, Any]:
        response = self._request("POST", f"/accounts/{account_hash}/orders", json_body=order_payload, allow_empty=True)
        location = response.headers.get("Location") or response.headers.get("location")
        broker_order_id = location.rstrip("/").split("/")[-1] if location else None
        body = response.body if isinstance(response.body, dict) else {}
        return {
            "status_code": response.status_code,
            "location": location,
            "broker_order_id": broker_order_id,
            "body": body,
            "headers": response.headers,
        }

    def preview_order(self, account_hash: str, order_payload: dict[str, Any]) -> dict[str, Any]:
        payload = self._request_json("POST", f"/accounts/{account_hash}/previewOrder", json_body=order_payload)
        if isinstance(payload, dict):
            return payload
        raise SchwabBrokerHttpError("Expected object payload from Schwab preview-order endpoint.")

    def cancel_order(self, account_hash: str, broker_order_id: str) -> dict[str, Any]:
        response = self._request("DELETE", f"/accounts/{account_hash}/orders/{broker_order_id}", allow_empty=True)
        return {
            "status_code": response.status_code,
            "body": response.body if isinstance(response.body, dict) else {},
            "headers": response.headers,
        }

    def replace_order(self, account_hash: str, broker_order_id: str, order_payload: dict[str, Any]) -> dict[str, Any]:
        response = self._request("PUT", f"/accounts/{account_hash}/orders/{broker_order_id}", json_body=order_payload, allow_empty=True)
        return {
            "status_code": response.status_code,
            "body": response.body if isinstance(response.body, dict) else {},
            "headers": response.headers,
        }

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        response = self._request(method, path, query=query, json_body=json_body)
        if response.body is None:
            raise SchwabBrokerHttpError(f"Expected JSON payload from {path}, but the response body was empty.")
        if isinstance(response.body, (dict, list)):
            return response.body
        raise SchwabBrokerHttpError(f"Expected JSON payload from {path}, received: {response.text[:200]!r}")

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        allow_empty: bool = False,
    ) -> SchwabBrokerHttpResponse:
        url = f"{self._base_url}{path}"
        if query:
            url = f"{url}?{urlencode({key: _encode_http_value(value) for key, value in query.items()})}"

        body_bytes: bytes | None = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._oauth_client.get_access_token()}",
        }
        if json_body is not None:
            body_bytes = json.dumps(json_body, sort_keys=True).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(url=url, method=method, headers=headers, data=body_bytes)
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                raw = response.read()
                parsed_headers = dict(response.headers.items())
                text = raw.decode("utf-8") if raw else ""
                parsed_body = _parse_body(text, allow_empty=allow_empty)
                return SchwabBrokerHttpResponse(
                    status_code=int(getattr(response, "status", response.getcode())),
                    headers=parsed_headers,
                    body=parsed_body,
                    text=text,
                )
        except HTTPError as exc:
            raw = exc.read()
            text = raw.decode("utf-8", errors="replace") if raw else ""
            detail = text.strip()
            try:
                payload = json.loads(text) if text else None
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, dict):
                detail = _http_error_detail(payload) or detail
            raise SchwabBrokerHttpError(
                f"Schwab trader HTTP error {exc.code} for {method} {path}: {detail or exc.reason}"
            ) from exc
        except URLError as exc:
            raise SchwabBrokerHttpError(f"Schwab trader transport error for {method} {path}: {exc}") from exc

    def _fields_query(self, fields: list[str] | None) -> dict[str, str] | None:
        if not fields:
            return None
        return {"fields": ",".join(fields)}


def _encode_http_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _parse_body(text: str, *, allow_empty: bool) -> dict[str, Any] | list[Any] | None:
    if not text.strip():
        if allow_empty:
            return None
        raise SchwabBrokerHttpError("Expected JSON response body from Schwab trader API, but it was empty.")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SchwabBrokerHttpError(f"Expected JSON response from Schwab trader API, received: {text[:200]!r}") from exc


def _http_error_detail(payload: dict[str, Any]) -> str:
    direct = str(payload.get("message") or payload.get("error") or "").strip()
    detail_candidates: list[str] = []
    for key in ("errors", "details", "validationErrors", "fieldErrors"):
        raw = payload.get(key)
        if not isinstance(raw, list):
            continue
        for item in raw:
            if isinstance(item, dict):
                piece = str(
                    item.get("message")
                    or item.get("detail")
                    or item.get("error")
                    or item.get("reason")
                    or item.get("field")
                    or ""
                ).strip()
            else:
                piece = str(item or "").strip()
            if piece and piece not in detail_candidates:
                detail_candidates.append(piece)
    if detail_candidates:
        prefix = direct if direct and direct not in detail_candidates else ""
        return ": ".join(part for part in (prefix, "; ".join(detail_candidates)) if part)
    return direct


BrokerHttpClient = SchwabBrokerHttpClient
BrokerHttpError = SchwabBrokerHttpError
BrokerHttpResponse = SchwabBrokerHttpResponse
