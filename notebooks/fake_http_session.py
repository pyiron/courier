from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class FakeRequest:
    method: str


class FakeResponse:
    def __init__(
        self,
        *,
        url: str = "https://example.test/api",
        status_code: int = 200,
        text: str = "ok",
        request: FakeRequest | None = None,
        raise_for_status_exc: Exception | None = None,
    ):
        self.url = url
        self.status_code = status_code
        self.text = text
        self.request = request
        self._raise_for_status_exc = raise_for_status_exc

    def raise_for_status(self) -> None:
        if self._raise_for_status_exc is not None:
            raise self._raise_for_status_exc


RouteValue = FakeResponse | Callable[[dict[str, Any]], FakeResponse]


class FakeSession:
    """Small fake for `requests.Session` used in demonstration notebooks.

    It records `.request(...)` calls and can return canned responses either from:
    - a method+url routing table (`routes`)
    - a FIFO queue (`responses`)
    - a single default response (`response`)

    This is *only* intended to make notebooks executable without network access.
    """

    def __init__(
        self,
        *,
        response: FakeResponse | None = None,
        responses: list[FakeResponse] | None = None,
        routes: dict[tuple[str, str], RouteValue] | None = None,
    ):
        self.headers: dict[str, str] = {}
        self.calls: list[dict] = []

        self._routes = dict(routes) if routes else {}
        self._responses: list[FakeResponse] | None = (
            list(responses) if responses else None
        )
        self._response = response if response is not None else FakeResponse()

    def request(self, **kwargs):
        self.calls.append(kwargs)

        method = str(kwargs.get("method", "")).upper()
        url = str(kwargs.get("url", ""))

        if method and url and (method, url) in self._routes:
            value = self._routes[(method, url)]
            resp = value(kwargs) if callable(value) else value
            if getattr(resp, "request", None) is None:
                resp.request = FakeRequest(method=method)
            if getattr(resp, "url", None) in (None, ""):
                resp.url = url
            return resp

        if self._responses is not None:
            if not self._responses:
                raise RuntimeError("FakeSession response queue exhausted")
            resp = self._responses.pop(0)
            if getattr(resp, "request", None) is None:
                resp.request = FakeRequest(method=method or "HTTP")
            if getattr(resp, "url", None) in (None, ""):
                resp.url = url
            return resp

        resp = self._response
        if getattr(resp, "request", None) is None:
            resp.request = FakeRequest(method=method or "HTTP")
        if getattr(resp, "url", None) in (None, ""):
            resp.url = url
        return resp
