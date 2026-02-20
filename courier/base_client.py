# base class for clients supported by courier

from dataclasses import dataclass
from typing import Any

import requests

from courier.http.auth import bearer_headers
from courier.http.request import read_json, read_text
from courier.http.session import create_session
from courier.http.url import normalize_base_url


@dataclass
class BaseClient:
    """
    Base client providing shared HTTP and address normalization.

    Parameters
    ----------
    address
        Server address as host[:port] or URL including scheme.
    token
        Optional bearer token.
    default_scheme
        Scheme used if `address` does not include one.
    verify
        TLS verification passed to `requests` (True/False or path to CA bundle).
    timeout
        Request timeout in seconds (float) or (connect, read) tuple.
    session
        Optional externally managed requests session.
    """

    address: str
    token: str | None = None
    default_scheme: str = "https"
    verify: bool | str = True
    timeout: float | tuple[float, float] = 30.0
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        self.base_url = normalize_base_url(
            self.address, default_scheme=self.default_scheme
        )
        if self.session is None:
            self.session = create_session()

        # Apply auth header to the session (service may override/extend).
        self.session.headers.update(bearer_headers(self.token))

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        headers: dict[str, str] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        resp = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            data=data,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify,
            stream=stream,
        )
        return resp

    def _get_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        return read_json(self._request("GET", url, params=params))

    def _get_text(self, url: str, *, params: dict[str, Any] | None = None) -> str:
        return read_text(self._request("GET", url, params=params))

    def _post_json(
        self, url: str, *, json: Any | None = None, data: Any | None = None
    ) -> Any:
        return read_json(self._request("POST", url, json=json, data=data))

    def _post_text(
        self, url: str, *, json: Any | None = None, data: Any | None = None
    ) -> str:
        return read_text(self._request("POST", url, json=json, data=data))

    def _delete_text(self, url: str) -> str:
        return read_text(self._request("DELETE", url))
