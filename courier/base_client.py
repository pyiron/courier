"""Internal HTTP client base class.

`BaseClient` is intended as a parent class for service-specific courier clients
(e.g. Ontodocker). It centralizes a small set of shared concerns:

- normalization of the user-provided server address into a base URL
- creation/configuration of a `requests.Session`
- convenience helpers for common request/response patterns

This class is not meant to be instantiated directly by end users; public user
APIs should be exposed through service clients built on top of this base.

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

from typing import Any

import requests

from courier.transport.auth import bearer_headers
from courier.transport.request import read_json, read_text
from courier.transport.session import create_session
from courier.transport.url import normalize_base_url


class BaseClient:
    """Internal base class for courier service clients."""

    def __init__(
        self,
        address: str,
        *,
        token: str | None = None,
        default_scheme: str = "https",
        verify: bool | str = True,
        timeout: float | tuple[float, float] = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        # NOTE: This class is intended for use by courier-internal service clients.
        # Store state privately and expose via properties.
        self._address = address
        self._default_scheme = default_scheme
        self._verify = verify
        self._timeout = timeout

        self._base_url = normalize_base_url(
            self._address, default_scheme=self._default_scheme
        )

        self._session = session if session is not None else create_session()

        # token is mutable; use the setter to keep session headers in sync
        self._token: str | None = None
        self.token = token

    @property
    def address(self) -> str:
        return self._address

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def default_scheme(self) -> str:
        return self._default_scheme

    @property
    def session(self) -> requests.Session:
        return self._session

    @property
    def token(self) -> str | None:
        return self._token

    @token.setter
    def token(self, token: str | None) -> None:
        self._token = token

        # Keep the session's Authorization header in sync with the current token.
        # bearer_headers() returns an empty dict if token is None/blank.
        self._session.headers.pop("Authorization", None)
        self._session.headers.update(bearer_headers(self._token))

    @property
    def verify(self) -> bool | str:
        return self._verify

    @verify.setter
    def verify(self, verify: bool | str) -> None:
        self._verify = verify

    @property
    def timeout(self) -> float | tuple[float, float]:
        return self._timeout

    @timeout.setter
    def timeout(self, timeout: float | tuple[float, float]) -> None:
        self._timeout = timeout

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        files: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        return self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            data=data,
            files=files,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify,
            stream=stream,
        )

    def _get_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return read_text(self._request("GET", url, params=params, headers=headers))

    def _get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        return read_json(self._request("GET", url, params=params, headers=headers))

    def _put_text(
        self,
        url: str,
        *,
        data: Any | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return read_text(
            self._request("PUT", url, data=data, json=json, headers=headers)
        )

    def _post_text(
        self,
        url: str,
        *,
        data: Any | None = None,
        json: Any | None = None,
        files: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return read_text(
            self._request(
                "POST", url, data=data, json=json, files=files, headers=headers
            )
        )

    def _delete_text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        return read_text(self._request("DELETE", url, headers=headers))
