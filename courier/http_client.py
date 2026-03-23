"""Public HTTP client for courier.

`HttpClient` provides a concrete HTTP(S) client that can be used directly by users
or subclassed by service-specific courier clients. It centralizes a small set of
shared concerns:

- normalization of the user-provided server address into a base URL
- creation/configuration of a `requests.Session`
- convenience helpers for common request/response patterns

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


class HttpClient:
    """Public HTTP client for http-based courier service clients."""

    _ALLOWED_DEFAULT_SCHEMES: tuple[str, ...] = ("http", "https")

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
        # Store state privately and expose via properties.
        self._address = address
        self._default_scheme = self._validate_default_scheme(default_scheme)

        # validated through property setters
        self._verify: bool | str
        self.verify = verify
        self._timeout: float | tuple[float, float]
        self.timeout = timeout

        self._base_url = normalize_base_url(
            self._address, default_scheme=self._default_scheme
        )

        self._session = session if session is not None else create_session()

        # token is mutable; use the setter to keep session headers in sync
        self._token: str | None = None
        self.token = token

    @classmethod
    def _validate_default_scheme(cls, default_scheme: str) -> str:
        if not default_scheme or not str(default_scheme).strip():
            raise ValueError("default_scheme must be a non-empty string")
        scheme = str(default_scheme).strip().lower()
        if scheme not in cls._ALLOWED_DEFAULT_SCHEMES:
            raise ValueError(
                f"default_scheme must be one of {cls._ALLOWED_DEFAULT_SCHEMES}, got {default_scheme!r}"
            )
        return scheme

    @staticmethod
    def _validate_timeout(
        timeout: object,
    ) -> float | tuple[float, float]:
        if isinstance(timeout, (int, float)):
            if timeout <= 0:
                raise ValueError("timeout must be > 0")
            return float(timeout)

        if not isinstance(timeout, tuple):
            raise TypeError(
                "timeout must be a positive number (seconds) or a (connect, read) tuple with length 2"
            )
        if len(timeout) != 2:
            raise TypeError(
                "timeout must be a positive number (seconds) or a (connect, read) tuple with length 2"
            )
        connect, read = timeout
        if not isinstance(connect, (int, float)) or not isinstance(read, (int, float)):
            raise TypeError(
                "timeout tuple must be (connect_timeout, read_timeout) with numeric values"
            )
        if connect <= 0 or read <= 0:
            raise ValueError(
                "timeout tuple must be (connect_timeout, read_timeout) with both values > 0"
            )
        return (float(connect), float(read))

    @staticmethod
    def _validate_verify(verify: object) -> bool | str:
        if isinstance(verify, bool):
            return verify
        if isinstance(verify, str):
            if not verify.strip():
                raise ValueError("verify must not be an empty string")
            return verify
        raise TypeError("verify must be a bool or a non-empty string")

    @staticmethod
    def _normalize_token(token: str | None) -> str | None:
        if token is None:
            return None
        stripped = token.strip()
        return stripped or None

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
        self._token = self._normalize_token(token)

        # Keep the session's Authorization header in sync with the current token.
        self._session.headers.pop("Authorization", None)
        self._session.headers.update(bearer_headers(self._token))

    @property
    def verify(self) -> bool | str:
        return self._verify

    @verify.setter
    def verify(self, verify: bool | str) -> None:
        self._verify = self._validate_verify(verify)

    @property
    def timeout(self) -> float | tuple[float, float]:
        return self._timeout

    @timeout.setter
    def timeout(self, timeout: float | tuple[float, float]) -> None:
        self._timeout = self._validate_timeout(timeout)

    def request(
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

    def get_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return read_text(self.request("GET", url, params=params, headers=headers))

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        return read_json(self.request("GET", url, params=params, headers=headers))

    def put_text(
        self,
        url: str,
        *,
        data: Any | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return read_text(
            self.request("PUT", url, data=data, json=json, headers=headers)
        )

    def post_text(
        self,
        url: str,
        *,
        data: Any | None = None,
        json: Any | None = None,
        files: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return read_text(
            self.request(
                "POST", url, data=data, json=json, files=files, headers=headers
            )
        )

    def delete_text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        return read_text(self.request("DELETE", url, headers=headers))
