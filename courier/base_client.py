# base class for clients supported by courier

from typing import Any

import requests

from courier.http.auth import bearer_headers
from courier.http.request import read_json, read_text
from courier.http.session import create_session
from courier.http.url import normalize_base_url


class BaseClient:
    """
    Base client providing shared HTTP behavior and address normalization.

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
        self.address = address
        self.token = token
        self.default_scheme = default_scheme
        self.verify = verify
        self.timeout = timeout

        self.base_url = normalize_base_url(
            self.address, default_scheme=self.default_scheme
        )

        self.session = session if session is not None else create_session()
        self.session.headers.update(bearer_headers(self.token))

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

    def _get_text(self, url: str, *, params: dict[str, Any] | None = None) -> str:
        return read_text(self._request("GET", url, params=params))

    def _get_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        return read_json(self._request("GET", url, params=params))

    def _put_text(
        self, url: str, *, data: Any | None = None, json: Any | None = None
    ) -> str:
        return read_text(self._request("PUT", url, data=data, json=json))

    def _post_text(
        self,
        url: str,
        *,
        data: Any | None = None,
        json: Any | None = None,
        files: dict[str, Any] | None = None,
    ) -> str:
        return read_text(self._request("POST", url, data=data, json=json, files=files))

    def _delete_text(self, url: str) -> str:
        return read_text(self._request("DELETE", url))
