"""Internal CKAN client."""

from __future__ import annotations

import requests

from courier.http_client import HttpClient
from courier.services.ckan.actions import ActionsResource


class CkanClient(HttpClient):
    """Small internal client for CKAN-backed service adapters."""

    def __init__(
        self,
        address: str,
        *,
        api_token: str | None = None,
        default_scheme: str = "https",
        verify: bool | str = True,
        timeout: float | tuple[float, float] = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__(
            address,
            token=None,
            default_scheme=default_scheme,
            verify=verify,
            timeout=timeout,
            session=session,
        )
        self._api_token: str | None = None
        self.api_token = api_token
        self.action = ActionsResource(self)

    @property
    def api_token(self) -> str | None:
        """Return the current raw CKAN API token, if any."""
        return self._api_token

    @api_token.setter
    def api_token(self, api_token: str | None) -> None:
        """Set the raw CKAN API token and synchronize the Authorization header."""
        self._api_token = self._normalize_token(api_token)
        self.session.headers.pop("Authorization", None)
        if self._api_token is not None:
            self.session.headers["Authorization"] = self._api_token
