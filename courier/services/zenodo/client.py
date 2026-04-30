"""Zenodo client wiring service resources together."""

from __future__ import annotations

import requests

from courier.http_client import HttpClient
from courier.services.zenodo.depositions import DepositionsResource
from courier.services.zenodo.files import FilesResource
from courier.services.zenodo.licenses import LicensesResource
from courier.transport.url import normalize_base_url


class ZenodoClient(HttpClient):
    """Client for Zenodo depositions, files, and metadata helpers."""

    def __init__(
        self,
        address: str | None = None,
        *,
        token: str | None = None,
        sandbox: bool = False,
        verify: bool | str = True,
        timeout: float | tuple[float, float] = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        address = address or ("sandbox.zenodo.org" if sandbox else "zenodo.org")
        normalized = normalize_base_url(
            address,
            default_scheme="https",
            allowed_schemes=("https",),
        )

        super().__init__(
            normalized,
            token=token,
            default_scheme="https",
            verify=verify,
            timeout=timeout,
            session=session,
        )

        self.sandbox: bool = sandbox
        self.depositions: DepositionsResource = DepositionsResource(self)
        self.files: FilesResource = FilesResource(self)
        self.licenses: LicensesResource = LicensesResource(self)
