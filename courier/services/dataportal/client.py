"""Material Digital Dataportal client."""

from __future__ import annotations

import requests

from courier.services.ckan.client import CkanClient
from courier.services.dataportal.assets import AssetsResource
from courier.services.dataportal.datasets import DatasetsResource

DEFAULT_DATAPORTAL_ADDRESS = "dataportal.material-digital.de"


class DataportalClient(CkanClient):
    """Client for the CKAN-backed Material Digital Dataportal."""

    def __init__(
        self,
        address: str | None = None,
        *,
        api_token: str | None = None,
        default_scheme: str = "https",
        verify: bool | str = True,
        timeout: float | tuple[float, float] = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__(
            address or DEFAULT_DATAPORTAL_ADDRESS,
            api_token=api_token,
            default_scheme=default_scheme,
            verify=verify,
            timeout=timeout,
            session=session,
        )
        self.assets = AssetsResource(self)
        self.datasets = DatasetsResource(self)
