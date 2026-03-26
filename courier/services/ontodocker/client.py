"""Ontodocker client wiring resources together."""

from __future__ import annotations

import requests

from courier.http_client import HttpClient
from courier.services.ontodocker.datasets import DatasetsResource
from courier.services.ontodocker.endpoints import EndpointsResource
from courier.services.ontodocker.sparql import SparqlResource


class OntodockerClient(HttpClient):
    """Client for interacting with an Ontodocker service.

    Parameters
    ----------
    address
        Server address as host[:port] or URL including scheme.
    token
        Optional bearer token.
    default_scheme
        Scheme used if `address` does not include one.
    verify
        TLS verification passed to `requests`.
    timeout
        Request timeout.
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
        super().__init__(
            address,
            token=token,
            default_scheme=default_scheme,
            verify=verify,
            timeout=timeout,
            session=session,
        )
        self.endpoints = EndpointsResource(self)
        self.datasets = DatasetsResource(self)
        self.sparql = SparqlResource(self)
