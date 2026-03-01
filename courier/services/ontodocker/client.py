"""
Ontodocker client wiring resources together.
"""

from courier.base_client import BaseClient
from courier.services.ontodocker.datasets import DatasetsResource
from courier.services.ontodocker.endpoints import EndpointsResource
from courier.services.ontodocker.sparql import SparqlResource


class OntodockerClient(BaseClient):
    """
    Client for interacting with an Ontodocker service.

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

    def __init__(self, address: str, *, token: str | None = None, **kwargs) -> None:
        super().__init__(address, token=token, **kwargs)
        self.endpoints = EndpointsResource(self)
        self.datasets = DatasetsResource(self)
        self.sparql = SparqlResource(self)
