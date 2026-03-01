"""
Endpoint discovery for Ontodocker.
"""

from dataclasses import dataclass

from courier.http.url import join_url
from courier.services.ontodocker._compat import (
    extract_dataset_names,
    parse_endpoints_response,
)
from courier.services.ontodocker.models import EndpointInfo


@dataclass
class EndpointsResource:
    """
    Ontodocker endpoint discovery and normalization.
    """

    client: "OntodockerClient"
    rectify_legacy: bool = True

    def list_raw(self) -> list[str]:
        """
        Fetch the raw SPARQL endpoint URLs from `/api/v1/endpoints`.

        Returns
        -------
        endpoints
            List of endpoint URLs as strings.
        """
        url = join_url(self.client.base_url, segments=["api", "v1", "endpoints"])
        text = self.client._get_text(url)
        return parse_endpoints_response(text, rectify=self.rectify_legacy)

    def list(self) -> list[EndpointInfo]:
        """
        List available dataset endpoints.

        Returns
        -------
        endpoints
            EndpointInfo objects containing dataset name and SPARQL endpoint URL.
        """
        endpoints = self.list_raw()
        dataset_names = extract_dataset_names(endpoints)

        # Keep the returned order stable as far as possible
        out: list[EndpointInfo] = []
        for ds, ep in zip(dataset_names, endpoints, strict=False):
            out.append(EndpointInfo(dataset=ds, sparql_endpoint=ep))
        return out
