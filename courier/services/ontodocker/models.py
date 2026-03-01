"""
Data models for Ontodocker interactions.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class EndpointInfo:
    """
    Endpoint information for a dataset.

    Parameters
    ----------
    dataset
        Dataset name.
    sparql_endpoint
        Full SPARQL endpoint URL for the dataset.
    """

    dataset: str
    sparql_endpoint: str
