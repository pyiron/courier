"""
SPARQL querying for Ontodocker datasets.
"""

from dataclasses import dataclass

import pandas as pd
from SPARQLWrapper import SPARQLWrapper

from courier.exceptions import ValidationError
from courier.http.url import join_url
from courier.services.ontodocker._compat import make_dataframe


@dataclass
class SparqlResource:
    """
    SPARQL operations for Ontodocker.
    """

    client: "OntodockerClient"

    def endpoint(self, dataset: str) -> str:
        """
        Build the SPARQL endpoint URL for a dataset.

        Parameters
        ----------
        dataset
            Dataset name.

        Returns
        -------
        endpoint
            Full SPARQL endpoint URL.
        """
        if not dataset or not dataset.strip():
            raise ValidationError("dataset must be non-empty")

        return join_url(
            self.client.base_url,
            segments=["api", "v1", "jena", dataset.strip(), "sparql"],
        )

    def query(self, dataset: str, query: str, columns: list[str]) -> pd.DataFrame:
        """
        Execute a SPARQL query against a dataset and return a pandas DataFrame.

        Parameters
        ----------
        dataset
            Dataset name.
        query
            SPARQL query string.
        columns
            Column labels for the resulting DataFrame.

        Returns
        -------
        df
            Query result as a pandas DataFrame.

        Raises
        ------
        ValidationError
            If `dataset`, `query`, or `columns` are invalid.
        Exception
            Any exceptions raised by SPARQLWrapper or result conversion.
        """
        if not query or not query.strip():
            raise ValidationError("query must be non-empty")
        if not columns:
            raise ValidationError("columns must be a non-empty list of strings")

        endpoint = self.endpoint(dataset)

        sparql = SPARQLWrapper(endpoint)
        sparql.setReturnFormat("json")
        if self.client.token:
            sparql.addCustomHttpHeader("Authorization", f"Bearer {self.client.token}")
        sparql.setQuery(query)

        result = sparql.queryAndConvert()
        return make_dataframe(result, columns)
