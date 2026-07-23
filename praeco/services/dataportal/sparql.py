"""SPARQL endpoint discovery and querying for Dataportal datasets."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias
from urllib.parse import urlsplit

import pandas as pd
import requests

from praeco.exceptions import ValidationError
from praeco.services.dataportal.models import (
    DataportalAssetInfo,
    DataportalDatasetInfo,
)
from praeco.transport.request import read_text

if TYPE_CHECKING:
    from praeco.services.dataportal.client import DataportalClient

SparqlTarget: TypeAlias = str | DataportalDatasetInfo | DataportalAssetInfo


@dataclass
class SparqlResource:
    """Dataportal-specific SPARQL operations."""

    client: DataportalClient

    def endpoint(self, target: SparqlTarget) -> str:
        """Resolve an explicit or dataset-associated SPARQL endpoint URL."""
        if isinstance(target, DataportalAssetInfo):
            if target.url is None:
                raise ValidationError("SPARQL asset does not include a URL")
            return _absolute_http_url(target.url, field_name="SPARQL asset URL")

        if isinstance(target, DataportalDatasetInfo):
            return _dataset_endpoint(target)

        value = _required_string(target, "SPARQL target")
        if _is_absolute_http_url(value):
            return value
        if "://" in value:
            raise ValidationError("SPARQL endpoint must be an absolute HTTP(S) URL")
        return _dataset_endpoint(self.client.datasets.show(value))

    def query_raw(
        self,
        target: SparqlTarget,
        query: str,
        *,
        accept: str = "application/sparql-results+json",
    ) -> str:
        """Execute a SPARQL query and return the response body as text."""
        query_text = _required_string(query, "query")
        accept_value = _required_string(accept, "accept")
        endpoint = self.endpoint(target)
        params = {"query": query_text}
        headers = {"Accept": accept_value}

        if _same_origin(endpoint, self.client.base_url):
            return self.client.get_text(endpoint, params=params, headers=headers)

        response = requests.get(
            endpoint,
            params=params,
            headers=headers,
            timeout=self.client.timeout,
            verify=self.client.verify,
        )
        return read_text(response)

    def query_json(
        self,
        target: SparqlTarget,
        query: str,
    ) -> dict[str, Any]:
        """Execute a SPARQL query and decode its JSON result."""
        result = json.loads(
            self.query_raw(
                target,
                query,
                accept="application/sparql-results+json",
            )
        )
        if not isinstance(result, dict):
            raise ValidationError("SPARQL JSON response must be an object")
        return result

    def query_df(
        self,
        target: SparqlTarget,
        query: str,
        columns: list[str],
    ) -> pd.DataFrame:
        """Execute a SELECT query and return requested bindings as a DataFrame."""
        normalized_columns = _columns(columns)
        return _make_dataframe(
            self.query_json(target, query),
            normalized_columns,
        )


def _dataset_endpoint(dataset: DataportalDatasetInfo) -> str:
    resources = dataset.raw.get("resources")
    if not isinstance(resources, list):
        raise ValidationError("dataset does not include resource metadata")

    endpoints: list[str] = []
    for resource in resources:
        if not isinstance(resource, Mapping):
            continue
        format_value = resource.get("format")
        if (
            not isinstance(format_value, str)
            or format_value.strip().lower() != "sparql"
        ):
            continue
        url = resource.get("url")
        if not isinstance(url, str):
            raise ValidationError("SPARQL resource does not include a URL")
        endpoints.append(_absolute_http_url(url, field_name="SPARQL resource URL"))

    if not endpoints:
        raise ValidationError("dataset does not include a SPARQL resource")
    if len(endpoints) > 1:
        raise ValidationError("dataset includes multiple SPARQL resources")
    return endpoints[0]


def _absolute_http_url(value: str, *, field_name: str) -> str:
    url = _required_string(value, field_name)
    if not _is_absolute_http_url(url):
        raise ValidationError(f"{field_name} must be an absolute HTTP(S) URL")
    return url


def _is_absolute_http_url(value: str) -> bool:
    parts = urlsplit(value)
    return parts.scheme.lower() in {"http", "https"} and bool(parts.netloc)


def _same_origin(left: str, right: str) -> bool:
    return _origin(left) == _origin(right)


def _origin(value: str) -> tuple[str, str, int | None]:
    parts = urlsplit(value)
    scheme = parts.scheme.lower()
    port = parts.port
    if port is None:
        port = 443 if scheme == "https" else 80 if scheme == "http" else None
    return scheme, (parts.hostname or "").lower(), port


def _columns(columns: list[str]) -> list[str]:
    if (
        not isinstance(columns, list)
        or not columns
        or any(not isinstance(column, str) or not column.strip() for column in columns)
    ):
        raise ValidationError("columns must be a non-empty list of strings")
    return [column.strip() for column in columns]


def _make_dataframe(
    result: Mapping[str, Any],
    columns: list[str],
) -> pd.DataFrame:
    raw_results = result.get("results")
    if not isinstance(raw_results, Mapping):
        raise ValidationError("SPARQL JSON response must include results")
    raw_bindings = raw_results.get("bindings")
    if not isinstance(raw_bindings, list):
        raise ValidationError("SPARQL JSON response must include results.bindings")

    rows: list[list[Any | None]] = []
    for binding in raw_bindings:
        if not isinstance(binding, Mapping):
            raise ValidationError("SPARQL result bindings must be objects")
        row: list[Any | None] = []
        for column in columns:
            value = binding.get(column)
            row.append(value.get("value") if isinstance(value, Mapping) else None)
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def _required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string")
    return value.strip()
