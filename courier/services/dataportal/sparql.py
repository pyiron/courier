"""SPARQL endpoint discovery and querying for Dataportal datasets."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias
from urllib.parse import urlsplit

from courier.exceptions import ValidationError
from courier.services.dataportal.models import (
    DataportalAssetInfo,
    DataportalDatasetInfo,
)

if TYPE_CHECKING:
    from courier.services.dataportal.client import DataportalClient

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


def _dataset_endpoint(dataset: DataportalDatasetInfo) -> str:
    resources = dataset.raw.get("resources")
    if not isinstance(resources, list):
        raise ValidationError("dataset does not include resource metadata")

    endpoints: list[str] = []
    for resource in resources:
        if not isinstance(resource, Mapping):
            continue
        format_value = resource.get("format")
        if not isinstance(format_value, str) or format_value.strip().lower() != "sparql":
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


def _required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string")
    return value.strip()
