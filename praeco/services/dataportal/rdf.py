"""DCAT RDF retrieval for Dataportal datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from praeco.exceptions import ValidationError
from praeco.services.dataportal.models import DataportalDatasetInfo
from praeco.transport.url import join_url, quote_path_segment

if TYPE_CHECKING:
    from praeco.services.dataportal.client import DataportalClient

_RDF_FORMATS = {"jsonld", "n3", "rdf", "ttl", "xml"}


@dataclass
class RdfResource:
    """Retrieve DCAT RDF representations of Dataportal datasets."""

    client: DataportalClient

    def dataset_url(
        self,
        dataset: str | DataportalDatasetInfo,
        *,
        format: str = "ttl",
    ) -> str:
        """Build the ckanext-dcat RDF URL for a dataset."""
        dataset_id = _dataset_id(dataset)
        rdf_format = _rdf_format(format)
        return join_url(
            self.client.base_url,
            segments=[
                "dataset",
                quote_path_segment(
                    f"{dataset_id}.{rdf_format}",
                    field_name="dataset RDF path",
                ),
            ],
        )

    def dataset(
        self,
        dataset: str | DataportalDatasetInfo,
        *,
        format: str = "ttl",
    ) -> str:
        """Retrieve a dataset's DCAT RDF representation as text."""
        return self.client.get_text(self.dataset_url(dataset, format=format))


def _dataset_id(dataset: str | DataportalDatasetInfo) -> str:
    if isinstance(dataset, DataportalDatasetInfo):
        return dataset.id
    text = str(dataset).strip()
    if not text:
        raise ValidationError("dataset id must be non-empty")
    return text


def _rdf_format(format: object) -> str:
    if not isinstance(format, str):
        raise ValidationError("RDF format must be a string")
    value = format.strip().lower()
    if value not in _RDF_FORMATS:
        supported = ", ".join(sorted(_RDF_FORMATS))
        raise ValidationError(
            f"unsupported RDF format {format!r}; expected one of: {supported}"
        )
    return value
