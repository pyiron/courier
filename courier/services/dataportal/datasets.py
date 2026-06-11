"""Dataportal dataset operations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias

from courier.exceptions import ValidationError
from courier.metadata import PublicationMetadata
from courier.services.dataportal.metadata import DataportalMetadata
from courier.services.dataportal.models import (
    DataportalDatasetInfo,
    DataportalDatasetSearchResult,
)

if TYPE_CHECKING:
    from courier.services.dataportal.client import DataportalClient

DatasetMetadata: TypeAlias = DataportalMetadata | Mapping[str, Any]


@dataclass
class DatasetsResource:
    """Dataportal-facing dataset operations."""

    client: DataportalClient

    def create(self, metadata: DatasetMetadata) -> DataportalDatasetInfo:
        """Create a Dataportal dataset."""
        package = self.client.packages.create(_metadata_payload(metadata))
        return DataportalDatasetInfo.from_ckan(package)

    def show(self, dataset: str | DataportalDatasetInfo) -> DataportalDatasetInfo:
        """Retrieve a Dataportal dataset by id, name, or dataset model."""
        package = self.client.packages.show(_dataset_id(dataset))
        return DataportalDatasetInfo.from_ckan(package)

    def search(
        self,
        query: str | None = None,
        **filters: Any,
    ) -> DataportalDatasetSearchResult:
        """Search Dataportal datasets."""
        result = self.client.packages.search(query, **filters)
        return DataportalDatasetSearchResult.from_ckan(result)

    def patch(
        self,
        dataset: str | DataportalDatasetInfo,
        metadata: DatasetMetadata,
    ) -> DataportalDatasetInfo:
        """Partially update a Dataportal dataset."""
        package = self.client.packages.patch(
            _dataset_id(dataset),
            _metadata_payload(metadata),
        )
        return DataportalDatasetInfo.from_ckan(package)

    def delete(self, dataset: str | DataportalDatasetInfo) -> None:
        """Delete a Dataportal dataset."""
        self.client.packages.delete(_dataset_id(dataset))


def _dataset_id(dataset: str | DataportalDatasetInfo) -> str:
    if isinstance(dataset, DataportalDatasetInfo):
        return dataset.id
    text = str(dataset).strip()
    if not text:
        raise ValidationError("dataset id must be non-empty")
    return text


def _metadata_payload(metadata: DatasetMetadata) -> dict[str, Any]:
    if isinstance(metadata, DataportalMetadata):
        return metadata.to_payload()
    if isinstance(metadata, PublicationMetadata):
        raise ValidationError(
            "dataset metadata must be wrapped in DataportalMetadata; "
            "plain PublicationMetadata is not accepted"
        )
    if isinstance(metadata, Mapping):
        return dict(metadata)
    raise ValidationError("dataset metadata must be DataportalMetadata or a mapping")
