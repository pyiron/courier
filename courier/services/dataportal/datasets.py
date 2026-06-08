"""Dataportal dataset operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from courier.exceptions import ValidationError
from courier.services.dataportal.models import (
    DataportalDatasetInfo,
    DataportalDatasetSearchResult,
)

if TYPE_CHECKING:
    from courier.services.dataportal.client import DataportalClient


@dataclass
class DatasetsResource:
    """Dataportal-facing dataset operations."""

    client: DataportalClient

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
