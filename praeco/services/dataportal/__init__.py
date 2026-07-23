"""Material Digital Dataportal client and metadata adapter."""

from courier.services.dataportal.client import DataportalClient
from courier.services.dataportal.metadata import DataportalMetadata
from courier.services.dataportal.models import (
    DataportalAssetInfo,
    DataportalDatasetInfo,
    DataportalDatasetSearchResult,
)

__all__ = [
    "DataportalAssetInfo",
    "DataportalClient",
    "DataportalDatasetInfo",
    "DataportalDatasetSearchResult",
    "DataportalMetadata",
]
