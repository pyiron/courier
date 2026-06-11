"""Material Digital Dataportal client and metadata adapter."""

from courier.services.dataportal.client import DataportalClient
from courier.services.dataportal.metadata import DataportalMetadata
from courier.services.dataportal.models import (
    DataportalDatasetInfo,
    DataportalDatasetSearchResult,
)

__all__ = [
    "DataportalClient",
    "DataportalDatasetInfo",
    "DataportalDatasetSearchResult",
    "DataportalMetadata",
]
