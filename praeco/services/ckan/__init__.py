"""Internal CKAN substrate for service-specific clients."""

from praeco.services.ckan.client import CkanClient
from praeco.services.ckan.exceptions import CkanApiError

__all__ = ["CkanApiError", "CkanClient"]
