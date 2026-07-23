"""CKAN-specific exceptions."""

from praeco.exceptions import HttpError


class CkanApiError(HttpError):
    """Raised when a CKAN action response fails or is malformed."""
