"""Zenodo-specific exceptions."""

from __future__ import annotations

from dataclasses import dataclass

from courier.exceptions import HttpError
from courier.services.zenodo.models import ZenodoFieldError


@dataclass
class ZenodoApiError(HttpError):
    """Raised when Zenodo returns an API error."""

    errors: list[ZenodoFieldError] | None = None


class ZenodoValidationError(ZenodoApiError):
    """Raised when Zenodo rejects submitted data with field-level errors."""


class ZenodoAuthenticationError(ZenodoApiError):
    """Raised when Zenodo rejects the access token."""


class ZenodoPermissionError(ZenodoApiError):
    """Raised when the access token lacks permission for the request."""


class ZenodoNotFoundError(ZenodoApiError):
    """Raised when a Zenodo resource does not exist."""
