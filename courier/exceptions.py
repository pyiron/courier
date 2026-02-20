# central source for exceptions thrown by courier

from dataclasses import dataclass
from typing import Any


class CourierError(Exception):
    """Base exception for courier."""


@dataclass
class HttpError(CourierError):
    """Raised when an HTTP request fails."""

    method: str
    url: str
    status_code: int | None = None
    message: str | None = None
    response_text: str | None = None
    payload: Any | None = None

    def __str__(self) -> str:
        parts = [f"{self.method} {self.url}"]
        if self.status_code is not None:
            parts.append(f"status={self.status_code}")
        if self.message:
            parts.append(self.message)
        return " | ".join(parts)


class InvalidAddressError(CourierError, ValueError):
    """Raised when a provided server address cannot be normalized."""


class ValidationError(CourierError, ValueError):
    """Raised when user input is invalid (e.g. empty dataset name)."""
