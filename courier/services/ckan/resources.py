"""CKAN resource skeleton."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from courier.services.ckan.client import CkanClient


@dataclass
class ResourcesResource:
    """Namespace for CKAN resource actions."""

    client: CkanClient
