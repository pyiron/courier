"""CKAN package resource skeleton."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from courier.services.ckan.client import CkanClient


@dataclass
class PackagesResource:
    """Namespace for CKAN package actions."""

    client: CkanClient
