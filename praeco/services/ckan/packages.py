"""CKAN package resource skeleton."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanPackageInfo, CkanPackageSearchResult

if TYPE_CHECKING:
    from courier.services.ckan.client import CkanClient


@dataclass
class PackagesResource:
    """CKAN package actions."""

    client: CkanClient

    def create(self, payload: Mapping[str, Any]) -> CkanPackageInfo:
        """Create a CKAN package."""
        return CkanPackageInfo.from_dict(
            self.client.action.call("package_create", payload)
        )

    def show(self, package: str | CkanPackageInfo) -> CkanPackageInfo:
        """Show a CKAN package by id, name, or package model."""
        result = self.client.action.call("package_show", {"id": _package_id(package)})
        return CkanPackageInfo.from_dict(result)

    def search(
        self,
        query: str | None = None,
        **filters: Any,
    ) -> CkanPackageSearchResult:
        """Search CKAN packages."""
        payload = dict(filters)
        if query is not None:
            payload["q"] = query
        return CkanPackageSearchResult.from_dict(
            self.client.action.call("package_search", payload)
        )

    def patch(
        self,
        package: str | CkanPackageInfo,
        payload: Mapping[str, Any],
    ) -> CkanPackageInfo:
        """Partially update a CKAN package."""
        data = dict(payload)
        data["id"] = _package_id(package)
        return CkanPackageInfo.from_dict(self.client.action.call("package_patch", data))

    def delete(self, package: str | CkanPackageInfo) -> None:
        """Delete a CKAN package."""
        _ = self.client.action.call("package_delete", {"id": _package_id(package)})


def _package_id(package: str | CkanPackageInfo) -> str:
    value = package.id if isinstance(package, CkanPackageInfo) else package
    text = str(value).strip()
    if not text:
        raise ValidationError("package id must be non-empty")
    return text
