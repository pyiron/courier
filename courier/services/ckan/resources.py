"""CKAN resource skeleton."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanResourceInfo

if TYPE_CHECKING:
    from courier.services.ckan.client import CkanClient


@dataclass
class ResourcesResource:
    """CKAN resource actions."""

    client: CkanClient

    def create(self, payload: Mapping[str, Any]) -> CkanResourceInfo:
        """Create a CKAN resource."""
        return CkanResourceInfo.from_dict(
            self.client.action.call("resource_create", payload)
        )

    def show(self, resource: str | CkanResourceInfo) -> CkanResourceInfo:
        """Show a CKAN resource by id or resource model."""
        result = self.client.action.call("resource_show", {"id": _resource_id(resource)})
        return CkanResourceInfo.from_dict(result)

    def patch(
        self,
        resource: str | CkanResourceInfo,
        payload: Mapping[str, Any],
    ) -> CkanResourceInfo:
        """Partially update a CKAN resource."""
        data = dict(payload)
        data["id"] = _resource_id(resource)
        return CkanResourceInfo.from_dict(
            self.client.action.call("resource_patch", data)
        )

    def delete(self, resource: str | CkanResourceInfo) -> None:
        """Delete a CKAN resource."""
        _ = self.client.action.call("resource_delete", {"id": _resource_id(resource)})


def _resource_id(resource: str | CkanResourceInfo) -> str:
    if isinstance(resource, CkanResourceInfo):
        return resource.id
    text = str(resource).strip()
    if not text:
        raise ValidationError("resource id must be non-empty")
    return text
