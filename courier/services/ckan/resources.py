"""CKAN resource skeleton."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanResourceInfo

if TYPE_CHECKING:
    from courier.services.ckan.client import CkanClient

UploadPath = str | Path


@dataclass
class ResourcesResource:
    """CKAN resource actions."""

    client: CkanClient

    def create(
        self,
        payload: Mapping[str, Any],
        *,
        upload: UploadPath | None = None,
    ) -> CkanResourceInfo:
        """Create a CKAN resource."""
        if upload is not None:
            return self._create_with_upload(payload, upload)
        return CkanResourceInfo.from_dict(
            self.client.action.call("resource_create", payload)
        )

    def show(self, resource: str | CkanResourceInfo) -> CkanResourceInfo:
        """Show a CKAN resource by id or resource model."""
        result = self.client.action.call(
            "resource_show", {"id": _resource_id(resource)}
        )
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

    def _create_with_upload(
        self,
        payload: Mapping[str, Any],
        upload: UploadPath,
    ) -> CkanResourceInfo:
        data = dict(payload)
        if "upload" in data:
            raise ValidationError(
                "payload must not include an upload field when upload is provided"
            )

        path = Path(upload)
        filename = path.name.strip()
        if not filename:
            raise ValidationError("upload filename must be non-empty")

        with path.open("rb") as file:
            result = self.client.action.call(
                "resource_create",
                data,
                files={"upload": (filename, file)},
            )
        return CkanResourceInfo.from_dict(result)


def _resource_id(resource: str | CkanResourceInfo) -> str:
    value = resource.id if isinstance(resource, CkanResourceInfo) else resource
    text = str(value).strip()
    if not text:
        raise ValidationError("resource id must be non-empty")
    return text
