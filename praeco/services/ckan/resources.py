"""CKAN resource skeleton."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from praeco.exceptions import ValidationError
from praeco.services.ckan.models import CkanResourceInfo

if TYPE_CHECKING:
    from praeco.services.ckan.client import CkanClient

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
        content_type: str | None = None,
    ) -> CkanResourceInfo:
        """Create a CKAN resource."""
        if upload is not None:
            return self._create_with_upload(
                payload,
                upload,
                content_type=content_type,
            )
        if content_type is not None:
            raise ValidationError("content_type requires an upload path")
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
        *,
        content_type: str | None,
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
        if not path.is_file():
            raise ValidationError(f"upload path must be a file: {path}")
        normalized_content_type = _optional_string(content_type, "content_type")

        with path.open("rb") as file:
            upload_file: tuple[str, Any] | tuple[str, Any, str]
            if normalized_content_type is None:
                upload_file = (filename, file)
            else:
                upload_file = (filename, file, normalized_content_type)
            result = self.client.action.call(
                "resource_create",
                data,
                files={"upload": upload_file},
            )
        return CkanResourceInfo.from_dict(result)


def _resource_id(resource: str | CkanResourceInfo) -> str:
    value = resource.id if isinstance(resource, CkanResourceInfo) else resource
    text = str(value).strip()
    if not text:
        raise ValidationError("resource id must be non-empty")
    return text


def _optional_string(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        raise ValidationError(f"{field_name} must be non-empty")
    return text
