"""Zenodo deposition lifecycle operations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from praeco.exceptions import ValidationError
from praeco.services.zenodo._response import read_zenodo_json, read_zenodo_text
from praeco.services.zenodo._urls import (
    deposition_action_url,
    deposition_url,
    depositions_url,
)
from praeco.services.zenodo.metadata import ZenodoMetadata
from praeco.services.zenodo.models import DepositionInfo

if TYPE_CHECKING:
    from praeco.services.zenodo.client import ZenodoClient


@dataclass
class DepositionsResource:
    """Zenodo deposition CRUD and action operations."""

    client: ZenodoClient

    def list(
        self,
        *,
        q: str | None = None,
        page: int | None = None,
        size: int | None = None,
    ) -> list[DepositionInfo]:
        """List depositions for the authenticated user."""
        params = _compact_params(q=q, page=page, size=size)
        resp = self.client.request(
            "GET", depositions_url(self.client.base_url), params=params
        )
        payload = read_zenodo_json(resp)
        if not isinstance(payload, list):
            raise ValidationError("Zenodo depositions response must be a list")
        return [DepositionInfo.from_dict(item) for item in payload]

    def create(
        self,
        metadata: ZenodoMetadata | Mapping[str, Any] | None = None,
        *,
        prereserve_doi: bool = False,
    ) -> DepositionInfo:
        """Create a new deposition draft."""
        payload = _metadata_payload(metadata)
        if prereserve_doi:
            payload.setdefault("metadata", {})["prereserve_doi"] = True

        resp = self.client.request(
            "POST",
            depositions_url(self.client.base_url),
            json=payload,
        )
        return DepositionInfo.from_dict(read_zenodo_json(resp))

    def get(self, deposition: int | str | DepositionInfo) -> DepositionInfo:
        """Retrieve a deposition by id or response object."""
        url = deposition_url(self.client.base_url, _deposition_id(deposition))
        return DepositionInfo.from_dict(
            read_zenodo_json(self.client.request("GET", url))
        )

    def set_metadata(
        self,
        deposition: int | str | DepositionInfo,
        metadata: ZenodoMetadata | Mapping[str, Any],
    ) -> DepositionInfo:
        """Replace deposition metadata."""
        url = deposition_url(self.client.base_url, _deposition_id(deposition))
        resp = self.client.request("PUT", url, json=_metadata_payload(metadata))
        return DepositionInfo.from_dict(read_zenodo_json(resp))

    def publish(self, deposition: int | str | DepositionInfo) -> DepositionInfo:
        """Publish a deposition."""
        return self._action(deposition, "publish")

    def edit(self, deposition: int | str | DepositionInfo) -> DepositionInfo:
        """Unlock a published deposition for editing."""
        return self._action(deposition, "edit")

    def discard(self, deposition: int | str | DepositionInfo) -> DepositionInfo:
        """Discard an active edit session."""
        return self._action(deposition, "discard")

    def new_version(self, deposition: int | str | DepositionInfo) -> DepositionInfo:
        """Create a new version and return the new draft deposition."""
        original = self._action(deposition, "newversion")
        latest_draft = original.links.latest_draft
        if not latest_draft:
            raise ValidationError("Zenodo response did not include links.latest_draft")
        return DepositionInfo.from_dict(
            read_zenodo_json(self.client.request("GET", latest_draft))
        )

    def delete(self, deposition: int | str | DepositionInfo) -> None:
        """Delete an unpublished deposition draft."""
        url = deposition_url(self.client.base_url, _deposition_id(deposition))
        _ = read_zenodo_text(self.client.request("DELETE", url))

    def _action(
        self, deposition: int | str | DepositionInfo, action: str
    ) -> DepositionInfo:
        url = deposition_action_url(
            self.client.base_url,
            _deposition_id(deposition),
            action,
        )
        return DepositionInfo.from_dict(
            read_zenodo_json(self.client.request("POST", url))
        )


def _metadata_payload(
    metadata: ZenodoMetadata | Mapping[str, Any] | None,
) -> dict[str, Any]:
    if metadata is None:
        return {}
    if isinstance(metadata, ZenodoMetadata):
        return metadata.to_payload()
    try:
        payload = dict(metadata)
    except TypeError as exc:
        raise ValidationError(
            "Zenodo deposition metadata must be ZenodoMetadata, a mapping, or None"
        ) from exc
    if isinstance(payload.get("metadata"), Mapping):
        return payload
    return {"metadata": payload}


def _deposition_id(deposition: int | str | DepositionInfo) -> int | str:
    if isinstance(deposition, DepositionInfo):
        return deposition.id
    return deposition


def _compact_params(**params: Any) -> dict[str, Any] | None:
    compact = {key: value for key, value in params.items() if value is not None}
    return compact or None
