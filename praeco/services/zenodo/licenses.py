"""Zenodo license lookup operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from praeco.exceptions import ValidationError
from praeco.services.zenodo._response import read_zenodo_json
from praeco.services.zenodo._urls import license_url, licenses_url
from praeco.services.zenodo.models import LicenseInfo

if TYPE_CHECKING:
    from praeco.services.zenodo.client import ZenodoClient


@dataclass
class LicensesResource:
    """Read-only access to Zenodo licenses."""

    client: ZenodoClient

    def list(
        self,
        *,
        query: str | None = None,
        page: int | None = None,
        size: int | None = None,
    ) -> list[LicenseInfo]:
        """Search Zenodo license metadata."""
        params = _compact_params(q=query, page=page, size=size)
        payload = read_zenodo_json(
            self.client.request(
                "GET", licenses_url(self.client.base_url), params=params
            )
        )
        items = _license_items(payload)
        return [LicenseInfo.from_dict(item) for item in items]

    def get(self, license_id: str) -> LicenseInfo:
        """Retrieve one Zenodo license."""
        payload = read_zenodo_json(
            self.client.request("GET", license_url(self.client.base_url, license_id))
        )
        return LicenseInfo.from_dict(payload)


def _compact_params(**params: Any) -> dict[str, Any] | None:
    compact = {key: value for key, value in params.items() if value is not None}
    return compact or None


def _license_items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValidationError("Zenodo licenses response must be an object")

    hits = payload.get("hits")
    if not isinstance(hits, dict):
        raise ValidationError("Zenodo licenses response must include hits")

    items = hits.get("hits")
    if not isinstance(items, list):
        raise ValidationError(
            "Zenodo licenses response must include hits.hits as a list"
        )

    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValidationError("Zenodo license entries must be objects")
        out.append(item)
    return out
