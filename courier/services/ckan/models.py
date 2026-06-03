"""Small CKAN response models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast


@dataclass(frozen=True)
class CkanResourceInfo:
    """Important fields returned by CKAN resource actions."""

    id: str
    name: str | None
    package_id: str | None
    url: str | None
    format: str | None
    mimetype: str | None
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CkanResourceInfo:
        return cls(
            id=str(data["id"]),
            name=_optional_string(data.get("name")),
            package_id=_optional_string(data.get("package_id")),
            url=_optional_string(data.get("url")),
            format=_optional_string(data.get("format")),
            mimetype=_optional_string(data.get("mimetype")),
            raw=dict(data),
        )


@dataclass(frozen=True)
class CkanPackageInfo:
    """Important fields returned by CKAN package actions."""

    id: str
    name: str
    title: str | None
    resources: list[CkanResourceInfo]
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CkanPackageInfo:
        raw_resources = data.get("resources")
        resources = (
            cast(list[Any], raw_resources) if isinstance(raw_resources, list) else []
        )
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            title=_optional_string(data.get("title")),
            resources=[
                CkanResourceInfo.from_dict(item)
                for item in resources
                if isinstance(item, dict) and "id" in item
            ],
            raw=dict(data),
        )


@dataclass(frozen=True)
class CkanPackageSearchResult:
    """Parsed CKAN package search response."""

    count: int
    results: list[CkanPackageInfo]
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CkanPackageSearchResult:
        raw_results = data.get("results")
        results = cast(list[Any], raw_results) if isinstance(raw_results, list) else []
        return cls(
            count=int(data.get("count", 0)),
            results=[
                CkanPackageInfo.from_dict(item)
                for item in results
                if isinstance(item, dict) and "id" in item and "name" in item
            ],
            raw=dict(data),
        )


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
