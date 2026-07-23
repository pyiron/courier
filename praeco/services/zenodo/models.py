"""Data models for Zenodo interactions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast


@dataclass(frozen=True)
class DepositionLinks:
    """Important links returned with a Zenodo deposition."""

    self_url: str
    html: str | None = None
    bucket: str | None = None
    files: str | None = None
    publish: str | None = None
    edit: str | None = None
    discard: str | None = None
    latest_draft: str | None = None
    latest_draft_html: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DepositionLinks:
        return cls(
            self_url=str(data.get("self", "")),
            html=_optional_string(data.get("html")),
            bucket=_optional_string(data.get("bucket")),
            files=_optional_string(data.get("files")),
            publish=_optional_string(data.get("publish")),
            edit=_optional_string(data.get("edit")),
            discard=_optional_string(data.get("discard")),
            latest_draft=_optional_string(data.get("latest_draft")),
            latest_draft_html=_optional_string(data.get("latest_draft_html")),
        )


@dataclass(frozen=True)
class DepositionInfo:
    """Parsed Zenodo deposition response."""

    id: int
    conceptrecid: str | None
    record_id: int | None
    submitted: bool
    state: str
    title: str
    links: DepositionLinks
    metadata: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DepositionInfo:
        raw_links = data.get("links")
        links = cast(dict[str, Any], raw_links) if isinstance(raw_links, dict) else {}
        raw_metadata = data.get("metadata")
        metadata = (
            cast(dict[str, Any], raw_metadata) if isinstance(raw_metadata, dict) else {}
        )
        return cls(
            id=int(data["id"]),
            conceptrecid=_optional_string(data.get("conceptrecid")),
            record_id=_optional_int(data.get("record_id")),
            submitted=bool(data.get("submitted", False)),
            state=str(data.get("state", "")),
            title=str(data.get("title", "")),
            links=DepositionLinks.from_dict(links),
            metadata=dict(metadata),
        )


@dataclass(frozen=True)
class UploadedFileInfo:
    """Normalized uploaded-file response from bucket or deposition-file APIs."""

    id: str | None
    filename: str
    checksum: str | None
    size: int | None
    mimetype: str | None
    links: dict[str, str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UploadedFileInfo:
        raw_links = data.get("links")
        links = cast(dict[str, Any], raw_links) if isinstance(raw_links, dict) else {}
        filename = data.get("filename") or data.get("name") or data.get("key") or ""
        return cls(
            id=_optional_string(data.get("id") or data.get("version_id")),
            filename=str(filename),
            checksum=_optional_string(data.get("checksum")),
            size=_optional_int(data.get("size") or data.get("filesize")),
            mimetype=_optional_string(data.get("mimetype")),
            links={str(key): str(value) for key, value in links.items()},
        )


@dataclass(frozen=True)
class LicenseInfo:
    """Zenodo license metadata."""

    id: str
    title: str
    url: str | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LicenseInfo:
        return cls(
            id=str(data["id"]),
            title=_localized_text(data.get("title")),
            url=_optional_string(_license_url(data)),
        )


@dataclass(frozen=True)
class ZenodoFieldError:
    """A field-level validation error returned by Zenodo."""

    field: str | None
    message: str


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    return int(str(value))


def _localized_text(value: object) -> str:
    if isinstance(value, dict):
        english = value.get("en")
        if isinstance(english, str) and english:
            return english
        for item in value.values():
            if isinstance(item, str) and item:
                return item
        return ""
    if value is None:
        return ""
    return str(value)


def _license_url(data: dict[str, Any]) -> object:
    props = data.get("props")
    if isinstance(props, dict):
        return props.get("url")
    return data.get("url")
