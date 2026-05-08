"""Zenodo deposition file operations."""

from __future__ import annotations

from builtins import list as builtin_list
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from courier.exceptions import ValidationError
from courier.services.zenodo._response import read_zenodo_json, read_zenodo_text
from courier.services.zenodo._urls import (
    bucket_file_url,
    deposition_file_url,
    deposition_files_url,
)
from courier.services.zenodo.models import DepositionInfo, UploadedFileInfo

if TYPE_CHECKING:
    from courier.services.zenodo.client import ZenodoClient


@dataclass
class FilesResource:
    """Zenodo draft file operations."""

    client: ZenodoClient

    def list(self, deposition: int | str | DepositionInfo) -> list[UploadedFileInfo]:
        """List files attached to a deposition."""
        url = deposition_files_url(self.client.base_url, _deposition_id(deposition))
        payload = read_zenodo_json(self.client.request("GET", url))
        if not isinstance(payload, list):
            raise ValidationError("Zenodo file list response must be a list")
        return [UploadedFileInfo.from_dict(item) for item in payload]

    def upload(
        self,
        deposition: int | str | DepositionInfo,
        path: str | Path,
        *,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> UploadedFileInfo:
        """Upload a file through the deposition bucket link."""
        path = Path(path)
        filename = (filename or path.name).strip()
        if not filename:
            raise ValidationError("filename must be non-empty")

        deposition_info = self._ensure_deposition(deposition)
        if not deposition_info.links.bucket:
            raise ValidationError("deposition does not include a bucket upload link")

        headers = {"Content-Type": content_type} if content_type else None
        with path.open("rb") as file:
            resp = self.client.request(
                "PUT",
                bucket_file_url(deposition_info.links.bucket, filename),
                data=file,
                headers=headers,
            )
        return UploadedFileInfo.from_dict(read_zenodo_json(resp))

    def upload_many(
        self,
        deposition: int | str | DepositionInfo,
        paths: Sequence[str | Path],
    ) -> builtin_list[UploadedFileInfo]:
        """Upload multiple files to a deposition."""
        return [self.upload(deposition, path) for path in paths]

    def rename(
        self,
        deposition: int | str | DepositionInfo,
        file_id: str,
        name: str,
    ) -> UploadedFileInfo:
        """Rename an uploaded deposition file."""
        name = name.strip()
        if not name:
            raise ValidationError("name must be non-empty")

        url = deposition_file_url(
            self.client.base_url, _deposition_id(deposition), file_id
        )
        resp = self.client.request("PUT", url, json={"filename": name})
        return UploadedFileInfo.from_dict(read_zenodo_json(resp))

    def delete(self, deposition: int | str | DepositionInfo, file_id: str) -> None:
        """Delete a file from an unpublished deposition."""
        url = deposition_file_url(
            self.client.base_url, _deposition_id(deposition), file_id
        )
        _ = read_zenodo_text(self.client.request("DELETE", url))

    def _ensure_deposition(
        self,
        deposition: int | str | DepositionInfo,
    ) -> DepositionInfo:
        if isinstance(deposition, DepositionInfo):
            if deposition.links.bucket:
                return deposition
            return self.client.depositions.get(deposition.id)
        return self.client.depositions.get(deposition)


def _deposition_id(deposition: int | str | DepositionInfo) -> int | str:
    if isinstance(deposition, DepositionInfo):
        return deposition.id
    return deposition
