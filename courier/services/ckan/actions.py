"""CKAN Action API access."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from courier.services.ckan.response import read_ckan_result
from courier.transport.url import join_url, quote_path_segment

if TYPE_CHECKING:
    from courier.services.ckan.client import CkanClient


def action_url(base_url: str, action: str) -> str:
    """Build the URL for a CKAN action endpoint."""
    return join_url(
        base_url,
        segments=[
            "api",
            "3",
            "action",
            quote_path_segment(action, field_name="action"),
        ],
    )


@dataclass
class ActionsResource:
    """Low-level CKAN Action API wrapper."""

    client: CkanClient

    def call(
        self,
        action: str,
        data: Mapping[str, Any] | None = None,
        *,
        files: dict[str, Any] | None = None,
    ) -> Any:
        """Call a CKAN action and return its unwrapped result."""
        payload = dict(data) if data is not None else {}
        request_kwargs: dict[str, Any]
        if files is None:
            request_kwargs = {"json": payload}
        else:
            request_kwargs = {"data": payload, "files": files}
        resp = self.client.request(
            "POST",
            action_url(self.client.base_url, action),
            **request_kwargs,
        )
        return read_ckan_result(resp)
