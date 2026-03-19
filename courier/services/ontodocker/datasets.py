"""Dataset CRUD operations for Ontodocker."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

from courier.exceptions import ValidationError
from courier.transport.url import join_url

if TYPE_CHECKING:
    from courier.services.ontodocker.client import OntodockerClient


@dataclass
class DatasetsResource:
    """Ontodocker dataset CRUD operations."""

    client: OntodockerClient

    def list(self) -> list[str]:
        """List dataset names.

        Returns
        -------
        datasets
            Dataset identifiers.
        """
        return sorted({e.dataset for e in self.client.endpoints.list()})

    def create(self, name: str) -> str:
        """Create an empty dataset.

        Parameters
        ----------
        name
            Dataset name.

        Returns
        -------
        response_text
            Response body returned by the server.

        Raises
        ------
        ValidationError
            If `name` is empty/blank.
        """
        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")

        url = join_url(
            self.client.base_url, segments=["api", "v1", "jena", name.strip()]
        )
        return self.client._put_text(url)

    def delete(self, name: str) -> str:
        """Delete a dataset.

        Parameters
        ----------
        name
            Dataset name.

        Returns
        -------
        response_text
            Response body returned by the server.

        Raises
        ------
        ValidationError
            If `name` is empty/blank.
        """
        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")

        url = join_url(
            self.client.base_url, segments=["api", "v1", "jena", name.strip()]
        )
        return self.client._delete_text(url)

    def download_turtle(
        self,
        name: str,
        filename: str | Path | None = None,
    ) -> str:
        """Download a dataset as Turtle text.

        Parameters
        ----------
        name
            Dataset name.
        filename
            If provided, write the Turtle content to this file (UTF-8).

        Returns
        -------
        ttl
            Turtle document as text (also returned when `filename` is provided).

        Raises
        ------
        ValidationError
            If `name` is empty/blank, or `filename` is blank when provided.
        OSError
            If the file cannot be written (e.g. permissions, missing directory).
        """

        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")

        if isinstance(filename, str) and not filename.strip():
            raise ValidationError("filename must be a non-empty path (str) or None")

        url = join_url(
            self.client.base_url, segments=["api", "v1", "jena", name.strip()]
        )
        content = self.client._get_text(url)
        if filename is not None:
            path = Path(filename)
            _ = path.write_text(content, encoding="utf-8")
            print(f"Wrote {path}.")

        return content

    def upload_turtlefile(self, name: str, turtlefile: str) -> str:
        """Upload a Turtle (.ttl) file into an existing dataset.

        Parameters
        ----------
        name
            Dataset name.
        turtlefile
            Path to a Turtle file on disk.

        Returns
        -------
        response_text
            Response body returned by the server.

        Raises
        ------
        ValidationError
            If `name` or `turtlefile` is empty/blank.
        FileNotFoundError
            If `turtlefile` does not exist.
        PermissionError
            If `turtlefile` cannot be read.
        """
        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")
        if not turtlefile or not turtlefile.strip():
            raise ValidationError("turtlefile must be a non-empty path")

        url = join_url(
            self.client.base_url, segments=["api", "v1", "jena", name.strip()]
        )

        with open(turtlefile, "rb") as f:
            return self.client._post_text(url, files={"file": f})

    def upload_graph(
        self,
        name: str,
        graph: object,
        *,
        filename: str | Path | None = None,
        encoding: str = "utf-8",
    ) -> str:
        """Serialize an rdflib.Graph to Turtle and upload it into an existing dataset.

        Ontodocker requires uploads in Turtle format. This method serializes the
        provided graph in-memory and uploads it as multipart form data, using the
        same endpoint and form field name as `upload_turtlefile`.

        If `filename` is provided, the serialized Turtle is also written to that path
        (UTF-8).

        Parameters
        ----------
        name
            Dataset name.
        graph
            An ``rdflib.Graph`` instance.
        filename
            If provided, also write the serialized Turtle text to this file (UTF-8).
        encoding
            Encoding used when converting Turtle text to bytes and when writing
            to `filename`.

        Returns
        -------
        response_text
            Response body returned by the server.

        Raises
        ------
        ValidationError
            If `name` is empty/blank, `graph` is not an rdflib.Graph, or `filename`
            is blank when provided.
        ImportError
            If `rdflib` is not installed.
        OSError
            If `filename` is provided and cannot be written.
        """
        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")

        if isinstance(filename, str) and not filename.strip():
            raise ValidationError(
                "filename must be a non-empty path (str/Path) or None"
            )

        try:
            import rdflib  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError("upload_graph requires rdflib.") from e

        if not isinstance(graph, rdflib.Graph):
            raise ValidationError("graph must be an rdflib.Graph instance")

        try:
            ttl = graph.serialize(format="turtle")
        except Exception as e:
            raise ValidationError(
                "Failed to serialize graph to Turtle using rdflib.Graph.serialize(format='turtle')."
            ) from e

        if isinstance(ttl, bytes):
            ttl_bytes = ttl
            ttl_text = ttl.decode(encoding, errors="strict")
        else:
            ttl_text = str(ttl)
            ttl_bytes = ttl_text.encode(encoding)

        if filename is not None:
            _ = Path(filename).write_text(ttl_text, encoding=encoding)

        url = join_url(
            self.client.base_url, segments=["api", "v1", "jena", name.strip()]
        )

        bio = BytesIO(ttl_bytes)
        files = {"file": ("graph.ttl", bio, "text/turtle")}
        return self.client._post_text(url, files=files)
