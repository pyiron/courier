"""Dataset CRUD operations for Ontodocker."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import rdflib

from courier.exceptions import ValidationError
from courier.transport.url import join_url

if TYPE_CHECKING:
    from courier.services.ontodocker.client import OntodockerClient


@dataclass
class DatasetsResource:
    """Ontodocker dataset CRUD operations."""

    client: OntodockerClient

    def _dataset_url(self, dataset_name: str) -> str:
        dataset = dataset_name.strip()
        return join_url(self.client.base_url, segments=["api", "v1", "jena", dataset])

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

        return self.client.put_text(self._dataset_url(name))

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

        return self.client.delete_text(self._dataset_url(name))

    def fetch_turtle(self, name: str) -> str:
        """Fetch a dataset as Turtle text.

        Parameters
        ----------
        name
            Dataset name.

        Returns
        -------
        ttl
            Turtle document as text.

        Raises
        ------
        ValidationError
            If `name` is empty/blank.
        """
        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")

        return self.client.get_text(self._dataset_url(name))

    def download_turtle(self, name: str, filename: str | Path) -> Path:
        """Download a dataset and save it to a Turtle file.

        Parameters
        ----------
        name
            Dataset name.
        filename
            Output file path written with UTF-8 encoding.

        Returns
        -------
        path
            Path written to disk.

        Raises
        ------
        ValidationError
            If `name` is empty/blank, or `filename` is blank when provided as a string.
        OSError
            If the file cannot be written (e.g. permissions, missing directory).
        """
        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")
        if isinstance(filename, str) and not filename.strip():
            raise ValidationError("filename must be a non-empty path (str/Path)")

        path = Path(filename)
        content = self.fetch_turtle(name)
        _ = path.write_text(content, encoding="utf-8")
        return path

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

        with open(turtlefile, "rb") as f:
            return self.client.post_text(self._dataset_url(name), files={"file": f})

    def upload_graph(
        self,
        name: str,
        graph: rdflib.Graph,
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
        Exception
            Exceptions raised by ``graph.serialize(format="turtle")``.
        OSError
            If `filename` is provided and cannot be written.
        """
        if not name or not name.strip():
            raise ValidationError("dataset name must be non-empty")

        if isinstance(filename, str) and not filename.strip():
            raise ValidationError(
                "filename must be a non-empty path (str/Path) or None"
            )

        if not isinstance(graph, rdflib.Graph):
            raise ValidationError("graph must be an rdflib.Graph instance")

        ttl = graph.serialize(format="turtle")

        if isinstance(ttl, bytes):
            ttl_bytes = ttl
            ttl_text = ttl.decode(encoding, errors="strict")
        else:
            ttl_text = str(ttl)
            ttl_bytes = ttl_text.encode(encoding)

        if filename is not None:
            _ = Path(filename).write_text(ttl_text, encoding=encoding)

        bio = BytesIO(ttl_bytes)
        files = {"file": ("graph.ttl", bio, "text/turtle")}
        return self.client.post_text(self._dataset_url(name), files=files)
