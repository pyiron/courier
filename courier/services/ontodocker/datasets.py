"""
Dataset CRUD operations for Ontodocker.
"""

from dataclasses import dataclass

from courier.exceptions import ValidationError
from courier.http.url import join_url


@dataclass
class DatasetsResource:
    """
    Ontodocker dataset CRUD operations.
    """

    client: "OntodockerClient"

    def list(self) -> list[str]:
        """
        List dataset names.

        Returns
        -------
        datasets
            Dataset identifiers.
        """
        return sorted({e.dataset for e in self.client.endpoints.list()})

    def create(self, name: str) -> str:
        """
        Create an empty dataset.

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
        """
        Delete a dataset.

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

    def download_turtle(self, name: str) -> str:
        """
        Download a dataset as Turtle text.

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

        url = join_url(
            self.client.base_url, segments=["api", "v1", "jena", name.strip()]
        )
        return self.client._get_text(url)

    def upload_turtlefile(self, name: str, turtlefile: str) -> str:
        """
        Upload a Turtle (.ttl) file into an existing dataset.

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
