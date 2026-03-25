"""Legacy Ontodocker functional API (deprecated).

This module used to provide a set of standalone functions for interacting with an
Ontodocker/Fuseki deployment.

The preferred API is now :class:`courier.services.ontodocker.OntodockerClient`.
The legacy functions are kept for backwards compatibility and forward all
requests to the new client implementation.

All functions in this module emit a :class:`DeprecationWarning`.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd

from courier.services.ontodocker import OntodockerClient
from courier.services.ontodocker._compat import (
    extract_dataset_names,
    make_dataframe,
    parse_endpoints_response,
    rectify_endpoints,
)

__all__ = [
    "rectify_endpoints",
    "get_all_dataset_sparql_endpoints",
    "extract_dataset_names",
    "download_dataset_as_turtle_file",
    "create_empty_dataset",
    "upload_turtlefile",
    "delete_dataset",
    "make_dataframe",
    "send_query",
]


def _warn_deprecated(name: str) -> None:
    warnings.warn(
        f"courier.ontodocker.{name} is deprecated; use courier.services.ontodocker.OntodockerClient instead.",
        DeprecationWarning,
        stacklevel=2,
    )


def _validate_host_address(address: str) -> str:
    # Preserve legacy behavior/documentation: address is expected without scheme.
    if not address or not address.strip():
        raise ValueError(
            "address must be a non-empty host (without scheme), e.g. 'ontodocker.example.org'"
        )
    if "://" in address:
        raise ValueError(
            "address must not include a scheme (remove 'http://...' or 'https://...')"
        )
    return address.strip()


def get_all_dataset_sparql_endpoints(
    address: str,
    token: str | None = None,
    *,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
    rectify: bool = True,
) -> list[str]:
    _warn_deprecated("get_all_dataset_sparql_endpoints")

    address = _validate_host_address(address)
    client = OntodockerClient(
        address,
        token=token,
        default_scheme=scheme,
        timeout=(float(timeout[0]), float(timeout[1])),
        verify=verify,
    )
    client.endpoints.rectify_legacy = rectify
    return client.endpoints.list_raw()


def download_dataset_as_turtle_file(
    address: str,
    dataset_name: str,
    *,
    token: str | None = None,
    turtlefile_name: str | None = None,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
) -> str:
    _warn_deprecated("download_dataset_as_turtle_file")

    address = _validate_host_address(address)

    if not dataset_name or not dataset_name.strip():
        raise ValueError("dataset_name must be non-empty")

    if turtlefile_name is None:
        turtlefile_name = str(Path.cwd() / f"{dataset_name.strip()}.ttl")
        warnings.warn(
            "No path/filename to save the turtle file to was explicitly"
            f" provided. It is saved under {turtlefile_name}",
            UserWarning,
            stacklevel=2,
        )

    client = OntodockerClient(
        address,
        token=token,
        default_scheme=scheme,
        timeout=(float(timeout[0]), float(timeout[1])),
        verify=verify,
    )

    # New API returns content; legacy returns filename.
    _ = client.datasets.download_turtle(dataset_name.strip(), turtlefile_name)
    return turtlefile_name


def create_empty_dataset(
    address: str,
    dataset_name: str,
    *,
    token: str | None = None,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
) -> str:
    _warn_deprecated("create_empty_dataset")

    address = _validate_host_address(address)

    if not dataset_name or not dataset_name.strip():
        raise ValueError("dataset_name must be non-empty")

    client = OntodockerClient(
        address,
        token=token,
        default_scheme=scheme,
        timeout=(float(timeout[0]), float(timeout[1])),
        verify=verify,
    )
    return client.datasets.create(dataset_name.strip())


def upload_turtlefile(
    address: str,
    dataset_name: str,
    turtlefile: str | None = None,
    *,
    token: str | None = None,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
) -> str:
    _warn_deprecated("upload_turtlefile")

    address = _validate_host_address(address)

    if not dataset_name or not dataset_name.strip():
        raise ValueError("dataset_name must be non-empty")

    if turtlefile is None or not turtlefile.strip():
        raise ValueError("A turtlefile must be provided.")

    client = OntodockerClient(
        address,
        token=token,
        default_scheme=scheme,
        timeout=(float(timeout[0]), float(timeout[1])),
        verify=verify,
    )
    return client.datasets.upload_turtlefile(dataset_name.strip(), turtlefile)


def delete_dataset(
    address: str,
    dataset_name: str,
    *,
    token: str | None = None,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
) -> str:
    _warn_deprecated("delete_dataset")

    address = _validate_host_address(address)

    if not dataset_name or not dataset_name.strip():
        raise ValueError("dataset_name must be non-empty")

    client = OntodockerClient(
        address,
        token=token,
        default_scheme=scheme,
        timeout=(float(timeout[0]), float(timeout[1])),
        verify=verify,
    )
    return client.datasets.delete(dataset_name.strip())


def send_query(
    endpoint: str,
    query: str,
    columns: list[str] | None = None,
    *,
    token: str | None = None,
    print_to_screen: bool = False,
) -> pd.DataFrame:
    _warn_deprecated("send_query")

    if not endpoint or not endpoint.strip():
        raise ValueError("endpoint must be non-empty.")

    if not query or not query.strip():
        raise ValueError("query must be non-empty.")

    if columns is None:
        raise ValueError("Please provide columns for the expected response.")

    endpoint = endpoint.strip()
    if "://" not in endpoint:
        raise ValueError(
            "endpoint must include a URL scheme, e.g. 'https://example.org/api/v1/jena/ds/sparql'."
        )

    parts = urlsplit(endpoint)
    base = f"{parts.scheme}://{parts.netloc}"

    # Reuse robust extraction logic from the new implementation.
    dataset = extract_dataset_names([endpoint])[0]

    client = OntodockerClient(base, token=token)
    result_df = client.sparql.query_df(dataset, query.strip(), columns=columns)

    if print_to_screen:
        print(f'Sending query to "{endpoint}". Result:')
        print(result_df)
        print("")

    return result_df


# Backwards-compatible name for the parser.
# The old API implicitly parsed literal `list[str]` responses via `ast.literal_eval`.
# Keeping this import here makes it easy for users to migrate.
_parse_endpoints_response = parse_endpoints_response
