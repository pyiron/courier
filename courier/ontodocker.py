import ast
import os
import warnings
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

import pandas as pd
import requests
from SPARQLWrapper import SPARQLWrapper


def rectify_endpoints(result: str) -> str:
    """
    Normalize legacy Ontodocker `/api/v1/endpoints` responses.

    Older (buggy) Ontodocker deployments may return endpoint URLs with incorrect scheme
    (`http:` instead of `https:`) and malformed base paths such as `:None/api/jena` or
    `:443/api/jena` (missing the `/api/v1` segment). This helper applies a small set of
    string replacements to rewrite such URLs into the expected form
    `https://<host>/api/v1/jena/<dataset>/...`.

    Parameters
    ----------
    result
        Raw response string returned by the Ontodocker endpoints API.

    Returns
    -------
    str
        The normalized response string with legacy URL patterns rewritten.
    """
    result = result.replace("http:", "https:")
    result = result.replace(":None/api/jena", "/api/v1/jena")
    result = result.replace(":443/api/jena", "/api/v1/jena")
    return result


def get_all_dataset_sparql_endpoints(
    address: str,
    token: str | None = None,
    *,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
    rectify: bool = True,
) -> list[str]:
    """
    Fetch all SPARQL endpoint URLs from an Ontodocker/Fuseki instance.

    This function calls the Ontodocker/Fuseki API endpoint ``/api/v1/endpoints``.
    Returns the list of dataset SPARQL endpoints as strings.

    Parameters
    ----------
    address
        Base host of the Ontodocker/Fuseki instance, without the
        scheme and without trailing slashes, e.g. ``"ontodocker.example.org"``.
    token
        Bearer token for authentication. If ``None``, the request is sent without an
        Authorization header.
    timeout
        ``(connect_timeout, read_timeout)`` in seconds, forwarded to ``requests.get``.
    verify
        Passed to ``requests.get`` as TLS certificate verification setting. If ``True``, use
        a CA bundle path. Use ``False`` only if you understand the security implications.
    scheme
        URL scheme, typically ``"https"`` or ``"http"``.
    rectify
        If ``True``, run the response through ``rectify_endpoints`` to normalize older
        server responses

    Returns
    -------
    list[str]
        A list of endpoint URLs.

    Raises
    ------
    ValueError
        If ``address`` is empty or contains a scheme (e.g. starts with ``http://``).
    requests.exceptions.RequestException
        For network-related errors (DNS failure, connection errors, timeouts, TLS issues).
    RuntimeError
        If the response cannot be decoded or cannot be converted into ``list[str]``.
    """
    if not address or not address.strip():
        raise ValueError(
            "address must be a non-empty host (without scheme), e.g. 'ontodocker.example.org'"
        )
    if "://" in address:
        raise ValueError(
            "address must not include a scheme (remove 'http://...' or 'https://...')"
        )

    url = f"{scheme}://{address}/api/v1/endpoints"

    headers = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.get(url, headers=headers, timeout=timeout, verify=verify)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch endpoints from {url}: {e}") from e

    try:
        result = resp.content.decode()  # server typically returns text/JSON-ish content
    except UnicodeDecodeError as e:
        raise RuntimeError(
            f"Failed to decode response body as text from {url}: {e}"
        ) from e

    # Fix older Ontodocker responses
    if rectify:
        try:
            result = rectify_endpoints(result)
        except Exception as e:
            raise RuntimeError(
                f"Failed to rectify endpoints response into list[str]: {e}"
            ) from e

    # convert str -> list[str]
    try:
        result = ast.literal_eval(result)
    except (SyntaxError, ValueError, MemoryError) as e:
        raise RuntimeError(
            f"Failed to convert response to a list of strings: {e}"
        ) from e

    # ensure the conversion succeeded
    if not isinstance(result, list) or not all(isinstance(x, str) for x in result):
        raise TypeError(
            f"ast succeded, but did not convert to a list[str], but to something else. Basetype returned by ast is {type(result)}"
        )

    return result


def extract_dataset_names(sparql_endpoints: list[str]) -> list[str]:
    """
    Extract dataset names from Ontodocker/Fuseki SPARQL endpoint URLs.

    Each input URL is expected to have a path of the form
    ``/api/v1/jena/<dataset>/sparql`` (or ``/api/v1/jena/<dataset>``). The function parses
    the URL, ignores scheme/host/query/fragment, and returns the ``<dataset>`` path segment
    for each endpoint.

    Parameters
    ----------
    sparql_endpoints
        List of SPARQL endpoint URLs (typically returned by the Ontodocker
        ``/api/v1/endpoints`` API).

    Returns
    -------
    list[str]
        Dataset names extracted from the endpoints, in the same order as the input.

    Raises
    ------
    ValueError
        If an endpoint does not match the expected Ontodocker/Fuseki path layout.
    """
    datasetnames: list[str] = []

    for endpoint in sparql_endpoints:
        parts = urlsplit(endpoint)
        path = parts.path.rstrip("/")  # normalize trailing slash
        segments = [s for s in path.split("/") if s]

        # expected: ["api","v1","jena", "<dataset>", "sparql"]
        if (
            len(segments) >= 5
            and segments[:3] == ["api", "v1", "jena"]
            and segments[-1] == "sparql"
        ):
            datasetnames.append(segments[3])
            # fallback: expected: ["api","v1","jena","<dataset>"]
        elif len(segments) >= 4 and segments[:3] == ["api", "v1", "jena"]:
            datasetnames.append(segments[3])
        else:
            raise ValueError(f"Unexpected SPARQL endpoint format: {endpoint}")

    return datasetnames


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
    """
    Download a dataset from an Ontodocker/Fuseki instance and save it as a Turtle (.ttl) file.

    The dataset is fetched from the Ontodocker REST endpoint
    ``{scheme}://{address}/api/v1/jena/{dataset_name}`` and written verbatim to a local
    file. If no output path is provided, the file is saved as ``<cwd>/<dataset_name>.ttl``
    and a ``UserWarning`` is emitted.

    Parameters
    ----------
    address
        Base host (and optional port) of the Ontodocker/Fuseki instance, without scheme,
        e.g. ``"ontodocker.example.org"`` or ``"ontodocker.example.org:8443"``.
    dataset_name
        Plain dataset name as used in the Ontodocker/Jena path (the segment after ``/jena/``).
    token
        Optional bearer token used for authentication. If ``None``, no Authorization header
        is sent.
    turtlefile_name
        Target path (including filename) for the Turtle file. If ``None``, defaults to
        ``<cwd>/<dataset_name>.ttl``.
    timeout
        ``(connect_timeout, read_timeout)`` in seconds, forwarded to ``requests.get``.
    verify
        TLS certificate verification setting forwarded to ``requests.get``. Can be a boolean
        or a path to a CA bundle.
    scheme
        URL scheme, typically ``"https"`` or ``"http"``.

    Returns
    -------
    str
        The path to the written Turtle file (i.e., ``turtlefile_name`` after defaulting).

    Raises
    ------
    ValueError
        If ``address`` is empty, contains a scheme (``://``), or if ``dataset_name`` is empty.
    RuntimeError
        If the request fails (network/TLS/HTTP error) or if the response body cannot be
        decoded as text.
    OSError
        If writing the output file fails (e.g., permission issues, invalid path).
    """
    if not address or not address.strip():
        raise ValueError(
            "address must be a non-empty host (without scheme), e.g. 'ontodocker.example.org'"
        )
    if "://" in address:
        raise ValueError(
            "address must not include a scheme (remove 'http://...' or 'https://...')"
        )

    if not dataset_name or not dataset_name.strip():
        raise ValueError("datset_name must be non-empty")

    url = f"{scheme}://{address}/api/v1/jena/{dataset_name}"

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        resp = requests.get(url, headers=headers, timeout=timeout, verify=verify)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch turtlefile from {url}: {e}") from e

    try:
        result = resp.content.decode()  # server typically returns text/JSON-ish content
    except UnicodeDecodeError as e:
        raise RuntimeError(
            f"Failed to decode response body as text from {url}: {e}"
        ) from e

    if turtlefile_name is None:
        cwd = os.getcwd()
        turtlefile_name = f"{cwd}/{dataset_name}.ttl"
        warnings.warn(
            f"No path/filename to save the turltfile to was explicitly provided. It is saved under {turtlefile_name}",
            UserWarning,
            stacklevel=2,
        )

    with open(turtlefile_name, "w") as file:
        file.write(result)

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
    """
    Create an empty dataset on an Ontodocker/Fuseki instance.

    This function issues an HTTP PUT request to the Ontodocker/Jena endpoint
    ``{scheme}://{address}/api/v1/jena/{dataset_name}`` to create an (initially empty)
    dataset with the given name. The server response body is returned as a decoded string.

    Parameters
    ----------
    address
        Base host (and optional port) of the Ontodocker/Fuseki instance, without scheme,
        e.g. ``"ontodocker.example.org"`` or ``"ontodocker.example.org:8443"``.
    dataset_name
        Plain dataset name to create (the segment after ``/jena/``).
    token
        Optional bearer token used for authentication. If ``None``, no Authorization header
        is sent.
    timeout
        ``(connect_timeout, read_timeout)`` in seconds, forwarded to ``requests.put``.
    verify
        TLS certificate verification setting forwarded to ``requests.put``. Can be a boolean
        or a path to a CA bundle.
    scheme
        URL scheme, typically ``"https"`` or ``"http"``.

    Returns
    -------
    str
        The decoded response body returned by the server.

    Raises
    ------
    ValueError
        If ``address`` is empty, contains a scheme (``://``), or if ``dataset_name`` is empty.
    RuntimeError
        If the request fails (network/TLS/HTTP error) or if the response body cannot be
        decoded as text.
    """
    if not address or not address.strip():
        raise ValueError(
            "address must be a non-empty host (without scheme), e.g. 'ontodocker.example.org'"
        )
    if "://" in address:
        raise ValueError(
            "address must not include a scheme (remove 'http://...' or 'https://...')"
        )

    if not dataset_name or not dataset_name.strip():
        raise ValueError("datset_name must be non-empty")

    url = f"{scheme}://{address}/api/v1/jena/{dataset_name}"

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        resp = requests.put(url, headers=headers, timeout=timeout, verify=verify)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Failed to create dataset '{dataset_name}' at {url}: {e}"
        ) from e

    try:
        result = resp.content.decode()  # server typically returns text/JSON-ish content
    except UnicodeDecodeError as e:
        raise RuntimeError(
            f"Failed to decode response body as text from {url}: {e}"
        ) from e

    return result


def upload_turtlefile(
    address: str,  # ontodocker/fuseki base address without scheme/protocol and delimiter (eg 'https://')
    dataset_name: str,  # plain dataset name
    turtlefile: str | None = None,
    *,
    token: str | None = None,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
) -> str:
    """
    Upload a Turtle (.ttl) file into an existing dataset on an Ontodocker/Fuseki instance.

    The file is uploaded via an HTTP POST request to the Ontodocker/Jena endpoint
    ``{scheme}://{address}/api/v1/jena/{dataset_name}`` using multipart form data
    (``files={'file': <binary file handle>}``). The server response body is returned
    as a decoded string.

    Parameters
    ----------
    address
        Base host (and optional port) of the Ontodocker/Fuseki instance, without scheme,
        e.g. ``"ontodocker.example.org"`` or ``"ontodocker.example.org:8443"``.
    dataset_name
        Target dataset name (the segment after ``/jena/``).
    turtlefile
        Path to the Turtle file to upload. Must be a readable file on disk.
    token
        Optional bearer token used for authentication. If ``None``, no Authorization header
        is sent.
    timeout
        ``(connect_timeout, read_timeout)`` in seconds, forwarded to ``requests.post``.
    verify
        TLS certificate verification setting forwarded to ``requests.post``. Can be a boolean
        or a path to a CA bundle.
    scheme
        URL scheme, typically ``"https"`` or ``"http"``.

    Returns
    -------
    str
        The decoded response body returned by the server.

    Raises
    ------
    ValueError
        If ``address`` is empty, contains a scheme (``://``), if ``dataset_name`` is empty,
        or if ``turtlefile`` is ``None``.
    FileNotFoundError
        If ``turtlefile`` does not exist.
    PermissionError
        If ``turtlefile`` cannot be opened for reading.
    RuntimeError
        If the request fails (network/TLS/HTTP error) or if the response body cannot be
        decoded as text.
    """
    if not address or not address.strip():
        raise ValueError(
            "address must be a non-empty host (without scheme), e.g. 'ontodocker.example.org'"
        )
    if "://" in address:
        raise ValueError(
            "address must not include a scheme (remove 'http://...' or 'https://...')"
        )

    if not dataset_name or not dataset_name.strip():
        raise ValueError("datset_name must be non-empty")

    if turtlefile is None:
        raise ValueError("A turtlefile mut be provided.")

    url = f"{scheme}://{address}/api/v1/jena/{dataset_name}"

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        resp = requests.post(
            url,
            headers=headers,
            files={"file": open(turtlefile, "rb")},
            timeout=timeout,
            verify=verify,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Failed to upload turtlefile '{turtlefile}' to dataset '{dataset_name}' at {url}: {e}"
        ) from e

    try:
        result = resp.content.decode()  # server typically returns text/JSON-ish content
    except UnicodeDecodeError as e:
        raise RuntimeError(
            f"Failed to decode response body as text from {url}: {e}"
        ) from e

    return result


def delete_dataset(
    address: str,
    dataset_name: str,
    *,
    token: str | None = None,
    timeout: tuple[int, int] = (5, 5),
    verify: bool = True,
    scheme: str = "https",
) -> str:
    """
    Delete a dataset from an Ontodocker/Fuseki instance.

    This function issues an HTTP DELETE request to the Ontodocker/Jena endpoint
    ``{scheme}://{address}/api/v1/jena/{dataset_name}`` to remove the dataset and its
    contents from the server. The server response body is returned as a decoded string.

    Parameters
    ----------
    address
        Base host (and optional port) of the Ontodocker/Fuseki instance, without scheme,
        e.g. ``"ontodocker.example.org"`` or ``"ontodocker.example.org:8443"``.
    dataset_name
        Plain dataset name to delete (the segment after ``/jena/``).
    token
        Optional bearer token used for authentication. If ``None``, no Authorization header
        is sent.
    timeout
        ``(connect_timeout, read_timeout)`` in seconds, forwarded to ``requests.delete``.
    verify
        TLS certificate verification setting forwarded to ``requests.delete``. Can be a boolean
        or a path to a CA bundle.
    scheme
        URL scheme, typically ``"https"`` or ``"http"``.

    Returns
    -------
    str
        The decoded response body returned by the server.

    Raises
    ------
    ValueError
        If ``address`` is empty, contains a scheme (``://``), or if ``dataset_name`` is empty.
    RuntimeError
        If the request fails (network/TLS/HTTP error) or if the response body cannot be
        decoded as text.
    """
    if not address or not address.strip():
        raise ValueError(
            "address must be a non-empty host (without scheme), e.g. 'ontodocker.example.org'"
        )
    if "://" in address:
        raise ValueError(
            "address must not include a scheme (remove 'http://...' or 'https://...')"
        )

    if not dataset_name or not dataset_name.strip():
        raise ValueError("datset_name must be non-empty")

    url = f"{scheme}://{address}/api/v1/jena/{dataset_name}"

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        resp = requests.delete(url, headers=headers, timeout=timeout, verify=verify)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Failed to delete dataset '{dataset_name}' at {url}: {e}"
        ) from e

    try:
        result = resp.content.decode()  # server typically returns text/JSON-ish content
    except UnicodeDecodeError as e:
        raise RuntimeError(
            f"Failed to decode response body as text from {url}: {e}"
        ) from e

    return result


def make_dataframe(
    result: Mapping[str, Any],
    columns: Sequence[str],
) -> pd.DataFrame:
    """
    Convert a SPARQL JSON result (SPARQL Protocol / SPARQLWrapper-style) into a pandas DataFrame.

    The function expects a mapping that follows the typical SPARQL JSON structure:
    ``result["results"]["bindings"]`` is iterated, and for each binding the ``["value"]``
    field is collected. The collected rows are returned as a ``pandas.DataFrame`` with the
    provided column names.

    Parameters
    ----------
    result
        SPARQL JSON result object containing ``{"results": {"bindings": [...]}}`` where each
        binding maps variable names to dictionaries that include a ``"value"`` entry.
    columns
        Column labels for the resulting DataFrame. Must match the row width implied by the
        bindings.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing one row per binding.

    Raises
    ------
    KeyError
        If the expected keys (``"results"``, ``"bindings"``, ``"value"``) are missing.
    ValueError
        If the number of extracted values per row does not match ``len(columns)``.
    """
    liste = []
    for r in result["results"]["bindings"]:
        row = []
        for k in r.keys():
            row.append(r[k]["value"])
            liste.append(row)
            df = pd.DataFrame(liste, columns=columns)
    return df


def send_query(
    endpoint: str,
    query: str,
    columns: list[str] | None = None,
    *,
    token: str | None = None,
    print_to_screen: bool = False,
) -> pd.DataFrame:
    """
        Execute a SPARQL query against an endpoint and return the results as a pandas DataFrame.

    The query is executed using ``SPARQLWrapper`` with JSON output. The returned SPARQL JSON
        result is converted into a ``pandas.DataFrame`` via ``make_dataframe``, using the
        provided ``columns`` as DataFrame column labels. Optionally, the endpoint and resulting
        DataFrame are printed to stdout.

    Parameters
        ----------
        endpoint
        Full SPARQL endpoint URL to query (e.g. ``"https://host/api/v1/jena/<dataset>/sparql"``).
        query
        SPARQL query string to execute.
        columns
        Column labels for the expected result table. Must match the number/order of values
        extracted by ``make_dataframe``.
        token
        Optional bearer token for authenticated endpoints. If provided, an
        ``Authorization: Bearer <token>`` header is added to the request.
        print_to_screen
        If ``True``, print the endpoint and the resulting DataFrame to stdout.

    Returns
        -------
        pandas.DataFrame
        DataFrame containing the query results.

    Raises
        ------
        ValueError
        If ``endpoint`` or ``query`` is empty/blank, or if ``columns`` is ``None``.
        Exception
        Any exceptions raised by ``SPARQLWrapper`` during query execution or conversion.
    """
    if not endpoint or not endpoint.strip():
        raise ValueError("endpoint must be non-empty.")

    if not query or not query.strip():
        raise ValueError("query must be non-empty.")

    if columns is None:
        raise ValueError("Please provide columns for the expected response.")

    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat("json")
    if token is not None:
        sparql.addCustomHttpHeader("Authorization", f"Bearer {token}")
        sparql.setQuery(query)
        result = sparql.queryAndConvert()
        result_df = make_dataframe(result, columns)
    if print_to_screen:
        print(f'Sending query to "{endpoint}". Result:')
        print(result_df)
        print("")
    return result_df
