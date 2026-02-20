"""
Compatibility helpers for Ontodocker deployments with legacy quirks.
"""

import ast
from urllib.parse import urlsplit

import pandas as pd


def rectify_endpoints(result: str) -> str:
    """
    Normalize legacy Ontodocker `/api/v1/endpoints` responses.

    Parameters
    ----------
    result
        Raw response string returned by the endpoints API.

    Returns
    -------
    normalized
        Normalized response string.
    """
    result = result.replace("http:", "https:")
    result = result.replace(":None/api/jena", "/api/v1/jena")
    result = result.replace(":443/api/jena", "/api/v1/jena")
    return result


def parse_endpoints_response(text: str, *, rectify: bool = True) -> list[str]:
    """
    Parse Ontodocker `/api/v1/endpoints` response into a list of endpoint URLs.

    Parameters
    ----------
    text
        Raw response body. Some Ontodocker deployments return a Python literal
        representation of a list rather than JSON.
    rectify
        If True, apply `rectify_endpoints` before parsing.

    Returns
    -------
    endpoints
        List of endpoint URLs.

    Raises
    ------
    ValueError
        If the response cannot be parsed into `list[str]`.
    """
    if rectify:
        text = rectify_endpoints(text)

    try:
        value = ast.literal_eval(text)
    except (SyntaxError, ValueError, MemoryError) as e:
        raise ValueError(
            f"Failed to parse endpoints response as Python literal list: {e}"
        ) from e

    if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
        raise ValueError(
            f"Endpoints response did not parse to list[str] (got {type(value)})."
        )

    return value


def extract_dataset_names(sparql_endpoints: list[str]) -> list[str]:
    """
    Extract dataset names from Ontodocker/Fuseki SPARQL endpoint URLs.

    Parameters
    ----------
    sparql_endpoints
        List of SPARQL endpoint URLs.

    Returns
    -------
    dataset_names
        Dataset names extracted from the endpoints.

    Raises
    ------
    ValueError
        If an endpoint does not match the expected path format.
    """
    dataset_names: list[str] = []
    for endpoint in sparql_endpoints:
        parts = urlsplit(endpoint)
        path = parts.path.rstrip("/")
        segments = [s for s in path.split("/") if s]

        # expected: ["api","v1","jena", "<dataset>", "sparql"] (or without trailing "sparql")
        if (
            len(segments) >= 5
            and segments[:3] == ["api", "v1", "jena"]
            and segments[-1] == "sparql"
        ) or (len(segments) >= 4 and segments[:3] == ["api", "v1", "jena"]):
            dataset_names.append(segments[3])
        else:
            raise ValueError(f"Unexpected SPARQL endpoint format: {endpoint}")

    return dataset_names


def make_dataframe(result: dict, columns: list[str]) -> pd.DataFrame:
    """
    Convert a SPARQL JSON result into a pandas DataFrame.

    Parameters
    ----------
    result
        SPARQL JSON result containing `result["results"]["bindings"]`.
    columns
        Column labels for the resulting DataFrame.

    Returns
    -------
    df
        DataFrame containing one row per binding.

    Raises
    ------
    KeyError
        If expected keys are missing.
    ValueError
        If extracted row lengths do not match `len(columns)`.
    """
    rows: list[list[str]] = []
    for binding in result["results"]["bindings"]:
        row: list[str] = []
        for key in binding:
            row.append(binding[key]["value"])
        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    return df
