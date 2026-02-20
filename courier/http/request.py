# request/response handling

from typing import Any

import requests

from courier.exceptions import HttpError


def raise_for_status_with_body(resp: requests.Response) -> None:
    """
    Raise `HttpError` if the response indicates an HTTP error.

    Parameters
    ----------
    resp
        Response object.

    Raises
    ------
    HttpError
        If status code is 4xx/5xx. Includes response text where available.
    """
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        text = None
        try:
            text = resp.text
        except Exception:
            text = None
        raise HttpError(
            method=resp.request.method if resp.request else "HTTP",
            url=resp.url,
            status_code=resp.status_code,
            message=str(e),
            response_text=text,
        ) from e


def read_json(resp: requests.Response) -> Any:
    """
    Decode JSON response after checking status.

    Parameters
    ----------
    resp
        Response object.

    Returns
    -------
    payload
        Parsed JSON payload.

    Raises
    ------
    HttpError
        If status indicates error or JSON decoding fails.
    """
    raise_for_status_with_body(resp)
    try:
        return resp.json()
    except Exception as e:
        raise HttpError(
            method=resp.request.method if resp.request else "HTTP",
            url=resp.url,
            status_code=resp.status_code,
            message="Failed to decode JSON response.",
            response_text=getattr(resp, "text", None),
        ) from e


def read_text(resp: requests.Response) -> str:
    """
    Decode text response after checking status.

    Parameters
    ----------
    resp
        Response object.

    Returns
    -------
    text
        Response body as text.

    Raises
    ------
    HttpError
        If status indicates error.
    """
    raise_for_status_with_body(resp)
    return resp.text
