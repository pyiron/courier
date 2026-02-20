# create requests.Session used for http request done by courier

import requests


def create_session(*, headers: dict[str, str] | None = None) -> requests.Session:
    """
    Create a configured `requests.Session`.

    Parameters
    ----------
    headers
        Default headers to apply to the session.

    Returns
    -------
    session
        A `requests.Session` instance.
    """
    s = requests.Session()
    if headers:
        s.headers.update(headers)
    return s
