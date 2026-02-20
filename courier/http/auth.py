# authentication mechanisms used by courier

from collections.abc import Mapping


def bearer_headers(token: str | None) -> dict[str, str]:
    """
    Construct Authorization headers for a bearer token.

    Parameters
    ----------
    token
        Bearer token or None.

    Returns
    -------
    headers
        Header dict. Empty if `token` is None/blank.
    """
    if token and token.strip():
        return {"Authorization": f"Bearer {token.strip()}"}
    return {}
