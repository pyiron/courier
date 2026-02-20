# courier/http/url.py
from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from courier.exceptions import InvalidAddressError, ValidationError


def normalize_base_url(
    address: str,
    *,
    default_scheme: str = "https",
    allowed_schemes: tuple[str, ...] = ("http", "https"),
    require_host_only: bool = True,
) -> str:
    """
    Normalize a user-supplied server address to a base URL.

    Accepts either a plain host[:port] (e.g. ``"example.org:8080"``) or a URL with
    scheme (e.g. ``"https://example.org"``). If no scheme is provided,
    ``default_scheme`` is used. The return value is normalized to
    ``"scheme://host[:port]"`` without a trailing slash.

    Parameters
    ----------
    address
        Server address as host[:port] or URL including scheme.
    default_scheme
        Scheme to use if `address` does not include one.
    allowed_schemes
        Allowed URL schemes. The parsed/selected scheme must be one of these.
    require_host_only
        If True, reject `address` values that include a path, query, or fragment.

    Returns
    -------
    base_url
        Normalized base URL of the form ``"scheme://host[:port]"`` (no trailing slash).

    Raises
    ------
    InvalidAddressError
        If `address` is empty/blank, does not contain a host, contains a disallowed
        scheme, or (when `require_host_only` is True) includes a path/query/fragment.
    """
    if not address or not address.strip():
        raise InvalidAddressError(
            "address must be a non-empty host, e.g. 'ontodocker.example.org'"
        )

    raw = address.strip()
    candidate = raw if "://" in raw else f"{default_scheme}://{raw}"

    parts = urlsplit(candidate)

    scheme = (parts.scheme or default_scheme).lower()
    if scheme not in allowed_schemes:
        raise InvalidAddressError(
            f"Unsupported URL scheme '{scheme}'. Allowed: {allowed_schemes}"
        )

    if not parts.netloc:
        raise InvalidAddressError(
            "address must include a host (and optional port), e.g. 'example.org' or 'https://example.org'"
        )

    if require_host_only:
        if parts.path not in ("", "/") or parts.query or parts.fragment:
            raise InvalidAddressError(
                "address must be host[:port] only (no path/query/fragment). Example: 'https://example.org:8080'"
            )

    return urlunsplit((scheme, parts.netloc, "", "", ""))


def join_url(base: str, *, segments: list[str]) -> str:
    """
    Join path segments onto a base URL.

    Ensures exactly one ``"/"`` between components by stripping redundant slashes
    from `base` and each entry in `segments`. Empty or slash-only segments are
    ignored.

    Parameters
    ----------
    base
        Base URL, typically of the form ``"scheme://host[:port]"``.
    segments
        Path segments to append to `base` (e.g. ``["api", "v1", "jena"]``).

    Returns
    -------
    url
        The joined URL consisting of `base` followed by the provided path segments.

    Raises
    ------
    ValidationError
        If `base` is empty/blank.
    """
    if not base or not base.strip():
        raise ValidationError("base must be a non-empty URL")

    base = base.rstrip("/")
    cleaned = [s.strip("/") for s in segments if s and s.strip("/")]
    return base + ("/" + "/".join(cleaned) if cleaned else "")
