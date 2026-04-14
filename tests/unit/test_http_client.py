import unittest

import requests

from courier import HttpClient
from courier.exceptions import HttpError


class _FakeRequest:
    def __init__(self, method: str):
        self.method = method


class _FakeResponse:
    def __init__(
        self,
        *,
        url: str = "https://example.test/api",
        status_code: int = 200,
        text: str = "ok",
        request: _FakeRequest | None = None,
        raise_for_status_exc: Exception | None = None,
        json_value=None,
        json_exc: Exception | None = None,
    ):
        self.url = url
        self.status_code = status_code
        self.text = text
        self.request = request
        self._raise_for_status_exc = raise_for_status_exc
        self._json_value = json_value
        self._json_exc = json_exc

        self.raise_for_status_called = False
        self.json_called = False

    def raise_for_status(self):
        self.raise_for_status_called = True
        if self._raise_for_status_exc is not None:
            raise self._raise_for_status_exc
        return None

    def json(self):
        self.json_called = True
        if self._json_exc is not None:
            raise self._json_exc
        return self._json_value


class _FakeSession:
    def __init__(self):
        self.headers: dict[str, str] = {}
        self.calls: list[dict] = []
        self.response: _FakeResponse = _FakeResponse()

    def request(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class TestHttpClientInit(unittest.TestCase):
    def test_base_url_is_normalized(self):
        s = _FakeSession()
        c = HttpClient("example.org", session=s)
        self.assertEqual(c.base_url, "https://example.org")

    def test_default_scheme_is_used_when_address_has_no_scheme(self):
        s = _FakeSession()
        c = HttpClient("example.org", default_scheme="http", session=s)
        self.assertEqual(c.base_url, "http://example.org")

    def test_bearer_token_header_is_set_when_token_present(self):
        s = _FakeSession()
        _ = HttpClient("example.org", token="abc", session=s)
        self.assertEqual(s.headers.get("Authorization"), "Bearer abc")

    def test_bearer_token_is_stripped(self):
        s = _FakeSession()
        _ = HttpClient("example.org", token="  abc  ", session=s)
        self.assertEqual(s.headers.get("Authorization"), "Bearer abc")

    def test_token_is_mutable_and_updates_authorization_header(self):
        s = _FakeSession()
        c = HttpClient("example.org", token="abc", session=s)
        c.token = "def"
        self.assertEqual(s.headers.get("Authorization"), "Bearer def")

    def test_token_can_be_cleared_and_removes_authorization_header(self):
        s = _FakeSession()
        c = HttpClient("example.org", token="abc", session=s)
        c.token = None
        self.assertIsNone(s.headers.get("Authorization"))

    def test_token_set_to_whitespace_only_clears(self):
        s = _FakeSession()
        c = HttpClient("example.org", token="abc", session=s)
        c.token = "   "
        self.assertIsNone(c.token)
        self.assertIsNone(s.headers.get("Authorization"))

    def test_no_authorization_header_when_token_is_none(self):
        s = _FakeSession()
        _ = HttpClient("example.org", token=None, session=s)
        self.assertIsNone(s.headers.get("Authorization"))

    def test_address_property(self):
        s = _FakeSession()
        c = HttpClient("example.org", session=s)
        self.assertEqual(c.address, "example.org")

    def test_default_scheme_property(self):
        s = _FakeSession()
        c = HttpClient("example.org", session=s)
        self.assertEqual(c.default_scheme, "https")

    def test_token_property_getter(self):
        s = _FakeSession()
        c = HttpClient("example.org", token="abc", session=s)
        self.assertEqual(c.token, "abc")

    def test_session_property_returns_injected_session(self):
        s = _FakeSession()
        c = HttpClient("example.org", session=s)
        self.assertIs(c.session, s)

    def test_timeout_property_returns_validated_value(self):
        s = _FakeSession()
        c = HttpClient("example.org", timeout=5, session=s)
        self.assertEqual(c.timeout, 5.0)

    def test_verify_property_returns_validated_value(self):
        s = _FakeSession()
        c = HttpClient("example.org", verify=False, session=s)
        self.assertFalse(c.verify)


class TestHttpClientValidation(unittest.TestCase):
    def test_timeout_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, r"timeout must be > 0"):
            _ = HttpClient("example.org", timeout=0)
        with self.assertRaisesRegex(ValueError, r"timeout must be > 0"):
            _ = HttpClient("example.org", timeout=-1)
        with self.assertRaisesRegex(ValueError, r"both values > 0"):
            _ = HttpClient("example.org", timeout=(1, 0))

    def test_timeout_type_is_checked(self):
        with self.assertRaisesRegex(TypeError, r"length 2"):
            _ = HttpClient("example.org", timeout=(1, 2, 3))
        with self.assertRaisesRegex(TypeError, r"length 2"):
            _ = HttpClient("example.org", timeout=True)
        with self.assertRaisesRegex(TypeError, r"length 2"):
            _ = HttpClient("example.org", timeout=False)

    def test_timeout_tuple_with_non_numeric_raises_type_error(self):
        with self.assertRaisesRegex(TypeError, r"numeric values"):
            _ = HttpClient("example.org", timeout=("a", "b"))

    def test_default_scheme_is_validated(self):
        with self.assertRaisesRegex(ValueError, r"must be one of"):
            _ = HttpClient("example.org", default_scheme="ftp")

    def test_empty_default_scheme_raises(self):
        with self.assertRaisesRegex(ValueError, r"non-empty string"):
            _ = HttpClient("example.org", default_scheme="")

    def test_verify_must_be_bool_or_nonempty_string(self):
        with self.assertRaisesRegex(ValueError, r"empty string"):
            _ = HttpClient("example.org", verify="")
        with self.assertRaisesRegex(TypeError, r"bool or a non-empty string"):
            _ = HttpClient("example.org", verify=object())

    def test_verify_as_nonempty_string_is_accepted(self):
        s = _FakeSession()
        c = HttpClient("example.org", verify="/path/to/ca.pem", session=s)
        self.assertEqual(c.verify, "/path/to/ca.pem")


class TestHttpClientRequest(unittest.TestCase):
    def test_request_passes_through_common_arguments(self):
        s = _FakeSession()
        c = HttpClient("example.org", verify=False, timeout=(1.0, 2.0), session=s)

        _ = c.request(
            "POST",
            "https://example.org/api",
            params={"a": 1},
            json={"x": 2},
            data=b"raw",
            files={"f": b"1"},
            headers={"X-Test": "1"},
            stream=True,
        )

        self.assertEqual(len(s.calls), 1)
        call = s.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "https://example.org/api")
        self.assertEqual(call["params"], {"a": 1})
        self.assertEqual(call["json"], {"x": 2})
        self.assertEqual(call["data"], b"raw")
        self.assertEqual(call["files"], {"f": b"1"})
        self.assertEqual(call["headers"], {"X-Test": "1"})
        self.assertEqual(call["timeout"], (1.0, 2.0))
        self.assertFalse(call["verify"])
        self.assertTrue(call["stream"])


class TestHttpClientConvenienceMethods(unittest.TestCase):
    def test_get_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="hello")
        c = HttpClient("example.org", session=s)

        out = c.get_text("https://example.org/hello")
        self.assertEqual(out, "hello")

        self.assertEqual(len(s.calls), 1)
        self.assertEqual(s.calls[0]["method"], "GET")
        self.assertEqual(s.calls[0]["url"], "https://example.org/hello")

    def test_get_json_returns_decoded_json(self):
        s = _FakeSession()
        s.response = _FakeResponse(json_value={"ok": True})
        c = HttpClient("example.org", session=s)

        out = c.get_json("https://example.org/json")
        self.assertEqual(out, {"ok": True})

    def test_post_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="posted")
        c = HttpClient("example.org", session=s)

        out = c.post_text("https://example.org/post", data="x")
        self.assertEqual(out, "posted")
        self.assertEqual(s.calls[0]["method"], "POST")

    def test_put_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="put")
        c = HttpClient("example.org", session=s)

        out = c.put_text("https://example.org/put", json={"a": 1})
        self.assertEqual(out, "put")
        self.assertEqual(s.calls[0]["method"], "PUT")

    def test_delete_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="deleted")
        c = HttpClient("example.org", session=s)

        out = c.delete_text("https://example.org/del")
        self.assertEqual(out, "deleted")
        self.assertEqual(s.calls[0]["method"], "DELETE")

    def test_http_error_is_propagated_as_http_error(self):
        s = _FakeSession()
        s.response = _FakeResponse(
            status_code=404,
            text="Not Found",
            request=_FakeRequest("GET"),
            raise_for_status_exc=requests.HTTPError("404 Client Error"),
        )
        c = HttpClient("example.org", session=s)

        with self.assertRaises(HttpError) as ctx:
            _ = c.get_text("https://example.org/missing")

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(ctx.exception.method, "GET")


if __name__ == "__main__":
    unittest.main()
