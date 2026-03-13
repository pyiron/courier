import unittest

import requests

from courier.base_client import BaseClient
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


class TestBaseClientInit(unittest.TestCase):
    def test_base_url_is_normalized(self):
        s = _FakeSession()
        c = BaseClient("example.org", session=s)
        self.assertEqual(c.base_url, "https://example.org")

    def test_default_scheme_is_used_when_address_has_no_scheme(self):
        s = _FakeSession()
        c = BaseClient("example.org", default_scheme="http", session=s)
        self.assertEqual(c.base_url, "http://example.org")

    def test_bearer_token_header_is_set_when_token_present(self):
        s = _FakeSession()
        _ = BaseClient("example.org", token="abc", session=s)
        self.assertEqual(s.headers.get("Authorization"), "Bearer abc")

    def test_bearer_token_is_stripped(self):
        s = _FakeSession()
        _ = BaseClient("example.org", token="  abc  ", session=s)
        self.assertEqual(s.headers.get("Authorization"), "Bearer abc")

    def test_no_authorization_header_when_token_is_none(self):
        s = _FakeSession()
        _ = BaseClient("example.org", token=None, session=s)
        self.assertIsNone(s.headers.get("Authorization"))


class TestBaseClientRequest(unittest.TestCase):
    def test_request_passes_through_common_arguments(self):
        s = _FakeSession()
        c = BaseClient("example.org", verify=False, timeout=(1.0, 2.0), session=s)

        _ = c._request(
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


class TestBaseClientConvenienceMethods(unittest.TestCase):
    def test_get_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="hello")
        c = BaseClient("example.org", session=s)

        out = c._get_text("https://example.org/hello")
        self.assertEqual(out, "hello")

        self.assertEqual(len(s.calls), 1)
        self.assertEqual(s.calls[0]["method"], "GET")
        self.assertEqual(s.calls[0]["url"], "https://example.org/hello")

    def test_get_json_returns_decoded_json(self):
        s = _FakeSession()
        s.response = _FakeResponse(json_value={"ok": True})
        c = BaseClient("example.org", session=s)

        out = c._get_json("https://example.org/json")
        self.assertEqual(out, {"ok": True})

    def test_post_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="posted")
        c = BaseClient("example.org", session=s)

        out = c._post_text("https://example.org/post", data="x")
        self.assertEqual(out, "posted")
        self.assertEqual(s.calls[0]["method"], "POST")

    def test_put_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="put")
        c = BaseClient("example.org", session=s)

        out = c._put_text("https://example.org/put", json={"a": 1})
        self.assertEqual(out, "put")
        self.assertEqual(s.calls[0]["method"], "PUT")

    def test_delete_text_returns_response_text(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="deleted")
        c = BaseClient("example.org", session=s)

        out = c._delete_text("https://example.org/del")
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
        c = BaseClient("example.org", session=s)

        with self.assertRaises(HttpError) as ctx:
            _ = c._get_text("https://example.org/missing")

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(ctx.exception.method, "GET")


if __name__ == "__main__":
    unittest.main()
