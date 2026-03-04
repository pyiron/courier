import unittest

from courier.exceptions import InvalidAddressError, ValidationError
from courier.transport.url import join_url, normalize_base_url


class TestNormalizeBaseUrl(unittest.TestCase):
    def test_blank_address_raises(self):
        for addr in ["", " ", "\n\t"]:
            with self.subTest(addr=addr), self.assertRaises(InvalidAddressError):
                _ = normalize_base_url(addr)

    def test_host_only_gets_default_scheme(self):
        self.assertEqual(normalize_base_url("example.org"), "https://example.org")

    def test_host_port_preserved(self):
        self.assertEqual(
            normalize_base_url("example.org:8080"),
            "https://example.org:8080",
        )

    def test_explicit_scheme_respected(self):
        self.assertEqual(normalize_base_url("http://example.org"), "http://example.org")

    def test_disallowed_scheme_raises(self):
        with self.assertRaises(InvalidAddressError):
            _ = normalize_base_url("ftp://example.org")

    def test_path_query_fragment_rejected_by_default(self):
        cases = [
            "https://example.org/api",
            "https://example.org/?x=1",
            "https://example.org/#frag",
            "https://example.org/api?x=1#frag",
        ]
        for addr in cases:
            with self.subTest(addr=addr), self.assertRaises(InvalidAddressError):
                _ = normalize_base_url(addr)

    def test_path_query_fragment_allowed_when_require_host_only_false(self):
        self.assertEqual(
            normalize_base_url(
                "https://example.org/api?x=1#frag",
                require_host_only=False,
            ),
            "https://example.org",
        )


class TestJoinUrl(unittest.TestCase):
    def test_blank_base_raises(self):
        for base in ["", " ", "\n"]:
            with self.subTest(base=base), self.assertRaises(ValidationError):
                _ = join_url(base, segments=["api"])

    def test_join_normalizes_slashes(self):
        self.assertEqual(
            join_url("https://example.org/", segments=["/api/", "v1", "/jena/"]),
            "https://example.org/api/v1/jena",
        )

    def test_ignores_empty_or_slash_only_segments(self):
        self.assertEqual(
            join_url("https://example.org/", segments=["", "/", "api"]),
            "https://example.org/api",
        )

    def test_empty_segments_returns_base_without_trailing_slash(self):
        self.assertEqual(
            join_url("https://example.org/", segments=[]), "https://example.org"
        )


if __name__ == "__main__":
    _ = unittest.main()
