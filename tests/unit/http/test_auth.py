import unittest

from courier.http.auth import bearer_headers


class TestBearerHeaders(unittest.TestCase):
    def test_valid_token_returns_authorization_header(self):
        result = bearer_headers("mytoken")
        self.assertEqual(result, {"Authorization": "Bearer mytoken"})

    def test_none_token_returns_empty_dict(self):
        result = bearer_headers(None)
        self.assertEqual(result, {})

    def test_empty_string_returns_empty_dict(self):
        result = bearer_headers("")
        self.assertEqual(result, {})

    def test_whitespace_only_token_returns_empty_dict(self):
        result = bearer_headers("   ")
        self.assertEqual(result, {})

    def test_token_with_surrounding_whitespace_is_stripped(self):
        result = bearer_headers("  mytoken  ")
        self.assertEqual(result, {"Authorization": "Bearer mytoken"})

    def test_bearer_prefix_is_present(self):
        result = bearer_headers("abc123")
        self.assertIn("Authorization", result)
        self.assertTrue(result["Authorization"].startswith("Bearer "))

    def test_token_value_preserved_in_header(self):
        token = "eyJhbGciOiJSUzI1NiJ9.payload.signature"
        result = bearer_headers(token)
        self.assertEqual(result["Authorization"], f"Bearer {token}")
