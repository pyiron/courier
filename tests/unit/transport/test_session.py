import unittest

import requests

from courier.transport import create_session


class TestCreateSession(unittest.TestCase):
    def test_returns_requests_session(self):
        s = create_session()
        self.assertIsInstance(s, requests.Session)

    def test_no_headers_does_not_set_custom_header(self):
        s = create_session()
        self.assertIsNone(s.headers.get("X-Test"))

    def test_headers_are_applied_when_passed(self):
        s = create_session(headers={"X-Test": "abc", "Authorization": "Bearer token"})
        self.assertEqual(s.headers.get("X-Test"), "abc")
        self.assertEqual(s.headers.get("Authorization"), "Bearer token")


if __name__ == "__main__":
    unittest.main()
