import unittest
from typing import Any, cast

from courier.services.ckan import CkanClient

from ._helpers import FakeResponse, FakeSession


class TestCkanClientInit(unittest.TestCase):
    def test_base_url_is_normalized(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        self.assertEqual(client.base_url, "https://ckan.test")
        self.assertIs(client.action.client, client)
        self.assertIs(client.packages.client, client)
        self.assertIs(client.resources.client, client)

    def test_default_scheme_is_used_when_address_has_no_scheme(self):
        client = CkanClient(
            "ckan.test",
            default_scheme="http",
            session=cast(Any, FakeSession()),
        )

        self.assertEqual(client.base_url, "http://ckan.test")

    def test_api_token_sets_raw_authorization_header(self):
        session = FakeSession()

        _ = CkanClient("ckan.test", api_token="abc", session=cast(Any, session))

        self.assertEqual(session.headers["Authorization"], "abc")

    def test_api_token_is_stripped(self):
        session = FakeSession()

        client = CkanClient(
            "ckan.test",
            api_token="  abc  ",
            session=cast(Any, session),
        )

        self.assertEqual(client.api_token, "abc")
        self.assertEqual(session.headers["Authorization"], "abc")

    def test_api_token_can_be_changed(self):
        session = FakeSession()
        client = CkanClient("ckan.test", api_token="abc", session=cast(Any, session))

        client.api_token = "def"

        self.assertEqual(client.api_token, "def")
        self.assertEqual(session.headers["Authorization"], "def")

    def test_api_token_can_be_cleared(self):
        session = FakeSession()
        client = CkanClient("ckan.test", api_token="abc", session=cast(Any, session))

        client.api_token = None

        self.assertIsNone(client.api_token)
        self.assertNotIn("Authorization", session.headers)

    def test_blank_api_token_clears_header(self):
        session = FakeSession()
        client = CkanClient("ckan.test", api_token="abc", session=cast(Any, session))

        client.api_token = "  "

        self.assertIsNone(client.api_token)
        self.assertNotIn("Authorization", session.headers)


class TestActionsResource(unittest.TestCase):
    def test_call_posts_to_action_endpoint_and_unwraps_result(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": {"id": "dataset"}})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        result = client.action.call("package_show", {"id": "dataset"})

        self.assertEqual(result, {"id": "dataset"})
        self.assertEqual(session.calls[0]["method"], "POST")
        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/package_show",
        )
        self.assertEqual(session.calls[0]["json"], {"id": "dataset"})

    def test_call_uses_empty_payload_when_data_is_omitted(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": []})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        result = client.action.call("package_list")

        self.assertEqual(result, [])
        self.assertEqual(session.calls[0]["json"], {})

    def test_call_quotes_action_name(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": []})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        _ = client.action.call("weird action")

        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/weird%20action",
        )

    def test_call_rejects_blank_action_name(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(ValueError, "action must be non-empty"):
            client.action.call(" ")


if __name__ == "__main__":
    unittest.main()
