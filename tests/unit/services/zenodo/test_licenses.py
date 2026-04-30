import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.zenodo import ZenodoClient

from ._helpers import FakeResponse, FakeSession


class TestLicensesResource(unittest.TestCase):
    def test_list_without_filters_uses_no_query_params(self):
        session = FakeSession([FakeResponse(json_value=[])])
        c = ZenodoClient(session=cast(Any, session))

        licenses = c.licenses.list()

        self.assertEqual(licenses, [])
        self.assertIsNone(session.calls[0]["params"])

    def test_list_passes_query_parameters(self):
        session = FakeSession(
            [FakeResponse(json_value=[{"id": "cc-by-4.0", "title": "CC BY 4.0"}])]
        )
        c = ZenodoClient(session=cast(Any, session))

        licenses = c.licenses.list(query="cc-by", page=2, size=10)

        self.assertEqual(licenses[0].id, "cc-by-4.0")
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/licenses",
        )
        self.assertEqual(
            session.calls[0]["params"],
            {"q": "cc-by", "page": 2, "size": 10},
        )

    def test_list_rejects_non_list_response(self):
        session = FakeSession([FakeResponse(json_value={"id": "cc-by-4.0"})])
        c = ZenodoClient(session=cast(Any, session))

        with self.assertRaisesRegex(ValidationError, "must be a list"):
            c.licenses.list()

    def test_get_uses_license_endpoint(self):
        session = FakeSession(
            [
                FakeResponse(
                    json_value={
                        "id": "cc-by-4.0",
                        "title": "Creative Commons Attribution 4.0",
                        "url": "https://creativecommons.org/licenses/by/4.0/",
                    }
                )
            ]
        )
        c = ZenodoClient(session=cast(Any, session))

        license_info = c.licenses.get("cc-by-4.0")

        self.assertEqual(
            license_info.url, "https://creativecommons.org/licenses/by/4.0/"
        )
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/licenses/cc-by-4.0",
        )


if __name__ == "__main__":
    unittest.main()
