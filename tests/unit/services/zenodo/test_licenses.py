import unittest
from typing import Any, cast

from courier.services.zenodo import ZenodoClient

from ._helpers import FakeResponse, FakeSession


class TestLicensesResource(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
