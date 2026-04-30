import unittest
from typing import Any, cast

from courier.services.zenodo import ZenodoClient

from ._helpers import FakeResponse, FakeSession, deposition_payload


class TestDepositionsResource(unittest.TestCase):
    def test_create_empty_draft_with_prereserved_doi(self):
        session = FakeSession(
            [FakeResponse(status_code=201, json_value=deposition_payload())]
        )
        c = ZenodoClient(session=cast(Any, session))

        draft = c.depositions.create(prereserve_doi=True)

        self.assertEqual(draft.id, 42)
        self.assertEqual(session.calls[0]["method"], "POST")
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/deposit/depositions",
        )
        self.assertEqual(
            session.calls[0]["json"],
            {"metadata": {"prereserve_doi": True}},
        )

    def test_set_metadata_wraps_raw_metadata_dict(self):
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.set_metadata(42, {"title": "Draft"})

        self.assertEqual(session.calls[0]["method"], "PUT")
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/deposit/depositions/42",
        )
        self.assertEqual(session.calls[0]["json"], {"metadata": {"title": "Draft"}})

    def test_publish_posts_to_action_endpoint(self):
        session = FakeSession(
            [FakeResponse(status_code=202, json_value=deposition_payload())]
        )
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.publish(42)

        self.assertEqual(session.calls[0]["method"], "POST")
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/deposit/depositions/42/actions/publish",
        )

    def test_new_version_follows_latest_draft_link(self):
        latest = "https://zenodo.org/api/deposit/depositions/43"
        session = FakeSession(
            [
                FakeResponse(
                    status_code=201,
                    json_value=deposition_payload(42, latest_draft=latest),
                ),
                FakeResponse(json_value=deposition_payload(43)),
            ]
        )
        c = ZenodoClient(session=cast(Any, session))

        draft = c.depositions.new_version(42)

        self.assertEqual(draft.id, 43)
        self.assertEqual(session.calls[1]["method"], "GET")
        self.assertEqual(session.calls[1]["url"], latest)


if __name__ == "__main__":
    unittest.main()
