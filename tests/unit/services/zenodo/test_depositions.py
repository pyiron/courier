import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.metadata import Person, PublicationMetadata
from courier.services.zenodo import ZenodoClient
from courier.services.zenodo._urls import deposition_action_url
from courier.services.zenodo.metadata import Creator, ZenodoMetadata
from courier.services.zenodo.models import DepositionInfo

from ._helpers import FakeResponse, FakeSession, deposition_payload


def _publication_metadata() -> PublicationMetadata:
    return PublicationMetadata(
        title="courier",
        description="Python client.",
        publication_date="2026-04-21",
        creators=[Person(name="Doe, Jane")],
        keywords=["python"],
        license="Apache-2.0",
    )


class TestDepositionsResource(unittest.TestCase):
    def test_deposition_action_url_rejects_unknown_action(self):
        with self.assertRaisesRegex(ValidationError, "unsupported deposition action"):
            deposition_action_url("https://zenodo.org", 42, "archive")

    def test_list_passes_filters_and_parses_depositions(self):
        session = FakeSession([FakeResponse(json_value=[deposition_payload()])])
        c = ZenodoClient(session=cast(Any, session))

        depositions = c.depositions.list(q="courier", page=2, size=5)

        self.assertEqual(depositions[0].id, 42)
        self.assertEqual(session.calls[0]["method"], "GET")
        self.assertEqual(
            session.calls[0]["params"], {"q": "courier", "page": 2, "size": 5}
        )

    def test_list_without_filters_uses_no_query_params(self):
        session = FakeSession([FakeResponse(json_value=[])])
        c = ZenodoClient(session=cast(Any, session))

        depositions = c.depositions.list()

        self.assertEqual(depositions, [])
        self.assertIsNone(session.calls[0]["params"])

    def test_list_rejects_non_list_response(self):
        session = FakeSession([FakeResponse(json_value={"id": 42})])
        c = ZenodoClient(session=cast(Any, session))

        with self.assertRaisesRegex(ValidationError, "must be a list"):
            c.depositions.list()

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

    def test_create_preserves_wrapped_raw_metadata_payload(self):
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.create({"metadata": {"title": "Draft"}})

        self.assertEqual(session.calls[0]["json"], {"metadata": {"title": "Draft"}})

    def test_create_adds_prereserved_doi_to_wrapped_raw_metadata_payload(self):
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.create(
            {"metadata": {"title": "Draft"}},
            prereserve_doi=True,
        )

        self.assertEqual(
            session.calls[0]["json"],
            {"metadata": {"title": "Draft", "prereserve_doi": True}},
        )

    def test_create_serializes_metadata_model(self):
        metadata = ZenodoMetadata.software()
        metadata.title = "courier"
        metadata.publication_date = "2026-04-21"
        metadata.description = "Python client."
        metadata.license = "Apache-2.0"
        metadata.creators.append(Creator(name="Doe, Jane"))
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.create(metadata)

        self.assertEqual(
            session.calls[0]["json"]["metadata"]["upload_type"],
            "software",
        )

    def test_create_serializes_publication_metadata_adapter(self):
        metadata = ZenodoMetadata.software(_publication_metadata())
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.create(metadata)

        self.assertEqual(
            session.calls[0]["json"],
            {
                "metadata": {
                    "upload_type": "software",
                    "publication_date": "2026-04-21",
                    "title": "courier",
                    "creators": [{"name": "Doe, Jane"}],
                    "description": "Python client.",
                    "access_right": "open",
                    "license": "Apache-2.0",
                    "keywords": ["python"],
                    "language": "eng",
                }
            },
        )

    def test_create_adds_prereserved_doi_to_publication_metadata_adapter(self):
        metadata = ZenodoMetadata.software(_publication_metadata())
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.create(metadata, prereserve_doi=True)

        self.assertTrue(session.calls[0]["json"]["metadata"]["prereserve_doi"])

    def test_get_accepts_deposition_info(self):
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))
        deposition = DepositionInfo.from_dict(deposition_payload())

        draft = c.depositions.get(deposition)

        self.assertEqual(draft.id, 42)
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/deposit/depositions/42",
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

    def test_set_metadata_preserves_wrapped_raw_metadata_payload(self):
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.set_metadata(42, {"metadata": {"title": "Draft"}})

        self.assertEqual(session.calls[0]["json"], {"metadata": {"title": "Draft"}})

    def test_set_metadata_serializes_publication_metadata_adapter(self):
        metadata = ZenodoMetadata.software(_publication_metadata())
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.set_metadata(42, metadata)

        self.assertEqual(session.calls[0]["method"], "PUT")
        self.assertEqual(
            session.calls[0]["json"]["metadata"]["title"],
            "courier",
        )
        self.assertEqual(
            session.calls[0]["json"]["metadata"]["upload_type"],
            "software",
        )

    def test_plain_publication_metadata_is_rejected(self):
        session = FakeSession([FakeResponse(json_value=deposition_payload())])
        c = ZenodoClient(session=cast(Any, session))

        with self.assertRaisesRegex(ValidationError, "ZenodoMetadata"):
            c.depositions.create(cast(Any, _publication_metadata()))

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

    def test_edit_and_discard_post_to_action_endpoint(self):
        session = FakeSession(
            [
                FakeResponse(status_code=202, json_value=deposition_payload()),
                FakeResponse(status_code=202, json_value=deposition_payload()),
            ]
        )
        c = ZenodoClient(session=cast(Any, session))

        _ = c.depositions.edit(42)
        _ = c.depositions.discard(42)

        self.assertEqual(
            [call["url"] for call in session.calls],
            [
                "https://zenodo.org/api/deposit/depositions/42/actions/edit",
                "https://zenodo.org/api/deposit/depositions/42/actions/discard",
            ],
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

    def test_new_version_requires_latest_draft_link(self):
        session = FakeSession(
            [FakeResponse(status_code=201, json_value=deposition_payload())]
        )
        c = ZenodoClient(session=cast(Any, session))

        with self.assertRaisesRegex(ValidationError, "latest_draft"):
            c.depositions.new_version(42)

    def test_delete_uses_deposition_endpoint(self):
        session = FakeSession([FakeResponse(status_code=204, text="")])
        c = ZenodoClient(session=cast(Any, session))

        c.depositions.delete(42)

        self.assertEqual(session.calls[0]["method"], "DELETE")
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/deposit/depositions/42",
        )


if __name__ == "__main__":
    unittest.main()
