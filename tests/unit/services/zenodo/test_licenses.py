import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.zenodo import ZenodoClient
from courier.services.zenodo.models import LicenseInfo

from ._helpers import FakeResponse, FakeSession


def _license_payload(
    license_id: str = "cc-by-4.0",
    *,
    title: object = None,
    url: str = "https://creativecommons.org/licenses/by/4.0/legalcode",
) -> dict[str, object]:
    return {
        "id": license_id,
        "title": (
            title
            if title is not None
            else {"en": "Creative Commons Attribution 4.0 International"}
        ),
        "props": {"url": url},
    }


def _license_search_payload(*items: dict[str, object]) -> dict[str, object]:
    return {"hits": {"hits": list(items), "total": len(items)}}


class TestLicensesResource(unittest.TestCase):
    def test_list_without_filters_uses_no_query_params(self):
        session = FakeSession([FakeResponse(json_value=_license_search_payload())])
        c = ZenodoClient(session=cast(Any, session))

        licenses = c.licenses.list()

        self.assertEqual(licenses, [])
        self.assertIsNone(session.calls[0]["params"])

    def test_list_passes_query_parameters(self):
        session = FakeSession(
            [FakeResponse(json_value=_license_search_payload(_license_payload()))]
        )
        c = ZenodoClient(session=cast(Any, session))

        licenses = c.licenses.list(query="cc-by", page=2, size=10)

        self.assertEqual(licenses[0].id, "cc-by-4.0")
        self.assertEqual(
            licenses[0].title,
            "Creative Commons Attribution 4.0 International",
        )
        self.assertEqual(
            licenses[0].url,
            "https://creativecommons.org/licenses/by/4.0/legalcode",
        )
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/vocabularies/licenses",
        )
        self.assertEqual(
            session.calls[0]["params"],
            {"q": "cc-by", "page": 2, "size": 10},
        )

    def test_list_rejects_malformed_search_response(self):
        cases = [
            ([], "must be an object"),
            ({}, "must include hits"),
            ({"hits": {}}, "must include hits.hits"),
            ({"hits": {"hits": ["cc-by-4.0"]}}, "entries must be objects"),
        ]

        for payload, message in cases:
            with (
                self.subTest(payload=payload),
                self.assertRaisesRegex(ValidationError, message),
            ):
                session = FakeSession([FakeResponse(json_value=payload)])
                c = ZenodoClient(session=cast(Any, session))
                c.licenses.list()

    def test_get_uses_license_endpoint(self):
        session = FakeSession([FakeResponse(json_value=_license_payload())])
        c = ZenodoClient(session=cast(Any, session))

        license_info = c.licenses.get("cc-by-4.0")

        self.assertEqual(
            license_info.url, "https://creativecommons.org/licenses/by/4.0/legalcode"
        )
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/vocabularies/licenses/cc-by-4.0",
        )

    def test_get_accepts_fallback_title_and_flat_url(self):
        session = FakeSession(
            [
                FakeResponse(
                    json_value={
                        "id": "custom",
                        "title": {"de": "Benutzerdefinierte Lizenz"},
                        "url": "https://example.org/license",
                    }
                )
            ]
        )
        c = ZenodoClient(session=cast(Any, session))

        license_info = c.licenses.get("custom")

        self.assertEqual(license_info.title, "Benutzerdefinierte Lizenz")
        self.assertEqual(license_info.url, "https://example.org/license")

    def test_license_info_accepts_title_fallbacks(self):
        cases = [
            ({"en": ""}, ""),
            (None, ""),
            ("Plain title", "Plain title"),
        ]

        for title, expected in cases:
            with self.subTest(title=title):
                license_info = LicenseInfo.from_dict({"id": "custom", "title": title})
                self.assertEqual(license_info.title, expected)


if __name__ == "__main__":
    unittest.main()
