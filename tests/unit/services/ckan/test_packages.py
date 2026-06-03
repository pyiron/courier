import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.ckan import CkanClient
from courier.services.ckan.models import CkanPackageInfo

from ._helpers import FakeResponse, FakeSession


def package_payload(
    *,
    package_id: str = "pkg-1",
    name: str = "dataset",
    title: str | None = "Dataset",
) -> dict[str, Any]:
    return {
        "id": package_id,
        "name": name,
        "title": title,
        "resources": [
            {
                "id": "res-1",
                "name": "data.ttl",
                "package_id": package_id,
                "url": "https://example.test/data.ttl",
                "format": "ttl",
                "mimetype": "text/turtle",
            }
        ],
        "custom": "preserved",
    }


class TestCkanPackageModels(unittest.TestCase):
    def test_package_info_parses_stable_fields_and_preserves_raw_payload(self):
        payload = package_payload()

        info = CkanPackageInfo.from_dict(payload)

        self.assertEqual(info.id, "pkg-1")
        self.assertEqual(info.name, "dataset")
        self.assertEqual(info.title, "Dataset")
        self.assertEqual(info.raw["custom"], "preserved")
        self.assertEqual(info.resources[0].id, "res-1")
        self.assertEqual(info.resources[0].format, "ttl")


class TestPackagesResource(unittest.TestCase):
    def test_create_calls_package_create_and_parses_response(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": package_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        info = client.packages.create({"name": "dataset", "title": "Dataset"})

        self.assertEqual(info.name, "dataset")
        self.assertEqual(session.calls[0]["method"], "POST")
        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/package_create",
        )
        self.assertEqual(
            session.calls[0]["json"],
            {"name": "dataset", "title": "Dataset"},
        )

    def test_show_calls_package_show_with_id(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": package_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        info = client.packages.show("dataset")

        self.assertEqual(info.id, "pkg-1")
        self.assertEqual(
            session.calls[0]["url"], "https://ckan.test/api/3/action/package_show"
        )
        self.assertEqual(session.calls[0]["json"], {"id": "dataset"})

    def test_show_accepts_package_model(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": package_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))
        package = CkanPackageInfo.from_dict(package_payload(package_id="pkg-2"))

        _ = client.packages.show(package)

        self.assertEqual(session.calls[0]["json"], {"id": "pkg-2"})

    def test_search_calls_package_search_with_query_and_filters(self):
        session = FakeSession(
            [
                FakeResponse(
                    json_value={
                        "success": True,
                        "result": {"count": 1, "results": [package_payload()]},
                    }
                )
            ]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        result = client.packages.search("steel", rows=5)

        self.assertEqual(result.count, 1)
        self.assertEqual(result.results[0].name, "dataset")
        self.assertEqual(
            session.calls[0]["url"], "https://ckan.test/api/3/action/package_search"
        )
        self.assertEqual(session.calls[0]["json"], {"rows": 5, "q": "steel"})

    def test_search_without_query_uses_filters_only(self):
        session = FakeSession(
            [
                FakeResponse(
                    json_value={
                        "success": True,
                        "result": {"count": 0, "results": []},
                    }
                )
            ]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        result = client.packages.search(rows=5)

        self.assertEqual(result.count, 0)
        self.assertEqual(session.calls[0]["json"], {"rows": 5})

    def test_patch_adds_package_id_to_payload(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": package_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        info = client.packages.patch("dataset", {"title": "New title"})

        self.assertEqual(info.title, "Dataset")
        self.assertEqual(
            session.calls[0]["url"], "https://ckan.test/api/3/action/package_patch"
        )
        self.assertEqual(
            session.calls[0]["json"], {"title": "New title", "id": "dataset"}
        )

    def test_patch_uses_package_model_id(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": package_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))
        package = CkanPackageInfo.from_dict(package_payload(package_id="pkg-2"))

        _ = client.packages.patch(package, {"title": "New title"})

        self.assertEqual(session.calls[0]["json"]["id"], "pkg-2")

    def test_delete_calls_package_delete(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": None})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        result = client.packages.delete("dataset")

        self.assertIsNone(result)
        self.assertEqual(
            session.calls[0]["url"], "https://ckan.test/api/3/action/package_delete"
        )
        self.assertEqual(session.calls[0]["json"], {"id": "dataset"})

    def test_blank_package_id_is_rejected(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(ValidationError, "package id must be non-empty"):
            client.packages.show(" ")


if __name__ == "__main__":
    unittest.main()
