import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.ckan import CkanClient
from courier.services.ckan.models import CkanResourceInfo

from ._helpers import FakeResponse, FakeSession


def resource_payload(
    *,
    resource_id: str = "res-1",
    package_id: str = "pkg-1",
    name: str = "data.ttl",
) -> dict[str, Any]:
    return {
        "id": resource_id,
        "name": name,
        "package_id": package_id,
        "url": "https://example.test/data.ttl",
        "format": "ttl",
        "mimetype": "text/turtle",
        "custom": "preserved",
    }


class TestCkanResourceModels(unittest.TestCase):
    def test_resource_info_parses_stable_fields_and_preserves_raw_payload(self):
        payload = resource_payload()

        info = CkanResourceInfo.from_dict(payload)

        self.assertEqual(info.id, "res-1")
        self.assertEqual(info.name, "data.ttl")
        self.assertEqual(info.package_id, "pkg-1")
        self.assertEqual(info.url, "https://example.test/data.ttl")
        self.assertEqual(info.format, "ttl")
        self.assertEqual(info.mimetype, "text/turtle")
        self.assertEqual(info.raw["custom"], "preserved")

    def test_resource_info_accepts_missing_optional_fields(self):
        info = CkanResourceInfo.from_dict({"id": "res-1"})

        self.assertIsNone(info.name)
        self.assertIsNone(info.package_id)
        self.assertIsNone(info.url)
        self.assertIsNone(info.format)
        self.assertIsNone(info.mimetype)


class TestResourcesResource(unittest.TestCase):
    def test_create_calls_resource_create_and_parses_response(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": resource_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        info = client.resources.create(
            {"package_id": "pkg-1", "name": "data.ttl", "url": "https://example.test"}
        )

        self.assertEqual(info.id, "res-1")
        self.assertEqual(session.calls[0]["method"], "POST")
        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/resource_create",
        )
        self.assertEqual(
            session.calls[0]["json"],
            {
                "package_id": "pkg-1",
                "name": "data.ttl",
                "url": "https://example.test",
            },
        )

    def test_create_with_upload_uses_multipart_request(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": resource_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        with TemporaryDirectory() as directory:
            path = Path(directory) / "data.ttl"
            path.write_text("@prefix x: <https://example.test/> .")

            info = client.resources.create(
                {"package_id": "pkg-1", "name": "data.ttl"},
                upload=path,
            )

        self.assertEqual(info.id, "res-1")
        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/resource_create",
        )
        self.assertEqual(
            session.calls[0]["data"],
            {"package_id": "pkg-1", "name": "data.ttl"},
        )
        self.assertIsNone(session.calls[0]["json"])
        upload = session.calls[0]["files"]["upload"]
        self.assertEqual(upload[0], "data.ttl")
        self.assertTrue(upload[1].closed)

    def test_create_with_upload_rejects_payload_upload_field(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with TemporaryDirectory() as directory:
            path = Path(directory) / "data.ttl"
            path.write_text("@prefix x: <https://example.test/> .")

            with self.assertRaisesRegex(ValidationError, "upload field"):
                client.resources.create(
                    {"package_id": "pkg-1", "upload": "not-a-file"},
                    upload=path,
                )

    def test_create_with_upload_rejects_empty_path(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(
            ValidationError,
            "upload filename must be non-empty",
        ):
            client.resources.create({"package_id": "pkg-1"}, upload="")

    def test_create_with_upload_rejects_non_file_path(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with TemporaryDirectory() as directory:
            for path in (Path(directory), Path(directory) / "missing.ttl"):
                with (
                    self.subTest(path=path),
                    self.assertRaisesRegex(
                        ValidationError, "upload path must be a file"
                    ),
                ):
                    client.resources.create({"package_id": "pkg-1"}, upload=path)

    def test_create_with_upload_sets_multipart_content_type(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": resource_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        with TemporaryDirectory() as directory:
            path = Path(directory) / "data.ttl"
            path.write_text("@prefix x: <https://example.test/> .")

            _ = client.resources.create(
                {"package_id": "pkg-1"},
                upload=path,
                content_type="text/turtle",
            )

        upload = session.calls[0]["files"]["upload"]
        self.assertEqual(upload[0], "data.ttl")
        self.assertTrue(upload[1].closed)
        self.assertEqual(upload[2], "text/turtle")

    def test_content_type_requires_upload_path(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(ValidationError, "requires an upload path"):
            client.resources.create(
                {"package_id": "pkg-1", "url": "https://example.test/data.ttl"},
                content_type="text/turtle",
            )

    def test_blank_upload_content_type_is_rejected(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with TemporaryDirectory() as directory:
            path = Path(directory) / "data.ttl"
            path.write_text("@prefix x: <https://example.test/> .")

            with self.assertRaisesRegex(ValidationError, "content_type"):
                client.resources.create(
                    {"package_id": "pkg-1"},
                    upload=path,
                    content_type=" ",
                )

    def test_show_calls_resource_show_with_id(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": resource_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        info = client.resources.show("res-1")

        self.assertEqual(info.name, "data.ttl")
        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/resource_show",
        )
        self.assertEqual(session.calls[0]["json"], {"id": "res-1"})

    def test_show_accepts_resource_model(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": resource_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))
        resource = CkanResourceInfo.from_dict(resource_payload(resource_id="res-2"))

        _ = client.resources.show(resource)

        self.assertEqual(session.calls[0]["json"], {"id": "res-2"})

    def test_patch_adds_resource_id_to_payload(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": resource_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        info = client.resources.patch("res-1", {"description": "Updated"})

        self.assertEqual(info.id, "res-1")
        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/resource_patch",
        )
        self.assertEqual(
            session.calls[0]["json"],
            {"description": "Updated", "id": "res-1"},
        )

    def test_patch_uses_resource_model_id(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": resource_payload()})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))
        resource = CkanResourceInfo.from_dict(resource_payload(resource_id="res-2"))

        _ = client.resources.patch(resource, {"description": "Updated"})

        self.assertEqual(session.calls[0]["json"]["id"], "res-2")

    def test_delete_calls_resource_delete(self):
        session = FakeSession(
            [FakeResponse(json_value={"success": True, "result": None})]
        )
        client = CkanClient("ckan.test", session=cast(Any, session))

        result = client.resources.delete("res-1")

        self.assertIsNone(result)
        self.assertEqual(
            session.calls[0]["url"],
            "https://ckan.test/api/3/action/resource_delete",
        )
        self.assertEqual(session.calls[0]["json"], {"id": "res-1"})

    def test_blank_resource_id_is_rejected(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(ValidationError, "resource id must be non-empty"):
            client.resources.show(" ")

    def test_blank_resource_model_id_is_rejected(self):
        client = CkanClient("ckan.test", session=cast(Any, FakeSession()))
        resource = CkanResourceInfo.from_dict(resource_payload(resource_id=" "))

        with self.assertRaisesRegex(ValidationError, "resource id must be non-empty"):
            client.resources.show(resource)


if __name__ == "__main__":
    unittest.main()
