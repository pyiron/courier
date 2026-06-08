import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanPackageInfo, CkanResourceInfo
from courier.services.dataportal import (
    DataportalAssetInfo,
    DataportalClient,
    DataportalDatasetInfo,
)

from ._helpers import FakeSession


def resource_payload(
    *,
    resource_id: str = "res-1",
    package_id: str = "pkg-1",
) -> dict[str, Any]:
    return {
        "id": resource_id,
        "package_id": package_id,
        "name": "data.csv",
        "description": "Measurements.",
        "url": "https://example.test/data.csv",
        "format": "csv",
        "mimetype": "text/csv",
    }


class StubResourcesResource:
    def __init__(self):
        self.calls: list[tuple[Any, ...]] = []

    def create(self, payload: dict[str, Any], **kwargs: Any) -> CkanResourceInfo:
        self.calls.append(("create", payload, kwargs))
        return CkanResourceInfo.from_dict(resource_payload())

    def show(self, resource: str) -> CkanResourceInfo:
        self.calls.append(("show", resource))
        return CkanResourceInfo.from_dict(resource_payload())

    def patch(
        self,
        resource: str,
        payload: dict[str, Any],
    ) -> CkanResourceInfo:
        self.calls.append(("patch", resource, payload))
        return CkanResourceInfo.from_dict(resource_payload())

    def delete(self, resource: str) -> None:
        self.calls.append(("delete", resource))


def client_with_stub() -> tuple[DataportalClient, StubResourcesResource]:
    client = DataportalClient(session=cast(Any, FakeSession()))
    resources = StubResourcesResource()
    client.resources = cast(Any, resources)
    return client, resources


class TestAssetsResource(unittest.TestCase):
    def test_upload_delegates_file_and_metadata(self):
        client, resources = client_with_stub()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "measurements.csv"
            path.write_text("temperature,value\n300,1\n")

            asset = client.assets.upload(
                "pkg-1",
                path,
                name="data.csv",
                description="Measurements.",
                format="csv",
                content_type="text/csv",
            )

        self.assertEqual(asset.id, "res-1")
        self.assertEqual(
            resources.calls,
            [
                (
                    "create",
                    {
                        "package_id": "pkg-1",
                        "name": "data.csv",
                        "description": "Measurements.",
                        "format": "csv",
                        "mimetype": "text/csv",
                    },
                    {"upload": path, "content_type": "text/csv"},
                )
            ],
        )

    def test_upload_uses_filename_as_default_name(self):
        client, resources = client_with_stub()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "measurements.csv"
            path.write_text("temperature,value\n300,1\n")

            _ = client.assets.upload("pkg-1", path)

        self.assertEqual(resources.calls[0][1]["name"], "measurements.csv")
        self.assertEqual(
            resources.calls[0][2],
            {"upload": path, "content_type": None},
        )

    def test_upload_rejects_non_file_path(self):
        client, resources = client_with_stub()

        with (
            TemporaryDirectory() as directory,
            self.assertRaisesRegex(ValidationError, "must be a file"),
        ):
            client.assets.upload("pkg-1", directory)

        self.assertEqual(resources.calls, [])

    def test_upload_rejects_blank_optional_metadata(self):
        client, resources = client_with_stub()

        with TemporaryDirectory() as directory:
            path = Path(directory) / "measurements.csv"
            path.write_text("temperature,value\n300,1\n")

            for field_name in ("name", "description", "format", "content_type"):
                with (
                    self.subTest(field_name=field_name),
                    self.assertRaisesRegex(ValidationError, field_name),
                ):
                    client.assets.upload(
                        "pkg-1",
                        path,
                        **{field_name: " "},
                    )

        self.assertEqual(resources.calls, [])

    def test_create_url_delegates_ckan_payload_and_converts_result(self):
        client, resources = client_with_stub()

        asset = client.assets.create_url(
            "pkg-1",
            url="https://example.test/data.csv",
            name="data.csv",
            description="Measurements.",
            format="csv",
        )

        self.assertIsInstance(asset, DataportalAssetInfo)
        self.assertEqual(asset.id, "res-1")
        self.assertEqual(
            resources.calls,
            [
                (
                    "create",
                    {
                        "package_id": "pkg-1",
                        "url": "https://example.test/data.csv",
                        "name": "data.csv",
                        "description": "Measurements.",
                        "format": "csv",
                    },
                    {},
                )
            ],
        )

    def test_create_url_accepts_dataset_model(self):
        client, resources = client_with_stub()
        dataset = DataportalDatasetInfo.from_ckan(
            CkanPackageInfo.from_dict({"id": "pkg-2", "name": "dataset"})
        )

        _ = client.assets.create_url(
            dataset,
            url="https://example.test/data.csv",
        )

        self.assertEqual(resources.calls[0][1]["package_id"], "pkg-2")

    def test_create_url_rejects_non_http_absolute_urls(self):
        client, resources = client_with_stub()

        for url in ("data.csv", "/data.csv", "ftp://example.test/data.csv", " "):
            with (
                self.subTest(url=url),
                self.assertRaisesRegex(ValidationError, "url"),
            ):
                client.assets.create_url("pkg-1", url=url)

        self.assertEqual(resources.calls, [])

    def test_create_url_rejects_blank_optional_fields(self):
        client, resources = client_with_stub()

        for field_name in ("name", "description", "format"):
            with (
                self.subTest(field_name=field_name),
                self.assertRaisesRegex(ValidationError, field_name),
            ):
                client.assets.create_url(
                    "pkg-1",
                    url="https://example.test/data.csv",
                    **{field_name: " "},
                )

        self.assertEqual(resources.calls, [])

    def test_show_accepts_asset_model_and_uses_its_id(self):
        client, resources = client_with_stub()
        asset = DataportalAssetInfo.from_ckan(
            CkanResourceInfo.from_dict(resource_payload(resource_id="res-2"))
        )

        result = client.assets.show(asset)

        self.assertEqual(result.id, "res-1")
        self.assertEqual(resources.calls, [("show", "res-2")])

    def test_patch_delegates_raw_metadata_mapping(self):
        client, resources = client_with_stub()
        payload = {"name": "renamed.csv", "description": "Updated."}

        asset = client.assets.patch("res-1", payload)

        self.assertEqual(asset.id, "res-1")
        self.assertEqual(resources.calls, [("patch", "res-1", payload)])

    def test_delete_delegates_asset_id(self):
        client, resources = client_with_stub()

        result = client.assets.delete("res-1")

        self.assertIsNone(result)
        self.assertEqual(resources.calls, [("delete", "res-1")])

    def test_blank_dataset_and_asset_ids_are_rejected(self):
        client, resources = client_with_stub()

        with self.assertRaisesRegex(ValidationError, "dataset id"):
            client.assets.create_url(" ", url="https://example.test/data.csv")
        with self.assertRaisesRegex(ValidationError, "asset id"):
            client.assets.show(" ")

        self.assertEqual(resources.calls, [])


if __name__ == "__main__":
    unittest.main()
