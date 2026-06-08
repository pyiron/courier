import unittest
from typing import Any

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanResourceInfo
from courier.services.dataportal.models import DataportalAssetInfo


def resource_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": "res-1",
        "package_id": "pkg-1",
        "name": "data.ttl",
        "description": "RDF data.",
        "url": "https://example.test/data.ttl",
        "format": "ttl",
        "mimetype": "text/turtle",
        "size": 128,
        "custom": "preserved",
    }
    payload.update(overrides)
    return payload


class TestDataportalAssetInfo(unittest.TestCase):
    def test_from_ckan_parses_asset_fields_and_preserves_raw_payload(self):
        asset = DataportalAssetInfo.from_ckan(
            CkanResourceInfo.from_dict(resource_payload())
        )

        self.assertEqual(asset.id, "res-1")
        self.assertEqual(asset.dataset_id, "pkg-1")
        self.assertEqual(asset.name, "data.ttl")
        self.assertEqual(asset.description, "RDF data.")
        self.assertEqual(asset.url, "https://example.test/data.ttl")
        self.assertEqual(asset.format, "ttl")
        self.assertEqual(asset.content_type, "text/turtle")
        self.assertEqual(asset.size, 128)
        self.assertEqual(asset.raw["custom"], "preserved")

    def test_filesize_is_used_when_size_is_absent(self):
        payload = resource_payload(filesize="42")
        del payload["size"]

        asset = DataportalAssetInfo.from_ckan(CkanResourceInfo.from_dict(payload))

        self.assertEqual(asset.size, 42)

    def test_optional_fields_default_to_none(self):
        asset = DataportalAssetInfo.from_ckan(
            CkanResourceInfo.from_dict({"id": "res-1"})
        )

        self.assertIsNone(asset.dataset_id)
        self.assertIsNone(asset.name)
        self.assertIsNone(asset.description)
        self.assertIsNone(asset.url)
        self.assertIsNone(asset.format)
        self.assertIsNone(asset.content_type)
        self.assertIsNone(asset.size)

    def test_invalid_size_is_rejected(self):
        for value in (True, "large", -1):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValidationError, "asset size"),
            ):
                DataportalAssetInfo.from_ckan(
                    CkanResourceInfo.from_dict(resource_payload(size=value))
                )


if __name__ == "__main__":
    unittest.main()
