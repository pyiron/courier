import unittest
from typing import Any

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanPackageInfo, CkanPackageSearchResult
from courier.services.dataportal.models import (
    DataportalDatasetInfo,
    DataportalDatasetSearchResult,
)


def package_payload(
    *,
    package_id: str = "pkg-1",
    name: str = "steel-data",
) -> dict[str, Any]:
    return {
        "id": package_id,
        "name": name,
        "title": "Steel data",
        "notes": "Heat treatment measurements.",
        "owner_org": "materials-org",
        "private": False,
        "type": "dataset",
        "resources": [{"id": "res-1", "name": "data.ttl"}],
        "custom": "preserved",
    }


class TestDataportalDatasetInfo(unittest.TestCase):
    def test_from_ckan_parses_dataset_fields_and_preserves_raw_payload(self):
        package = CkanPackageInfo.from_dict(package_payload())

        dataset = DataportalDatasetInfo.from_ckan(package)

        self.assertEqual(dataset.id, "pkg-1")
        self.assertEqual(dataset.name, "steel-data")
        self.assertEqual(dataset.title, "Steel data")
        self.assertEqual(dataset.notes, "Heat treatment measurements.")
        self.assertEqual(dataset.owner_org, "materials-org")
        self.assertFalse(dataset.private)
        self.assertEqual(dataset.dataset_type, "dataset")
        self.assertEqual(dataset.raw["custom"], "preserved")
        self.assertEqual(dataset.raw["resources"][0]["id"], "res-1")

    def test_optional_fields_default_to_none(self):
        package = CkanPackageInfo.from_dict(
            {
                "id": "pkg-1",
                "name": "steel-data",
            }
        )

        dataset = DataportalDatasetInfo.from_ckan(package)

        self.assertIsNone(dataset.title)
        self.assertIsNone(dataset.notes)
        self.assertIsNone(dataset.owner_org)
        self.assertIsNone(dataset.private)
        self.assertIsNone(dataset.dataset_type)

    def test_string_private_value_is_parsed_without_truthiness_coercion(self):
        payload = package_payload()
        payload["private"] = "false"

        dataset = DataportalDatasetInfo.from_ckan(CkanPackageInfo.from_dict(payload))

        self.assertFalse(dataset.private)

    def test_supported_private_representations_are_parsed(self):
        cases = [
            (1, True),
            (0, False),
            (" true ", True),
        ]

        for value, expected in cases:
            with self.subTest(value=value):
                payload = package_payload()
                payload["private"] = value

                dataset = DataportalDatasetInfo.from_ckan(
                    CkanPackageInfo.from_dict(payload)
                )

                self.assertIs(dataset.private, expected)

    def test_invalid_private_value_is_rejected(self):
        for value in ("private", 0.0, 1.0):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValidationError, "private must be a boolean"),
            ):
                payload = package_payload()
                payload["private"] = value

                DataportalDatasetInfo.from_ckan(CkanPackageInfo.from_dict(payload))


class TestDataportalDatasetSearchResult(unittest.TestCase):
    def test_from_ckan_converts_results_and_preserves_raw_payload(self):
        raw = {
            "count": 1,
            "results": [package_payload()],
            "search_facets": {"organization": {}},
        }
        result = CkanPackageSearchResult.from_dict(raw)

        search = DataportalDatasetSearchResult.from_ckan(result)

        self.assertEqual(search.count, 1)
        self.assertEqual(search.results[0].name, "steel-data")
        self.assertEqual(search.raw["search_facets"], {"organization": {}})


if __name__ == "__main__":
    unittest.main()
