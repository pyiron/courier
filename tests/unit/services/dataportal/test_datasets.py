import unittest
from typing import Any, cast

from praeco.exceptions import ValidationError
from praeco.metadata import Person, PublicationMetadata
from praeco.services.ckan.models import CkanPackageInfo, CkanPackageSearchResult
from praeco.services.dataportal import (
    DataportalClient,
    DataportalDatasetInfo,
    DataportalMetadata,
)

from ._helpers import FakeSession


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
        "resources": [],
    }


class StubPackagesResource:
    def __init__(self):
        self.calls: list[tuple[Any, ...]] = []

    def show(self, package: str) -> CkanPackageInfo:
        self.calls.append(("show", package))
        return CkanPackageInfo.from_dict(package_payload())

    def create(self, payload: dict[str, Any]) -> CkanPackageInfo:
        self.calls.append(("create", payload))
        return CkanPackageInfo.from_dict(package_payload())

    def search(
        self, query: str | None = None, **filters: Any
    ) -> CkanPackageSearchResult:
        self.calls.append(("search", query, filters))
        return CkanPackageSearchResult.from_dict(
            {"count": 1, "results": [package_payload()]}
        )

    def patch(self, package: str, payload: dict[str, Any]) -> CkanPackageInfo:
        self.calls.append(("patch", package, payload))
        return CkanPackageInfo.from_dict(package_payload())

    def delete(self, package: str) -> None:
        self.calls.append(("delete", package))


def client_with_stub() -> tuple[DataportalClient, StubPackagesResource]:
    client = DataportalClient(session=cast(Any, FakeSession()))
    packages = StubPackagesResource()
    client.packages = cast(Any, packages)
    return client, packages


def publication_metadata() -> PublicationMetadata:
    return PublicationMetadata(
        title="Steel data",
        description="Heat treatment measurements.",
        creators=[Person(name="Doe, Jane")],
        keywords=["steel"],
        license="CC-BY-4.0",
    )


class TestDatasetsResource(unittest.TestCase):
    def test_create_serializes_dataportal_metadata_before_delegation(self):
        client, packages = client_with_stub()
        metadata = DataportalMetadata(
            metadata=publication_metadata(),
            owner_org="materials-org",
            private=False,
        )

        dataset = client.datasets.create(metadata)

        self.assertEqual(dataset.id, "pkg-1")
        self.assertEqual(
            packages.calls,
            [
                (
                    "create",
                    {
                        "name": "steel-data",
                        "title": "Steel data",
                        "notes": "Heat treatment measurements.",
                        "tags": [{"name": "steel"}],
                        "owner_org": "materials-org",
                        "private": False,
                        "license_id": "CC-BY-4.0",
                        "creator": [{"name": "Doe, Jane", "type": "Person"}],
                        "extras": [
                            {
                                "key": "creators",
                                "value": '[{"name":"Doe, Jane"}]',
                            }
                        ],
                    },
                )
            ],
        )

    def test_create_preserves_raw_mapping_escape_hatch(self):
        client, packages = client_with_stub()
        payload = {"name": "raw-dataset", "title": "Raw dataset"}

        dataset = client.datasets.create(payload)

        self.assertEqual(dataset.name, "steel-data")
        self.assertEqual(packages.calls, [("create", payload)])

    def test_show_delegates_to_packages_and_converts_result(self):
        client, packages = client_with_stub()

        dataset = client.datasets.show("steel-data")

        self.assertIsInstance(dataset, DataportalDatasetInfo)
        self.assertEqual(dataset.id, "pkg-1")
        self.assertEqual(packages.calls, [("show", "steel-data")])

    def test_show_accepts_dataset_model_and_uses_its_id(self):
        client, packages = client_with_stub()
        dataset = DataportalDatasetInfo.from_ckan(
            CkanPackageInfo.from_dict(package_payload(package_id="pkg-2"))
        )

        _ = client.datasets.show(dataset)

        self.assertEqual(packages.calls, [("show", "pkg-2")])

    def test_search_delegates_query_and_filters(self):
        client, packages = client_with_stub()

        result = client.datasets.search("steel", rows=5, fq="private:false")

        self.assertEqual(result.count, 1)
        self.assertEqual(result.results[0].name, "steel-data")
        self.assertEqual(
            packages.calls,
            [("search", "steel", {"rows": 5, "fq": "private:false"})],
        )

    def test_patch_serializes_dataportal_metadata_and_uses_dataset_id(self):
        client, packages = client_with_stub()
        dataset = DataportalDatasetInfo.from_ckan(
            CkanPackageInfo.from_dict(package_payload(package_id="pkg-2"))
        )
        metadata = DataportalMetadata(
            metadata=publication_metadata(),
            name="steel-data",
        )

        updated = client.datasets.patch(dataset, metadata)

        self.assertEqual(updated.id, "pkg-1")
        self.assertEqual(packages.calls[0][0:2], ("patch", "pkg-2"))
        self.assertEqual(packages.calls[0][2]["title"], "Steel data")

    def test_patch_preserves_raw_mapping_escape_hatch(self):
        client, packages = client_with_stub()
        payload = {"private": True}

        _ = client.datasets.patch("steel-data", payload)

        self.assertEqual(packages.calls, [("patch", "steel-data", payload)])

    def test_plain_publication_metadata_is_rejected_for_create_and_patch(self):
        client, packages = client_with_stub()
        publication = publication_metadata()

        for operation in (
            lambda: client.datasets.create(cast(Any, publication)),
            lambda: client.datasets.patch(
                "steel-data",
                cast(Any, publication),
            ),
        ):
            with (
                self.subTest(operation=operation),
                self.assertRaisesRegex(
                    ValidationError,
                    "wrapped in DataportalMetadata",
                ),
            ):
                operation()

        self.assertEqual(packages.calls, [])

    def test_invalid_metadata_type_is_rejected(self):
        client, packages = client_with_stub()

        with self.assertRaisesRegex(
            ValidationError,
            "DataportalMetadata or a mapping",
        ):
            client.datasets.create(cast(Any, object()))

        self.assertEqual(packages.calls, [])

    def test_delete_delegates_dataset_id(self):
        client, packages = client_with_stub()

        result = client.datasets.delete("steel-data")

        self.assertIsNone(result)
        self.assertEqual(packages.calls, [("delete", "steel-data")])

    def test_blank_dataset_id_is_rejected(self):
        client, packages = client_with_stub()

        with self.assertRaisesRegex(ValidationError, "dataset id must be non-empty"):
            client.datasets.show(" ")

        self.assertEqual(packages.calls, [])


if __name__ == "__main__":
    unittest.main()
