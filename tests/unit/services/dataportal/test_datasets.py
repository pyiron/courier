import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanPackageInfo, CkanPackageSearchResult
from courier.services.dataportal import (
    DataportalClient,
    DataportalDatasetInfo,
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

    def search(self, query: str | None = None, **filters: Any) -> CkanPackageSearchResult:
        self.calls.append(("search", query, filters))
        return CkanPackageSearchResult.from_dict(
            {"count": 1, "results": [package_payload()]}
        )

    def delete(self, package: str) -> None:
        self.calls.append(("delete", package))


def client_with_stub() -> tuple[DataportalClient, StubPackagesResource]:
    client = DataportalClient(session=cast(Any, FakeSession()))
    packages = StubPackagesResource()
    client.packages = cast(Any, packages)
    return client, packages


class TestDatasetsResource(unittest.TestCase):
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
