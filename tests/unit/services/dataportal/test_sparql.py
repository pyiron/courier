import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanPackageInfo, CkanResourceInfo
from courier.services.dataportal import (
    DataportalAssetInfo,
    DataportalClient,
    DataportalDatasetInfo,
)

from ._helpers import FakeSession


def dataset_info(resources: object) -> DataportalDatasetInfo:
    return DataportalDatasetInfo.from_ckan(
        CkanPackageInfo.from_dict(
            {
                "id": "pkg-1",
                "name": "steel-data",
                "resources": resources,
            }
        )
    )


class StubDatasetsResource:
    def __init__(self, dataset: DataportalDatasetInfo):
        self.dataset = dataset
        self.calls: list[str] = []

    def show(self, dataset: str) -> DataportalDatasetInfo:
        self.calls.append(dataset)
        return self.dataset


class TestSparqlEndpointDiscovery(unittest.TestCase):
    def test_absolute_http_url_is_used_directly(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        endpoint = client.sparql.endpoint("https://query.example.test/sparql")

        self.assertEqual(endpoint, "https://query.example.test/sparql")

    def test_non_http_url_is_rejected_instead_of_treated_as_dataset(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(ValidationError, "absolute HTTP"):
            client.sparql.endpoint("ftp://query.example.test/sparql")

    def test_asset_url_is_used(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        asset = DataportalAssetInfo.from_ckan(
            CkanResourceInfo.from_dict(
                {
                    "id": "res-1",
                    "url": "https://query.example.test/sparql",
                    "format": "SPARQL",
                }
            )
        )

        endpoint = client.sparql.endpoint(asset)

        self.assertEqual(endpoint, "https://query.example.test/sparql")

    def test_asset_without_url_is_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        asset = DataportalAssetInfo.from_ckan(
            CkanResourceInfo.from_dict({"id": "res-1", "format": "SPARQL"})
        )

        with self.assertRaisesRegex(ValidationError, "does not include a URL"):
            client.sparql.endpoint(asset)

    def test_dataset_model_discovers_single_sparql_resource(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        dataset = dataset_info(
            [
                {"id": "res-1", "format": "ttl", "url": "https://example.test/data"},
                {
                    "id": "res-2",
                    "format": "SPARQL",
                    "url": "https://query.example.test/sparql",
                },
            ]
        )

        endpoint = client.sparql.endpoint(dataset)

        self.assertEqual(endpoint, "https://query.example.test/sparql")

    def test_dataset_identifier_is_loaded_before_discovery(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        datasets = StubDatasetsResource(
            dataset_info(
                [
                    {
                        "id": "res-2",
                        "format": "sparql",
                        "url": "https://query.example.test/sparql",
                    }
                ]
            )
        )
        client.datasets = cast(Any, datasets)

        endpoint = client.sparql.endpoint("steel-data")

        self.assertEqual(endpoint, "https://query.example.test/sparql")
        self.assertEqual(datasets.calls, ["steel-data"])

    def test_missing_resource_metadata_is_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        dataset = dataset_info([])
        dataset.raw.pop("resources")

        with self.assertRaisesRegex(ValidationError, "resource metadata"):
            client.sparql.endpoint(dataset)

    def test_missing_sparql_resource_is_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        dataset = dataset_info(
            [{"id": "res-1", "format": "ttl", "url": "https://example.test/data"}]
        )

        with self.assertRaisesRegex(ValidationError, "does not include a SPARQL"):
            client.sparql.endpoint(dataset)

    def test_multiple_sparql_resources_are_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        dataset = dataset_info(
            [
                {
                    "id": "res-1",
                    "format": "SPARQL",
                    "url": "https://query.example.test/one",
                },
                {
                    "id": "res-2",
                    "format": "sparql",
                    "url": "https://query.example.test/two",
                },
            ]
        )

        with self.assertRaisesRegex(ValidationError, "multiple SPARQL"):
            client.sparql.endpoint(dataset)

    def test_invalid_sparql_resource_url_is_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        dataset = dataset_info(
            [{"id": "res-1", "format": "SPARQL", "url": "/fuseki/query"}]
        )

        with self.assertRaisesRegex(ValidationError, "absolute HTTP"):
            client.sparql.endpoint(dataset)

    def test_blank_target_is_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(ValidationError, "SPARQL target"):
            client.sparql.endpoint(" ")


if __name__ == "__main__":
    unittest.main()
