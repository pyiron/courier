import unittest
from typing import Any, cast
from unittest import mock

import pandas as pd
import pandas.testing as pdt

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanPackageInfo, CkanResourceInfo
from courier.services.dataportal import (
    DataportalAssetInfo,
    DataportalClient,
    DataportalDatasetInfo,
)

from ._helpers import FakeResponse, FakeSession


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
                "invalid resource",
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

    def test_sparql_resource_without_url_is_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        dataset = dataset_info([{"id": "res-1", "format": "SPARQL"}])

        with self.assertRaisesRegex(ValidationError, "does not include a URL"):
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


class TestSparqlQueries(unittest.TestCase):
    def test_query_raw_uses_client_session_for_same_origin_endpoint(self):
        session = FakeSession()
        session.response.text = '{"results":{"bindings":[]}}'
        client = DataportalClient(
            api_token="secret",
            session=cast(Any, session),
        )

        text = client.sparql.query_raw(
            "https://dataportal.material-digital.de/fuseki/query",
            " SELECT * WHERE { ?s ?p ?o } ",
        )

        self.assertEqual(text, '{"results":{"bindings":[]}}')
        self.assertEqual(session.calls[0]["method"], "GET")
        self.assertEqual(
            session.calls[0]["params"],
            {"query": "SELECT * WHERE { ?s ?p ?o }"},
        )
        self.assertEqual(
            session.calls[0]["headers"],
            {"Accept": "application/sparql-results+json"},
        )
        self.assertEqual(session.headers["Authorization"], "secret")

    def test_query_raw_uses_unauthenticated_request_for_cross_origin_endpoint(self):
        session = FakeSession()
        client = DataportalClient(
            api_token="secret",
            verify=False,
            timeout=12,
            session=cast(Any, session),
        )
        response = FakeResponse()
        response.text = "external result"

        with mock.patch(
            "courier.services.dataportal.sparql.requests.get",
            return_value=response,
        ) as get:
            text = client.sparql.query_raw(
                "https://query.example.test/sparql",
                "ASK {}",
                accept="text/plain",
            )

        self.assertEqual(text, "external result")
        self.assertEqual(session.calls, [])
        get.assert_called_once_with(
            "https://query.example.test/sparql",
            params={"query": "ASK {}"},
            headers={"Accept": "text/plain"},
            timeout=12.0,
            verify=False,
        )
        self.assertNotIn("Authorization", get.call_args.kwargs["headers"])

    def test_query_raw_validates_query_and_accept_before_request(self):
        session = FakeSession()
        client = DataportalClient(session=cast(Any, session))

        with self.assertRaisesRegex(ValidationError, "query"):
            client.sparql.query_raw(
                "https://query.example.test/sparql",
                " ",
            )
        with self.assertRaisesRegex(ValidationError, "accept"):
            client.sparql.query_raw(
                "https://query.example.test/sparql",
                "ASK {}",
                accept=" ",
            )

        self.assertEqual(session.calls, [])

    def test_query_json_decodes_object_response(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        result = {"boolean": True}

        with mock.patch.object(
            client.sparql,
            "query_raw",
            return_value='{"boolean": true}',
        ) as query_raw:
            actual = client.sparql.query_json(
                "https://query.example.test/sparql",
                "ASK {}",
            )

        self.assertEqual(actual, result)
        query_raw.assert_called_once_with(
            "https://query.example.test/sparql",
            "ASK {}",
            accept="application/sparql-results+json",
        )

    def test_query_json_rejects_non_object_json(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        with (
            mock.patch.object(client.sparql, "query_raw", return_value="[]"),
            self.assertRaisesRegex(ValidationError, "must be an object"),
        ):
            client.sparql.query_json(
                "https://query.example.test/sparql",
                "SELECT * WHERE {}",
            )

    def test_query_json_propagates_json_decode_error(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        with (
            mock.patch.object(client.sparql, "query_raw", return_value="not json"),
            self.assertRaises(ValueError),
        ):
            client.sparql.query_json(
                "https://query.example.test/sparql",
                "SELECT * WHERE {}",
            )

    def test_query_df_preserves_columns_and_maps_unbound_values_to_none(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        result = {
            "head": {"vars": ["a", "b"]},
            "results": {
                "bindings": [
                    {"b": {"value": "2"}, "a": {"value": "1"}},
                    {"a": {"value": "3"}},
                ]
            },
        }

        with mock.patch.object(client.sparql, "query_json", return_value=result):
            frame = client.sparql.query_df(
                "https://query.example.test/sparql",
                "SELECT ?a ?b WHERE {}",
                ["a", "b"],
            )

        expected = pd.DataFrame([["1", "2"], ["3", None]], columns=["a", "b"])
        pdt.assert_frame_equal(frame, expected)

    def test_query_df_validates_columns_before_query(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        for columns in ([], ["a", " "], cast(Any, ("a",))):
            with (
                self.subTest(columns=columns),
                mock.patch.object(client.sparql, "query_json") as query_json,
                self.assertRaisesRegex(ValidationError, "columns"),
            ):
                client.sparql.query_df(
                    "https://query.example.test/sparql",
                    "SELECT ?a WHERE {}",
                    columns,
                )
            query_json.assert_not_called()

    def test_query_df_rejects_malformed_sparql_results(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        cases = [
            ({}, "include results"),
            ({"results": {}}, "results.bindings"),
            ({"results": {"bindings": ["invalid"]}}, "bindings must be objects"),
        ]

        for result, message in cases:
            with (
                self.subTest(result=result),
                mock.patch.object(client.sparql, "query_json", return_value=result),
                self.assertRaisesRegex(ValidationError, message),
            ):
                client.sparql.query_df(
                    "https://query.example.test/sparql",
                    "SELECT ?a WHERE {}",
                    ["a"],
                )


if __name__ == "__main__":
    unittest.main()
