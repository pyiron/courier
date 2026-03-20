import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pandas as pd
import rdflib

from courier.exceptions import ValidationError
from courier.services.ontodocker import OntodockerClient
from courier.services.ontodocker.models import EndpointInfo


class _FakeRequest:
    def __init__(self, method: str):
        self.method = method


class _FakeResponse:
    def __init__(
        self,
        *,
        url: str = "https://example.test/api",
        status_code: int = 200,
        text: str = "ok",
        request: _FakeRequest | None = None,
        raise_for_status_exc: Exception | None = None,
        json_value=None,
        json_exc: Exception | None = None,
    ):
        self.url = url
        self.status_code = status_code
        self.text = text
        self.request = request
        self._raise_for_status_exc = raise_for_status_exc
        self._json_value = json_value
        self._json_exc = json_exc

    def raise_for_status(self):
        if self._raise_for_status_exc is not None:
            raise self._raise_for_status_exc
        return None

    def json(self):
        if self._json_exc is not None:
            raise self._json_exc
        return self._json_value


class _FakeSession:
    def __init__(self):
        self.headers: dict[str, str] = {}
        self.calls: list[dict] = []
        self.response: _FakeResponse = _FakeResponse()

    def request(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class TestOntodockerClientInit(unittest.TestCase):
    def test_resources_are_initialized(self):
        s = _FakeSession()
        c = OntodockerClient("example.org", session=s)

        self.assertIs(c.endpoints.client, c)
        self.assertIs(c.datasets.client, c)
        self.assertIs(c.sparql.client, c)


class TestEndpointsResource(unittest.TestCase):
    def test_list_raw_uses_endpoints_api_and_rectifies_legacy(self):
        s = _FakeSession()
        s.response = _FakeResponse(
            text="['http://example.org:None/api/jena/ds/sparql']",
            request=_FakeRequest("GET"),
        )
        c = OntodockerClient("https://example.org", session=s)

        out = c.endpoints.list_raw()

        self.assertEqual(out, ["https://example.org/api/v1/jena/ds/sparql"])
        self.assertEqual(len(s.calls), 1)
        self.assertEqual(s.calls[0]["method"], "GET")
        self.assertEqual(s.calls[0]["url"], "https://example.org/api/v1/endpoints")

    def test_list_parses_dataset_names(self):
        s = _FakeSession()
        s.response = _FakeResponse(
            text=(
                "['https://example.org/api/v1/jena/a/sparql',"
                " 'https://example.org/api/v1/jena/b/sparql']"
            ),
            request=_FakeRequest("GET"),
        )
        c = OntodockerClient("https://example.org", session=s)

        out = c.endpoints.list()

        self.assertEqual(
            out,
            [
                EndpointInfo(
                    dataset="a",
                    sparql_endpoint="https://example.org/api/v1/jena/a/sparql",
                ),
                EndpointInfo(
                    dataset="b",
                    sparql_endpoint="https://example.org/api/v1/jena/b/sparql",
                ),
            ],
        )


class TestDatasetsResource(unittest.TestCase):
    def test_list_returns_unique_sorted_datasets(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        c.endpoints.list = mock.Mock(
            return_value=[
                EndpointInfo(dataset="b", sparql_endpoint="x"),
                EndpointInfo(dataset="a", sparql_endpoint="y"),
                EndpointInfo(dataset="b", sparql_endpoint="z"),
            ]
        )

        self.assertEqual(c.datasets.list(), ["a", "b"])

    def test_create_validates_name(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)
        with self.assertRaises(ValidationError):
            _ = c.datasets.create("")
        with self.assertRaises(ValidationError):
            _ = c.datasets.create("   ")

    def test_create_uses_put(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="created", request=_FakeRequest("PUT"))
        c = OntodockerClient("https://example.org", session=s)

        out = c.datasets.create(" ds ")

        self.assertEqual(out, "created")
        self.assertEqual(s.calls[0]["method"], "PUT")
        self.assertEqual(s.calls[0]["url"], "https://example.org/api/v1/jena/ds")

    def test_delete_validates_name(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)
        with self.assertRaises(ValidationError):
            _ = c.datasets.delete("")
        with self.assertRaises(ValidationError):
            _ = c.datasets.delete("   ")

    def test_delete_uses_delete(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="deleted", request=_FakeRequest("DELETE"))
        c = OntodockerClient("https://example.org", session=s)

        out = c.datasets.delete(" ds ")

        self.assertEqual(out, "deleted")
        self.assertEqual(s.calls[0]["method"], "DELETE")
        self.assertEqual(s.calls[0]["url"], "https://example.org/api/v1/jena/ds")

    def test_download_turtle_validates_name_and_filename(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        with self.assertRaises(ValidationError):
            _ = c.datasets.download_turtle("", filename=None)

        with self.assertRaises(ValidationError):
            _ = c.datasets.download_turtle("ds", filename="   ")

    def test_download_turtle_writes_file_when_requested(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="@prefix : <x> .", request=_FakeRequest("GET"))
        c = OntodockerClient("https://example.org", session=s)

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.ttl"
            out = c.datasets.download_turtle("ds", filename=path)

            self.assertEqual(out, "@prefix : <x> .")
            self.assertEqual(path.read_text(encoding="utf-8"), "@prefix : <x> .")

    def test_upload_turtlefile_validates_inputs(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        with self.assertRaises(ValidationError):
            _ = c.datasets.upload_turtlefile("", "x.ttl")

        with self.assertRaises(ValidationError):
            _ = c.datasets.upload_turtlefile("ds", "")

    def test_upload_turtlefile_uses_post_with_file_field(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="ok", request=_FakeRequest("POST"))
        c = OntodockerClient("https://example.org", session=s)

        with TemporaryDirectory() as tmp:
            turtlefile = Path(tmp) / "in.ttl"
            turtlefile.write_text("@prefix : <x> .", encoding="utf-8")

            out = c.datasets.upload_turtlefile("ds", str(turtlefile))

        self.assertEqual(out, "ok")
        self.assertEqual(s.calls[0]["method"], "POST")
        self.assertEqual(s.calls[0]["url"], "https://example.org/api/v1/jena/ds")
        self.assertIn("file", s.calls[0]["files"])

    def test_upload_graph_validates_name_and_filename(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        graph = rdflib.Graph()

        with self.assertRaises(ValidationError):
            _ = c.datasets.upload_graph("", graph)

        with self.assertRaises(ValidationError):
            _ = c.datasets.upload_graph("ds", graph, filename="   ")

    def test_upload_graph_validates_graph_type(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        with self.assertRaises(ValidationError):
            _ = c.datasets.upload_graph("ds", object())

    def test_upload_graph_propagates_serialize_errors(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        class _FakeGraph(rdflib.Graph):
            def serialize(self, *, format: str):
                raise RuntimeError("boom")

        with self.assertRaisesRegex(RuntimeError, "boom"):
            _ = c.datasets.upload_graph("ds", _FakeGraph())

    def test_upload_graph_serializes_bytes_and_posts(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="ok", request=_FakeRequest("POST"))
        c = OntodockerClient("https://example.org", session=s)

        class _FakeGraph(rdflib.Graph):
            def serialize(self, *, format: str):
                self.format = format
                return b"@prefix : <x> ."

        out = c.datasets.upload_graph("ds", _FakeGraph())

        self.assertEqual(out, "ok")
        self.assertEqual(s.calls[0]["method"], "POST")
        self.assertEqual(s.calls[0]["url"], "https://example.org/api/v1/jena/ds")
        posted = s.calls[0]["files"]["file"]
        self.assertEqual(posted[0], "graph.ttl")
        self.assertEqual(posted[2], "text/turtle")

    def test_upload_graph_serializes_and_posts(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="ok", request=_FakeRequest("POST"))
        c = OntodockerClient("https://example.org", session=s)

        class _FakeGraph(rdflib.Graph):
            def serialize(self, *, format: str):
                self.format = format
                return "@prefix : <x> ."

        g = _FakeGraph()
        with TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "graph.ttl"
            out = c.datasets.upload_graph("ds", g, filename=out_path)

            self.assertEqual(out, "ok")
            self.assertEqual(out_path.read_text(encoding="utf-8"), "@prefix : <x> .")

        self.assertEqual(s.calls[0]["method"], "POST")
        self.assertEqual(s.calls[0]["url"], "https://example.org/api/v1/jena/ds")
        posted = s.calls[0]["files"]["file"]
        self.assertEqual(posted[0], "graph.ttl")
        self.assertEqual(posted[2], "text/turtle")


class TestSparqlResource(unittest.TestCase):
    def test_endpoint_validates_dataset(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)
        with self.assertRaises(ValidationError):
            _ = c.sparql.endpoint("")

    def test_endpoint_builds_url(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)
        self.assertEqual(
            c.sparql.endpoint(" ds "),
            "https://example.org/api/v1/jena/ds/sparql",
        )

    def test_query_raw_validates_dataset_and_query(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        with self.assertRaises(ValidationError):
            _ = c.sparql.query_raw("", "SELECT * WHERE {}")

        with self.assertRaises(ValidationError):
            _ = c.sparql.query_raw("ds", "")

    def test_query_raw_uses_get_with_query_param_and_accept_header(self):
        s = _FakeSession()
        s.response = _FakeResponse(text="{}", request=_FakeRequest("GET"))
        c = OntodockerClient("https://example.org", session=s)

        out = c.sparql.query_raw("ds", "SELECT * WHERE { ?s ?p ?o }")

        self.assertEqual(out, "{}")
        self.assertEqual(s.calls[0]["method"], "GET")
        self.assertEqual(
            s.calls[0]["url"],
            "https://example.org/api/v1/jena/ds/sparql",
        )
        self.assertEqual(
            s.calls[0]["params"],
            {"query": "SELECT * WHERE { ?s ?p ?o }"},
        )
        self.assertEqual(
            s.calls[0]["headers"],
            {"Accept": "application/sparql-results+json"},
        )

    def test_query_df_validates_query_and_columns(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", session=s)

        with self.assertRaises(ValidationError):
            _ = c.sparql.query_df("ds", "", columns=["a"])

        with self.assertRaises(ValidationError):
            _ = c.sparql.query_df("ds", "SELECT ?a WHERE {}", columns=[])

    def test_query_df_uses_sparqlwrapper_and_adds_auth_header_when_token_set(self):
        s = _FakeSession()
        c = OntodockerClient("https://example.org", token="abc", session=s)

        class _FakeSparqlWrapper:
            def __init__(self, endpoint: str):
                self.endpoint = endpoint
                self.headers: dict[str, str] = {}
                self.query = None
                self.return_format = None

            def setReturnFormat(self, fmt: str):
                self.return_format = fmt

            def addCustomHttpHeader(self, key: str, value: str):
                self.headers[key] = value

            def setQuery(self, query: str):
                self.query = query

            def queryAndConvert(self):
                return {
                    "results": {
                        "bindings": [
                            {"a": {"value": "1"}, "b": {"value": "2"}},
                        ]
                    }
                }

        with mock.patch(
            "courier.services.ontodocker.sparql.SPARQLWrapper", _FakeSparqlWrapper
        ):
            df = c.sparql.query_df("ds", "SELECT ?a ?b WHERE {}", columns=["a", "b"])

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(list(df.columns), ["a", "b"])
        self.assertEqual(df.iloc[0].tolist(), ["1", "2"])


if __name__ == "__main__":
    unittest.main()
