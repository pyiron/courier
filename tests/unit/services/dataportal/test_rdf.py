import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.services.ckan.models import CkanPackageInfo
from courier.services.dataportal import DataportalClient, DataportalDatasetInfo

from ._helpers import FakeResponse, FakeSession


class TestRdfResource(unittest.TestCase):
    def test_dataset_url_uses_dcat_endpoint_shape(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        url = client.rdf.dataset_url("steel data", format="TTL")

        self.assertEqual(
            url,
            "https://dataportal.material-digital.de/dataset/steel%20data.ttl",
        )

    def test_dataset_url_accepts_dataset_model_and_uses_id(self):
        client = DataportalClient(session=cast(Any, FakeSession()))
        dataset = DataportalDatasetInfo.from_ckan(
            CkanPackageInfo.from_dict({"id": "pkg-1", "name": "steel-data"})
        )

        url = client.rdf.dataset_url(dataset, format="jsonld")

        self.assertEqual(
            url,
            "https://dataportal.material-digital.de/dataset/pkg-1.jsonld",
        )

    def test_supported_formats_are_accepted(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        for format in ("ttl", "rdf", "xml", "jsonld", "n3"):
            with self.subTest(format=format):
                self.assertTrue(
                    client.rdf.dataset_url("pkg-1", format=format).endswith(
                        f".{format}"
                    )
                )

    def test_invalid_dataset_and_format_are_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        with self.assertRaisesRegex(ValidationError, "dataset id"):
            client.rdf.dataset_url(" ")
        with self.assertRaisesRegex(ValidationError, "unsupported RDF format"):
            client.rdf.dataset_url("pkg-1", format="csv")

    def test_non_string_format_is_rejected(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        for format in (None, 1, []):
            with (
                self.subTest(format=format),
                self.assertRaisesRegex(ValidationError, "RDF format must be a string"),
            ):
                client.rdf.dataset_url("pkg-1", format=cast(Any, format))

    def test_dataset_retrieves_text_from_generated_url(self):
        session = FakeSession()
        session.response = FakeResponse()
        session.response.text = "@prefix dcat: <http://www.w3.org/ns/dcat#> ."
        client = DataportalClient(session=cast(Any, session))

        text = client.rdf.dataset("pkg-1", format="ttl")

        self.assertEqual(text, "@prefix dcat: <http://www.w3.org/ns/dcat#> .")
        self.assertEqual(session.calls[0]["method"], "GET")
        self.assertEqual(
            session.calls[0]["url"],
            "https://dataportal.material-digital.de/dataset/pkg-1.ttl",
        )


if __name__ == "__main__":
    unittest.main()
