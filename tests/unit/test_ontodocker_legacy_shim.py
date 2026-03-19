import unittest
import warnings
from unittest import mock

import pandas as pd

import courier


class TestOntodockerLegacyShim(unittest.TestCase):
    def test_legacy_functions_emit_deprecation_warning(self):
        with (
            mock.patch("courier.ontodocker.OntodockerClient"),
            self.assertWarns(DeprecationWarning),
        ):
            _ = courier.get_all_dataset_sparql_endpoints("example.org")

    def test_get_all_dataset_sparql_endpoints_delegates_and_passes_options(self):
        fake_client = mock.Mock()
        fake_client.endpoints.list_raw.return_value = [
            "https://example.org/api/v1/jena/ds/sparql"
        ]
        fake_client.endpoints.rectify_legacy = True

        with (
            mock.patch(
                "courier.ontodocker.OntodockerClient", return_value=fake_client
            ) as m,
            self.assertWarns(DeprecationWarning),
        ):
            out = courier.get_all_dataset_sparql_endpoints(
                "example.org",
                token="abc",
                timeout=(1, 2),
                verify=False,
                scheme="http",
                rectify=False,
            )

        self.assertEqual(out, ["https://example.org/api/v1/jena/ds/sparql"])
        m.assert_called_once_with(
            "example.org",
            token="abc",
            default_scheme="http",
            timeout=(1.0, 2.0),
            verify=False,
        )
        self.assertFalse(fake_client.endpoints.rectify_legacy)
        fake_client.endpoints.list_raw.assert_called_once_with()

    def test_download_dataset_as_turtle_file_returns_path_and_warns_on_default(self):
        fake_client = mock.Mock()
        fake_client.datasets.download_turtle.return_value = "ttl-content"

        with (
            mock.patch("courier.ontodocker.OntodockerClient", return_value=fake_client),
            mock.patch("courier.ontodocker.os.getcwd", return_value="/tmp"),
            warnings.catch_warnings(record=True) as w,
        ):
            warnings.simplefilter("always")
            out = courier.download_dataset_as_turtle_file("example.org", "ds")

        self.assertTrue(
            any(issubclass(warning.category, DeprecationWarning) for warning in w)
        )
        self.assertTrue(any(issubclass(warning.category, UserWarning) for warning in w))
        self.assertEqual(out, "/tmp/ds.ttl")
        fake_client.datasets.download_turtle.assert_called_once()

    def test_send_query_delegates_to_sparql_query_df(self):
        fake_client = mock.Mock()
        fake_client.sparql.query_df.return_value = pd.DataFrame([["1"]], columns=["a"])

        with (
            mock.patch("courier.ontodocker.OntodockerClient", return_value=fake_client),
            self.assertWarns(DeprecationWarning),
        ):
            df = courier.ontodocker.send_query(
                "https://example.org/api/v1/jena/ds/sparql",
                "SELECT ?a WHERE {}",
                columns=["a"],
                token="abc",
            )

        self.assertIsInstance(df, pd.DataFrame)
        fake_client.sparql.query_df.assert_called_once_with(
            "ds",
            "SELECT ?a WHERE {}",
            columns=["a"],
        )


if __name__ == "__main__":
    unittest.main()
