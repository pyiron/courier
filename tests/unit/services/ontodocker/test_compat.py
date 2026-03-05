import unittest

import pandas as pd
import pandas.testing as pdt

from courier.services.ontodocker import _compat


class TestRectifyEndpoints(unittest.TestCase):
    def test_http_replaced_with_https(self):
        result = _compat.rectify_endpoints("http://example.com/api")
        self.assertIn("https://", result)
        self.assertNotIn("http://", result)

    def test_none_port_replaced(self):
        result = _compat.rectify_endpoints(
            "https://example.com:None/api/jena/ds/sparql"
        )
        self.assertIn("/api/v1/jena", result)
        self.assertNotIn(":None", result)

    def test_443_port_replaced(self):
        result = _compat.rectify_endpoints("https://example.com:443/api/jena/ds/sparql")
        self.assertIn("/api/v1/jena", result)
        self.assertNotIn(":443", result)

    def test_already_normalized_unchanged(self):
        url = "https://example.com/api/v1/jena/ds/sparql"
        self.assertEqual(_compat.rectify_endpoints(url), url)

    def test_combined_replacements(self):
        raw = "http://example.com:None/api/jena/ds/sparql"
        result = _compat.rectify_endpoints(raw)
        self.assertIn("https://", result)
        self.assertIn("/api/v1/jena", result)


class TestParseEndpointsResponse(unittest.TestCase):
    def test_valid_list_of_strings(self):
        text = "['https://example.com/api/v1/jena/ds/sparql']"
        result = _compat.parse_endpoints_response(text, rectify=False)
        self.assertEqual(result, ["https://example.com/api/v1/jena/ds/sparql"])

    def test_multiple_endpoints(self):
        text = "['https://a.com/api/v1/jena/ds1/sparql', 'https://b.com/api/v1/jena/ds2/sparql']"
        result = _compat.parse_endpoints_response(text, rectify=False)
        self.assertEqual(len(result), 2)

    def test_rectify_applied_by_default(self):
        text = "['http://example.com:None/api/jena/ds/sparql']"
        result = _compat.parse_endpoints_response(text)
        self.assertTrue(result[0].startswith("https://"))
        self.assertIn("/api/v1/jena", result[0])

    def test_rectify_false_skips_normalization(self):
        text = "['http://example.com/path']"
        result = _compat.parse_endpoints_response(text, rectify=False)
        self.assertEqual(result, ["http://example.com/path"])

    def test_invalid_literal_raises_value_error(self):
        with self.assertRaises(ValueError):
            _compat.parse_endpoints_response("not a valid literal", rectify=False)

    def test_non_list_raises_value_error(self):
        with self.assertRaises(ValueError):
            _compat.parse_endpoints_response("{'a': 1}", rectify=False)

    def test_list_of_non_strings_raises_value_error(self):
        with self.assertRaises(ValueError):
            _compat.parse_endpoints_response("[1, 2, 3]", rectify=False)


class TestExtractDatasetNames(unittest.TestCase):
    def test_sparql_suffix(self):
        endpoints = ["https://example.com/api/v1/jena/mydataset/sparql"]
        self.assertEqual(_compat.extract_dataset_names(endpoints), ["mydataset"])

    def test_no_sparql_suffix(self):
        endpoints = ["https://example.com/api/v1/jena/mydataset"]
        self.assertEqual(_compat.extract_dataset_names(endpoints), ["mydataset"])

    def test_multiple_endpoints(self):
        endpoints = [
            "https://example.com/api/v1/jena/ds1/sparql",
            "https://example.com/api/v1/jena/ds2",
        ]
        self.assertEqual(_compat.extract_dataset_names(endpoints), ["ds1", "ds2"])

    def test_unexpected_format_raises_value_error(self):
        with self.assertRaises(ValueError):
            _compat.extract_dataset_names(["https://example.com/wrong/path"])

    def test_trailing_slash_handled(self):
        endpoints = ["https://example.com/api/v1/jena/mydataset/sparql/"]
        self.assertEqual(_compat.extract_dataset_names(endpoints), ["mydataset"])

    def test_empty_list(self):
        self.assertEqual(_compat.extract_dataset_names([]), [])


class TestMakeDataframe(unittest.TestCase):
    def _make_result(self, vars_: list[str], bindings: list[dict]) -> dict:
        return {
            "head": {"vars": vars_},
            "results": {"bindings": bindings},
        }

    def test_basic_conversion(self):
        result = self._make_result(
            ["name", "value"],
            [{"name": {"value": "Alice"}, "value": {"value": "42"}}],
        )
        df = _compat.make_dataframe(result, ["name", "value"])
        expected = pd.DataFrame([["Alice", "42"]], columns=["name", "value"])
        pdt.assert_frame_equal(df, expected)

    def test_fallback_to_columns_when_no_head_vars(self):
        result = {
            "results": {
                "bindings": [{"x": {"value": "hello"}, "y": {"value": "world"}}]
            }
        }
        df = _compat.make_dataframe(result, ["x", "y"])
        self.assertEqual(list(df.columns), ["x", "y"])

    def test_empty_bindings(self):
        result = self._make_result(["a", "b"], [])
        df = _compat.make_dataframe(result, ["a", "b"])
        self.assertEqual(list(df.columns), ["a", "b"])
        self.assertEqual(len(df), 0)

    def test_multiple_rows(self):
        result = self._make_result(
            ["x"],
            [{"x": {"value": "1"}}, {"x": {"value": "2"}}],
        )
        df = _compat.make_dataframe(result, ["x"])
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df["x"]), ["1", "2"])

    def test_missing_variable_in_binding_raises_value_error(self):
        result = self._make_result(
            ["a", "b"],
            [{"a": {"value": "1"}}],  # "b" is missing
        )
        with self.assertRaises(ValueError):
            _compat.make_dataframe(result, ["a", "b"])
