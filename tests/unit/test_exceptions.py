import unittest

from courier.exceptions import (
    CourierError,
    HttpError,
    InvalidAddressError,
    ValidationError,
)


class TestExceptionHierarchy(unittest.TestCase):
    def test_inheritance(self):
        self.assertTrue(issubclass(CourierError, Exception))
        self.assertTrue(issubclass(InvalidAddressError, CourierError))
        self.assertTrue(issubclass(InvalidAddressError, ValueError))
        self.assertTrue(issubclass(ValidationError, CourierError))
        self.assertTrue(issubclass(ValidationError, ValueError))
        self.assertTrue(issubclass(HttpError, CourierError))


class TestHttpError(unittest.TestCase):
    def test_fields_roundtrip(self):
        err = HttpError(
            method="GET",
            url="https://example.test/api",
            status_code=418,
            message="no coffee",
            response_text="teapot",
            payload={"a": 1},
        )

        self.assertEqual(err.method, "GET")
        self.assertEqual(err.url, "https://example.test/api")
        self.assertEqual(err.status_code, 418)
        self.assertEqual(err.message, "no coffee")
        self.assertEqual(err.response_text, "teapot")
        self.assertEqual(err.payload, {"a": 1})

    def test_str_includes_status_and_message_when_present(self):
        err = HttpError(
            method="POST",
            url="https://example.test/submit",
            status_code=500,
            message="boom",
        )

        self.assertEqual(
            str(err),
            "POST https://example.test/submit | status=500 | boom",
        )

    def test_str_omits_status_and_message_when_absent(self):
        err = HttpError(method="DELETE", url="https://example.test/item/1")
        self.assertEqual(str(err), "DELETE https://example.test/item/1")


if __name__ == "__main__":
    unittest.main()
