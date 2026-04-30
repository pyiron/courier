import json
import unittest
from typing import Any, cast

from courier.services.zenodo import ZenodoValidationError
from courier.services.zenodo._response import read_zenodo_json

from ._helpers import FakeRequest, FakeResponse


class TestZenodoResponse(unittest.TestCase):
    def test_structured_validation_error_is_preserved(self):
        response = FakeResponse(
            status_code=400,
            text=json.dumps(
                {
                    "message": "Validation error",
                    "status": 400,
                    "errors": [
                        {
                            "field": "metadata.creators.0.name",
                            "message": "Name is required.",
                        }
                    ],
                }
            ),
            json_value={
                "message": "Validation error",
                "status": 400,
                "errors": [
                    {
                        "field": "metadata.creators.0.name",
                        "message": "Name is required.",
                    }
                ],
            },
            request=FakeRequest("PUT"),
        )

        with self.assertRaises(ZenodoValidationError) as ctx:
            read_zenodo_json(cast(Any, response))

        self.assertEqual(ctx.exception.message, "Validation error")
        assert ctx.exception.errors is not None
        self.assertEqual(ctx.exception.errors[0].field, "metadata.creators.0.name")


if __name__ == "__main__":
    unittest.main()
