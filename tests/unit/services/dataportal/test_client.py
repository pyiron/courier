import unittest
from typing import Any, cast

import courier
from courier.metadata import Person, PublicationMetadata
from courier.services.dataportal import DataportalClient

from ._helpers import FakeSession


class TestDataportalClient(unittest.TestCase):
    def test_default_address_is_material_digital_dataportal(self):
        client = DataportalClient(session=cast(Any, FakeSession()))

        self.assertEqual(
            client.base_url,
            "https://dataportal.material-digital.de",
        )
        self.assertIs(client.assets.client, client)
        self.assertIs(client.action.client, client)
        self.assertIs(client.packages.client, client)
        self.assertIs(client.resources.client, client)
        self.assertIs(client.datasets.client, client)

    def test_custom_address_and_default_scheme_are_supported(self):
        client = DataportalClient(
            "localhost:5000",
            default_scheme="http",
            session=cast(Any, FakeSession()),
        )

        self.assertEqual(client.base_url, "http://localhost:5000")

    def test_api_token_uses_ckan_raw_authorization_header(self):
        session = FakeSession()

        client = DataportalClient(
            api_token="  token  ",
            session=cast(Any, session),
        )

        self.assertEqual(client.api_token, "token")
        self.assertEqual(session.headers["Authorization"], "token")

    def test_client_is_not_exported_from_top_level_package(self):
        self.assertFalse(hasattr(courier, "DataportalClient"))

    def test_client_construction_does_not_accept_publication_metadata(self):
        metadata = PublicationMetadata(
            title="Dataset",
            description="Data.",
            creators=[Person(name="Doe, Jane")],
        )

        with self.assertRaises(TypeError):
            cast(Any, DataportalClient)(
                metadata=metadata,
                session=cast(Any, FakeSession()),
            )


if __name__ == "__main__":
    unittest.main()
