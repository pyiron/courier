import unittest
from dataclasses import fields
from typing import Any, cast

import praeco
from praeco.metadata import Person, PublicationMetadata
from praeco.services.dataportal import DataportalMetadata


def publication_metadata() -> PublicationMetadata:
    return PublicationMetadata(
        title="Reusable dataset",
        description="Service-neutral publication metadata.",
        creators=[Person(name="Doe, Jane")],
        keywords=["materials"],
        license="CC-BY-4.0",
    )


class TestDataportalMetadataBoundary(unittest.TestCase):
    def test_adapter_contains_only_publication_reference_and_ckan_fields(self):
        self.assertEqual(
            {item.name for item in fields(DataportalMetadata)},
            {
                "metadata",
                "name",
                "owner_org",
                "private",
                "groups",
                "extras",
                "dataset_type",
                "publisher",
                "contact",
                "modified",
                "identifier",
            },
        )

    def test_generic_fields_cannot_be_passed_to_adapter_constructor(self):
        generic_fields = {
            "title": "Duplicate title",
            "notes": "Duplicate description",
            "tags": ["duplicate"],
            "license_id": "MIT",
            "version": "2.0",
        }

        for field_name, value in generic_fields.items():
            with (
                self.subTest(field_name=field_name),
                self.assertRaises(TypeError),
            ):
                DataportalMetadata(
                    metadata=publication_metadata(),
                    **cast(Any, {field_name: value}),
                )

    def test_generic_payload_fields_are_sourced_from_publication_metadata(self):
        publication = publication_metadata()

        payload = DataportalMetadata(
            metadata=publication,
            name="explicit-name",
        ).to_payload()

        self.assertEqual(payload["title"], publication.title)
        self.assertEqual(payload["notes"], publication.description)
        self.assertEqual(
            payload["tags"],
            [{"name": keyword} for keyword in publication.keywords],
        )
        self.assertEqual(payload["license_id"], publication.license)

    def test_adapter_is_not_exported_from_top_level_package(self):
        self.assertFalse(hasattr(praeco, "DataportalMetadata"))


if __name__ == "__main__":
    unittest.main()
