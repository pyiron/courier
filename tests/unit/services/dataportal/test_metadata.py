import json
import unittest
from typing import Any, cast

from courier.exceptions import ValidationError
from courier.metadata import (
    Contributor,
    Person,
    PublicationMetadata,
    RelatedIdentifier,
)
from courier.services.dataportal import DataportalMetadata


def publication_metadata(**overrides: Any) -> PublicationMetadata:
    values: dict[str, Any] = {
        "title": "Steel Heat Treatment Data",
        "description": "Measurements from a heat treatment experiment.",
        "publication_date": "2026-06-08",
        "creators": [
            Person(
                family_name="Doe",
                given_names="Jane",
                affiliation="MPI-SusMat",
                orcid="0000-0000-0000-0000",
            )
        ],
        "contributors": [
            Contributor(
                person=Person(name="Curator, Chris"),
                role="DataCurator",
            )
        ],
        "keywords": ["steel", "heat treatment"],
        "license": "CC-BY-4.0",
        "doi": "10.1234/example",
        "version": "1.0.0",
        "language": "eng",
        "related_identifiers": [
            RelatedIdentifier(
                identifier="10.1234/source",
                relation="isDerivedFrom",
                resource_type="dataset",
            )
        ],
    }
    values.update(overrides)
    return PublicationMetadata(**values)


def extras_by_key(payload: dict[str, Any]) -> dict[str, str]:
    return {item["key"]: item["value"] for item in payload.get("extras", [])}


class TestDataportalMetadata(unittest.TestCase):
    def test_serializes_publication_and_dataportal_fields(self):
        metadata = DataportalMetadata(
            metadata=publication_metadata(),
            owner_org="materials-org",
            private=False,
            groups=["heat-treatment"],
            extras={"pmd_profile": "dataset-v1"},
            dataset_type="dataset",
        )

        payload = metadata.to_payload()

        self.assertEqual(payload["name"], "steel-heat-treatment-data")
        self.assertEqual(payload["title"], "Steel Heat Treatment Data")
        self.assertEqual(
            payload["notes"],
            "Measurements from a heat treatment experiment.",
        )
        self.assertEqual(
            payload["tags"],
            [{"name": "steel"}, {"name": "heat treatment"}],
        )
        self.assertEqual(payload["license_id"], "CC-BY-4.0")
        self.assertEqual(payload["version"], "1.0.0")
        self.assertEqual(payload["owner_org"], "materials-org")
        self.assertFalse(payload["private"])
        self.assertEqual(payload["groups"], [{"name": "heat-treatment"}])
        self.assertEqual(payload["type"], "dataset")

        extras = extras_by_key(payload)
        self.assertEqual(extras["pmd_profile"], "dataset-v1")
        self.assertEqual(extras["publication_date"], "2026-06-08")
        self.assertEqual(extras["doi"], "10.1234/example")
        self.assertEqual(extras["language"], "eng")
        self.assertEqual(
            json.loads(extras["creators"]),
            [
                {
                    "affiliation": "MPI-SusMat",
                    "family_name": "Doe",
                    "given_names": "Jane",
                    "orcid": "0000-0000-0000-0000",
                }
            ],
        )
        self.assertEqual(
            json.loads(extras["contributors"]),
            [
                {
                    "person": {"name": "Curator, Chris"},
                    "role": "DataCurator",
                }
            ],
        )
        self.assertEqual(
            json.loads(extras["related_identifiers"]),
            [
                {
                    "identifier": "10.1234/source",
                    "relation": "isDerivedFrom",
                    "resource_type": "dataset",
                }
            ],
        )

    def test_explicit_name_is_trimmed(self):
        metadata = DataportalMetadata(
            metadata=publication_metadata(),
            name="  explicit-dataset-name  ",
        )

        self.assertEqual(metadata.to_payload()["name"], "explicit-dataset-name")

    def test_optional_publication_values_are_omitted(self):
        publication = publication_metadata(
            publication_date=None,
            contributors=[],
            keywords=[],
            license=None,
            doi=None,
            version=None,
            language=None,
            related_identifiers=[],
        )

        payload = DataportalMetadata(metadata=publication).to_payload()
        extras = extras_by_key(payload)

        self.assertEqual(payload["tags"], [])
        self.assertNotIn("license_id", payload)
        self.assertNotIn("version", payload)
        self.assertEqual(set(extras), {"creators"})

    def test_generated_extra_collision_is_rejected(self):
        with self.assertRaisesRegex(ValidationError, "publication_date"):
            DataportalMetadata(
                metadata=publication_metadata(),
                extras={"publication_date": "override"},
            )

    def test_duplicate_normalized_extra_keys_are_rejected(self):
        with self.assertRaisesRegex(
            ValidationError,
            "duplicate extra key after normalization: 'profile'",
        ):
            DataportalMetadata(
                metadata=publication_metadata(),
                extras={"profile": "first", " profile ": "second"},
            )

    def test_blank_dataportal_fields_are_rejected(self):
        cases = [
            ({"name": " "}, "name"),
            ({"owner_org": " "}, "owner_org"),
            ({"dataset_type": " "}, "dataset_type"),
            ({"groups": [" "]}, "group"),
            ({"extras": {" ": "value"}}, "extra key"),
        ]

        for kwargs, message in cases:
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaisesRegex(ValidationError, message),
            ):
                DataportalMetadata(metadata=publication_metadata(), **kwargs)

    def test_private_must_be_boolean(self):
        with self.assertRaisesRegex(ValidationError, "private must be a boolean"):
            DataportalMetadata(
                metadata=publication_metadata(),
                private=cast(Any, "false"),
            )

    def test_groups_and_extras_require_expected_container_types(self):
        cases = [
            ({"groups": None}, "groups must be a list"),
            ({"groups": "foo"}, "groups must be a list"),
            ({"extras": None}, "extras must be a dict"),
            ({"extras": []}, "extras must be a dict"),
        ]

        for kwargs, message in cases:
            with (
                self.subTest(kwargs=kwargs),
                self.assertRaisesRegex(ValidationError, message),
            ):
                DataportalMetadata(
                    metadata=publication_metadata(),
                    **cast(Any, kwargs),
                )

    def test_extra_values_must_be_strings(self):
        with self.assertRaisesRegex(ValidationError, "extra values must be strings"):
            DataportalMetadata(
                metadata=publication_metadata(),
                extras=cast(Any, {"priority": 1}),
            )

    def test_metadata_must_be_publication_metadata(self):
        with self.assertRaisesRegex(ValidationError, "PublicationMetadata"):
            DataportalMetadata(metadata=cast(Any, {"title": "raw"}))

    def test_name_is_required_when_title_cannot_form_ascii_slug(self):
        with self.assertRaisesRegex(ValidationError, "name must be provided"):
            DataportalMetadata(
                metadata=publication_metadata(title="材料"),
            )

    def test_serialization_revalidates_mutable_specific_fields(self):
        metadata = DataportalMetadata(metadata=publication_metadata())
        metadata.groups.append(" ")

        with self.assertRaisesRegex(ValidationError, "group"):
            metadata.to_payload()


if __name__ == "__main__":
    unittest.main()
