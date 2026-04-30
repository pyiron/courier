import unittest
from datetime import date

from courier.exceptions import ValidationError
from courier.services.zenodo import Creator, ZenodoMetadata


class TestZenodoMetadata(unittest.TestCase):
    def test_software_metadata_serializes_to_zenodo_payload(self):
        md = ZenodoMetadata.software()
        md.title = "courier"
        md.publication_date = date(2026, 4, 21)
        md.description = "Python client."
        md.license = "Apache-2.0"
        md.creators.append(Creator(family_name="Doe", given_names="Jane"))
        md.add_keyword("python")

        self.assertEqual(
            md.to_payload(),
            {
                "metadata": {
                    "upload_type": "software",
                    "publication_date": "2026-04-21",
                    "title": "courier",
                    "creators": [{"name": "Doe, Jane"}],
                    "description": "Python client.",
                    "access_right": "open",
                    "license": "Apache-2.0",
                    "keywords": ["python"],
                }
            },
        )

    def test_from_dict_accepts_metadata_payload(self):
        md = ZenodoMetadata.from_dict(
            {
                "metadata": {
                    "upload_type": "dataset",
                    "publication_date": "2026-04-21",
                    "title": "dataset",
                    "creators": [{"name": "Doe, Jane", "orcid": "0000"}],
                    "description": "Data.",
                    "access_right": "open",
                    "license": "cc-by-4.0",
                    "keywords": ["science", ""],
                    "communities": [{"identifier": "pyiron"}],
                    "grants": [{"id": "10.13039/501100000000::12345"}],
                }
            }
        )

        self.assertEqual(md.upload_type, "dataset")
        self.assertEqual(md.creators[0].name, "Doe, Jane")
        self.assertEqual(md.creators[0].orcid, "0000")
        self.assertEqual(md.keywords, ["science"])
        self.assertEqual(md.communities[0].identifier, "pyiron")
        self.assertEqual(md.grants[0].id, "10.13039/501100000000::12345")
        self.assertEqual(md.to_api_dict()["publication_date"], "2026-04-21")

    def test_creator_can_use_explicit_name(self):
        creator = Creator(name="Zenodo Team", affiliation="CERN")
        self.assertEqual(
            creator.to_api_dict(),
            {"name": "Zenodo Team", "affiliation": "CERN"},
        )

    def test_required_fields_are_validated(self):
        md = ZenodoMetadata.software()
        with self.assertRaisesRegex(ValidationError, "publication_date"):
            md.validate()

    def test_open_access_requires_license(self):
        md = ZenodoMetadata.dataset()
        md.title = "dataset"
        md.publication_date = "2026-04-21"
        md.description = "Data."
        md.creators.append(Creator(name="Doe, Jane"))

        with self.assertRaisesRegex(ValidationError, "license"):
            md.validate()


if __name__ == "__main__":
    unittest.main()
