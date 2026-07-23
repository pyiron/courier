import ast
import unittest
from pathlib import Path
from typing import Any

from praeco import Person, PublicationMetadata


def _generic_repository_payload(metadata: PublicationMetadata) -> dict[str, Any]:
    """Test-only adapter proving PublicationMetadata is not Zenodo-specific."""
    return {
        "name": metadata.title,
        "summary": metadata.description,
        "released": (
            metadata.publication_date.isoformat() if metadata.publication_date else None
        ),
        "authors": [_person_label(person) for person in metadata.creators],
        "tags": list(metadata.keywords),
        "license_id": metadata.license,
    }


def _person_label(person: Person) -> str:
    if person.name:
        return person.name
    return f"{person.given_names} {person.family_name}"


class TestPublicationMetadataBoundary(unittest.TestCase):
    def test_publication_metadata_can_feed_non_zenodo_adapter(self):
        metadata = PublicationMetadata(
            title="Reusable dataset",
            description="Dataset shared across publication services.",
            publication_date="2026-05-29",
            creators=[
                Person(family_name="Doe", given_names="Jane"),
                Person(name="Smith, Chris"),
            ],
            keywords=["dataset", "workflow"],
            license="MIT",
        )

        payload = _generic_repository_payload(metadata)

        self.assertEqual(
            payload,
            {
                "name": "Reusable dataset",
                "summary": "Dataset shared across publication services.",
                "released": "2026-05-29",
                "authors": ["Jane Doe", "Smith, Chris"],
                "tags": ["dataset", "workflow"],
                "license_id": "MIT",
            },
        )

    def test_common_metadata_module_has_no_service_imports(self):
        tree = ast.parse(Path("praeco/metadata.py").read_text(encoding="utf-8"))

        imported_modules = {
            module_name
            for node in ast.walk(tree)
            for module_name in _imported_modules(node)
        }

        self.assertFalse(
            any(module.startswith("praeco.services") for module in imported_modules),
            imported_modules,
        )


def _imported_modules(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Import):
        return tuple(alias.name for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        if node.level:
            if node.module:
                return (f"praeco.{node.module}",)
            return tuple(f"praeco.{alias.name}" for alias in node.names)
        if node.module:
            return (node.module,)
    return ()


if __name__ == "__main__":
    unittest.main()
