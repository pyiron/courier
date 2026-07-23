import importlib
import unittest
from importlib.metadata import PackageNotFoundError
from unittest import mock

import praeco
import praeco.metadata as metadata_models
import praeco.services as services
from praeco.services.dataportal import DataportalClient
from praeco.services.ontodocker import OntodockerClient
from praeco.services.zenodo import ZenodoClient


class TestImports(unittest.TestCase):
    def test_imports_praeco_package(self):
        module = importlib.import_module("praeco")

        self.assertIs(module, praeco)


class TestVersion(unittest.TestCase):
    def test_version(self):
        version = praeco.__version__
        print(version)
        self.assertTrue(version.startswith("0"))

    def test_version_uses_installed_metadata_when_available(self):
        with mock.patch("importlib.metadata.version", return_value="1.2.3"):
            reloaded = importlib.reload(praeco)

        self.assertEqual(reloaded.__version__, "1.2.3")
        importlib.reload(praeco)

    def test_version_falls_back_when_package_metadata_is_missing(self):
        with mock.patch(
            "importlib.metadata.version",
            side_effect=PackageNotFoundError,
        ):
            reloaded = importlib.reload(praeco)

        self.assertEqual(reloaded.__version__, "0.0.0+unknown")
        importlib.reload(praeco)


class TestPublicApi(unittest.TestCase):
    def test_service_clients_are_top_level_imports(self):
        self.assertIs(praeco.DataportalClient, DataportalClient)
        self.assertIs(praeco.OntodockerClient, OntodockerClient)
        self.assertIs(praeco.ZenodoClient, ZenodoClient)

    def test_service_clients_are_services_imports(self):
        self.assertIs(services.DataportalClient, DataportalClient)
        self.assertIs(services.OntodockerClient, OntodockerClient)
        self.assertIs(services.ZenodoClient, ZenodoClient)

    def test_publication_metadata_models_are_top_level_imports(self):
        self.assertIs(praeco.Contributor, metadata_models.Contributor)
        self.assertIs(praeco.Person, metadata_models.Person)
        self.assertIs(praeco.PublicationMetadata, metadata_models.PublicationMetadata)
        self.assertIs(praeco.RelatedIdentifier, metadata_models.RelatedIdentifier)
