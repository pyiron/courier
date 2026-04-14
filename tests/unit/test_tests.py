import importlib
import unittest
from importlib.metadata import PackageNotFoundError
from unittest import mock

import courier


class TestVersion(unittest.TestCase):
    def test_version(self):
        version = courier.__version__
        print(version)
        self.assertTrue(version.startswith("0"))

    def test_version_uses_installed_metadata_when_available(self):
        with mock.patch("importlib.metadata.version", return_value="1.2.3"):
            reloaded = importlib.reload(courier)

        self.assertEqual(reloaded.__version__, "1.2.3")
        importlib.reload(courier)

    def test_version_falls_back_when_package_metadata_is_missing(self):
        with mock.patch(
            "importlib.metadata.version",
            side_effect=PackageNotFoundError,
        ):
            reloaded = importlib.reload(courier)

        self.assertEqual(reloaded.__version__, "0.0.0+unknown")
        importlib.reload(courier)
