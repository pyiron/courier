import importlib
import unittest
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
