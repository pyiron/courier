import unittest

import courier


class TestVersion(unittest.TestCase):
    def test_version(self):
        version = courier.__version__
        print(version)
        self.assertTrue(version.startswith("0"))
