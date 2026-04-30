import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from courier.services.zenodo import ZenodoClient
from courier.services.zenodo._urls import bucket_file_url
from courier.services.zenodo.models import DepositionInfo

from ._helpers import FakeResponse, FakeSession, deposition_payload


class TestFilesResource(unittest.TestCase):
    def test_bucket_file_url_quotes_filename_segment(self):
        self.assertEqual(
            bucket_file_url("https://zenodo.org/api/files/bucket", "my file.zip"),
            "https://zenodo.org/api/files/bucket/my%20file.zip",
        )

    def test_upload_uses_deposition_bucket_link(self):
        session = FakeSession(
            [
                FakeResponse(
                    status_code=200,
                    json_value={
                        "key": "artifact.zip",
                        "version_id": "v1",
                        "checksum": "md5:abc",
                        "size": 123,
                        "mimetype": "application/zip",
                        "links": {
                            "self": "https://zenodo.org/api/files/bucket/artifact.zip"
                        },
                    },
                )
            ]
        )
        c = ZenodoClient(session=cast(Any, session))
        deposition = DepositionInfo.from_dict(deposition_payload())

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "artifact.zip"
            _ = path.write_bytes(b"payload")
            uploaded = c.files.upload(deposition, path, content_type="application/zip")

        self.assertEqual(uploaded.filename, "artifact.zip")
        self.assertEqual(uploaded.size, 123)
        self.assertEqual(session.calls[0]["method"], "PUT")
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/files/bucket-id/artifact.zip",
        )
        self.assertEqual(
            session.calls[0]["headers"], {"Content-Type": "application/zip"}
        )

    def test_upload_fetches_deposition_when_only_id_is_given(self):
        session = FakeSession(
            [
                FakeResponse(json_value=deposition_payload()),
                FakeResponse(json_value={"key": "artifact.zip", "size": 1}),
            ]
        )
        c = ZenodoClient(session=cast(Any, session))

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "artifact.zip"
            _ = path.write_bytes(b"x")
            _ = c.files.upload(42, path)

        self.assertEqual(session.calls[0]["method"], "GET")
        self.assertEqual(session.calls[1]["method"], "PUT")

    def test_rename_uses_file_endpoint(self):
        session = FakeSession(
            [FakeResponse(json_value={"id": "file-1", "filename": "renamed.txt"})]
        )
        c = ZenodoClient(session=cast(Any, session))

        out = c.files.rename(42, "file-1", "renamed.txt")

        self.assertEqual(out.filename, "renamed.txt")
        self.assertEqual(session.calls[0]["method"], "PUT")
        self.assertEqual(
            session.calls[0]["url"],
            "https://zenodo.org/api/deposit/depositions/42/files/file-1",
        )
        self.assertEqual(session.calls[0]["json"], {"filename": "renamed.txt"})


if __name__ == "__main__":
    unittest.main()
