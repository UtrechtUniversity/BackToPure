import unittest
from unittest.mock import MagicMock

import fulltext_deposit as fd


def _response(status=200, body=None, text=""):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = body if body is not None else {}
    response.text = text
    return response


class UploadTests(unittest.TestCase):
    def test_returns_the_key_from_a_successful_upload(self):
        session = MagicMock()
        session.put.return_value = _response(body={"key": "abc-123", "size": 42})

        key = fd.upload_pdf(session, b"%PDF-1.7 stub", "paper.pdf")

        self.assertEqual("abc-123", key)

    def test_sends_raw_bytes_with_a_pdf_content_type(self):
        session = MagicMock()
        session.put.return_value = _response(body={"key": "abc-123"})

        fd.upload_pdf(session, b"%PDF-1.7 stub", "paper.pdf")

        _args, kwargs = session.put.call_args
        self.assertEqual(b"%PDF-1.7 stub", kwargs["data"])
        self.assertEqual("application/pdf", kwargs["headers"]["Content-Type"])

    def test_missing_key_is_an_error_not_a_silent_none(self):
        session = MagicMock()
        session.put.return_value = _response(body={"size": 42})

        with self.assertRaises(fd.DepositError) as ctx:
            fd.upload_pdf(session, b"%PDF-", "paper.pdf")
        self.assertIn("no upload key", str(ctx.exception).lower())

    def test_error_status_is_reported_with_its_status(self):
        session = MagicMock()
        session.put.return_value = _response(status=413, text="too large")

        with self.assertRaises(fd.DepositError) as ctx:
            fd.upload_pdf(session, b"%PDF-", "paper.pdf")
        self.assertIn("413", str(ctx.exception))


class FileEntryTests(unittest.TestCase):
    def _candidate(self, version="publishedVersion", licence="cc-by"):
        return {
            "url": "https://publisher.org/a.pdf",
            "version": version,
            "license": licence,
            "access_status": "open",
        }

    def test_entry_points_at_the_uploaded_file(self):
        entry = fd.build_file_electronic_version(self._candidate(), "key-1", "a.pdf")

        self.assertEqual("FileElectronicVersion", entry["typeDiscriminator"])
        self.assertEqual("key-1", entry["file"]["uploadedFile"]["key"])
        self.assertEqual("a.pdf", entry["file"]["fileName"])
        self.assertEqual("application/pdf", entry["file"]["mimeType"])

    def test_version_type_is_taken_from_the_candidate_never_hardcoded(self):
        published = fd.build_file_electronic_version(self._candidate(), "k", "a.pdf")
        accepted = fd.build_file_electronic_version(
            self._candidate(version="acceptedVersion"), "k", "a.pdf"
        )

        self.assertTrue(published["versionType"]["uri"].endswith("publishersversion"))
        self.assertTrue(accepted["versionType"]["uri"].endswith("authorsversion"))
        self.assertEqual("Accepted author manuscript", accepted["versionType"]["term"]["en_GB"])

    def test_known_licence_becomes_a_uri(self):
        entry = fd.build_file_electronic_version(self._candidate(), "k", "a.pdf")

        self.assertTrue(entry["licenseType"]["uri"].endswith("cc_by"))
        self.assertNotIn("userDefinedLicense", entry)

    def test_unmapped_licence_is_kept_as_free_text(self):
        entry = fd.build_file_electronic_version(
            self._candidate(licence="publisher-specific"), "k", "a.pdf"
        )

        self.assertNotIn("licenseType", entry)
        self.assertEqual("publisher-specific", entry["userDefinedLicense"])

    def test_no_licence_sets_neither_field(self):
        entry = fd.build_file_electronic_version(self._candidate(licence=None), "k", "a.pdf")

        self.assertNotIn("licenseType", entry)
        self.assertNotIn("userDefinedLicense", entry)

    def test_unknown_version_refuses_rather_than_guessing(self):
        with self.assertRaises(ValueError):
            fd.build_file_electronic_version(self._candidate(version="draft"), "k", "a.pdf")
