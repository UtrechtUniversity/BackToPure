import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

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


class ProcessFullTextTests(unittest.TestCase):
    """Deposit is a chain of network steps; each failure must land in the row
    as a reason rather than aborting the run."""

    def _csv(self):
        import pandas as pd

        return pd.DataFrame([
            {
                "to_be_updated": "X", "updated": " ",
                "doi": "10.1000/a", "pure_uuid": "rec-1",
                "candidate_url": "https://publisher.org/a.pdf",
                "version": "publishedVersion", "licence": "cc-by",
                "access_status": "open",
            }
        ])

    def test_deposits_and_records_the_previous_versions(self):
        import tempfile

        import apply_updates_to_pure as apply_mod

        record = {"uuid": "rec-1", "electronicVersions": [{"typeDiscriminator": "DoiElectronicVersion"}]}
        entries = []
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            apply_mod, "_resolve_output_directory", return_value=tmpdir
        ), patch.object(apply_mod, "download_and_validate", return_value=(b"%PDF-", "a.pdf")), patch.object(
            apply_mod, "upload_pdf", return_value="key-1"
        ), patch.object(apply_mod.requests, "get", return_value=MagicMock(status_code=200, json=lambda: record)), patch.object(
            apply_mod.requests, "put", return_value=MagicMock(status_code=200)
        ), patch.object(apply_mod, "_append_apply_manifest_entry", side_effect=entries.append):
            apply_mod.process_full_text("to_be_updated.csv", self._csv())

        self.assertEqual(1, len(entries))
        self.assertEqual("rec-1", entries[0]["record_uuid"])
        self.assertEqual(
            [{"typeDiscriminator": "DoiElectronicVersion"}],
            entries[0]["previous_electronic_versions"],
            "the snapshot is what rollback restores",
        )

    def test_a_failed_upload_records_no_manifest_entry(self):
        import tempfile

        import apply_updates_to_pure as apply_mod
        import fulltext_deposit as fd_mod

        entries = []
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            apply_mod, "_resolve_output_directory", return_value=tmpdir
        ), patch.object(apply_mod, "download_and_validate", return_value=(b"%PDF-", "a.pdf")), patch.object(
            apply_mod, "upload_pdf", side_effect=fd_mod.DepositError("HTTP 500")
        ), patch.object(apply_mod, "_append_apply_manifest_entry", side_effect=entries.append):
            apply_mod.process_full_text("to_be_updated.csv", self._csv())

        self.assertEqual([], entries)

    def test_unticked_rows_are_not_deposited(self):
        import tempfile

        import apply_updates_to_pure as apply_mod

        frame = self._csv()
        frame.loc[0, "to_be_updated"] = ""
        entries = []
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            apply_mod, "_resolve_output_directory", return_value=tmpdir
        ), patch.object(apply_mod, "download_and_validate") as download, patch.object(
            apply_mod, "_append_apply_manifest_entry", side_effect=entries.append
        ):
            apply_mod.process_full_text("to_be_updated.csv", frame)

        download.assert_not_called()
        self.assertEqual([], entries)

    def test_dispatch_reaches_full_text_without_a_json_file(self):
        """The full text job writes only a CSV; the JSON-gated dispatch would skip it."""
        import inspect

        import apply_updates_to_pure as apply_mod

        source = inspect.getsource(apply_mod.main)
        self.assertIn("deposit_full_text", source)

    def test_deposited_rows_are_written_back_to_the_csv(self):
        """Without this, the review CSV still shows to_be_updated='X' for rows
        already deposited, and a rerun on the same artifact directory would
        deposit them a second time (jobs.py unlinks the manifest per run)."""
        import tempfile

        import apply_updates_to_pure as apply_mod

        record = {"uuid": "rec-1", "electronicVersions": []}
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(apply_mod, "_resolve_output_directory", return_value=tmpdir), patch.object(
                apply_mod, "download_and_validate", return_value=(b"%PDF-", "a.pdf")
            ), patch.object(apply_mod, "upload_pdf", return_value="key-1"), patch.object(
                apply_mod.requests, "get", return_value=MagicMock(status_code=200, json=lambda: record)
            ), patch.object(apply_mod.requests, "put", return_value=MagicMock(status_code=200)), patch.object(
                apply_mod, "_append_apply_manifest_entry"
            ):
                apply_mod.process_full_text("to_be_updated.csv", self._csv())

            import pandas as pd

            saved = pd.read_csv(os.path.join(tmpdir, "to_be_updated.csv"), keep_default_na=False)
            self.assertEqual("", str(saved.loc[0, "to_be_updated"]).strip())
            self.assertEqual("x", str(saved.loc[0, "updated"]).strip().lower())

    def test_a_connection_error_on_upload_does_not_abort_the_run(self):
        import tempfile

        import requests as requests_mod

        import apply_updates_to_pure as apply_mod

        frame = pd.concat([self._csv(), self._csv()], ignore_index=True)
        frame.loc[0, "doi"] = "10.1000/broken"
        frame.loc[1, "doi"] = "10.1000/ok"
        record = {"uuid": "rec-1", "electronicVersions": []}
        entries = []

        calls = {"count": 0}

        def upload_side_effect(session, payload, filename):
            calls["count"] += 1
            if calls["count"] == 1:
                raise requests_mod.exceptions.ConnectionError("boom")
            return "key-2"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(apply_mod, "_resolve_output_directory", return_value=tmpdir), patch.object(
                apply_mod, "download_and_validate", return_value=(b"%PDF-", "a.pdf")
            ), patch.object(apply_mod, "upload_pdf", side_effect=upload_side_effect), patch.object(
                apply_mod.requests, "get", return_value=MagicMock(status_code=200, json=lambda: record)
            ), patch.object(apply_mod.requests, "put", return_value=MagicMock(status_code=200)), patch.object(
                apply_mod, "_append_apply_manifest_entry", side_effect=entries.append
            ):
                apply_mod.process_full_text("to_be_updated.csv", frame)

        self.assertEqual(1, len(entries), "the second row must still be deposited")
        self.assertEqual("rec-1", entries[0]["record_uuid"])

    def test_a_put_error_does_not_abort_the_run_and_logs_unknown_state(self):
        import tempfile

        import requests as requests_mod

        import apply_updates_to_pure as apply_mod

        frame = pd.concat([self._csv(), self._csv()], ignore_index=True)
        frame.loc[0, "doi"] = "10.1000/broken"
        frame.loc[1, "doi"] = "10.1000/ok"
        record = {"uuid": "rec-1", "electronicVersions": []}
        entries = []

        calls = {"count": 0}

        def put_side_effect(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise requests_mod.exceptions.Timeout("timed out")
            return MagicMock(status_code=200)

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(apply_mod, "_resolve_output_directory", return_value=tmpdir), patch.object(
                apply_mod, "download_and_validate", return_value=(b"%PDF-", "a.pdf")
            ), patch.object(apply_mod, "upload_pdf", return_value="key-1"), patch.object(
                apply_mod.requests, "get", return_value=MagicMock(status_code=200, json=lambda: record)
            ), patch.object(apply_mod.requests, "put", side_effect=put_side_effect), patch.object(
                apply_mod, "_append_apply_manifest_entry", side_effect=entries.append
            ), self.assertLogs("btp", level="ERROR") as logs:
                apply_mod.process_full_text("to_be_updated.csv", frame)

        self.assertEqual(1, len(entries), "the second row must still be deposited")
        self.assertTrue(
            any("unknown" in message.lower() for message in logs.output),
            "the PUT failure log must say the record's state is unknown",
        )

    def test_a_doi_that_does_not_normalise_is_skipped_before_depositing(self):
        import tempfile

        import apply_updates_to_pure as apply_mod

        frame = self._csv()
        frame.loc[0, "doi"] = "not-a-doi"
        entries = []
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            apply_mod, "_resolve_output_directory", return_value=tmpdir
        ), patch.object(apply_mod, "download_and_validate") as download, patch.object(
            apply_mod, "_append_apply_manifest_entry", side_effect=entries.append
        ):
            apply_mod.process_full_text("to_be_updated.csv", frame)

        download.assert_not_called()
        self.assertEqual([], entries)
