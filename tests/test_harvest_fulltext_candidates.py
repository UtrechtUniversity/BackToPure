import unittest
from unittest.mock import MagicMock, patch

import harvest_fulltext_candidates as hfc


class AttachedFileTests(unittest.TestCase):
    def test_file_electronic_version_counts_as_attached(self):
        record = {"electronicVersions": [{"typeDiscriminator": "FileElectronicVersion"}]}
        self.assertTrue(hfc.has_attached_file(record))

    def test_link_only_does_not_count_as_attached(self):
        record = {"electronicVersions": [{"typeDiscriminator": "LinkElectronicVersion"}]}
        self.assertFalse(hfc.has_attached_file(record))

    def test_no_electronic_versions_is_not_attached(self):
        self.assertFalse(hfc.has_attached_file({}))


class ExamineOutputTests(unittest.TestCase):
    ENTRY = {"doi": "10.1/a", "pure_uuid": "uuid-1", "title": "A paper"}

    def _work(self, version="publishedVersion"):
        return {
            "doi": "https://doi.org/10.1/a",
            "best_oa_location": {
                "pdf_url": "https://publisher.org/a.pdf",
                "version": version,
                "license": "cc-by",
                "is_oa": True,
            },
        }

    def test_valid_candidate_is_reported_as_attachable(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=self._work()), patch.object(
            hfc, "preflight", return_value=MagicMock(ok=True, detail="ok")
        ), patch.object(hfc, "download_and_validate", return_value=(b"%PDF-1.7", "a.pdf")):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("X", row["to_be_updated"])
        self.assertEqual("ok", row["validation"])
        self.assertEqual("https://publisher.org/a.pdf", row["candidate_url"])
        self.assertEqual(8, row["size_bytes"])

    def test_accepted_manuscript_is_reported_with_a_reason(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=self._work("acceptedVersion")):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertIn("published version", row["reason"])

    def test_no_oa_location_is_reported_with_a_reason(self):
        with patch.object(hfc, "fetch_openalex_work", return_value={"doi": "https://doi.org/10.1/a"}):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertIn("no open access location", row["reason"])

    def test_html_landing_page_is_reported_not_dropped(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=self._work()), patch.object(
            hfc, "preflight", return_value=MagicMock(ok=False, detail="URL resolves to HTML landing/challenge page, not a PDF file.")
        ):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertIn("HTML", row["reason"])

    def test_download_failure_is_reported_not_dropped(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=self._work()), patch.object(
            hfc, "preflight", return_value=MagicMock(ok=True, detail="ok")
        ), patch.object(hfc, "download_and_validate", side_effect=ValueError("HTTP 404 while fetching candidate.")):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertIn("404", row["reason"])

    def test_every_row_has_all_columns(self):
        with patch.object(hfc, "fetch_openalex_work", return_value={"doi": "x"}):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual(set(hfc.REVIEW_COLUMNS), set(row))


class MainFlowTests(unittest.TestCase):
    def test_run_where_every_fetch_fails_raises(self):
        """A run that validated nothing must not report zero coverage as a result."""
        rows = [
            {"to_be_updated": "", "validation": "rejected", "reason": "HTTP 500 while fetching candidate."}
            for _ in range(3)
        ]

        with self.assertRaises(RuntimeError) as ctx:
            hfc.guard_against_total_failure(rows)

        self.assertIn("every", str(ctx.exception).lower())

    def test_mixed_results_do_not_raise(self):
        rows = [
            {"to_be_updated": "X", "validation": "ok", "reason": ""},
            {"to_be_updated": "", "validation": "rejected", "reason": "HTTP 500 while fetching candidate."},
        ]

        hfc.guard_against_total_failure(rows)

    def test_rows_without_candidates_are_not_counted_as_failures(self):
        rows = [{"to_be_updated": "", "validation": "", "reason": "no open access location in OpenAlex"}]

        hfc.guard_against_total_failure(rows)
