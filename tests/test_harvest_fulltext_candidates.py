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

    def test_openalex_non_200_is_reported_as_a_failed_lookup(self):
        with patch.object(
            hfc, "fetch_openalex_work",
            side_effect=hfc.OpenAlexLookupError("OpenAlex lookup failed: HTTP 503"),
        ):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertEqual("rejected", row["validation"])
        self.assertIn("503", row["reason"])

    def test_openalex_request_exception_is_reported_as_a_failed_lookup(self):
        with patch.object(
            hfc, "fetch_openalex_work",
            side_effect=hfc.OpenAlexLookupError("OpenAlex lookup failed: connection error"),
        ):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertEqual("rejected", row["validation"])
        self.assertIn("connection error", row["reason"])

    def test_openalex_404_is_a_genuine_absence_not_a_failure(self):
        with patch.object(hfc, "fetch_openalex_work", return_value={}):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertEqual("", row["validation"])
        self.assertIn("no open access location", row["reason"])


class FetchOpenalexWorkTests(unittest.TestCase):
    def test_404_returns_empty_dict(self):
        session = MagicMock()
        session.get.return_value = MagicMock(status_code=404)
        self.assertEqual({}, hfc.fetch_openalex_work("10.1/a", session))

    def test_non_200_non_404_raises_lookup_error(self):
        session = MagicMock()
        session.get.return_value = MagicMock(status_code=503)
        with self.assertRaises(hfc.OpenAlexLookupError):
            hfc.fetch_openalex_work("10.1/a", session)

    def test_request_exception_raises_lookup_error(self):
        import requests
        session = MagicMock()
        session.get.side_effect = requests.RequestException("boom")
        with self.assertRaises(hfc.OpenAlexLookupError):
            hfc.fetch_openalex_work("10.1/a", session)


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

    def test_all_failed_openalex_lookups_raise(self):
        """A systemic OpenAlex outage must not be reported as near-zero coverage."""
        with patch.object(
            hfc, "fetch_openalex_work",
            side_effect=hfc.OpenAlexLookupError("OpenAlex lookup failed: HTTP 503"),
        ):
            rows = [
                hfc.examine_output(
                    {"doi": f"10.1/{i}", "pure_uuid": f"u{i}", "title": "t"},
                    MagicMock(), MagicMock(),
                )
                for i in range(3)
            ]

        with self.assertRaises(RuntimeError):
            hfc.guard_against_total_failure(rows)
