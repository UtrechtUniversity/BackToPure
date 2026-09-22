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


def _doi_version(doi, access="open", version="Final published version"):
    return {
        "typeDiscriminator": "DoiElectronicVersion",
        "doi": doi,
        "accessType": {"uri": f"/dk/atira/pure/core/openaccesspermission/{access}"},
        "versionType": {"term": {"en_GB": version}},
    }


class OpenDoiVersionTests(unittest.TestCase):
    """The first real run deposited 63 PDFs, and every one of them landed on a
    record that already linked its own DOI as an open full text. Only
    FileElectronicVersion was being checked, so the link was invisible."""

    DOI = "10.1038/s41598-018-25436-2"

    def test_own_doi_open_and_published_blocks_the_deposit(self):
        record = {"electronicVersions": [_doi_version(self.DOI)]}
        self.assertTrue(hfc.has_open_version_for_own_doi(record, self.DOI))

    def test_doi_comparison_ignores_url_prefix_and_case(self):
        record = {"electronicVersions": [_doi_version(self.DOI)]}
        self.assertTrue(
            hfc.has_open_version_for_own_doi(record, "https://doi.org/10.1038/S41598-018-25436-2")
        )

    def test_unknown_access_still_allows_the_deposit(self):
        """An Unknown access link says nothing about reachability, so a
        deposited file still adds something."""
        record = {"electronicVersions": [_doi_version(self.DOI, access="unknown")]}
        self.assertFalse(hfc.has_open_version_for_own_doi(record, self.DOI))

    def test_a_different_doi_does_not_block_the_deposit(self):
        record = {"electronicVersions": [_doi_version("10.1234/other")]}
        self.assertFalse(hfc.has_open_version_for_own_doi(record, self.DOI))

    def test_open_link_to_another_version_does_not_block_the_deposit(self):
        record = {"electronicVersions": [_doi_version(self.DOI, version="Submitted manuscript")]}
        self.assertFalse(hfc.has_open_version_for_own_doi(record, self.DOI))

    def test_policy_widens_which_versions_count_as_already_present(self):
        record = {"electronicVersions": [_doi_version(self.DOI, version="Accepted author manuscript")]}
        self.assertFalse(hfc.has_open_version_for_own_doi(record, self.DOI))
        self.assertTrue(
            hfc.has_open_version_for_own_doi(record, self.DOI, version_policy="published_accepted")
        )

    def test_record_without_a_doi_is_never_blocked(self):
        record = {"electronicVersions": [_doi_version(self.DOI)]}
        self.assertFalse(hfc.has_open_version_for_own_doi(record, ""))


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
        # The reason must name both what was rejected and what would be allowed,
        # since this column is what a librarian reads when deciding the policy.
        self.assertIn("acceptedVersion", row["reason"])
        self.assertIn("publishedVersion", row["reason"])
        self.assertIn("published", row["reason"])

    def test_no_oa_location_is_reported_with_a_reason(self):
        with patch.object(hfc, "fetch_openalex_work", return_value={"doi": "https://doi.org/10.1/a"}):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertIn("no open access location", row["reason"])

    def test_html_landing_page_is_reported_not_dropped(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=self._work()), patch.object(
            hfc, "preflight", return_value=MagicMock(
                ok=False, detail="URL resolves to HTML landing/challenge page, not a PDF file.",
                http_status=None, transport_failure=False,
            )
        ):
            row = hfc.examine_output(self.ENTRY, MagicMock(), MagicMock())

        self.assertEqual("", row["to_be_updated"])
        self.assertIn("HTML", row["reason"])

    def test_download_failure_is_reported_not_dropped(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=self._work()), patch.object(
            hfc, "preflight", return_value=MagicMock(ok=True, detail="ok", transport_failure=False)
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


def _oa_work(version="publishedVersion"):
    return {
        "doi": "https://doi.org/10.1/a",
        "best_oa_location": {
            "pdf_url": "https://publisher.org/a.pdf",
            "version": version,
            "license": "cc-by",
            "is_oa": True,
        },
    }


class MainFlowTests(unittest.TestCase):
    def test_run_where_every_fetch_fails_with_a_transport_error_raises(self):
        """A run that validated nothing for transport reasons must not report zero coverage as a result."""
        rows = [
            {
                "to_be_updated": "", "validation": "rejected",
                "reason": "HTTP 500 while fetching candidate.", "transport_failure": True,
            }
            for _ in range(3)
        ]

        with self.assertRaises(RuntimeError) as ctx:
            hfc.guard_against_total_failure(rows)

        self.assertIn("every", str(ctx.exception).lower())

    def test_mixed_results_do_not_raise(self):
        rows = [
            {"to_be_updated": "X", "validation": "ok", "reason": ""},
            {
                "to_be_updated": "", "validation": "rejected",
                "reason": "HTTP 500 while fetching candidate.", "transport_failure": True,
            },
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

    def test_all_paywalled_or_landing_page_rejections_do_not_raise(self):
        """A healthy run whose honest answer is 'nothing attachable' must not abort."""
        with patch.object(hfc, "fetch_openalex_work", return_value=_oa_work()):
            with patch.object(
                hfc, "preflight",
                return_value=MagicMock(
                    ok=False, detail="HTTP 403 indicates protected access.",
                    http_status=403, transport_failure=False,
                ),
            ):
                rows = [
                    hfc.examine_output(
                        {"doi": f"10.1/{i}", "pure_uuid": f"u{i}", "title": "t"},
                        MagicMock(), MagicMock(),
                    )
                    for i in range(24)
                ]

        self.assertTrue(all(row["validation"] == "rejected" for row in rows))
        self.assertFalse(any(row.get("transport_failure") for row in rows))
        # Must not raise.
        hfc.guard_against_total_failure(rows)

    def test_five_hundred_from_candidate_host_is_a_transport_failure(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=_oa_work()):
            with patch.object(
                hfc, "preflight",
                return_value=MagicMock(
                    ok=False, detail="HTTP 503: source did not provide an accessible file.",
                    http_status=503, transport_failure=True,
                ),
            ):
                row = hfc.examine_output({"doi": "10.1/x", "pure_uuid": "u", "title": "t"}, MagicMock(), MagicMock())

        self.assertTrue(row.get("transport_failure"))

    def test_429_from_candidate_host_is_a_transport_failure_via_preflight(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=_oa_work()):
            with patch.object(
                hfc, "preflight",
                return_value=MagicMock(
                    ok=False, detail="HTTP 429: rate limited by source; not evidence the file is unavailable.",
                    http_status=429, transport_failure=True,
                ),
            ):
                row = hfc.examine_output({"doi": "10.1/x", "pure_uuid": "u", "title": "t"}, MagicMock(), MagicMock())

        self.assertTrue(row.get("transport_failure"))
        self.assertIn("rate limited", row["reason"])

    def test_429_from_candidate_host_is_a_transport_failure_via_download(self):
        with patch.object(hfc, "fetch_openalex_work", return_value=_oa_work()):
            with patch.object(
                hfc, "preflight", return_value=MagicMock(ok=True, detail="ok", transport_failure=False),
            ), patch.object(
                hfc, "download_and_validate",
                side_effect=hfc.CandidateFetchError(
                    "HTTP 429: rate limited by source while fetching candidate.",
                    status_code=429, transport_failure=True,
                ),
            ):
                row = hfc.examine_output({"doi": "10.1/x", "pure_uuid": "u", "title": "t"}, MagicMock(), MagicMock())

        self.assertTrue(row.get("transport_failure"))
        self.assertIn("rate limited", row["reason"])

    def test_all_429_run_trips_the_guard(self):
        """An all-429 run is throttling, not absence, and must trip the guard."""
        with patch.object(hfc, "fetch_openalex_work", return_value=_oa_work()):
            with patch.object(
                hfc, "preflight",
                return_value=MagicMock(
                    ok=False, detail="HTTP 429: rate limited by source; not evidence the file is unavailable.",
                    http_status=429, transport_failure=True,
                ),
            ):
                rows = [
                    hfc.examine_output(
                        {"doi": f"10.1/{i}", "pure_uuid": f"u{i}", "title": "t"},
                        MagicMock(), MagicMock(),
                    )
                    for i in range(5)
                ]

        with self.assertRaises(RuntimeError):
            hfc.guard_against_total_failure(rows)

    def test_review_file_is_written_even_when_the_guard_then_raises(self):
        """The evidence must survive an abort: write_review_file must run before the guard raises."""
        rows = [
            {
                "to_be_updated": "", "validation": "rejected",
                "reason": "HTTP 500 while fetching candidate.", "transport_failure": True,
            }
        ]
        calls = []

        with patch.object(hfc.enrich, "select_faculties", return_value=["fac"]), \
             patch.object(hfc.enrich, "select_persons_researchoutput", return_value=[{"doi": "10.1/x", "pure_uuid": "u", "title": "t"}]), \
             patch.object(hfc.enrich, "fetch_pure_researchoutputs", return_value={"by_uuid": {}}), \
             patch.object(hfc, "examine_output", return_value=rows[0]), \
             patch.object(hfc, "write_review_file", side_effect=lambda r: calls.append(list(r))) as mock_write, \
             patch.object(hfc, "default_registry", return_value=MagicMock()):
            with self.assertRaises(RuntimeError):
                hfc.main("all")

        mock_write.assert_called_once()
        self.assertEqual(1, len(calls[0]))
        self.assertTrue(calls[0][0].get("transport_failure"))
