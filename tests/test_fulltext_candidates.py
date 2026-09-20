import unittest

import fulltext_candidates as fc


class CanonicalUrlTests(unittest.TestCase):
    def test_strips_trailing_slash_and_lowercases_host(self):
        self.assertEqual(
            "https://example.org/a/b",
            fc.canonical_url("https://EXAMPLE.org/a/b/"),
        )

    def test_same_path_different_case_host_is_one_key(self):
        self.assertEqual(
            fc.canonical_url("https://Example.org/x"),
            fc.canonical_url("https://example.org/x"),
        )

    def test_extract_domain(self):
        self.assertEqual("doi.org", fc.extract_domain("https://doi.org/10.1/a"))
        self.assertIsNone(fc.extract_domain("not-a-url"))


class RankingTests(unittest.TestCase):
    def test_access_rank_orders_open_above_restricted(self):
        self.assertGreater(fc.access_rank("open"), fc.access_rank("likely_open"))
        self.assertGreater(fc.access_rank("likely_open"), fc.access_rank("unknown"))
        self.assertGreater(fc.access_rank("unknown"), fc.access_rank("restricted"))
        self.assertEqual(0, fc.access_rank(None))

    def test_pdf_outranks_landing_page(self):
        self.assertGreater(fc.link_type_rank("pdf"), fc.link_type_rank("publisher_landing"))
        self.assertGreater(fc.format_rank("pdf"), fc.format_rank("html"))

    def test_best_candidate_sorts_first(self):
        weak = {"url": "https://b.org/x", "link_type": "landing_page",
                "format": "html", "access_status": "unknown"}
        best = {"url": "https://a.org/x.pdf", "link_type": "pdf",
                "format": "pdf", "access_status": "open"}
        ranked = sorted([weak, best], key=fc.ranking_key)
        self.assertEqual(best["url"], ranked[0]["url"])


class DedupeTests(unittest.TestCase):
    def test_same_url_merges_and_keeps_best_access_status(self):
        candidates = [
            {"url": "https://a.org/x/", "access_status": "unknown", "format": "html",
             "link_type": "landing_page", "license": None, "version": None,
             "notes": ["seen in locations"]},
            {"url": "https://A.org/x", "access_status": "open", "format": "pdf",
             "link_type": "pdf", "license": "cc-by", "version": "publishedVersion",
             "notes": ["best_oa_location"]},
        ]

        merged = fc.dedupe_candidates(candidates)

        self.assertEqual(1, len(merged))
        self.assertEqual("open", merged[0]["access_status"])
        self.assertEqual("pdf", merged[0]["format"])
        self.assertEqual("cc-by", merged[0]["license"])
        self.assertEqual("publishedVersion", merged[0]["version"])
        self.assertEqual(["best_oa_location", "seen in locations"], merged[0]["notes"])

    def test_different_urls_are_kept_apart(self):
        candidates = [
            {"url": "https://a.org/x", "access_status": "open"},
            {"url": "https://b.org/y", "access_status": "open"},
        ]
        self.assertEqual(2, len(fc.dedupe_candidates(candidates)))


class ConfidenceTests(unittest.TestCase):
    def test_open_pdf_link_is_direct(self):
        candidate = {"url": "https://a.org/x.pdf", "link_type": "pdf", "access_status": "open"}
        self.assertEqual("direct", fc.confidence_for(candidate))

    def test_open_landing_page_is_likely(self):
        candidate = {"url": "https://a.org/x", "link_type": "publisher_landing",
                     "access_status": "likely_open"}
        self.assertEqual("likely", fc.confidence_for(candidate))

    def test_restricted_is_weak(self):
        candidate = {"url": "https://a.org/x", "link_type": "pdf", "access_status": "restricted"}
        self.assertEqual("weak", fc.confidence_for(candidate))


class PureMapperTests(unittest.TestCase):
    def test_open_maps_to_pure_open_permission(self):
        uri, term = fc.map_access_type("open")
        self.assertEqual("/dk/atira/pure/core/openaccesspermission/open", uri)
        self.assertEqual("Open", term)

    def test_unknown_value_falls_back_to_unknown_permission(self):
        uri, _term = fc.map_access_type("something else")
        self.assertEqual("/dk/atira/pure/core/openaccesspermission/unknown", uri)

    def test_cc_by_variants_map_to_one_uri(self):
        for value in ("cc-by", "CC BY", "cc_by"):
            with self.subTest(value=value):
                uri, term = fc.map_license_type(value)
                self.assertEqual("/dk/atira/pure/core/document/licenses/cc_by", uri)
                self.assertEqual("CC BY", term)

    def test_unmapped_licence_returns_no_uri_but_keeps_the_text(self):
        uri, term = fc.map_license_type("publisher-specific")
        self.assertIsNone(uri)
        self.assertEqual("publisher-specific", term)

    def test_filename_comes_from_url_when_no_content_disposition(self):
        self.assertEqual("article.pdf", fc.infer_pdf_filename("https://a.org/article.pdf", None))

    def test_filename_falls_back_when_url_has_no_name(self):
        self.assertTrue(fc.infer_pdf_filename("https://a.org/download?id=7", None).endswith(".pdf"))


class OpenAlexCandidateTests(unittest.TestCase):
    WORK = {
        "doi": "https://doi.org/10.1/a",
        "best_oa_location": {
            "pdf_url": "https://publisher.org/a.pdf",
            "landing_page_url": "https://publisher.org/a",
            "version": "publishedVersion",
            "license": "cc-by",
            "is_oa": True,
        },
        "locations": [
            {
                "pdf_url": "https://repo.uu.nl/a.pdf",
                "landing_page_url": "https://repo.uu.nl/a",
                "version": "acceptedVersion",
                "license": None,
                "is_oa": True,
            }
        ],
    }

    def test_pdf_url_becomes_a_pdf_candidate(self):
        candidates = fc.candidates_from_openalex_work(self.WORK)
        pdf = [c for c in candidates if c["url"] == "https://publisher.org/a.pdf"]
        self.assertEqual(1, len(pdf))
        self.assertEqual("pdf", pdf[0]["link_type"])
        self.assertEqual("publishedVersion", pdf[0]["version"])
        self.assertEqual("open", pdf[0]["access_status"])

    def test_locations_are_included_as_candidates(self):
        urls = {c["url"] for c in fc.candidates_from_openalex_work(self.WORK)}
        self.assertIn("https://repo.uu.nl/a.pdf", urls)

    def test_work_without_any_location_yields_nothing(self):
        self.assertEqual([], fc.candidates_from_openalex_work({"doi": "https://doi.org/10.1/b"}))

    def test_published_version_only_drops_accepted_manuscripts(self):
        kept = fc.published_version_only(fc.candidates_from_openalex_work(self.WORK))
        self.assertTrue(kept)
        self.assertTrue(all(c["version"] == "publishedVersion" for c in kept))
        self.assertNotIn("https://repo.uu.nl/a.pdf", {c["url"] for c in kept})

    def test_published_version_only_drops_candidates_with_no_version(self):
        self.assertEqual([], fc.published_version_only([{"url": "https://a.org/x", "version": None}]))


class VersionPolicyTests(unittest.TestCase):
    """Which versions may be deposited is a repository policy decision."""

    def _candidates(self):
        return [
            {"url": "https://a.org/pub.pdf", "version": "publishedVersion"},
            {"url": "https://b.org/acc.pdf", "version": "acceptedVersion"},
            {"url": "https://c.org/sub.pdf", "version": "submittedVersion"},
            {"url": "https://d.org/none.pdf", "version": None},
        ]

    def test_default_policy_is_published_only(self):
        self.assertEqual("published", fc.DEFAULT_VERSION_POLICY)
        kept = fc.filter_by_version_policy(self._candidates())
        self.assertEqual(["publishedVersion"], [c["version"] for c in kept])

    def test_published_accepted_admits_accepted_but_not_preprints(self):
        kept = fc.filter_by_version_policy(self._candidates(), "published_accepted")
        self.assertEqual(["publishedVersion", "acceptedVersion"], [c["version"] for c in kept])

    def test_any_admits_all_three_but_still_not_a_missing_version(self):
        kept = fc.filter_by_version_policy(self._candidates(), "any")
        self.assertEqual(
            ["publishedVersion", "acceptedVersion", "submittedVersion"],
            [c["version"] for c in kept],
        )

    def test_unknown_policy_raises_rather_than_falling_back_to_permissive(self):
        with self.assertRaises(ValueError) as ctx:
            fc.filter_by_version_policy(self._candidates(), "everything")
        self.assertIn("Unknown version policy", str(ctx.exception))

    def test_published_outranks_accepted_which_outranks_submitted(self):
        ranked = sorted(self._candidates(), key=fc.ranking_key)
        self.assertEqual("publishedVersion", ranked[0]["version"])
        self.assertEqual("acceptedVersion", ranked[1]["version"])
        self.assertEqual("submittedVersion", ranked[2]["version"])

    def test_best_version_wins_even_when_a_weaker_one_looks_better_otherwise(self):
        """Under a permissive policy a paper offering both must still yield the
        publisher version, not a preprint that happens to be a direct PDF."""
        candidates = [
            {"url": "https://c.org/sub.pdf", "version": "submittedVersion",
             "link_type": "pdf", "format": "pdf", "access_status": "open"},
            {"url": "https://a.org/pub", "version": "publishedVersion",
             "link_type": "publisher_landing", "format": None, "access_status": "unknown"},
        ]
        ranked = sorted(candidates, key=fc.ranking_key)
        self.assertEqual("publishedVersion", ranked[0]["version"])

    def test_pure_version_type_labels_each_version_truthfully(self):
        cases = {
            "publishedVersion": ("publishersversion", "Final published version"),
            "acceptedVersion": ("authorsversion", "Accepted author manuscript"),
            "submittedVersion": ("preprint", "Submitted manuscript"),
        }
        for version, (suffix, term) in cases.items():
            with self.subTest(version=version):
                uri, got_term = fc.pure_version_type_for(version)
                self.assertTrue(uri.endswith(suffix), uri)
                self.assertEqual(term, got_term)

    def test_pure_version_type_refuses_an_unknown_version(self):
        with self.assertRaises(ValueError):
            fc.pure_version_type_for("somethingElse")

    def test_published_version_only_still_means_published_only(self):
        kept = fc.published_version_only(self._candidates())
        self.assertEqual(["publishedVersion"], [c["version"] for c in kept])
