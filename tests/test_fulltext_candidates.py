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
