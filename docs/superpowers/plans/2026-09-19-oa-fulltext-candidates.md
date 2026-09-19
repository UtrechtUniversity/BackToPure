# OA Full-Text Candidates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new `full_text` job that reports, per faculty, which publications already in Pure could have an open-access publisher-version PDF attached — verified by actually fetching and validating each candidate.

**Architecture:** Three new modules in `src/`: pure ranking/mapping helpers ported from `doi_resolver`, a per-host rate limiter, and a network layer that preflights and validates PDFs. A job script wires them to the existing Ricgraph faculty selection and writes a review CSV. Registration as a new `JobType` makes the existing run/review/apply machinery apply; this plan implements run only.

**Tech Stack:** Python 3.12, `requests`, `pandas`, `unittest` (the repo's existing stack — no new dependencies).

**Spec:** `docs/superpowers/specs/2026-09-19-oa-fulltext-candidates-design.md`

## Global Constraints

- **No new dependencies.** `requirements.txt` stays as it is. In particular `pydantic` and `httpx` are NOT to be added — ported code must be rewritten to plain dicts and `requests`.
- **Published version only.** Candidates must be filtered to `version == "publishedVersion"` before anything else. This filter is load-bearing: `versionType: publishersversion` is hardcoded on the Pure file entry, so an unfiltered candidate would be mislabelled.
- **Module logger only.** Use `logger = setup_logging('btp', level=logging.INFO)` and `logger.x(...)`. Never `logging.x(...)` — those calls go nowhere.
- **No live network in tests.** Stub `requests` with `unittest.mock.patch`.
- **Every rejection is a recorded row with a reason**, never a silently dropped record.
- **Run `.venv/bin/python -m pytest -q` before every commit.** The suite is currently 181 passed, 5 skipped.

---

### Task 1: Pure candidate helpers

**Files:**
- Create: `src/fulltext_candidates.py`
- Test: `tests/test_fulltext_candidates.py`

**Interfaces:**
- Consumes: nothing
- Produces: `canonical_url(url: str) -> str`, `extract_domain(url: str) -> str | None`, `access_rank(status: str | None) -> int`, `link_type_rank(link_type: str | None) -> int`, `format_rank(fmt: str | None) -> int`, `ranking_key(candidate: dict) -> tuple`, `dedupe_candidates(candidates: list[dict]) -> list[dict]`, `confidence_for(candidate: dict) -> str`

Candidates are plain dicts with keys: `url`, `link_type`, `format`, `access_status`, `version`, `license`, `domain`, `notes` (list), `is_primary_from_source` (bool).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_fulltext_candidates.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k CanonicalUrl`
Expected: FAIL with `ModuleNotFoundError: No module named 'fulltext_candidates'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/fulltext_candidates.py
"""Pure helpers for ranking open-access full-text candidates.

Ported from doi_resolver (backend/app/services/fulltext.py and the Pure
mappers in backend/app/services/pure_enrichment.py). The aggregation entry
point there is deliberately NOT ported: it is pydantic-based and exists to
merge five discovery sources, while this job uses OpenAlex alone.

Candidates here are plain dicts, not models, so that no dependency is added.
"""
import logging
import re
from urllib.parse import urlparse

from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)


def canonical_url(url):
    parsed = urlparse((url or "").strip())
    normalized_path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{normalized_path}"


def extract_domain(url):
    parsed = urlparse(url or "")
    return parsed.netloc.lower() or None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k CanonicalUrl`
Expected: PASS (3 tests)

- [ ] **Step 5: Write the failing ranking test**

```python
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
```

- [ ] **Step 6: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k Ranking`
Expected: FAIL with `AttributeError: module 'fulltext_candidates' has no attribute 'access_rank'`

- [ ] **Step 7: Implement the rank helpers**

```python
def access_rank(access_status):
    if access_status == "open":
        return 4
    if access_status == "likely_open":
        return 3
    if access_status == "unknown":
        return 2
    if access_status == "restricted":
        return 1
    return 0


def link_type_rank(link_type):
    mapping = {
        "pdf": 7,
        "xml": 6,
        "html": 5,
        "repository_landing": 4,
        "publisher_landing": 3,
        "text_mining": 2,
        "landing_page": 1,
        "unknown": 0,
    }
    return mapping.get((link_type or "unknown").lower(), 0)


def format_rank(fmt):
    return {"pdf": 3, "xml": 2, "html": 1}.get((fmt or "").lower(), 0)


def ranking_key(candidate):
    return (
        -access_rank(candidate.get("access_status")),
        -link_type_rank(candidate.get("link_type")),
        -format_rank(candidate.get("format")),
        candidate.get("url") or "",
    )
```

- [ ] **Step 8: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k Ranking`
Expected: PASS (3 tests)

- [ ] **Step 9: Write the failing dedupe test**

This is the case ported from `doi_resolver/backend/tests/test_fulltext_service.py`.

```python
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
```

- [ ] **Step 10: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k Dedupe`
Expected: FAIL with `AttributeError: module 'fulltext_candidates' has no attribute 'dedupe_candidates'`

- [ ] **Step 11: Implement dedupe**

```python
def dedupe_candidates(candidates):
    """Merge candidates that point at the same URL, keeping the best of each field."""
    deduped = {}
    for candidate in candidates:
        key = canonical_url(candidate.get("url"))
        existing = deduped.get(key)
        if not existing:
            merged = dict(candidate)
            merged["notes"] = sorted(set(candidate.get("notes") or []))
            merged.setdefault("domain", extract_domain(candidate.get("url")))
            merged.setdefault("link_type", "unknown")
            merged.setdefault("access_status", "unknown")
            deduped[key] = merged
            continue
        existing["notes"] = sorted({*(existing.get("notes") or []), *(candidate.get("notes") or [])})
        if access_rank(candidate.get("access_status")) > access_rank(existing.get("access_status")):
            existing["access_status"] = candidate.get("access_status")
        if link_type_rank(candidate.get("link_type")) > link_type_rank(existing.get("link_type")):
            existing["link_type"] = candidate.get("link_type")
        if format_rank(candidate.get("format")) > format_rank(existing.get("format")):
            existing["format"] = candidate.get("format")
        if not existing.get("license") and candidate.get("license"):
            existing["license"] = candidate.get("license")
        if not existing.get("version") and candidate.get("version"):
            existing["version"] = candidate.get("version")
        if candidate.get("is_primary_from_source"):
            existing["is_primary_from_source"] = True
    return list(deduped.values())
```

- [ ] **Step 12: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k Dedupe`
Expected: PASS (2 tests)

- [ ] **Step 13: Write the failing confidence test**

```python
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
```

- [ ] **Step 14: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k Confidence`
Expected: FAIL with `AttributeError: module 'fulltext_candidates' has no attribute 'confidence_for'`

- [ ] **Step 15: Implement confidence**

```python
def confidence_for(candidate):
    """direct / likely / weak, expressed with the ranks rather than a second scorer."""
    access = access_rank(candidate.get("access_status"))
    link = link_type_rank(candidate.get("link_type"))
    if access >= 4 and link >= link_type_rank("pdf"):
        return "direct"
    if access >= 3:
        return "likely"
    return "weak"
```

- [ ] **Step 16: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`
Expected: PASS, 181 + 11 new tests

```bash
git add src/fulltext_candidates.py tests/test_fulltext_candidates.py
git commit -m "Add pure helpers for ranking OA full-text candidates

Ported from doi_resolver, rewritten to plain dicts so no dependency is added."
```

---

### Task 2: Pure access and licence mappers

**Files:**
- Modify: `src/fulltext_candidates.py`
- Test: `tests/test_fulltext_candidates.py`

**Interfaces:**
- Consumes: Task 1's module
- Produces: `map_access_type(value: str) -> tuple[str, str]`, `map_license_type(value: str) -> tuple[str | None, str]`, `infer_pdf_filename(url: str, content_disposition: str | None) -> str`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k PureMapper`
Expected: FAIL with `AttributeError: module 'fulltext_candidates' has no attribute 'map_access_type'`

- [ ] **Step 3: Implement the mappers**

```python
def map_access_type(value):
    """Pure open-access permission URI for a candidate's access status."""
    normalized = (value or "").strip().lower()
    if normalized in {"open", "gold", "green"}:
        return ("/dk/atira/pure/core/openaccesspermission/open", "Open")
    if normalized == "likely_open":
        return ("/dk/atira/pure/core/openaccesspermission/open", "Likely open")
    if normalized in {"restricted", "closed"}:
        return ("/dk/atira/pure/core/openaccesspermission/restricted", "Restricted")
    return ("/dk/atira/pure/core/openaccesspermission/unknown", "Unknown / No information")


def map_license_type(value):
    """Pure licence URI for a CC licence string, or (None, text) when unmapped."""
    normalized = (value or "").strip().lower().replace("-", " ").replace("_", " ")
    mapping = {
        "cc by": ("/dk/atira/pure/core/document/licenses/cc_by", "CC BY"),
        "cc by 4.0": ("/dk/atira/pure/core/document/licenses/cc_by", "CC BY"),
        "cc by sa": ("/dk/atira/pure/core/document/licenses/cc_by_sa", "CC BY-SA"),
        "cc by nc": ("/dk/atira/pure/core/document/licenses/cc_by_nc", "CC BY-NC"),
        "cc by nc sa": ("/dk/atira/pure/core/document/licenses/cc_by_nc_sa", "CC BY-NC-SA"),
        "cc by nd": ("/dk/atira/pure/core/document/licenses/cc_by_nd", "CC BY-ND"),
        "cc by nc nd": ("/dk/atira/pure/core/document/licenses/cc_by_nc_nd", "CC BY-NC-ND"),
    }
    return mapping.get(normalized, (None, (value or "").strip()))


def infer_pdf_filename(url, content_disposition):
    """Filename for the uploaded file, from Content-Disposition or the URL path."""
    if content_disposition:
        match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', content_disposition, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            if name:
                return name if name.lower().endswith(".pdf") else f"{name}.pdf"
    path_name = urlparse(url or "").path.rsplit("/", 1)[-1]
    if path_name and path_name.lower().endswith(".pdf"):
        return path_name
    return f"{path_name or 'fulltext'}.pdf"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k PureMapper`
Expected: PASS (6 tests, 3 subtests)

- [ ] **Step 5: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add src/fulltext_candidates.py tests/test_fulltext_candidates.py
git commit -m "Add Pure access-permission and licence mappers

Ported verbatim from doi_resolver: these URIs are the values we would
otherwise have to rediscover against a live Pure."
```

---

### Task 3: OpenAlex candidate extraction and the published-version filter

**Files:**
- Modify: `src/fulltext_candidates.py`
- Test: `tests/test_fulltext_candidates.py`

**Interfaces:**
- Consumes: Task 1 and 2
- Produces: `candidates_from_openalex_work(work: dict) -> list[dict]`, `published_version_only(candidates: list[dict]) -> list[dict]`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k OpenAlexCandidate`
Expected: FAIL with `AttributeError: module 'fulltext_candidates' has no attribute 'candidates_from_openalex_work'`

- [ ] **Step 3: Implement extraction and the filter**

```python
PUBLISHED_VERSION = "publishedVersion"


def _location_candidates(location, note):
    """A location can yield a PDF candidate and a landing-page candidate."""
    if not isinstance(location, dict):
        return []
    access_status = "open" if location.get("is_oa") else "unknown"
    version = location.get("version")
    licence = location.get("license")
    built = []
    if location.get("pdf_url"):
        built.append({
            "url": location["pdf_url"],
            "link_type": "pdf",
            "format": "pdf",
            "access_status": access_status,
            "version": version,
            "license": licence,
            "domain": extract_domain(location["pdf_url"]),
            "notes": [note],
        })
    if location.get("landing_page_url"):
        built.append({
            "url": location["landing_page_url"],
            "link_type": "publisher_landing",
            "format": None,
            "access_status": access_status,
            "version": version,
            "license": licence,
            "domain": extract_domain(location["landing_page_url"]),
            "notes": [note],
        })
    return built


def candidates_from_openalex_work(work):
    """Every OA location OpenAlex knows for one work, deduped and ranked."""
    work = work or {}
    candidates = _location_candidates(work.get("best_oa_location"), "best_oa_location")
    for location in work.get("locations") or []:
        candidates.extend(_location_candidates(location, "locations"))
    return sorted(dedupe_candidates(candidates), key=ranking_key)


def published_version_only(candidates):
    """Policy: only the publisher version may be deposited.

    Load-bearing. The Pure file entry hardcodes
    versionType=publishersversion, so an unfiltered candidate would be
    deposited under a label that is not true of it.
    """
    return [c for c in candidates if c.get("version") == PUBLISHED_VERSION]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_candidates.py -k OpenAlexCandidate`
Expected: PASS (5 tests)

- [ ] **Step 5: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add src/fulltext_candidates.py tests/test_fulltext_candidates.py
git commit -m "Extract OA candidates from OpenAlex works, published version only

The version filter is load-bearing: the Pure file entry hardcodes
versionType=publishersversion, so an accepted manuscript that slipped
through would be deposited under a false label."
```

---

### Task 4: Per-host rate limiter

**Files:**
- Create: `src/fulltext_ratelimit.py`
- Test: `tests/test_fulltext_ratelimit.py`

**Interfaces:**
- Consumes: nothing
- Produces: `TokenBucket(rate_per_second: float, capacity: float | None = None)` with `.acquire() -> None`, `RateLimiterRegistry(rates: dict[str, float], default_rate: float)` with `.bucket_for(host: str) -> TokenBucket` and `.acquire_for_url(url: str) -> None`, and `default_registry() -> RateLimiterRegistry`

Ported from `doi_resolver/backend/app/utils/ratelimit.py`, with `asyncio.Lock`/`asyncio.sleep` replaced by `threading.Lock`/`time.sleep` because this job uses a `ThreadPoolExecutor`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_fulltext_ratelimit.py
import threading
import time
import unittest

import fulltext_ratelimit as rl


class TokenBucketTests(unittest.TestCase):
    def test_first_acquire_is_immediate(self):
        bucket = rl.TokenBucket(rate_per_second=10)
        started = time.monotonic()
        bucket.acquire()
        self.assertLess(time.monotonic() - started, 0.05)

    def test_second_acquire_waits_for_the_rate(self):
        bucket = rl.TokenBucket(rate_per_second=20, capacity=1)
        bucket.acquire()
        started = time.monotonic()
        bucket.acquire()
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.04)

    def test_acquire_is_thread_safe(self):
        bucket = rl.TokenBucket(rate_per_second=200, capacity=1)
        errors = []

        def worker():
            try:
                bucket.acquire()
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual([], errors)


class RegistryTests(unittest.TestCase):
    def test_same_host_shares_one_bucket(self):
        registry = rl.RateLimiterRegistry(rates={}, default_rate=5)
        self.assertIs(registry.bucket_for("a.org"), registry.bucket_for("a.org"))

    def test_different_hosts_get_different_buckets(self):
        registry = rl.RateLimiterRegistry(rates={}, default_rate=5)
        self.assertIsNot(registry.bucket_for("a.org"), registry.bucket_for("b.org"))

    def test_per_host_rate_overrides_the_default(self):
        registry = rl.RateLimiterRegistry(rates={"slow.org": 0.5}, default_rate=5)
        self.assertEqual(0.5, registry.bucket_for("slow.org").rate_per_second)
        self.assertEqual(5, registry.bucket_for("other.org").rate_per_second)

    def test_default_registry_throttles_the_hosts_we_actually_hit(self):
        registry = rl.default_registry()
        for host in ("doi.org", "onlinelibrary.wiley.com", "dspace.library.uu.nl"):
            with self.subTest(host=host):
                self.assertLessEqual(registry.bucket_for(host).rate_per_second, 2)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_ratelimit.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'fulltext_ratelimit'`

- [ ] **Step 3: Implement the limiter**

```python
# src/fulltext_ratelimit.py
"""Per-host request throttling for full-text candidate validation.

Ported from doi_resolver (backend/app/utils/ratelimit.py), with asyncio
primitives replaced by threading ones because this job validates candidates
through a ThreadPoolExecutor.

Why per host rather than a global limit: over a 300-work sample of UU
publications, 44% of candidate URLs were on doi.org and the top three hosts
were 58% of all fetches. A global pool would hammer a handful of servers --
one of which (dspace.library.uu.nl) is UU's own repository. Throttling
arrives as 429s and Cloudflare challenges, which are served as HTML and are
correctly rejected by the validator, so it would show up in the report as
"no full text available" for papers that are perfectly available.
"""
import logging
import threading
import time
from urllib.parse import urlparse

from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

DEFAULT_RATE_PER_SECOND = 2.0

HOST_RATES = {
    "doi.org": 2.0,
    "dspace.library.uu.nl": 1.0,
    "onlinelibrary.wiley.com": 0.5,
    "link.springer.com": 0.5,
    "www.cambridge.org": 0.5,
    "pubs.acs.org": 0.5,
    "academic.oup.com": 0.5,
    "www.nature.com": 1.0,
}


class TokenBucket:
    """Allows rate_per_second acquisitions per second, bursting up to capacity."""

    def __init__(self, rate_per_second, capacity=None):
        self.rate_per_second = float(rate_per_second)
        self.capacity = float(capacity if capacity is not None else max(rate_per_second, 1.0))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity,
                    self._tokens + (now - self._updated) * self.rate_per_second,
                )
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1 - self._tokens) / self.rate_per_second
            # sleep outside the lock so other threads can refill and proceed
            time.sleep(wait)


class RateLimiterRegistry:
    """One bucket per host, created on first use."""

    def __init__(self, rates, default_rate):
        self._rates = dict(rates or {})
        self._default_rate = float(default_rate)
        self._buckets = {}
        self._lock = threading.Lock()

    def bucket_for(self, host):
        key = (host or "").lower()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self._rates.get(key, self._default_rate))
                self._buckets[key] = bucket
            return bucket

    def acquire_for_url(self, url):
        self.bucket_for(urlparse(url or "").netloc.lower()).acquire()


def default_registry():
    return RateLimiterRegistry(rates=HOST_RATES, default_rate=DEFAULT_RATE_PER_SECOND)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_ratelimit.py`
Expected: PASS (7 tests, 3 subtests)

- [ ] **Step 5: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add src/fulltext_ratelimit.py tests/test_fulltext_ratelimit.py
git commit -m "Add per-host rate limiting for full-text fetches

Measured over 300 UU works: 44% of candidate URLs are on doi.org and the
top three hosts are 58% of fetches. Without per-host throttling the report
would record publisher 429s as 'no full text available'."
```

---

### Task 5: Preflight and PDF validation

**Files:**
- Create: `src/fulltext_fetch.py`
- Test: `tests/test_fulltext_fetch.py`

**Interfaces:**
- Consumes: Task 2 (`infer_pdf_filename`), Task 4 (`default_registry`)
- Produces: `PreflightResult` (attributes `ok: bool`, `detail: str`, `http_status: int | None`, `content_type: str | None`), `preflight(session, url, registry=None) -> PreflightResult`, `download_and_validate(session, url, registry=None, max_bytes=MAX_PDF_BYTES) -> tuple[bytes, str]` raising `ValueError`, `MAX_PDF_BYTES: int`

- [ ] **Step 1: Write the failing preflight test**

```python
# tests/test_fulltext_fetch.py
import unittest
from unittest.mock import MagicMock

import fulltext_fetch as ff


def _response(status=200, headers=None, content=b""):
    response = MagicMock()
    response.status_code = status
    response.headers = headers or {}
    response.content = content
    return response


class PreflightTests(unittest.TestCase):
    def test_pdf_content_type_passes(self):
        session = MagicMock()
        session.head.return_value = _response(headers={"content-type": "application/pdf"})

        result = ff.preflight(session, "https://a.org/x.pdf")

        self.assertTrue(result.ok)

    def test_html_landing_page_is_rejected(self):
        session = MagicMock()
        session.head.return_value = _response(headers={"content-type": "text/html; charset=utf-8"})

        result = ff.preflight(session, "https://a.org/x")

        self.assertFalse(result.ok)
        self.assertIn("HTML", result.detail)

    def test_forbidden_is_rejected_as_protected(self):
        session = MagicMock()
        session.head.return_value = _response(status=403, headers={})

        result = ff.preflight(session, "https://a.org/x")

        self.assertFalse(result.ok)
        self.assertEqual(403, result.http_status)

    def test_unknown_content_type_is_allowed_through(self):
        """Deliberate: some publishers send octet-stream for real PDFs."""
        session = MagicMock()
        session.head.return_value = _response(headers={"content-type": "application/octet-stream"})

        self.assertTrue(ff.preflight(session, "https://a.org/x").ok)

    def test_head_failure_does_not_block_the_download(self):
        session = MagicMock()
        session.head.side_effect = Exception("HEAD unsupported")

        self.assertTrue(ff.preflight(session, "https://a.org/x").ok)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_fetch.py -k Preflight`
Expected: FAIL with `ModuleNotFoundError: No module named 'fulltext_fetch'`

- [ ] **Step 3: Implement preflight**

```python
# src/fulltext_fetch.py
"""Fetch and validate open-access full-text candidates.

Ported from doi_resolver (backend/app/services/pure_enrichment.py), rewritten
from async httpx to sync requests to match the rest of src/.

Every request goes through the per-host rate limiter first -- see
fulltext_ratelimit for why that is not optional.
"""
import logging

import requests

from fulltext_candidates import infer_pdf_filename
from fulltext_ratelimit import default_registry
from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

MAX_PDF_BYTES = 50 * 1024 * 1024
REQUEST_TIMEOUT = 60


class PreflightResult:
    def __init__(self, ok, detail, http_status=None, content_type=None):
        self.ok = ok
        self.detail = detail
        self.http_status = http_status
        self.content_type = content_type


def preflight(session, url, registry=None):
    """Cheap HEAD check before downloading.

    An unknown content-type passes deliberately: some publishers serve real
    PDFs as application/octet-stream. HTML is the signal we care about, since
    that is a landing page or a bot challenge rather than a file.
    """
    (registry or default_registry()).acquire_for_url(url)
    try:
        response = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
    except Exception:
        return PreflightResult(ok=True, detail="HEAD check unavailable; trying direct download.")

    content_type = (response.headers.get("content-type") or "").lower()
    if response.status_code >= 400:
        if response.status_code in {401, 403}:
            detail = f"HTTP {response.status_code} indicates protected access."
        elif response.status_code == 404:
            detail = "HTTP 404: source URL not found."
        else:
            detail = f"HTTP {response.status_code}: source did not provide an accessible file."
        return PreflightResult(False, detail, response.status_code, content_type or None)
    if "application/pdf" in content_type:
        return PreflightResult(True, "Direct PDF content-type detected.", content_type=content_type)
    if "text/html" in content_type:
        return PreflightResult(
            False,
            "URL resolves to HTML landing/challenge page, not a PDF file.",
            content_type=content_type,
        )
    return PreflightResult(True, "Preflight passed with unknown content-type.", content_type=content_type or None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_fetch.py -k Preflight`
Expected: PASS (5 tests)

- [ ] **Step 5: Write the failing download test**

```python
class DownloadTests(unittest.TestCase):
    PDF = b"%PDF-1.7\n stub"

    def test_valid_pdf_returns_bytes_and_filename(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/pdf"}, content=self.PDF
        )

        payload, filename = ff.download_and_validate(session, "https://a.org/article.pdf")

        self.assertEqual(self.PDF, payload)
        self.assertEqual("article.pdf", filename)

    def test_magic_bytes_accepted_without_pdf_content_type(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/octet-stream"}, content=self.PDF
        )

        payload, _filename = ff.download_and_validate(session, "https://a.org/x.pdf")

        self.assertEqual(self.PDF, payload)

    def test_html_body_is_rejected(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "text/html"}, content=b"<html>go away</html>"
        )

        with self.assertRaises(ValueError) as ctx:
            ff.download_and_validate(session, "https://a.org/x")
        self.assertIn("non-PDF", str(ctx.exception))

    def test_oversize_is_rejected(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/pdf"}, content=self.PDF + b"x" * 100
        )

        with self.assertRaises(ValueError) as ctx:
            ff.download_and_validate(session, "https://a.org/x.pdf", max_bytes=10)
        self.assertIn("size limit", str(ctx.exception))

    def test_error_status_serving_html_is_named_as_a_challenge(self):
        session = MagicMock()
        session.get.return_value = _response(
            status=403, headers={"content-type": "text/html"}, content=b"<html/>"
        )

        with self.assertRaises(ValueError) as ctx:
            ff.download_and_validate(session, "https://a.org/x.pdf")
        self.assertIn("HTML", str(ctx.exception))

    def test_rate_limiter_is_consulted_before_the_request(self):
        session = MagicMock()
        session.get.return_value = _response(
            headers={"content-type": "application/pdf"}, content=self.PDF
        )
        registry = MagicMock()

        ff.download_and_validate(session, "https://a.org/x.pdf", registry=registry)

        registry.acquire_for_url.assert_called_once_with("https://a.org/x.pdf")
```

- [ ] **Step 6: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_fetch.py -k Download`
Expected: FAIL with `AttributeError: module 'fulltext_fetch' has no attribute 'download_and_validate'`

- [ ] **Step 7: Implement download and validation**

```python
def download_and_validate(session, url, registry=None, max_bytes=MAX_PDF_BYTES):
    """Download a candidate and prove it is a PDF.

    Raises ValueError with a reason suitable for the review file's reason
    column. Callers record that reason rather than dropping the row.
    """
    (registry or default_registry()).acquire_for_url(url)
    response = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
    content_type = (response.headers.get("content-type") or "").lower()
    if response.status_code >= 400:
        if "text/html" in content_type:
            raise ValueError(
                f"HTTP {response.status_code} returned HTML instead of PDF "
                "(likely access protection/challenge)."
            )
        raise ValueError(f"HTTP {response.status_code} while fetching candidate.")

    payload = response.content or b""
    if len(payload) > max_bytes:
        raise ValueError(f"PDF candidate exceeds size limit ({len(payload)} bytes > {max_bytes} bytes).")
    if not ("application/pdf" in content_type or payload.startswith(b"%PDF-")):
        raise ValueError(
            f"Selected candidate returned non-PDF content (content-type={content_type or 'unknown'})."
        )
    return payload, infer_pdf_filename(url, response.headers.get("content-disposition"))
```

- [ ] **Step 8: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_fetch.py -k Download`
Expected: PASS (6 tests)

- [ ] **Step 9: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add src/fulltext_fetch.py tests/test_fulltext_fetch.py
git commit -m "Add preflight and PDF validation for full-text candidates

Ported from doi_resolver, httpx -> requests. Keeps the deliberate choice to
let an unknown content-type through: some publishers serve real PDFs as
application/octet-stream, while HTML means a landing page or bot challenge."
```

---

### Task 6: The harvest job script

**Files:**
- Create: `src/harvest_fulltext_candidates.py`
- Test: `tests/test_harvest_fulltext_candidates.py`

**Interfaces:**
- Consumes: Tasks 1-5, plus existing `enrich_pure_external_persons.select_faculties` and `.select_persons_researchoutput`, `enrich_pure_external_orgs.phase_timer`, `config.resolve_faculty_selection`
- Produces: `has_attached_file(pure_record: dict) -> bool`, `examine_output(entry: dict, session, registry) -> dict` (one review row), `REVIEW_COLUMNS: list[str]`, `main(faculty_choice: str, test_choice: str = 'yes') -> None`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_harvest_fulltext_candidates.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_harvest_fulltext_candidates.py -k AttachedFile`
Expected: FAIL with `ModuleNotFoundError: No module named 'harvest_fulltext_candidates'`

- [ ] **Step 3: Create the script with the attachment check**

```python
# src/harvest_fulltext_candidates.py
"""Report which Pure publications could have an OA publisher-version PDF attached.

Phase 1 of the full_text job: selects from Ricgraph by faculty, skips anything
that already has a file in Pure, asks OpenAlex for open-access locations,
keeps only the published version, and proves each candidate is really a PDF by
fetching it.

This writes a review file. It uploads nothing.
"""
import argparse
import logging
import os

import pandas as pd
import requests

import enrich_pure_external_persons as enrich
from config import OPENALEX_HEADERS
from enrich_pure_external_orgs import phase_timer
from fulltext_candidates import (
    candidates_from_openalex_work,
    confidence_for,
    published_version_only,
)
from fulltext_fetch import download_and_validate, preflight
from fulltext_ratelimit import default_registry
from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

REVIEW_COLUMNS = [
    'to_be_updated', 'updated', 'doi', 'pure_uuid', 'title', 'candidate_url',
    'version', 'licence', 'access_status', 'confidence', 'validation',
    'size_bytes', 'reason',
]


def _output_dir():
    return os.environ.get('BTP_OUTPUT_DIR', 'output/full_text')


def has_attached_file(pure_record):
    """True when Pure already holds a file for this output (not just a link)."""
    for version in (pure_record or {}).get('electronicVersions') or []:
        if isinstance(version, dict) and version.get('typeDiscriminator') == 'FileElectronicVersion':
            return True
    return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_harvest_fulltext_candidates.py -k AttachedFile`
Expected: PASS (3 tests)

- [ ] **Step 5: Write the failing row-building test**

```python
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
```

- [ ] **Step 6: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_harvest_fulltext_candidates.py -k ExamineOutput`
Expected: FAIL with `AttributeError: module 'harvest_fulltext_candidates' has no attribute 'fetch_openalex_work'`

- [ ] **Step 7: Implement OpenAlex lookup and row building**

```python
def _row(entry, **overrides):
    row = {column: '' for column in REVIEW_COLUMNS}
    row.update({
        'to_be_updated': '',
        'updated': ' ',
        'doi': entry.get('doi') or '',
        'pure_uuid': entry.get('pure_uuid') or '',
        'title': entry.get('title') or '',
        'size_bytes': '',
    })
    row.update(overrides)
    return row


def fetch_openalex_work(doi, session=None):
    """One OpenAlex work by DOI, or {} when it is not found."""
    getter = session.get if session is not None else requests.get
    try:
        response = getter(
            f"https://api.openalex.org/works/doi:{doi}",
            headers=OPENALEX_HEADERS,
            timeout=60,
        )
    except requests.RequestException as exc:
        logger.debug(f"OpenAlex lookup failed for {doi}: {exc}")
        return {}
    if response.status_code != 200:
        return {}
    try:
        return response.json()
    except ValueError:
        return {}


def examine_output(entry, session, registry):
    """Build one review row for one publication. Never returns None."""
    work = fetch_openalex_work(entry.get('doi'), session)
    candidates = published_version_only(candidates_from_openalex_work(work))
    if not candidates:
        any_candidate = candidates_from_openalex_work(work)
        reason = (
            'not the published version; policy is publisher version only'
            if any_candidate
            else 'no open access location in OpenAlex'
        )
        return _row(entry, reason=reason)

    candidate = candidates[0]
    row = _row(
        entry,
        candidate_url=candidate.get('url') or '',
        version=candidate.get('version') or '',
        licence=candidate.get('license') or '',
        access_status=candidate.get('access_status') or '',
        confidence=confidence_for(candidate),
    )

    checked = preflight(session, candidate['url'], registry)
    if not checked.ok:
        row['validation'] = 'rejected'
        row['reason'] = checked.detail
        return row

    try:
        payload, _filename = download_and_validate(session, candidate['url'], registry)
    except Exception as exc:
        row['validation'] = 'rejected'
        row['reason'] = str(exc)
        return row

    row['validation'] = 'ok'
    row['size_bytes'] = len(payload)
    row['to_be_updated'] = 'X'
    return row
```

- [ ] **Step 8: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_harvest_fulltext_candidates.py -k ExamineOutput`
Expected: PASS (6 tests)

- [ ] **Step 9: Write the failing main-flow test**

```python
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
```

- [ ] **Step 10: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_harvest_fulltext_candidates.py -k MainFlow`
Expected: FAIL with `AttributeError: module 'harvest_fulltext_candidates' has no attribute 'guard_against_total_failure'`

- [ ] **Step 11: Implement the guard and main**

```python
def guard_against_total_failure(rows):
    """A run where every attempted fetch failed is systemic, not a finding.

    Same rule as the harvests: reporting zero coverage after 100% network
    failure is a silently wrong answer.
    """
    attempted = [row for row in rows if row.get('validation')]
    if attempted and all(row.get('validation') == 'rejected' for row in attempted):
        raise RuntimeError(
            f"All {len(attempted)} candidate fetch(es) failed; refusing to report zero "
            "coverage. Check network access and the per-host rate limits, then rerun."
        )


def write_review_file(rows):
    output_dir = _output_dir()
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, 'to_be_updated.csv')
    pd.DataFrame(rows, columns=REVIEW_COLUMNS).to_csv(path, index=False, encoding='utf-8')
    logger.info(f"Full-text review file written: {os.path.abspath(path)} ({len(rows)} row(s))")
    return path


def main(faculty_choice, test_choice='yes'):
    logger.info("Script to report attachable open access full texts has started")
    logger.info(f"Run mode: test_choice={test_choice} (this harvest writes a review file only)")
    timings = []

    with phase_timer("select faculties", timings):
        faculties = enrich.select_faculties(faculty_choice)
    logger.info(f"Selected {len(faculties)} faculty key(s)")

    with phase_timer("select research outputs from Ricgraph", timings):
        outputs = enrich.select_persons_researchoutput(faculties)

    with phase_timer("fetch research outputs from Pure", timings):
        purejsons = enrich.fetch_pure_researchoutputs(outputs, allow_doi_fallback=False)
    by_uuid = purejsons.get('by_uuid') or {}

    session = requests.Session()
    registry = default_registry()
    rows = []
    skipped_with_file = 0

    with phase_timer("examine candidates", timings):
        for count, entry in enumerate(outputs, start=1):
            if count % 50 == 0:
                logger.info(f"Examined {count}/{len(outputs)} publication(s)")
            pure_record = by_uuid.get(str(entry.get('pure_uuid') or ''))
            if has_attached_file(pure_record or {}):
                skipped_with_file += 1
                rows.append(_row(entry, reason='Pure already holds a file for this output'))
                continue
            rows.append(examine_output(entry, session, registry))

    guard_against_total_failure(rows)
    write_review_file(rows)

    attachable = sum(1 for row in rows if row.get('to_be_updated') == 'X')
    logger.info(
        f"Full-text funnel: {len(outputs)} publication(s) examined, {skipped_with_file} already "
        f"hold a file, {attachable} with a validated publisher-version PDF"
    )
    breakdown = ", ".join(f"{label} {seconds:.1f}s" for label, seconds in timings)
    logger.info(f"Phase timings: {breakdown}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Report attachable OA full texts')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='uu faculty: faculteit geowetenschappen|organization_name',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='yes',
                        help='Run in test mode ("yes" or "no")')
    args = parser.parse_args()
    main(args.faculty_choice, args.test_choice)
```

- [ ] **Step 12: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_harvest_fulltext_candidates.py`
Expected: PASS (12 tests)

- [ ] **Step 13: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add src/harvest_fulltext_candidates.py tests/test_harvest_fulltext_candidates.py
git commit -m "Add the full-text candidate harvest script

Reports every publication examined, with a reason on each rejection, so the
file is a coverage document even where the answer is no."
```

---

### Task 7: Register the job type

**Files:**
- Modify: `app/models/jobs.py:7-12` (the `JobType` enum) and `JOB_TYPE_REGISTRY`
- Test: `tests/test_app_structure.py`

**Interfaces:**
- Consumes: Task 6's script path
- Produces: `JobType.FULL_TEXT` with value `"full_text"`, registered in `JOB_TYPE_REGISTRY`

- [ ] **Step 1: Write the failing test**

```python
class FullTextJobTypeTests(unittest.TestCase):
    def test_full_text_job_type_is_registered(self):
        from app.models.jobs import JOB_TYPE_REGISTRY, JobType, get_job_type_definition

        self.assertIn(JobType.FULL_TEXT, JOB_TYPE_REGISTRY)
        definition = get_job_type_definition(JobType.FULL_TEXT.value)
        self.assertEqual("src/harvest_fulltext_candidates.py", definition.script_path)
        self.assertEqual("output/full_text", definition.artifact_dir)
        self.assertEqual(("doi",), definition.identity_columns)
        self.assertIn("faculty_choice", definition.allowed_params)

    def test_full_text_job_declares_its_review_csv(self):
        from app.models.jobs import JobType, get_job_type_definition

        definition = get_job_type_definition(JobType.FULL_TEXT.value)
        self.assertIn("to_be_updated.csv", definition.required_csv)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_app_structure.py -k FullTextJobType`
Expected: FAIL with `AttributeError: FULL_TEXT`

- [ ] **Step 3: Register the job type**

In `app/models/jobs.py`, add to the enum:

```python
class JobType(str, Enum):
    INTERNAL_PERSONS = "internal_persons"
    EXTERNAL_PERSONS = "external_persons"
    EXTERNAL_ORGS = "external_orgs"
    RESEARCH_OUTPUTS = "research_outputs"
    DATASETS = "datasets"
    FULL_TEXT = "full_text"
```

and to `JOB_TYPE_REGISTRY`:

```python
    JobType.FULL_TEXT: JobTypeDefinition(
        job_type=JobType.FULL_TEXT,
        script_path="src/harvest_fulltext_candidates.py",
        artifact_dir="output/full_text",
        entity_label_plural="full texts",
        required_csv=("to_be_updated.csv",),
        allowed_params=("faculty_choice", "facultyChoice"),
        cli_param_aliases=(("faculty_choice", "facultyChoice"),),
        identity_columns=("doi",),
    ),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_app_structure.py -k FullTextJobType`
Expected: PASS (2 tests)

- [ ] **Step 5: Verify the job is creatable through the API**

Run:

```bash
curl -s -X POST http://127.0.0.1:5002/api/jobs -H 'Content-Type: application/json' \
  -d '{"jobType":"full_text","params":{"faculty_choice":"uu faculty: centre for science communications and culture|organization_name"}}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_type'), d.get('id'))"
```

Expected: `full_text job_xxxxxxxx` (restart the app first — it loads the registry at import).

- [ ] **Step 6: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add app/models/jobs.py tests/test_app_structure.py
git commit -m "Register the full_text job type

Phase 1 only: the job runs and writes a review file. Apply is not wired up."
```

---

### Task 8: End-to-end run on a small faculty

**Files:**
- No source changes expected. If this task needs code changes, they are bug fixes with their own tests.

**Interfaces:**
- Consumes: everything above

- [ ] **Step 1: Run against the smallest faculty**

```bash
PYTHONPATH=src .venv/bin/python src/harvest_fulltext_candidates.py \
  "uu faculty: centre for science communications and culture|organization_name" "yes"
```

Expected: completes, logs a funnel line and phase timings, writes `output/full_text/to_be_updated.csv`.

- [ ] **Step 2: Check the review file is coherent**

```bash
head -3 output/full_text/to_be_updated.csv
python3 -c "
import csv, collections
rows=list(csv.DictReader(open('output/full_text/to_be_updated.csv')))
print('rows:', len(rows))
print('attachable:', sum(1 for r in rows if r['to_be_updated']=='X'))
print('reasons:', collections.Counter(r['reason'] for r in rows).most_common(8))
"
```

Expected: every row has either `to_be_updated=X` or a non-empty `reason`. No row has both empty.

- [ ] **Step 3: Confirm no row was silently dropped**

```bash
python3 -c "
import csv
rows=list(csv.DictReader(open('output/full_text/to_be_updated.csv')))
bad=[r for r in rows if r['to_be_updated']!='X' and not r['reason']]
print('rows with neither a tick nor a reason:', len(bad))
assert not bad, bad[:3]
"
```

Expected: `0`

- [ ] **Step 4: Spot-check one validated candidate by hand**

Pick a row with `to_be_updated=X`, open its `candidate_url` in a browser, and confirm it is the published PDF of that DOI. This is the one check no test can do.

- [ ] **Step 5: Commit any fixes and report**

Report to the user: rows examined, how many already hold a file, how many are attachable, the reason breakdown, and the phase timings. Those numbers are the deliverable — they are what the repository team needs in order to answer the remaining policy questions in issue #6.

---

## Notes for the implementer

- `select_persons_researchoutput` returns dicts with `doi` and optionally `pure_uuid`. It does not return titles; `examine_output` reads `entry.get('title')` and will produce an empty title column. That is acceptable for this slice — if titles matter for review, take them from the Pure record in `by_uuid` rather than adding a Ricgraph call.
- `fetch_pure_researchoutputs` lives in `enrich_pure_external_persons` and returns `{"results": [...], "by_doi": {...}, "by_uuid": {...}}`. It raises when every batch fails, which is deliberate.
- Do not add a retry loop around `download_and_validate`. The rate limiter handles pacing; `doi_resolver` explicitly removed a second retry loop because it meant up to 12 PUTs to Pure for one action.
- This job will be slow. That is expected and is why the phase timing is there.
