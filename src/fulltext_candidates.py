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
        -version_rank(candidate.get("version")),
        -access_rank(candidate.get("access_status")),
        -link_type_rank(candidate.get("link_type")),
        -format_rank(candidate.get("format")),
        candidate.get("url") or "",
    )


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


def confidence_for(candidate):
    """direct / likely / weak, expressed with the ranks rather than a second scorer."""
    access = access_rank(candidate.get("access_status"))
    link = link_type_rank(candidate.get("link_type"))
    if access >= 4 and link >= link_type_rank("pdf"):
        return "direct"
    if access >= 3:
        return "likely"
    return "weak"


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


PUBLISHED_VERSION = "publishedVersion"
ACCEPTED_VERSION = "acceptedVersion"
SUBMITTED_VERSION = "submittedVersion"

# Version policy: which versions of a paper may be deposited into Pure.
# 'published' is the default because depositing an accepted manuscript or a
# preprint is a repository policy decision, not a technical one.
VERSION_POLICIES = {
    "published": (PUBLISHED_VERSION,),
    "published_accepted": (PUBLISHED_VERSION, ACCEPTED_VERSION),
    "any": (PUBLISHED_VERSION, ACCEPTED_VERSION, SUBMITTED_VERSION),
}
DEFAULT_VERSION_POLICY = "published"

# Pure's own version types, read from research-outputs/allowed-electronic-version-version-types.
# Every deposited file must carry the label that is true of it: labelling an
# accepted manuscript as the final published version is the bug issue #6 names.
PURE_VERSION_TYPES = {
    PUBLISHED_VERSION: (
        "/dk/atira/pure/researchoutput/electronicversion/versiontype/publishersversion",
        "Final published version",
    ),
    ACCEPTED_VERSION: (
        "/dk/atira/pure/researchoutput/electronicversion/versiontype/authorsversion",
        "Accepted author manuscript",
    ),
    SUBMITTED_VERSION: (
        "/dk/atira/pure/researchoutput/electronicversion/versiontype/preprint",
        "Submitted manuscript",
    ),
}


def version_rank(version):
    """Published beats accepted beats submitted.

    Used in ranking so that a paper offering several versions yields its best
    one, whatever the policy allows.
    """
    order = {PUBLISHED_VERSION: 3, ACCEPTED_VERSION: 2, SUBMITTED_VERSION: 1}
    return order.get(version, 0)


def versions_allowed_by(policy):
    """The versions a policy permits, or raise for an unknown policy.

    Raising matters: a typo must not silently fall back to the permissive end.
    """
    try:
        return VERSION_POLICIES[policy]
    except KeyError:
        raise ValueError(
            f"Unknown version policy {policy!r}. Valid values: {', '.join(sorted(VERSION_POLICIES))}"
        ) from None


def pure_version_type_for(version):
    """(uri, term) for Pure's electronic-version version type."""
    try:
        return PURE_VERSION_TYPES[version]
    except KeyError:
        raise ValueError(f"No Pure version type for {version!r}") from None


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


def filter_by_version_policy(candidates, policy=DEFAULT_VERSION_POLICY):
    """Keep only candidates whose version the policy allows."""
    allowed = versions_allowed_by(policy)
    return [c for c in candidates if c.get("version") in allowed]


def published_version_only(candidates):
    """Policy: only the publisher version may be deposited.

    Load-bearing. The Pure file entry hardcodes
    versionType=publishersversion, so an unfiltered candidate would be
    deposited under a label that is not true of it.
    """
    return [c for c in candidates if c.get("version") == PUBLISHED_VERSION]
