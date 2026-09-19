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
