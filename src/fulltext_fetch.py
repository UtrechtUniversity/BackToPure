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
