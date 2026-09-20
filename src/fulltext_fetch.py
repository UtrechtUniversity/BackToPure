"""Fetch and validate open-access full-text candidates.

Ported from doi_resolver (backend/app/services/pure_enrichment.py), rewritten
from async httpx to sync requests to match the rest of src/.

Every request goes through the per-host rate limiter first -- see
fulltext_ratelimit for why that is not optional.

HTTP 429 is deliberately NOT folded into the generic "no full text available"
bucket. The per-host rate limiter exists to keep hosts from throttling us in
the first place; if a 429 slips through anyway and gets reported as an
absence, that is the exact failure the limiter was built to prevent,
re-entering by the back door. So 429 is always treated as a transport-level
condition here: it gets a distinct, honest reason, and it is flagged so
callers count it toward guard_against_total_failure rather than a per-record
verdict. A single bounded retry, honouring Retry-After when present, is
attempted first -- not an elaborate retry framework (a sibling project found
that stacked retry loops caused up to 12 requests for one action), just one
extra try before giving up and reporting the 429 honestly.
"""
import logging
import time

import requests

from fulltext_candidates import infer_pdf_filename
from fulltext_ratelimit import default_registry
from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

MAX_PDF_BYTES = 50 * 1024 * 1024
REQUEST_TIMEOUT = 60
DOWNLOAD_CHUNK_SIZE = 65536
MAX_RETRY_AFTER_SECONDS = 5


class CandidateFetchError(ValueError):
    """A ValueError carrying the HTTP status and whether it is transport-level.

    Carrying these as structured fields (rather than leaving callers to parse
    the status code back out of the message text) avoids logic that silently
    degrades if the wording ever drifts.
    """

    def __init__(self, message, status_code=None, transport_failure=False):
        super().__init__(message)
        self.status_code = status_code
        self.transport_failure = transport_failure


class PreflightResult:
    def __init__(self, ok, detail, http_status=None, content_type=None, transport_failure=False):
        self.ok = ok
        self.detail = detail
        self.http_status = http_status
        self.content_type = content_type
        self.transport_failure = transport_failure


def _retry_after_seconds(response, cap=MAX_RETRY_AFTER_SECONDS):
    """Retry-After as a bounded number of seconds, or 0 when absent/unparseable."""
    value = (getattr(response, "headers", None) or {}).get("Retry-After")
    if not value:
        return 0
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return 0
    return max(0.0, min(seconds, cap))


def preflight(session, url, registry=None):
    """Cheap HEAD check before downloading.

    An unknown content-type passes deliberately: some publishers serve real
    PDFs as application/octet-stream. HTML is the signal we care about, since
    that is a landing page or a bot challenge rather than a file.
    """
    registry = registry or default_registry()
    registry.acquire_for_url(url)
    try:
        response = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
    except Exception:
        return PreflightResult(ok=True, detail="HEAD check unavailable; trying direct download.")

    if response.status_code == 429:
        wait = _retry_after_seconds(response)
        if wait:
            time.sleep(wait)
        registry.acquire_for_url(url)
        try:
            response = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        except Exception:
            return PreflightResult(ok=True, detail="HEAD check unavailable; trying direct download.")

    content_type = (response.headers.get("content-type") or "").lower()
    if response.status_code == 429:
        return PreflightResult(
            False,
            "HTTP 429: rate limited by source; not evidence the file is unavailable.",
            response.status_code,
            content_type or None,
            transport_failure=True,
        )
    if response.status_code >= 400:
        if response.status_code in {401, 403}:
            detail = f"HTTP {response.status_code} indicates protected access."
        elif response.status_code == 404:
            detail = "HTTP 404: source URL not found."
        else:
            detail = f"HTTP {response.status_code}: source did not provide an accessible file."
        return PreflightResult(
            False, detail, response.status_code, content_type or None,
            transport_failure=response.status_code >= 500,
        )
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

    Streams the body and aborts as soon as max_bytes is exceeded, rather than
    materialising the whole response first -- an unbounded or very large
    response should be rejected, not read into memory in full.

    Raises CandidateFetchError (a ValueError) with a reason suitable for the
    review file's reason column. Callers record that reason rather than
    dropping the row.
    """
    registry = registry or default_registry()
    registry.acquire_for_url(url)
    response = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, stream=True)
    try:
        if response.status_code == 429:
            wait = _retry_after_seconds(response)
            response.close()
            if wait:
                time.sleep(wait)
            registry.acquire_for_url(url)
            response = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, stream=True)

        content_type = (response.headers.get("content-type") or "").lower()
        if response.status_code == 429:
            raise CandidateFetchError(
                "HTTP 429: rate limited by source while fetching candidate.",
                status_code=429,
                transport_failure=True,
            )
        if response.status_code >= 400:
            if "text/html" in content_type:
                raise CandidateFetchError(
                    f"HTTP {response.status_code} returned HTML instead of PDF "
                    "(likely access protection/challenge).",
                    status_code=response.status_code,
                    transport_failure=response.status_code >= 500,
                )
            raise CandidateFetchError(
                f"HTTP {response.status_code} while fetching candidate.",
                status_code=response.status_code,
                transport_failure=response.status_code >= 500,
            )

        chunks = bytearray()
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            if not chunk:
                continue
            chunks.extend(chunk)
            if len(chunks) > max_bytes:
                raise ValueError(
                    f"PDF candidate exceeds size limit ({len(chunks)} bytes > {max_bytes} bytes)."
                )
        payload = bytes(chunks)
        if not ("application/pdf" in content_type or payload.startswith(b"%PDF-")):
            raise ValueError(
                f"Selected candidate returned non-PDF content (content-type={content_type or 'unknown'})."
            )
        return payload, infer_pdf_filename(url, response.headers.get("content-disposition"))
    finally:
        try:
            response.close()
        except Exception:
            pass
