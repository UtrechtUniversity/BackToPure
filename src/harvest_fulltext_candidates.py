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
    DEFAULT_VERSION_POLICY,
    filter_by_version_policy,
    published_version_only,
    versions_allowed_by,
)
from fulltext_fetch import CandidateFetchError, download_and_validate, preflight
from fulltext_ratelimit import default_registry
from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

REVIEW_COLUMNS = [
    'to_be_updated', 'updated', 'doi', 'pure_uuid', 'title', 'candidate_url',
    'version', 'licence', 'access_status', 'confidence', 'validation',
    'size_bytes', 'reason',
]


class OpenAlexLookupError(Exception):
    """The OpenAlex lookup itself failed (network/HTTP/parse error), not a genuine absence."""


def _is_transport_failure_from_download_error(exc):
    """Tell a broken run apart from a legitimate per-record verdict.

    A CandidateFetchError carries a structured transport_failure flag set by
    fulltext_fetch (true for a 429 or a 5xx from the candidate host, false for
    an ordinary 4xx like 403 paywalled or 404 gone -- a real answer about that
    record, not the run). Any other exception -- a raw connection error,
    timeout, or anything not even a ValueError -- is transport-level: the run
    itself is what's broken.
    """
    if isinstance(exc, CandidateFetchError):
        return exc.transport_failure
    if isinstance(exc, ValueError):
        return False
    return True


def _output_dir():
    return os.environ.get('BTP_OUTPUT_DIR', 'output/full_text')


def has_attached_file(pure_record):
    """True when Pure already holds a file for this output (not just a link)."""
    for version in (pure_record or {}).get('electronicVersions') or []:
        if isinstance(version, dict) and version.get('typeDiscriminator') == 'FileElectronicVersion':
            return True
    return False


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
    """One OpenAlex work by DOI, or {} when OpenAlex genuinely has no record (HTTP 404).

    Raises OpenAlexLookupError when the lookup itself failed (network error, a
    non-200/404 status, or unparseable JSON) -- that is a different fact from
    "no record" and must not be reported as one.
    """
    getter = session.get if session is not None else requests.get
    try:
        response = getter(
            f"https://api.openalex.org/works/doi:{doi}",
            headers=OPENALEX_HEADERS,
            timeout=60,
        )
    except requests.RequestException as exc:
        logger.debug(f"OpenAlex lookup failed for {doi}: {exc}")
        raise OpenAlexLookupError(f"OpenAlex lookup failed: {exc}") from exc
    if response.status_code == 404:
        return {}
    if response.status_code != 200:
        raise OpenAlexLookupError(f"OpenAlex lookup failed: HTTP {response.status_code}")
    try:
        return response.json()
    except ValueError as exc:
        raise OpenAlexLookupError("OpenAlex lookup failed: could not parse response") from exc


def examine_output(entry, session, registry, version_policy=DEFAULT_VERSION_POLICY):
    """Build one review row for one publication. Never returns None."""
    try:
        work = fetch_openalex_work(entry.get('doi'), session)
    except OpenAlexLookupError as exc:
        return _row(entry, validation='rejected', reason=str(exc), transport_failure=True)
    all_candidates = candidates_from_openalex_work(work)
    candidates = filter_by_version_policy(all_candidates, version_policy)
    if not candidates:
        if all_candidates:
            # Name what was rejected and what would have accepted it: this
            # column is what a librarian reads when deciding whether to widen
            # the policy.
            found = sorted({c.get('version') or 'unknown version' for c in all_candidates})
            allowed = ', '.join(versions_allowed_by(version_policy))
            reason = (
                f"not an allowed version (policy '{version_policy}' allows {allowed}); "
                f"candidate is {', '.join(found)}"
            )
        else:
            reason = 'no open access location in OpenAlex'
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
        # A 4xx (401/403/404/other) or an HTML landing page is a real answer
        # about this record. A 429 (rate limited) or a 5xx from the candidate
        # host means the host itself is failing/throttling, which is
        # transport-level -- see PreflightResult.transport_failure.
        if getattr(checked, 'transport_failure', False):
            row['transport_failure'] = True
        return row

    try:
        payload, _filename = download_and_validate(session, candidate['url'], registry)
    except Exception as exc:
        row['validation'] = 'rejected'
        row['reason'] = str(exc)
        if _is_transport_failure_from_download_error(exc):
            row['transport_failure'] = True
        return row

    row['validation'] = 'ok'
    row['size_bytes'] = len(payload)
    row['to_be_updated'] = 'X'
    return row


def guard_against_total_failure(rows):
    """A run that is itself broken must not report zero coverage as a finding.

    A per-record verdict -- 403 (paywalled), 404, an HTML landing page -- is
    a legitimate answer about that record, not evidence the run failed, and
    must not trip this guard even when every single record comes back that
    way. What must trip it is every attempted record failing for a
    transport-level reason: an OpenAlex lookup failure, a connection error, a
    timeout, or a 5xx from the candidate host. That pattern means the run
    itself, not the records, is broken.
    """
    attempted = [row for row in rows if row.get('validation')]
    if attempted and all(row.get('transport_failure') for row in attempted):
        raise RuntimeError(
            f"Every attempted candidate fetch failed with a transport-level error "
            f"({len(attempted)} of {len(attempted)}); refusing to report zero coverage. "
            "Check network access and the per-host rate limits, then rerun."
        )


def write_review_file(rows):
    output_dir = _output_dir()
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, 'to_be_updated.csv')
    pd.DataFrame(rows, columns=REVIEW_COLUMNS).to_csv(path, index=False, encoding='utf-8')
    logger.info(f"Full-text review file written: {os.path.abspath(path)} ({len(rows)} row(s))")
    return path


def main(faculty_choice, test_choice='yes', version_policy=DEFAULT_VERSION_POLICY):
    logger.info("Script to report attachable open access full texts has started")
    versions_allowed_by(version_policy)  # fail fast on a bad policy, before any network work
    logger.info(f"Version policy: {version_policy} (allows {', '.join(versions_allowed_by(version_policy))})")
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
            rows.append(examine_output(entry, session, registry, version_policy))

    # Write the evidence before the guard can raise: if the run turns out to
    # be broken, the operator still needs the per-row reasons to diagnose it.
    write_review_file(rows)
    guard_against_total_failure(rows)

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
    parser.add_argument('version_policy', type=str, nargs='?', default=DEFAULT_VERSION_POLICY,
                        help="Which versions may be deposited: published (default), published_accepted, or any")
    parser.add_argument('test_choice', type=str, nargs='?', default='yes',
                        help='Run in test mode ("yes" or "no")')
    args = parser.parse_args()
    main(args.faculty_choice, args.test_choice, args.version_policy)
