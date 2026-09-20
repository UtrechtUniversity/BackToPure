"""Deposit a validated open access PDF onto a Pure research output.

Two steps that belong together: Pure's file upload returns a key that
expires after two hours if nothing references it, so uploading and
attaching are one operation, not two phases.
"""
import logging

from config import PURE_BASE_URL, PURE_HEADERS
from fulltext_candidates import map_access_type, map_license_type, pure_version_type_for
from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

UPLOAD_TIMEOUT = 180


class DepositError(Exception):
    """A deposit step failed. The message becomes the row's reason."""


def upload_pdf(session, payload, filename):
    """Upload raw PDF bytes to Pure and return the upload key."""
    headers = dict(PURE_HEADERS)
    headers['Content-Type'] = 'application/pdf'
    headers['Accept'] = 'application/json'
    response = session.put(
        PURE_BASE_URL + 'research-outputs/file-uploads',
        headers=headers,
        data=payload,
        timeout=UPLOAD_TIMEOUT,
    )
    if response.status_code not in (200, 201):
        raise DepositError(
            f"Pure rejected the file upload for {filename}: HTTP {response.status_code} {response.text[:200]}"
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise DepositError(f"Pure returned a non-JSON upload response for {filename}") from exc
    key = body.get('key') or ((body.get('uploadedFile') or {}).get('key'))
    if not key:
        raise DepositError(f"Pure returned no upload key for {filename}")
    logger.debug(f"uploaded {filename} to Pure as {key}")
    return key


def build_file_electronic_version(candidate, upload_key, filename):
    """The electronicVersions entry that attaches an uploaded file.

    versionType comes from the candidate. Hardcoding publishersversion --
    which the sibling project does -- would label an accepted manuscript as
    the final published version once the version policy is widened.
    """
    access_uri, access_term = map_access_type(candidate.get('access_status') or 'open')
    version_uri, version_term = pure_version_type_for(candidate.get('version'))

    entry = {
        "typeDiscriminator": "FileElectronicVersion",
        "title": filename.rsplit('.', maxsplit=1)[0],
        "accessType": {"uri": access_uri, "term": {"en_GB": access_term}},
        "versionType": {"uri": version_uri, "term": {"en_GB": version_term}},
        "file": {
            "fileName": filename,
            "mimeType": "application/pdf",
            "uploadedFile": {"key": upload_key},
        },
    }

    licence = candidate.get('license')
    if licence:
        licence_uri, licence_term = map_license_type(licence)
        if licence_uri:
            entry["licenseType"] = {"uri": licence_uri, "term": {"en_GB": licence_term}}
        else:
            entry["userDefinedLicense"] = licence_term
    return entry
