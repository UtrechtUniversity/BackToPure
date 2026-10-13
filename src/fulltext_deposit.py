"""Deposit a validated open access PDF onto a Pure research output.

Two steps that belong together: Pure's file upload returns a key that
expires after two hours if nothing references it, so uploading and
attaching are one operation, not two phases.
"""
import logging
from apply_state import request_write

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
    response = request_write(session.put,
        PURE_BASE_URL + 'research-outputs/file-uploads',
        headers=headers,
        data=payload,
        timeout=UPLOAD_TIMEOUT,
        allow_redirects=False,
    )
    if response.status_code not in (200, 201):
        raise DepositError(
            f"Pure rejected the file upload for {filename}: HTTP {response.status_code} {response.text[:200]}"
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise DepositError(f"Pure returned a non-JSON upload response for {filename}") from exc
    if not isinstance(body, dict):
        raise DepositError(f'Pure returned an invalid upload response for {filename}')
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


def persisted_attachment_evidence(response, before):
    """Use only the acknowledged PUT representation, never a filename guess.

    A later GET cannot prove which attachment our request introduced. Missing
    response evidence must not turn an acknowledged deposit into a failed write.
    """
    from copy import deepcopy
    warning = {'added_electronic_version': None,
               'rollback_evidence_warning': 'Acknowledged deposit has no exact persisted attachment identity; operator reconciliation required'}
    try:
        after = response.json()
        before_version = before.get('version')
        version = after.get('version') if isinstance(after, dict) else None
        if (not isinstance(after, dict) or after.get('uuid') != before.get('uuid')
                or not isinstance(before_version, str) or not before_version.strip()
                or not isinstance(version, str) or not version.strip() or version == before_version):
            return warning
        previous = before.get('electronicVersions') or []
        versions = deepcopy(after.get('electronicVersions'))
        if not isinstance(previous, list) or not isinstance(versions, list) or len(versions) != len(previous) + 1:
            return warning
        ids = [item.get('pureId') for item in versions if isinstance(item, dict)]
        if len(ids) != len(versions) or any(value is None for value in ids) or len(set(ids)) != len(ids):
            return warning
        # Exact unchanged baseline plus a single new persisted entry avoids
        # attributing another client's file or a server-normalized ambiguity.
        for item in previous:
            matches = [i for i, candidate in enumerate(versions) if candidate == item]
            if len(matches) != 1:
                return warning
            versions.pop(matches[0])
        added = versions[0]
        if (added.get('typeDiscriminator') != 'FileElectronicVersion'
                or not isinstance(added.get('file'), dict) or added['file'].get('uploadedFile')):
            return warning
        return {'added_electronic_version': added, 'applied_version': version}
    except Exception:
        return warning
