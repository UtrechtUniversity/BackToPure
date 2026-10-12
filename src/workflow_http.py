"""Bounded safe reads and validation shared by harvest workflows.

Only GET and read-only POST search calls belong here. Never retry a write with
this helper: a lost response does not establish that the write failed.
"""
import logging
import hashlib
import json
import time
from urllib.parse import urlsplit

import requests

logger = logging.getLogger('btp')


class FatalWorkflowError(RuntimeError):
    """The source cannot be trusted; stop rather than publish incomplete results."""


def request_read(send, url, *, allowed_status=(), **kwargs):
    kwargs.setdefault('timeout', 30)
    endpoint = urlsplit(url).path  # Do not expose credentials or query strings.
    for attempt in range(3):
        try:
            response = send(url, **kwargs)
            if response.status_code in allowed_status:
                return response
            response.raise_for_status()
            if isinstance(response.status_code, int) and response.status_code >= 400:
                raise requests.HTTPError(response=response)
            return response
        except requests.RequestException as exc:
            response = getattr(exc, 'response', None)
            status = getattr(response, 'status_code', None)
            retryable = isinstance(exc, (requests.ConnectionError, requests.Timeout)) or status in (429, 500, 502, 503, 504)
            if not retryable or attempt == 2:
                detail = f'HTTP {status}' if status is not None else type(exc).__name__
                raise FatalWorkflowError(
                    f'Source request failed for {endpoint}: {detail}. '
                    'Harvest stopped; results are incomplete. Check source availability and credentials.'
                ) from exc
            logger.warning('Temporary source failure for %s; retry %s/2', endpoint, attempt + 1)
            time.sleep(0.5 * (2 ** attempt))


def json_object(response):
    try:
        data = response.json()
    except ValueError as exc:
        raise FatalWorkflowError('Source returned invalid JSON; harvest stopped.') from exc
    if not isinstance(data, dict):
        raise FatalWorkflowError('Source returned a non-object JSON response; harvest stopped.')
    return data


def records(data, key):
    values = data.get(key)
    if not isinstance(values, list):
        raise FatalWorkflowError(f'Source response field {key!r} must be a list; harvest stopped.')
    valid = [record for record in values if isinstance(record, dict)]
    if len(valid) != len(values):
        logger.warning('Source returned %s invalid %s records; skipping them', len(values) - len(valid), key)
    return valid


def check_page_progress(items, seen):
    """Stop APIs which ignore offsets instead of collecting duplicate pages forever."""
    if not items:
        return
    fingerprint = hashlib.sha256(json.dumps(items, sort_keys=True).encode()).digest()
    if fingerprint in seen:
        raise FatalWorkflowError('Source repeated a search page; pagination stopped to avoid incomplete results.')
    seen.add(fingerprint)
