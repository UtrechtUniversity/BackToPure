"""Durable apply evidence, independent of logs and final CSV export."""
import contextvars
import json
import os
from pathlib import Path
import requests
from workflow_http import FatalWorkflowError

current_apply = contextvars.ContextVar('current_apply', default=None)


def append_jsonl(path, entry):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(entry, sort_keys=True) + '\n')
        handle.flush()
        os.fsync(handle.fileno())


def journal_event(status, **values):
    context = current_apply.get()
    if context:
        path, identity, _children = context
        append_jsonl(path, {**identity, 'status': status, **values})


def record_child(person):
    context = current_apply.get()
    if context:
        context[2].append(person)
    journal_event('child_created', created_external_persons=[person])


def check_write_response(response):
    if response.status_code in (401, 403):
        raise FatalWorkflowError(f'Pure authorization failed (HTTP {response.status_code}); check API credentials and permissions.')
    if response.status_code >= 500 or response.status_code in (408, 429):
        raise FatalWorkflowError(f'Pure write outcome is unknown (HTTP {response.status_code}); reconcile in Pure before retrying.')
    return response


def request_write(call, url, **kwargs):
    """Never retry writes: a lost response does not imply the write failed."""
    kwargs.setdefault('timeout', 30)
    kwargs.setdefault('allow_redirects', False)
    try:
        return check_write_response(call(url, **kwargs))
    except requests.RequestException as exc:
        raise FatalWorkflowError('Pure write outcome is unknown after a connection failure; reconcile in Pure before retrying.') from exc


def created_uuid(response):
    try:
        body = response.json()
        uuid = body.get('uuid') if isinstance(body, dict) else None
    except ValueError:
        uuid = None
    if not uuid:
        raise FatalWorkflowError('Pure accepted a write but returned no valid UUID; reconcile the unknown outcome before retrying.')
    return uuid


def created_record_evidence(response):
    """Record acknowledged UUID and server version without a second remote read."""
    record_uuid = created_uuid(response)
    body = response.json()
    version = body.get('version')
    return {'uuid': record_uuid, 'created_version': version if isinstance(version, str) and version else None}
