"""Shared, conservative reservations for DOI creation and strict current-Pure lookup."""
import os
import re
import sqlite3
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from workflow_http import FatalWorkflowError, json_object, request_read, check_page_progress


def normalize_doi(value):
    if not isinstance(value, str):
        return ''
    value = value.strip()
    value = re.sub(r'^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)', '', value, flags=re.I)
    return value.strip().lower()


def tenant_key(base_url):
    parsed = urlsplit(base_url)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise FatalWorkflowError('Invalid Pure target URL; a tenant URL without credentials or query is required.')
    hostname = parsed.hostname.lower()
    if ':' in hostname:
        hostname = f'[{hostname}]'
    port = parsed.port
    if port and (parsed.scheme, port) not in (('https', 443), ('http', 80)):
        hostname += f':{port}'
    return urlunsplit((parsed.scheme.lower(), hostname, parsed.path.rstrip('/'), '', ''))


def runtime_db_path():
    """CLI must explicitly share the application's runtime, never an artifact-local DB."""
    configured = os.environ.get('BTP_RUNTIME_DB')
    if configured:
        path = Path(configured)
        if not path.is_absolute():
            raise FatalWorkflowError('BTP_RUNTIME_DB must be an absolute shared runtime database path.')
        return path
    raise FatalWorkflowError('Set BTP_RUNTIME_DB to the shared application database before creating records from the CLI.')


class Reservation:
    def __init__(self, db_path, tenant, doi, job_id, kind):
        self.db_path = Path(db_path)
        self.tenant, self.doi = tenant_key(tenant), normalize_doi(doi)
        if not self.doi.startswith('10.') or '/' not in self.doi or any(c.isspace() for c in self.doi):
            raise FatalWorkflowError('A valid normalized DOI is required before creating records.')
        self.job_id, self.kind, self.token = job_id, kind, uuid.uuid4().hex
        self.completed = None

    def connect(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.execute('''CREATE TABLE IF NOT EXISTS doi_write_reservations (
            tenant TEXT NOT NULL, doi TEXT NOT NULL, job_id TEXT NOT NULL,
            kind TEXT NOT NULL, token TEXT NOT NULL, state TEXT NOT NULL,
            record_uuid TEXT, error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant, doi))''')
        return connection

    def acquire(self):
        with self.connect() as connection:
            try:
                connection.execute('INSERT INTO doi_write_reservations (tenant, doi, job_id, kind, token, state) VALUES (?, ?, ?, ?, ?, ?)',
                                   (self.tenant, self.doi, self.job_id, self.kind, self.token, 'reserved'))
            except sqlite3.IntegrityError as exc:
                row = connection.execute('SELECT state, record_uuid FROM doi_write_reservations WHERE tenant=? AND doi=?', (self.tenant, self.doi)).fetchone()
                if row and row[0] in ('created', 'existing') and row[1]:
                    self.completed = row[1]
                    return self
                raise FatalWorkflowError(f'DOI {self.doi} is already reserved for this Pure tenant; reconcile the existing reservation before retrying.') from exc
        return self

    def finish(self, state, record_uuid=None, error=None):
        with self.connect() as connection:
            cursor = connection.execute('''UPDATE doi_write_reservations SET state=?, record_uuid=?, error=?, updated_at=CURRENT_TIMESTAMP
                WHERE tenant=? AND doi=? AND token=?''',
                (state, record_uuid, error, self.tenant, self.doi, self.token))
            if cursor.rowcount != 1:
                raise FatalWorkflowError('DOI reservation ownership was lost; stop and reconcile before retrying.')


def find_exact_doi(session, base_url, headers, kind, doi):
    endpoint = {'research_outputs': 'research-outputs', 'datasets': 'data-sets'}[kind]
    offset, full_count, matches, seen = 0, None, [], set()
    doi = normalize_doi(doi)
    while True:
        response = request_read(session.post, f'{base_url.rstrip("/")}/{endpoint}/search', headers=headers,
                                json={'searchString': doi, 'size': 100, 'offset': offset})
        body = json_object(response)
        count, items = body.get('count'), body.get('items')
        if type(count) is not int or count < 0 or not isinstance(items, list):
            raise FatalWorkflowError('Malformed Pure DOI lookup count/items; creation stopped.')
        if full_count is not None and count != full_count:
            raise FatalWorkflowError('Pure DOI lookup changed during pagination; creation stopped.')
        full_count = count
        if offset + len(items) > count or (not items and offset < count):
            raise FatalWorkflowError('Incomplete Pure DOI lookup pagination; creation stopped.')
        check_page_progress(items, seen)
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get('uuid'), str) or not item['uuid']:
                raise FatalWorkflowError('Malformed Pure DOI lookup record; creation stopped.')
            if kind == 'datasets':
                field = item.get('doi')
                if field is not None and not isinstance(field, dict):
                    raise FatalWorkflowError('Malformed dataset DOI; creation stopped.')
                values = [(field or {}).get('doi')]
            else:
                versions = item.get('electronicVersions')
                if versions is None:
                    versions = []
                if not isinstance(versions, list) or any(not isinstance(v, dict) for v in versions):
                    raise FatalWorkflowError('Malformed research output DOI; creation stopped.')
                values = [v.get('doi') for v in versions]
            if any(value is not None and not isinstance(value, str) for value in values):
                raise FatalWorkflowError('Malformed DOI value; creation stopped.')
            if any(normalize_doi(value) == doi for value in values):
                matches.append(item)
        offset += len(items)
        if offset == count:
            return matches


def create_once(session, base_url, headers, kind, doi, create):
    owner = os.environ.get('BTP_JOB_ID') or f'cli-{uuid.uuid4().hex}'
    reservation = Reservation(runtime_db_path(), base_url, doi, owner, kind).acquire()
    target_tenant = reservation.tenant
    try:
        matches = find_exact_doi(session, base_url, headers, kind, doi)
        other_kind = 'datasets' if kind == 'research_outputs' else 'research_outputs'
        matches += find_exact_doi(session, base_url, headers, other_kind, doi)
        if reservation.completed:
            match = next((item for item in matches if item['uuid'] == reservation.completed), None)
            if match is None:
                raise FatalWorkflowError('Previously completed DOI reservation no longer matches Pure; reconcile before retrying.')
            return {'success': True, 'uuid': match['uuid'], 'disposition': 'existing', 'target_tenant': target_tenant}
        if matches:
            reservation.finish('existing', matches[0]['uuid'])
            return {'success': True, 'uuid': matches[0]['uuid'], 'disposition': 'existing', 'target_tenant': target_tenant}
        # Persist before invoking create: the helper may create contributor records first.
        reservation.finish('writing')
        result = create()
        if result.get('success'):
            if not isinstance(result.get('uuid'), str) or not result['uuid']:
                raise FatalWorkflowError('Created record outcome is unknown: missing UUID; reconcile before retrying.')
            reservation.finish('created', result['uuid'])
            return {**result, 'disposition': 'created', 'target_tenant': target_tenant}
        reservation.finish('rejected', error=str(result.get('error') or 'Pure rejected creation'))
        return {**result, 'target_tenant': target_tenant}
    except BaseException as exc:
        # Even interrupted lookup reservations are retained conservatively. Never reclaim on time alone.
        if not reservation.completed:
            reservation.finish('unknown', error=str(exc))
        raise
