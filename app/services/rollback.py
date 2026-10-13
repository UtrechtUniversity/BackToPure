"""Conservative, journalled rollback of acknowledged job-owned changes.

SQLite transactions surround local intent/ack only; no transaction spans HTTP.
A writing intent is never replayed without manual outcome reconciliation.
"""
import json
import uuid
from copy import deepcopy

import requests

from app.db import connect_db
from safe_creation import tenant_key as normalize_tenant

RETRYABLE = {'pending', 'rejected', 'read_failed', 'cancelled'}
_MISSING = object()


class Conflict(ValueError):
    pass


def recover_rollback(service, job_id):
    """A lost acknowledgement is uncertain, even if Pure now looks restored."""
    with connect_db(service.db_path) as db:
        db.execute("""UPDATE job_change_set_items SET rollback_status='unknown',
            conflict_reason='Rollback write outcome unknown; reconcile saved intent and Pure audit history.'
            WHERE rollback_status='writing' AND change_set_id IN
            (SELECT id FROM job_change_sets WHERE job_id=?)""", (job_id,))
        db.execute("UPDATE rollback_attempts SET state='unknown',finished_at=?,error_message='Execution interrupted before acknowledgement' WHERE job_id=? AND state='writing'", (service._utcnow(), job_id))


def _leaves(value, path=()):
    if isinstance(value, dict) and value and not (path and path[-1] == 'country'):
        result = {}
        for key, child in value.items():
            result.update(_leaves(child, path + (key,)))
        return result
    return {path: value}


def _at(value, path):
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return _MISSING
        value = value[key]
    return value


def restore_address(current, old, applied):
    """Undo only changed leaves; Pure's merge needs null for added leaves."""
    before = _leaves(old or {}) if old else {}
    after = _leaves(applied or {}) if applied else {}
    restored = deepcopy(current) if isinstance(current, dict) else {}
    for path in before.keys() | after.keys():
        previous, expected = before.get(path, _MISSING), after.get(path, _MISSING)
        if previous == expected:
            continue
        actual = _at(current, path)
        if actual != expected and not (expected is _MISSING and actual is None):
            raise Conflict('An affected address field changed after apply')
        if not path:
            raise Conflict('Address structure has insufficient leaf ownership evidence')
        node = restored
        for key in path[:-1]:
            child = node.get(key)
            if child is None:
                node[key] = {}
            elif not isinstance(child, dict):
                raise Conflict('An affected address structure changed after apply')
            node = node[key]
        node[path[-1]] = None if previous is _MISSING else deepcopy(previous)
    return restored


class RollbackExecutor:
    def __init__(self, service, change_set, log_path, kind):
        self.service, self.change_set, self.log_path, self.kind = service, change_set, log_path, kind
        self.stats = dict(checked=0, rolled_back=0, conflicts=0, failed=0, unknown=0, cancelled=0)

    def outcome(self, items, status, reason=None, attempt=None):
        with connect_db(self.service.db_path) as db:
            for item in items:
                db.execute('UPDATE job_change_set_items SET rollback_status=?,conflict_reason=? WHERE id=?', (status, reason, item['id']))
            if attempt:
                db.execute('UPDATE rollback_attempts SET state=?,finished_at=?,error_message=? WHERE id=?', (status, self.service._utcnow(), reason, attempt))
        key = {'conflict': 'conflicts', 'rejected': 'failed', 'read_failed': 'failed'}.get(status, status)
        if key in self.stats:
            self.stats[key] += len(items)
        self.service._append_log(self.log_path, f"{status.upper()} {','.join(str(item['entity_uuid']) for item in items)}: {reason or 'Pure acknowledged rollback'}\n")

    def cancelled(self):
        if getattr(self.service, 'cancellation_requested', False):
            return True
        task = getattr(self.service, 'execution_task', None)
        if task:
            with connect_db(self.service.db_path) as db:
                row = db.execute("SELECT state,cancel_requested FROM execution_tasks WHERE id=?", (task['id'],)).fetchone()
            return row is None or row['state'] != 'running' or bool(row['cancel_requested'])
        return False  # Internal executor wrappers are used for isolated unit tests.

    def credentials(self):
        from app.services.jobs import _pure_credentials
        return _pure_credentials()

    def intent(self, items, url, payload, tenant):
        attempt = uuid.uuid4().hex
        with connect_db(self.service.db_path) as db:
            db.execute('BEGIN IMMEDIATE')
            for item in items:
                row = db.execute('SELECT rollback_status FROM job_change_set_items WHERE id=?', (item['id'],)).fetchone()
                # Internal wrapper tests can use synthetic items, but cannot replay durable evidence.
                if row is not None and row[0] not in RETRYABLE:
                    raise Conflict('Rollback item has already been claimed; reconcile first')
            db.execute("""INSERT INTO rollback_attempts(id,job_id,change_set_id,item_ids_json,target_tenant,method,url,payload_json,state,created_at)
                VALUES(?,?,?,?,?,'PUT',?,?,'writing',?)""", (attempt, self.change_set.get('job_id'), self.change_set.get('id'), json.dumps([i['id'] for i in items]), tenant, url, json.dumps(payload, sort_keys=True), self.service._utcnow()))
            for item in items:
                db.execute("UPDATE job_change_set_items SET rollback_status='writing',conflict_reason=NULL WHERE id=?", (item['id'],))
        return attempt

    def mutate(self, record, item):
        payload = item.get('new_value') or {}
        if payload.get('disposition') in {'existing', 'noop'}:
            raise Conflict('This change was not made by this job')
        if self.kind in {'research_outputs', 'datasets'}:
            expected = payload.get('created_version')
            if expected is None or record.get('version') != expected:
                raise Conflict('Created record version missing or changed; operator cleanup required; child external persons preserved')
            raise Conflict('Pure has no documented conditional DELETE; GET/version check cannot close the edit race. Operator cleanup required; child external persons preserved')
        if self.kind == 'full_text':
            added = payload.get('added_electronic_version')
            if not isinstance(added, dict) or added.get('pureId') is None:
                raise Conflict('No exact persisted attachment ownership evidence; historical snapshot cannot authorize removal')
            versions = record.get('electronicVersions')
            if not isinstance(versions, list):
                raise Conflict('Invalid electronicVersions representation')
            matches = [i for i, value in enumerate(versions) if isinstance(value, dict) and value.get('pureId') == added['pureId']]
            if len(matches) != 1 or versions[matches[0]] != added:
                raise Conflict('Job-owned attachment missing, duplicated, or changed after apply')
            record['electronicVersions'] = [value for i, value in enumerate(versions) if i != matches[0]]
            return
        if self.kind == 'external_orgs' and item.get('identifier_type') == 'address':
            field = payload.get('field') or item['field_name']
            old = item.get('old_value') or {}
            record[field] = restore_address(record.get(field), old.get('value'), payload.get('value'))
            return
        if self.kind == 'internal_persons' and item.get('field_name') == 'orcid':
            if record.get('orcid') != payload.get('value'):
                raise Conflict('Current ORCID does not match the job-applied value')
            record['orcid'] = item.get('old_value') or None
            return
        identifiers = record.get('identifiers')
        if not isinstance(identifiers, list) or not payload.get('id') or not payload.get('uri'):
            raise Conflict('Insufficient exact identifier ownership evidence')
        matches = [i for i, entry in enumerate(identifiers) if isinstance(entry, dict) and entry.get('id') == payload['id'] and isinstance(entry.get('type'), dict) and entry['type'].get('uri') == payload['uri']]
        if len(matches) != 1:
            raise Conflict('Job-applied identifier missing or duplicated')
        if item.get('old_value'):
            # Additive identifier apply never owns replacement of another identifier.
            raise Conflict('Historical replacement ownership unclear; operator reconciliation required')
        record['identifiers'] = [entry for i, entry in enumerate(identifiers) if i != matches[0]]

    def execute_group(self, items, endpoint, entity):
        self.stats['checked'] += len(items)
        if self.cancelled():
            self.outcome(items, 'cancelled', 'Cancelled before Pure read')
            return
        base, headers = self.credentials()
        tenant = normalize_tenant(base)
        owned = []
        for item in items:
            if (item.get('new_value') or {}).get('target_tenant') != tenant:
                self.outcome([item], 'conflict', 'Target tenant missing or different from apply evidence')
            else:
                owned.append(item)
        if not owned:
            return
        url = f'{base.rstrip("/")}/{endpoint}/{entity}'
        try:
            result = requests.get(url, headers=headers, timeout=30, allow_redirects=False)
            if self.cancelled():
                self.outcome(owned, 'cancelled', 'Cancelled after Pure read; no write sent')
                return
            if result.status_code == 404:
                raise Conflict('Record no longer exists in Pure')
            result.raise_for_status()
            if not 200 <= result.status_code < 300:
                raise Conflict('Pure read did not return a success response')
            record = result.json()
            version = record.get('version') if isinstance(record, dict) else None
            if not isinstance(record, dict) or record.get('uuid') != entity or not isinstance(version, str) or not version.strip():
                raise Conflict('Fresh UUID/version evidence missing or invalid')
        except Conflict as exc:
            self.outcome(owned, 'conflict', str(exc))
            return
        except Exception as exc:
            self.outcome(owned, 'read_failed', f'Could not load {endpoint}/{entity}: {exc}')
            return
        accepted = []
        for item in owned:
            candidate = deepcopy(record)
            try:
                self.mutate(candidate, item)
            except Conflict as exc:
                self.outcome([item], 'conflict', str(exc))
            else:
                record = candidate
                accepted.append(item)
        if not accepted:
            return
        if self.cancelled():
            self.outcome(accepted, 'cancelled', 'Cancelled before Pure write')
            return
        current_base, current_headers = self.credentials()
        if normalize_tenant(current_base) != tenant:
            self.outcome(accepted, 'conflict', 'Configured target changed before rollback write')
            return
        attempt = self.intent(accepted, url, record, tenant)
        if self.cancelled():
            self.outcome(accepted, 'cancelled', 'Cancelled after intent; no write sent', attempt)
            return
        try:
            result = requests.put(url, headers=current_headers, json=record, timeout=180 if self.kind == 'full_text' else 30, allow_redirects=False)
            code = result.status_code
        except Exception as exc:
            self.outcome(accepted, 'unknown', f'Rollback outcome unknown: {exc}; reconcile before retrying', attempt)
            return
        # Persistence/logging failures are execution failures, never transport
        # uncertainty. Do not downgrade an acknowledgement already committed.
        if 200 <= code < 300:
            self.outcome(accepted, 'rolled_back', attempt=attempt)
        elif 400 <= code < 500 and code not in {408, 429}:
            self.outcome(accepted, 'rejected', f'Pure definitively rejected rollback: HTTP {code}', attempt)
        else:
            self.outcome(accepted, 'unknown', f'Rollback outcome unknown: HTTP {code}; reconcile before retrying', attempt)

    def run(self):
        groups = {}
        endpoint = {'internal_persons': 'persons', 'external_persons': 'external-persons', 'external_orgs': 'external-organizations', 'full_text': 'research-outputs', 'research_outputs': 'research-outputs', 'datasets': 'data-sets'}[self.kind]
        for item in self.change_set['items']:
            if item['apply_status'] != 'applied':
                self.outcome([item], 'skipped', 'Apply was not acknowledged')
                continue
            if item['rollback_status'] == 'writing':
                self.outcome([item], 'unknown', 'Unacknowledged prior rollback intent; reconcile before retrying')
                continue
            if item['rollback_status'] not in RETRYABLE:
                if item['rollback_status'] in {'unknown', 'failed'}:
                    self.stats['unknown'] += 1
                elif item['rollback_status'] in {'conflict', 'skipped'}:
                    self.stats['conflicts'] += 1
                continue
            payload = item.get('new_value') or {}
            entity = payload.get('record_uuid') if self.kind in {'full_text', 'research_outputs', 'datasets'} else item['entity_uuid']
            if not entity:
                self.outcome([item], 'conflict', 'Missing acknowledged record UUID')
                continue
            # Internal scalars/identifiers need a fresh version for each logical write.
            key = (entity, item['id']) if self.kind == 'internal_persons' else (entity,)
            groups.setdefault(key, []).append(item)
        for key, items in groups.items():
            self.execute_group(items, endpoint, key[0])
        self.service._append_log(self.log_path, f'Rollback outcomes: {json.dumps(self.stats, sort_keys=True)}\n')
        return self.stats
