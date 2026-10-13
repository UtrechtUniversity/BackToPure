"""Rollback regressions use real durable ownership/storage and mocked Pure only."""
import copy
import json
import threading

import pytest
import requests

from app.db import connect_db, initialize_database
from app.services.jobs import JobService
from app.worker import Worker

TENANT = 'https://pure/ws/api'


def response(body=None, status=200):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body or {}).encode()
    result.url = TENANT
    return result


@pytest.fixture
def service(tmp_path, monkeypatch):
    initialize_database(tmp_path / 'jobs.sqlite')
    monkeypatch.setattr('app.services.jobs._pure_credentials', lambda: (TENANT + '/', {}))
    return JobService(tmp_path / 'jobs.sqlite', project_root=tmp_path)


def seed(service, kind, changes):
    service.create_job(job_id='source', job_type=kind, status='completed', created_at=service._utcnow())
    with connect_db(service.db_path) as db:
        db.execute("INSERT INTO job_change_sets(id,job_id,job_type,status,created_at,item_count,applied_item_count) VALUES('cs','source',?,'applied',?, ?, ?)", (kind, service._utcnow(), len(changes), len(changes)))
        for index, change in enumerate(changes):
            db.execute("""INSERT INTO job_change_set_items(change_set_id,item_key,entity_uuid,field_name,identifier_type,old_value_json,new_value_json,apply_status,rollback_status)
                VALUES('cs',?,?,?,?,?,?,'applied',?)""", (str(index), change.get('entity', 'person'), change.get('field', 'identifier'), change.get('type', 'other'), json.dumps(change.get('old')), json.dumps({'target_tenant': TENANT, **change['new']}), change.get('status', 'pending')))
    return service.get_job_change_set('source')


def states(service):
    return [item['rollback_status'] for item in service.get_job_change_set('source')['items']]


def identifier(value='id', entity='person', **kwargs):
    return {'entity': entity, 'new': {'id': value, 'uri': '/other'}, **kwargs}


def get_person(value='id', entity='person', version='opaque-v3'):
    return {'uuid': entity, 'version': version, 'identifiers': [{'id': value, 'type': {'uri': '/other'}}]}


def fulltext(service):
    added = {'pureId': 42, 'typeDiscriminator': 'FileElectronicVersion', 'file': {'id': 'file-job', 'fileName': 'a.pdf'}}
    seed(service, 'full_text', [{'entity': '10.1/a', 'new': {'record_uuid': 'record', 'previous_electronic_versions': [{'id': 1, 'doi': '10.1/a'}], 'added_electronic_version': added}}])
    return added


def test_fulltext_removes_only_owned_attachment_preserving_later_human_file(service, monkeypatch):
    added = fulltext(service)
    later = {'pureId': 43, 'file': {'id': 'file-human', 'fileName': 'a.pdf'}}
    current = {'uuid': 'record', 'version': 'opaque-v9', 'electronicVersions': [{'id': 1, 'doi': '10.1/a'}, added, later]}
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(current))
    sent = []
    monkeypatch.setattr(requests, 'put', lambda *a, **k: (sent.append(k['json']) or response()))
    service.rollback_job('source')
    assert sent[0]['electronicVersions'] == [{'id': 1, 'doi': '10.1/a'}, later]
    assert sent[0]['version'] == 'opaque-v9'
    assert states(service) == ['rolled_back']


@pytest.mark.parametrize('problem', ['missing_identity', 'edited_attachment', 'wrong_tenant', 'missing_version'])
def test_unclear_fulltext_ownership_never_writes(service, monkeypatch, problem):
    added = fulltext(service)
    current = {'uuid': 'record', 'version': 'opaque-v9', 'electronicVersions': [added]}
    if problem == 'missing_identity':
        with connect_db(service.db_path) as db:
            payload = json.loads(db.execute('SELECT new_value_json FROM job_change_set_items').fetchone()[0])
            payload.pop('added_electronic_version')
            db.execute('UPDATE job_change_set_items SET new_value_json=?', (json.dumps(payload),))
    elif problem == 'edited_attachment':
        current['electronicVersions'][0]['file']['fileName'] = 'edited.pdf'
    elif problem == 'wrong_tenant':
        monkeypatch.setattr('app.services.jobs._pure_credentials', lambda: ('https://other/ws/api/', {}))
    else:
        current.pop('version')
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(current))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: pytest.fail('Unowned write'))
    result = service.rollback_job('source')
    assert states(service) == ['conflict']
    assert result['status'] == 'failed'
    assert result['error_message']


def test_write_intent_releases_sqlite_and_ack_survives_later_process_death(service, monkeypatch):
    seed(service, 'internal_persons', [identifier('a', 'a'), identifier('b', 'b')])
    monkeypatch.setattr(requests, 'get', lambda url, **k: response(get_person(url.rsplit('/', 1)[1], url.rsplit('/', 1)[1])))
    def put(url, **kwargs):
        with connect_db(service.db_path) as db:
            db.execute('PRAGMA busy_timeout=50')
            db.execute("UPDATE jobs SET error_message='network callback can write' WHERE id='source'")
            assert db.execute("SELECT rollback_status FROM job_change_set_items WHERE entity_uuid=?", (url.rsplit('/', 1)[1],)).fetchone()[0] == 'writing'
        if url.endswith('/b'):
            raise SystemExit('simulated process death')
        return response()
    monkeypatch.setattr(requests, 'put', put)
    # Claim normally but simulate disappearance without the worker's finally
    # (actual process death cannot run that block).
    service.enqueue_action('source', 'rollback')
    original = Worker(service)
    assert original.acquire()
    service.execution_task = original.claim()
    try:
        with pytest.raises(SystemExit):
            service.rollback_job('source')
    finally:
        service.execution_task = None
        original.close()
    assert states(service) == ['rolled_back', 'writing']
    recovered = Worker(service)
    assert recovered.acquire()
    try:
        recovered.recover()
    finally:
        recovered.close()
    assert states(service) == ['rolled_back', 'unknown']
    assert service.get_results_dashboard()['totals']['rolled_back_items'] == 1
    with pytest.raises(ValueError):
        service.rollback_job('source')


def test_cancellation_during_get_prevents_put_synchronously(service, monkeypatch):
    seed(service, 'internal_persons', [identifier()])
    def get(*args, **kwargs):
        service.cancel_job('source')
        return response(get_person())
    monkeypatch.setattr(requests, 'get', get)
    monkeypatch.setattr(requests, 'put', lambda *a, **k: pytest.fail('Cancelled write'))
    result = service.rollback_job('source')
    assert states(service) == ['cancelled']
    assert result['status'] == 'failed'


def test_cancellation_during_put_still_records_acknowledged_success(service, monkeypatch):
    seed(service, 'internal_persons', [identifier('a', 'a'), identifier('b', 'b')])
    reads = []
    def get(url, **kwargs):
        reads.append(url)
        return response(get_person('a', 'a'))
    def put(*args, **kwargs):
        service.cancel_job('source')
        return response()
    monkeypatch.setattr(requests, 'get', get)
    monkeypatch.setattr(requests, 'put', put)
    result = service.rollback_job('source')
    assert states(service) == ['rolled_back', 'cancelled']
    assert len(reads) == 1
    assert result['status'] == 'failed'
    assert 'acknowledged 1 item(s)' in result['error_message']
    assert '1 cancelled item(s)' in result['error_message']


def test_two_direct_rollback_requests_have_one_execution(service, monkeypatch):
    seed(service, 'internal_persons', [identifier()])
    started, release = threading.Event(), threading.Event()
    writes, outcomes = [], []
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    validate = service._validate_rollback
    def pause_after_validation(job):
        validate(job)
        started.set()
        assert release.wait(5)
    monkeypatch.setattr(service, '_validate_rollback', pause_after_validation)
    def put(*a, **k):
        writes.append(k['json'])
        return response()
    monkeypatch.setattr(requests, 'put', put)
    def first():
        outcomes.append(service.rollback_job('source'))
    thread = threading.Thread(target=first)
    thread.start()
    try:
        assert started.wait(5)
        other = JobService(service.db_path, project_root=service.project_root)
        with pytest.raises(ValueError):
            other.rollback_job('source')
    finally:
        release.set()
        thread.join(5)
    assert len(writes) == 1
    assert states(service) == ['rolled_back']
    assert outcomes[0]['status'] == 'completed'
    with connect_db(service.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM jobs WHERE id LIKE 'rollback_%'").fetchone()[0] == 1


def test_direct_call_cannot_bypass_accepted_queue(service, monkeypatch):
    seed(service, 'internal_persons', [identifier()])
    service.enqueue_action('source', 'rollback')
    monkeypatch.setattr(requests, 'get', lambda *a, **k: pytest.fail('Queue bypass'))
    with pytest.raises(ValueError):
        service.rollback_job('source')


def test_external_person_get_failure_marks_every_affected_row_only(service, monkeypatch):
    seed(service, 'external_persons', [identifier('a', 'a'), identifier('b', 'a'), identifier('c', 'c')])
    def get(url, **kwargs):
        if url.endswith('/a'):
            raise requests.Timeout('read failed')
        return response(get_person('c', 'c'))
    monkeypatch.setattr(requests, 'get', get)
    monkeypatch.setattr(requests, 'put', lambda *a, **k: response())
    service.rollback_job('source')
    assert states(service) == ['read_failed', 'read_failed', 'rolled_back']


@pytest.mark.parametrize('error', [requests.Timeout('ambiguous'), 500, 408, 429])
def test_uncertain_write_is_unknown_and_cannot_be_retried(service, monkeypatch, error):
    seed(service, 'internal_persons', [identifier()])
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    writes = []
    def put(*a, **k):
        writes.append(k['json'])
        if isinstance(error, Exception):
            raise error
        return response(status=error)
    monkeypatch.setattr(requests, 'put', put)
    assert service.rollback_job('source')['status'] == 'failed'
    assert states(service) == ['unknown']
    with pytest.raises(ValueError):
        service.rollback_job('source')
    assert len(writes) == 1


def test_definitive_rejection_refetches_without_reusing_mutated_record(service, monkeypatch):
    seed(service, 'internal_persons', [identifier('a'), identifier('b')])
    current = {'uuid': 'person', 'version': 'opaque-v4', 'identifiers': [{'id': value, 'type': {'uri': '/other'}} for value in ['a', 'b']]}
    reads, writes = [], []
    def get(*a, **k):
        reads.append(1)
        return response(current)
    def put(*a, **k):
        writes.append(copy.deepcopy(k['json']))
        return response(status=400 if len(writes) == 1 else 200)
    monkeypatch.setattr(requests, 'get', get)
    monkeypatch.setattr(requests, 'put', put)
    service.rollback_job('source')
    assert len(reads) == 2
    assert writes[1]['identifiers'] == [{'id': 'a', 'type': {'uri': '/other'}}]
    assert states(service) == ['rejected', 'rolled_back']


def test_address_restores_owned_leaves_preserving_unrelated_nested_fields(service, monkeypatch):
    seed(service, 'external_orgs', [{'field': 'address', 'type': 'address', 'old': {'field': 'address', 'value': {'city': 'Old', 'country': {'uri': '/nl'}}}, 'new': {'field': 'address', 'value': {'city': 'Applied', 'country': {'uri': '/nl'}, 'postalCode': '123'}}}])
    current = {'uuid': 'person', 'version': 'opaque-v8', 'address': {'city': 'Applied', 'country': {'uri': '/nl', 'term': {'en_GB': 'Human label'}}, 'postalCode': '123', 'street': 'Human street'}}
    sent = []
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(current))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: (sent.append(k['json']) or response()))
    service.rollback_job('source')
    assert sent[0]['address'] == {'city': 'Old', 'country': {'uri': '/nl', 'term': {'en_GB': 'Human label'}}, 'postalCode': None, 'street': 'Human street'}
    assert states(service) == ['rolled_back']


@pytest.mark.parametrize('version', ['created-v2', 'human-v3', None])
@pytest.mark.parametrize('kind', ['research_outputs', 'datasets'])
def test_created_records_and_children_require_conditional_delete_operator_cleanup(service, monkeypatch, kind, version):
    seed(service, kind, [{'entity': '10.1/a', 'new': {'record_uuid': 'record', 'doi': '10.1/a', 'disposition': 'created', 'created_version': 'created-v2', 'created_external_persons': [{'uuid': 'child'}]}}])
    current = {'uuid': 'record', 'version': version, 'doi': {'doi': '10.1/a'}, 'electronicVersions': [{'doi': '10.1/a'}]}
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(current))
    monkeypatch.setattr(requests, 'delete', lambda *a, **k: pytest.fail('Unconditional delete'))
    result = service.rollback_job('source')
    assert states(service) == ['conflict']
    assert result['status'] == 'failed'
    item = service.get_job_change_set('source')['items'][0]
    assert 'conditional' in item['conflict_reason'].lower() or 'version' in item['conflict_reason'].lower()
    assert item['new_value']['created_external_persons'] == [{'uuid': 'child'}]


def test_legacy_failed_write_is_not_retryable(service, monkeypatch):
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: response())
    seed(service, 'external_persons', [identifier(status='failed')])
    with pytest.raises(ValueError):
        service.rollback_job('source')


def test_direct_rollback_executes_own_task_not_older_queued_harvest(service, monkeypatch):
    seed(service, 'internal_persons', [identifier()])
    service.create_job(job_id='older', job_type='internal_persons', created_at='2000-01-01')
    service.enqueue_action('older', 'run')
    monkeypatch.setattr(service, 'run_job', lambda *a: pytest.fail('Executed unrelated queued work'))
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: response())
    assert service.rollback_job('source')['status'] == 'completed'
    with connect_db(service.db_path) as db:
        assert db.execute("SELECT state FROM execution_tasks WHERE job_id='older'").fetchone()[0] == 'queued'


@pytest.mark.parametrize('status', ['pending', 'rejected', 'read_failed', 'cancelled'])
def test_retryable_items_use_fresh_verified_record(service, monkeypatch, status):
    seed(service, 'external_persons', [identifier(status=status)])
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: response())
    assert service.rollback_job('source')['status'] == 'completed'
    assert states(service) == ['rolled_back']


def test_later_country_classification_edit_conflicts_as_a_whole(service, monkeypatch):
    seed(service, 'external_orgs', [{'field': 'address', 'type': 'address', 'old': {'field': 'address', 'value': {'country': {'uri': '/old', 'term': {'en_GB': 'Old'}}}}, 'new': {'field': 'address', 'value': {'country': {'uri': '/new', 'term': {'en_GB': 'New'}}}}}])
    current = {'uuid': 'person', 'version': 'opaque-v8', 'address': {'country': {'uri': '/new', 'term': {'en_GB': 'New', 'nl_NL': 'Human country'}}}}
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(current))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: pytest.fail('Partial country restoration'))
    assert service.rollback_job('source')['status'] == 'failed'
    assert states(service) == ['conflict']


def test_log_failure_after_ack_does_not_downgrade_durable_success(service, monkeypatch):
    seed(service, 'internal_persons', [identifier()])
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: response())
    append = service._append_log
    def log(path, text):
        if text.startswith('ROLLED_BACK'):
            raise OSError('Disk log failure after DB acknowledgement')
        append(path, text)
    monkeypatch.setattr(service, '_append_log', log)
    service.rollback_job('source')
    assert states(service) == ['rolled_back']
    with connect_db(service.db_path) as db:
        assert db.execute('SELECT state FROM rollback_attempts').fetchone()[0] == 'rolled_back'


@pytest.mark.parametrize('retained', ['unknown', 'failed', 'conflict'])
def test_later_safe_work_does_not_claim_prior_blocked_items_were_restored(service, monkeypatch, retained):
    seed(service, 'internal_persons', [identifier('a', 'a', status=retained), identifier('b', 'b')])
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person('b', 'b')))
    monkeypatch.setattr(requests, 'put', lambda *a, **k: response())
    result = service.rollback_job('source')
    assert states(service) == [retained, 'rolled_back']
    assert result['status'] == 'failed'
    assert '1 item(s)' in result['error_message']


def test_cancellation_of_last_inflight_write_keeps_success_count_in_result(service, monkeypatch):
    seed(service, 'internal_persons', [identifier()])
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    def put(*a, **k):
        service.cancel_job('source')
        return response()
    monkeypatch.setattr(requests, 'put', put)
    result = service.rollback_job('source')
    assert states(service) == ['rolled_back']
    assert 'acknowledged 1 item(s)' in result['error_message']
    assert '0 cancelled item(s)' in result['error_message']


@pytest.mark.parametrize('status', [202, 204, 206])
def test_undocumented_success_response_is_unknown_without_dashboard_decrement(service, monkeypatch, status):
    seed(service, 'internal_persons', [identifier()])
    monkeypatch.setattr(requests, 'get', lambda *a, **k: response(get_person()))
    writes = []
    def put(*a, **k):
        writes.append(k['json'])
        return response(status=status)
    monkeypatch.setattr(requests, 'put', put)
    result = service.rollback_job('source')
    assert result['status'] == 'failed'
    assert states(service) == ['unknown']
    totals = service.get_results_dashboard()['totals']
    assert totals['rolled_back_items'] == 0
    assert totals['net_items'] == 1
    with pytest.raises(ValueError):
        service.rollback_job('source')
    assert len(writes) == 1
