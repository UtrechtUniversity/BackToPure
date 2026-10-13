import json
import threading
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from app.db import connect_db, initialize_database
from app.services.jobs import JobService
from app.worker import Worker
from tests.test_safe_apply_integration import reviewed_job


@pytest.fixture
def service(tmp_path):
    initialize_database(tmp_path / 'jobs.sqlite')
    return JobService(tmp_path / 'jobs.sqlite', project_root=Path(__file__).resolve().parents[1], runtime_root=tmp_path)

def save(service, job, filename, revision=None):
    return service.update_job_review_table(job['id'], file_name=filename, expected_row_count=2,
        expected_revision=revision, updates=[{'rowIndex': 1, 'to_be_updated': ''}])

def test_acceptance_freezes_inputs_for_tracking_and_execution(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    original_csv = (directory / filename).read_bytes()
    original_json = (directory / 'datatotal.json').read_bytes()
    service.enqueue_action(job['id'], 'apply')
    # Both review data and its corresponding source JSON can change outside the API.
    (directory / filename).write_text('to_be_updated,updated,PURE_UUID_PERS,new_id,new_value,uri\n,,evil,other,999,/other\n')
    (directory / 'datatotal.json').write_text('[]')
    process = Mock(returncode=0, stdout=None, communicate=Mock(return_value=('', '')))
    with patch('app.services.jobs.subprocess.Popen', return_value=process) as launch:
        worker = Worker(service)
        assert worker.acquire()
        try:
            worker.run_once()
        finally:
            worker.close()
    assert launch.call_count == 1
    env = launch.call_args.kwargs['env']
    frozen = Path(env['BTP_INPUT_DIR'])
    assert frozen != directory
    assert (frozen / filename).read_bytes() == original_csv
    assert (frozen / 'datatotal.json').read_bytes() == original_json
    assert Path(env['BTP_OUTPUT_DIR']) == directory
    items = service.get_job_change_set(job['id'])['items']
    assert {item['entity_uuid'] for item in items} == {'p'}
    assert {item['identifier_type'] for item in items} == {'orcid', 'other'}

def test_queued_apply_rejects_late_save(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    original = (directory / filename).read_bytes()
    service.enqueue_action(job['id'], 'apply')
    with pytest.raises(ValueError, match='review|accepted|unfinished'):
        service.update_job_review_table(job['id'], file_name=filename, expected_row_count=2,
            updates=[{'rowIndex': 1, 'to_be_updated': ''}])
    assert (directory / filename).read_bytes() == original

def test_review_revision_rejects_same_row_count_stale_tab(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    first = service.get_job_review_table(job['id'])
    assert first.get('revision'), 'review must expose a content revision'
    second = save(service, job, filename, first['revision'])
    assert second['revision'] != first['revision']
    with pytest.raises(ValueError, match='changed'):
        save(service, job, filename, first['revision'])

def test_save_and_acceptance_are_serialized(service, monkeypatch):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    entered, release = threading.Event(), threading.Event()
    original_get = service.get_job
    def gated_get(job_id):
        result = original_get(job_id)
        if threading.current_thread().name == 'save':
            entered.set()
            assert release.wait(5)
        return result
    monkeypatch.setattr(service, 'get_job', gated_get)
    errors = []
    def saving():
        try:
            service.update_job_review_table(job['id'], file_name=filename, expected_row_count=2,
                updates=[{'rowIndex': 1, 'to_be_updated': ''}])
        except Exception as exc:
            errors.append(exc)
    saver = threading.Thread(target=saving, name='save')
    saver.start()
    assert entered.wait(5)
    accepted = threading.Event()
    def accepting():
        service.enqueue_action(job['id'], 'apply')
        accepted.set()
    enqueuer = threading.Thread(target=accepting)
    enqueuer.start()
    try:
        assert not accepted.wait(.15), 'acceptance must wait for a save that owns the lock'
    finally:
        release.set()
        saver.join(5)
        enqueuer.join(5)
    assert not errors
    assert accepted.is_set()
    assert service.get_job(job['id'])['status'] == 'applying'
    with connect_db(service.db_path) as db:
        snapshot = json.loads(db.execute('SELECT apply_snapshot_json FROM jobs WHERE id=?', (job['id'],)).fetchone()[0])
    assert (Path(snapshot['directory']) / filename).read_bytes() == (directory / filename).read_bytes()
    assert service.get_job_review_table(job['id'])['rows'][1]['to_be_updated'] == ''


def test_tampered_snapshot_fails_before_process_or_tracking(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    service.enqueue_action(job['id'], 'apply')
    with connect_db(service.db_path) as db:
        snapshot = json.loads(db.execute('SELECT apply_snapshot_json FROM jobs WHERE id=?', (job['id'],)).fetchone()[0])
    (Path(snapshot['directory']) / filename).write_text('tampered')
    with patch('app.services.jobs.subprocess.Popen') as launch:
        worker = Worker(service)
        assert worker.acquire()
        try:
            worker.run_once()
        finally:
            worker.close()
    launch.assert_not_called()
    result = service.get_job(job['id'])
    assert result['status'] == 'failed'
    assert 'hash' in result['error_message'].lower()
    assert service.get_job_change_set(job['id']) is None


def test_atomic_save_failure_preserves_original_csv(service, monkeypatch):
    import os
    job, directory, filename = reviewed_job(service, 'internal_persons')
    original = (directory / filename).read_bytes()
    revision = service.get_job_review_table(job['id'])['revision']
    def fail_replace(*_):
        raise OSError('simulated replacement failure')
    monkeypatch.setattr(os, 'replace', fail_replace)
    with pytest.raises(OSError, match='replacement'):
        save(service, job, filename, revision)
    assert (directory / filename).read_bytes() == original
    assert not list(directory.glob('.review-*'))


def test_cancelled_queued_apply_keeps_frozen_evidence_and_rejects_save(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    service.enqueue_action(job['id'], 'apply')
    with connect_db(service.db_path) as db:
        snapshot = db.execute('SELECT apply_snapshot_json FROM jobs WHERE id=?', (job['id'],)).fetchone()[0]
    service.cancel_job(job['id'])
    assert service.get_job(job['id'])['status'] == 'failed'
    with connect_db(service.db_path) as db:
        assert db.execute('SELECT apply_snapshot_json FROM jobs WHERE id=?', (job['id'],)).fetchone()[0] == snapshot
    assert (Path(json.loads(snapshot)['directory']) / filename).exists()
    with pytest.raises(ValueError, match='accepted'):
        save(service, job, filename)


def test_child_uses_frozen_rows_and_original_result_directory(service, monkeypatch):
    import apply_updates_to_pure as apply
    job, directory, filename = reviewed_job(service, 'internal_persons')
    service.enqueue_action(job['id'], 'apply')
    with connect_db(service.db_path) as db:
        snapshot = json.loads(db.execute('SELECT apply_snapshot_json FROM jobs WHERE id=?', (job['id'],)).fetchone()[0])
    monkeypatch.setenv('BTP_INPUT_DIR', snapshot['directory'])
    monkeypatch.setenv('BTP_INPUT_HASHES', json.dumps(snapshot['hashes']))
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(directory))
    monkeypatch.setenv('BTP_APPLY_MANIFEST_FILE', str(directory / 'apply_manifest.jsonl'))
    monkeypatch.setenv('REFERER_PAGE', 'enrich_internal_persons_with_ids')
    monkeypatch.setenv('BTP_TARGET_CSV_FILES', filename)
    monkeypatch.setenv('BTP_TARGET_JSON_FILES', 'datatotal.json')
    (directory / filename).write_text('tampered mutable source')
    (directory / 'datatotal.json').write_text('[]')
    response = Mock(status_code=200, json=lambda: {'uuid': 'p', 'version': 'v1', 'identifiers': []})
    monkeypatch.setattr(apply.session, 'get', Mock(return_value=response))
    put = Mock(return_value=Mock(status_code=200, text='', json=lambda: {'uuid': 'p', 'version': 'v2'}))
    monkeypatch.setattr(apply.session, 'put', put)
    apply.main()
    assert put.call_count == 1
    payload = put.call_args.kwargs['json']
    assert payload['orcid'] == '123'
    assert {item['id'] for item in payload['identifiers']} == {'456'}
    result = service.get_job_review_table(job['id'])
    assert all(row['updated'] == 'X' for row in result['rows'])
    assert (directory / 'apply_journal.jsonl').exists()
    assert (directory / 'apply_manifest.jsonl').exists()
    assert (Path(snapshot['directory']) / filename).read_bytes() != (directory / filename).read_bytes()


def test_child_rejects_changed_frozen_bytes_before_writes(service, monkeypatch):
    import apply_updates_to_pure as apply
    from workflow_http import FatalWorkflowError
    job, directory, filename = reviewed_job(service, 'internal_persons')
    service.enqueue_action(job['id'], 'apply')
    with connect_db(service.db_path) as db:
        snapshot = json.loads(db.execute('SELECT apply_snapshot_json FROM jobs WHERE id=?', (job['id'],)).fetchone()[0])
    monkeypatch.setenv('BTP_INPUT_DIR', snapshot['directory'])
    monkeypatch.setenv('BTP_INPUT_HASHES', json.dumps(snapshot['hashes']))
    (Path(snapshot['directory']) / 'datatotal.json').write_text('[]')
    with pytest.raises(FatalWorkflowError, match='hash mismatch'):
        apply.read_directory_files(snapshot['directory'], {filename}, {'datatotal.json'})


def test_accepted_filenames_are_authoritative_after_metadata_edits(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    service.enqueue_action(job['id'], 'apply')
    service.update_job(job['id'], artifacts={'canOpen': True, 'canApply': True,
        'artifacts': {'directory': str(directory), 'csv': ['personstobeupdated_new.csv'], 'json': ['new.json']}})
    process = Mock(returncode=0, stdout=None, communicate=Mock(return_value=('', '')))
    with patch('app.services.jobs.subprocess.Popen', return_value=process) as launch:
        worker = Worker(service)
        assert worker.acquire()
        try:
            worker.run_once()
        finally:
            worker.close()
    env = launch.call_args.kwargs['env']
    assert env['BTP_TARGET_CSV_FILES'] == filename
    assert env['BTP_TARGET_JSON_FILES'] == 'datatotal.json'
    assert len(service.get_job_change_set(job['id'])['items']) == 2


def test_direct_apply_cannot_bypass_accepted_action(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    service.enqueue_action(job['id'], 'apply')
    with patch('app.services.jobs.subprocess.Popen') as launch:
        with pytest.raises(ValueError, match='needs_review'):
            service.apply_job(job['id'])
    launch.assert_not_called()


def test_direct_apply_freezes_before_tracking_or_launch(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    original = (directory / filename).read_bytes()
    process = Mock(returncode=0, stdout=None, communicate=Mock(return_value=('', '')))
    with patch('app.services.jobs.subprocess.Popen', return_value=process) as launch:
        service.apply_job(job['id'])
    env = launch.call_args.kwargs['env']
    with connect_db(service.db_path) as db:
        snapshot = json.loads(db.execute('SELECT apply_snapshot_json FROM jobs WHERE id=?', (job['id'],)).fetchone()[0])
    assert env['BTP_INPUT_DIR'] == snapshot['directory']
    assert (Path(snapshot['directory']) / filename).read_bytes() == original
    assert len(service.get_job_change_set(job['id'])['items']) == 2
