"""Execution failures must leave actionable, recoverable job state."""
import json
from unittest.mock import patch

import pytest

from app.db import initialize_database
from app.models import JobStatus
from app.services.jobs import JobService


@pytest.fixture
def service(tmp_path):
    db = tmp_path / 'jobs.sqlite'
    initialize_database(db)
    return JobService(db, project_root=tmp_path, runtime_root=tmp_path)


def harvest(service, code):
    script = service.project_root / 'src/enrich_internal_persons_with_ids.py'
    script.parent.mkdir(exist_ok=True)
    script.write_text(code)
    return service.create_job(job_id='harvest', job_type='internal_persons', created_at=service._utcnow())


def test_harvest_surfaces_real_streamed_traceback(service):
    harvest(service, "raise ValueError('Invalid person response from Pure')\n")
    job = service.run_job('harvest')
    assert job['status'] == 'failed'
    assert 'ValueError: Invalid person response from Pure' in job['error_message']


def test_empty_success_without_required_artifacts_is_failed(service):
    harvest(service, "print('Error connecting to Ricgraph')\n")
    job = service.run_job('harvest')
    assert job['status'] == 'failed'
    assert 'review files' in job['error_message']


def test_concurrent_harvest_cannot_claim_same_job(service):
    harvest(service, "print('unused')\n")
    service._claim_job('harvest', JobStatus.QUEUED, JobStatus.RUNNING)
    with pytest.raises(ValueError, match='already|state'):
        service._claim_job('harvest', JobStatus.QUEUED, JobStatus.RUNNING)


def test_manifest_loader_does_not_count_intent_or_missing_uuid(service):
    job = service.create_job(job_id='manifest', job_type='research_outputs', created_at=service._utcnow())
    directory = service.runtime_root / job['artifact_dir']
    directory.mkdir(parents=True)
    entries = [
        {'item_key': '10.1/a', 'status': 'pending'},
        {'item_key': '10.1/b', 'record_uuid': None},
        {'item_key': '10.1/c', 'record_uuid': 'created'},
    ]
    (directory / 'apply_manifest.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in entries))
    assert set(service._load_apply_manifest(job)) == {'10.1/c'}


def test_apply_capture_error_does_not_leave_job_applying(service):
    job = service.create_job(job_id='apply', job_type='research_outputs',
                             status=JobStatus.NEEDS_REVIEW, created_at=service._utcnow())
    (service.project_root / 'src').mkdir()
    (service.project_root / 'src/apply_updates_to_pure.py').write_text('')
    directory = service._artifact_directory(job['artifact_dir'])
    directory.mkdir(parents=True)
    (directory / 'to_be_updated.csv').write_text('to_be_updated,updated,doi\nX,,10.1/a\n')
    (directory / 'datatotal.json').write_text('[]')
    job['artifact_state']['artifacts'].update(csv=['to_be_updated.csv'], json=['datatotal.json'])
    real_get_job = service.get_job
    with patch.object(service, 'get_job', wraps=real_get_job) as get_job:
        ready = dict(job, canApply=True, results=dict(job['results'], ready_count=1))
        first = True
        def load(job_id):
            nonlocal first
            if first:
                first = False
                return ready
            return real_get_job(job_id)
        get_job.side_effect = load
        with patch.object(service, '_capture_apply_change_set', side_effect=OSError('disk full')):
            result = service.apply_job('apply')
    assert result['status'] == 'failed'
    assert 'disk full' in result['error_message']


@pytest.mark.parametrize('kind', ['internal_persons', 'external_persons', 'external_orgs',
                                  'research_outputs', 'datasets', 'full_text'])
def test_journal_recovers_success_if_csv_or_manifest_checkpoint_was_interrupted(service, kind):
    job = service.create_job(job_id='interrupted', job_type=kind, created_at=service._utcnow())
    directory = service.runtime_root / job['artifact_dir']
    directory.mkdir(parents=True)
    entity = 'person-1' if kind in ('internal_persons', 'external_persons', 'external_orgs') else '10.1/a'
    key = entity + '|orcid|123' if kind == 'internal_persons' else entity
    from app.db import connect_db
    with connect_db(service.db_path) as connection:
        connection.execute("INSERT INTO job_change_sets (id,job_id,job_type,status,created_at) VALUES ('cs',?,?, 'pending',?)",
                           (job['id'], kind, service._utcnow()))
        connection.execute("INSERT INTO job_change_set_items (change_set_id,item_key,entity_uuid,field_name,new_value_json) VALUES ('cs',?,?, 'identifier','{}')", (key, entity))
    (directory / 'apply_journal.jsonl').write_text(json.dumps({
        'filename': 'review.csv', 'item_key': entity, 'job_type': kind,
        'status': 'success', 'record_uuid': 'created-1',
    }) + '\n')
    assert service._finalize_apply_change_set(job, 'cs') == (1, 1)
    assert service.get_job_change_set(job['id'])['items'][0]['apply_status'] == 'applied'


def test_journal_unknown_outcome_is_not_reported_as_not_applied(service):
    from app.db import connect_db
    job = service.create_job(job_id='unknown', job_type='research_outputs', created_at=service._utcnow())
    directory = service.runtime_root / job['artifact_dir']
    directory.mkdir(parents=True)
    with connect_db(service.db_path) as connection:
        connection.execute("INSERT INTO job_change_sets (id,job_id,job_type,status,created_at) VALUES ('cs','unknown','research_outputs','pending',?)", (service._utcnow(),))
        connection.execute("INSERT INTO job_change_set_items (change_set_id,item_key,entity_uuid,field_name,new_value_json) VALUES ('cs','10.1/a','10.1/a','research_output','{}')")
    events = [
        {'item_key': '10.1/a', 'job_type': 'research_outputs', 'status': 'started'},
        {'item_key': '10.1/a', 'job_type': 'research_outputs', 'status': 'child_created',
         'created_external_persons': [{'uuid': 'child-1'}]},
    ]
    (directory / 'apply_journal.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in events))
    service._finalize_apply_change_set(job, 'cs')
    item = service.get_job_change_set(job['id'])['items'][0]
    assert item['apply_status'] == 'unknown'
    assert 'reconcile' in item['conflict_reason'].lower()
    assert item['new_value']['created_external_persons'] == [{'uuid': 'child-1'}]


def test_cancel_stale_apply_recovers_journal_without_replaying(service):
    from app.db import connect_db
    job = service.create_job(job_id='stale', job_type='research_outputs',
                             status=JobStatus.APPLYING, created_at=service._utcnow())
    directory = service.runtime_root / job['artifact_dir']
    directory.mkdir(parents=True)
    with connect_db(service.db_path) as connection:
        connection.execute("INSERT INTO job_change_sets (id,job_id,job_type,status,created_at) VALUES ('cs','stale','research_outputs','pending',?)", (service._utcnow(),))
        connection.execute("INSERT INTO job_change_set_items (change_set_id,item_key,entity_uuid,field_name,new_value_json) VALUES ('cs','10.1/a','10.1/a','research_output','{}')")
    (directory / 'apply_journal.jsonl').write_text(json.dumps({
        'job_type': 'research_outputs', 'item_key': '10.1/a',
        'status': 'success', 'record_uuid': 'created',
    }) + '\n')
    with patch.object(service, '_find_active_job_pids', return_value=[]):
        result = service.cancel_job(job['id'])
    assert result['status'] == 'failed'
    assert result['results']['updated_count'] == 1
    assert 'interrupted' in result['error_message'].lower()
    # A failed partial apply must still offer its confirmed writes for rollback.
    with patch.object(service, '_execute_research_outputs_rollback', return_value={
        'checked': 1, 'rolled_back': 1, 'conflicts': 0, 'failed': 0,
    }):
        assert service.rollback_job(job['id'])['status'] == 'completed'


def test_apply_snapshot_defers_network_to_fresh_apply_read(service):
    from unittest.mock import Mock

    import requests
    job = service.create_job(job_id='snapshot', job_type='external_persons', created_at=service._utcnow())
    directory = service.runtime_root / job['artifact_dir']
    directory.mkdir(parents=True)
    (directory / 'ext_pers_update.csv').write_text('to_be_updated,Pure_UUID,ORCID\nX,person-1,123\n')
    (directory / 'people.json').write_text('[{"uuid":"person-1"}]')
    job['artifact_state']['artifacts'].update(csv=['ext_pers_update.csv'], json=['people.json'])
    response = Mock(status_code=401)
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    with patch('app.services.rollback.requests.get', return_value=response) as get:
        assert service._collect_external_persons_apply_changes(job) == []
        get.assert_not_called()


def test_cancel_during_spawn_terminates_child_and_keeps_failed_status(service):
    from unittest.mock import MagicMock
    harvest(service, "print('unused')\n")
    process = MagicMock(returncode=0)
    process.communicate.return_value = ('', '')

    def start(*args, **kwargs):
        with patch.object(service, '_find_active_job_pids', return_value=[]):
            service.cancel_job('harvest')
        return process

    with patch('app.services.jobs.subprocess.Popen', side_effect=start):
        result = service.run_job('harvest')
    process.terminate.assert_called_once()
    assert result['status'] == 'failed'
    assert 'interrupted' in result['error_message'].lower()
