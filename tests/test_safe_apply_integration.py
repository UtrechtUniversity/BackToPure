import json
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

import apply_updates_to_pure as apply
from app import create_app
from app.db import init_db
from app.models import JobStatus
from app.services.jobs import JobService


@pytest.fixture
def service(tmp_path):
    app = create_app()
    app.config['BTP_DATA_DIR'] = str(tmp_path / 'data')
    init_db(app)
    return JobService(app.extensions['btp_db']['db_path'], project_root=Path(__file__).resolve().parents[1], runtime_root=tmp_path)


def reviewed_job(service, kind='research_outputs'):
    job = service.create_job(job_id='reviewed', job_type=kind, status=JobStatus.NEEDS_REVIEW, created_at=service._utcnow())
    directory = service.runtime_root / job['artifact_dir']
    directory.mkdir(parents=True)
    if kind == 'research_outputs':
        filename, data, payload = 'to_be_updated.csv', 'to_be_updated,updated,doi,title\nX,,10.1/a,A\n', []
    else:
        filename, data = 'personstobeupdated_test.csv', 'to_be_updated,updated,PURE_UUID_PERS,new_id,new_value,uri\nX,,p,orcid,123,\nX,,p,other,456,/other\n'
        payload = [{'uuid': 'p', 'identifiers': []}]
    (directory / filename).write_text(data)
    (directory / 'datatotal.json').write_text(json.dumps(payload))
    service.update_job(job['id'], artifacts={'canOpen': True, 'canApply': True, 'artifacts': {'directory': job['artifact_dir'], 'csv': [filename], 'json': ['datatotal.json']}}, results={'entity_label': 'records', 'ready_count': 1, 'updated_count': 0})
    return service.get_job(job['id']), directory, filename


def test_existing_doi_csv_handled_not_updated_or_reselected(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    session = Mock(status_code=200, json=lambda: {'count': 1, 'items': [{'uuid': 'old', 'electronicVersions': [{'doi': '10.1/a'}]}]})
    empty = Mock(status_code=200, json=lambda: {'count': 0, 'items': []})
    monkeypatch.setattr(apply.session, 'post', Mock(side_effect=[session, empty]))
    create = Mock()
    monkeypatch.setattr(apply.pure_researchoutputs, 'create_research_output', create)
    frame = pd.DataFrame([{'to_be_updated': 'X', 'updated': '', 'doi': '10.1/a'}])
    apply.process_research_output('update.csv', frame, [{'electronicVersions': [{'doi': '10.1/a'}]}])
    assert frame.loc[0, 'updated'] == ''
    assert frame.loc[0, 'to_be_updated'] == ''
    assert frame.loc[0, 'apply_status'] == 'existing'
    create.assert_not_called()
    # A stale selected CSV loaded on restart remains a handled no-op.
    frame.loc[0, 'to_be_updated'] = 'X'
    apply.process_research_output('update.csv', frame, [{'electronicVersions': [{'doi': '10.1/a'}]}])
    assert frame.loc[0, 'updated'] == ''
    assert frame.loc[0, 'to_be_updated'] == ''


@pytest.mark.parametrize('evidence', ['journal', 'manifest'])
def test_existing_result_completes_with_zero_owned_changes(service, evidence):
    job, directory, filename = reviewed_job(service)
    process = Mock(returncode=0, stdout=None)
    def finish():
        (directory / filename).write_text('to_be_updated,updated,doi,title,apply_status\n,,10.1/a,A,existing\n')
        entry = {'job_type': 'research_outputs', 'item_key': '10.1/a', 'record_uuid': 'old', 'status': 'success', 'disposition': 'existing', 'target_tenant': 'https://pure/ws/api'}
        (directory / f'apply_{evidence}.jsonl').write_text(json.dumps(entry) + '\n')
        return '', ''
    process.communicate.side_effect = finish
    with patch('app.services.jobs.subprocess.Popen', return_value=process):
        result = service.apply_job(job['id'])
    assert result['status'] == 'completed'
    assert result['results']['updated_count'] == 0
    assert result['results']['ready_count'] == 0
    changes = service.get_job_change_set(job['id'])
    assert changes['applied_item_count'] == 0
    assert changes['items'][0]['apply_status'] == 'handled'
    with pytest.raises(ValueError):
        service.rollback_job(job['id'])


def test_entity_success_owns_only_fields_actually_changed(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    change_set = service._capture_apply_change_set(job)
    (directory / filename).write_text('to_be_updated,updated,PURE_UUID_PERS,new_id,new_value,uri\n,X,p,orcid,123,\n,X,p,other,456,/other\n')
    event = {'job_type': 'internal_persons', 'item_key': 'p', 'status': 'success', 'record_uuid': 'p', 'disposition': 'updated', 'target_tenant': 'https://pure/ws/api',
             'owned_changes': [{'item_key': 'p|other|456', 'old_value': None, 'new_value': {'id': '456', 'uri': '/other'}}]}
    (directory / 'apply_journal.jsonl').write_text(json.dumps(event) + '\n')
    assert service._finalize_apply_change_set(job, change_set) == (1, 2)
    items = {item['identifier_type']: item for item in service.get_job_change_set(job['id'])['items']}
    assert items['orcid']['apply_status'] == 'handled'
    assert items['other']['apply_status'] == 'applied'
    assert items['other']['old_value'] is None
    assert items['other']['new_value']['target_tenant'] == 'https://pure/ws/api'


def test_writing_intent_is_unknown_even_if_csv_was_marked_updated(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    change_set = service._capture_apply_change_set(job)
    event = {'job_type': 'internal_persons', 'item_key': 'p', 'status': 'writing', 'target_tenant': 'https://pure/ws/api', 'owned_changes': []}
    (directory / 'apply_journal.jsonl').write_text(json.dumps(event) + '\n')
    assert service._finalize_apply_change_set(job, change_set) == (0, 2)
    assert all(item['apply_status'] == 'unknown' for item in service.get_job_change_set(job['id'])['items'])


def test_apply_rejects_journal_from_other_tenant(tmp_path, monkeypatch):
    from workflow_http import FatalWorkflowError
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    (tmp_path / 'apply_journal.jsonl').write_text(json.dumps({'filename': 'update.csv', 'job_type': 'research_outputs', 'item_key': '10.1/a', 'status': 'success', 'target_tenant': 'https://other-pure/ws/api'}) + '\n')
    frame = pd.DataFrame([{'to_be_updated': 'X', 'updated': '', 'doi': '10.1/a'}])
    with pytest.raises(FatalWorkflowError, match='different Pure tenant'):
        apply.process_research_output('update.csv', frame, [])


def test_created_response_version_is_preserved(monkeypatch):
    import pure_datasets
    response = Mock(status_code=201, json=lambda: {'uuid': 'created', 'version': 'v1'})
    monkeypatch.setattr(pure_datasets, '_ensure_dataset_external_people', lambda _: [])
    monkeypatch.setattr(pure_datasets.session, 'put', Mock(return_value=response))
    result = pure_datasets.create_dataset({})
    assert result['created_version'] == 'v1'


def test_mixed_internal_rows_checkpoint_only_actual_changes(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr(apply.session, 'get', Mock(return_value=Mock(status_code=200, json=lambda: {'uuid': 'p', 'version': 'v2', 'orcid': '123', 'identifiers': []})))
    put = Mock(return_value=Mock(status_code=200, text=''))
    monkeypatch.setattr(apply.session, 'put', put)
    frame = pd.DataFrame([{'to_be_updated': 'X', 'updated': '', 'PURE_UUID_PERS': 'p', 'new_id': 'orcid', 'new_value': '123'},
                          {'to_be_updated': 'X', 'updated': '', 'PURE_UUID_PERS': 'p', 'new_id': 'other', 'new_value': '456', 'uri': '/other'}])
    apply.process_internal_persons('update.csv', frame, [{'uuid': 'p', 'identifiers': []}])
    assert list(frame['updated']) == ['', 'X']
    assert list(frame['apply_status']) == ['noop', 'success']
    frame.loc[0, 'to_be_updated'] = 'X'
    apply.process_internal_persons('update.csv', frame, [{'uuid': 'p', 'identifiers': []}])
    assert list(frame['updated']) == ['', 'X']
    assert put.call_count == 1


def test_rejected_rows_without_trackable_fields_fail_job(service):
    job, directory, filename = reviewed_job(service, 'internal_persons')
    process = Mock(returncode=0, stdout=None)
    def finish():
        (directory / filename).write_text('to_be_updated,updated,PURE_UUID_PERS,apply_status\nX,,p,failed\n')
        return '', ''
    process.communicate.side_effect = finish
    with patch.object(service, '_capture_apply_change_set', return_value=None), patch('app.services.jobs.subprocess.Popen', return_value=process):
        result = service.apply_job(job['id'])
    assert result['status'] == 'failed'
