import json
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

import apply_updates_to_pure as apply


def rows():
    return pd.DataFrame([{'to_be_updated': 'X', 'updated': '', 'doi': '10.1/a'},
                         {'to_be_updated': 'X', 'updated': '', 'doi': '10.1/b'}])


def payloads():
    return [{'electronicVersions': [{'doi': '10.1/a'}]}, {'electronicVersions': [{'doi': '10.1/b'}]}]


def test_success_is_durable_before_next_create(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    def create(record):
        if record['electronicVersions'][0]['doi'].endswith('/a'):
            return {'success': True, 'uuid': 'saved'}
        events = [json.loads(line) for line in (tmp_path / 'apply_journal.jsonl').read_text().splitlines()]
        assert any(e['status'] == 'success' and e.get('record_uuid') == 'saved' for e in events)
        raise RuntimeError('later crash')
    monkeypatch.setattr(apply.pure_researchoutputs, 'create_research_output', create)
    with pytest.raises(RuntimeError):
        apply.process_research_output('update.csv', rows(), payloads())
    saved = pd.read_csv(tmp_path / 'update.csv', keep_default_na=False)
    assert saved.loc[0, 'updated'].lower() == 'x'


def test_previously_updated_row_never_replayed(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    frame = rows().iloc[:1].copy()
    frame.loc[0, 'updated'] = 'X'
    create = Mock(return_value={'success': True, 'uuid': 'duplicate'})
    monkeypatch.setattr(apply.pure_researchoutputs, 'create_research_output', create)
    apply.process_research_output('update.csv', frame, payloads())
    create.assert_not_called()


def test_bad_payload_record_is_skipped_and_failure_saved(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr(apply.pure_researchoutputs, 'create_research_output', lambda x: {'success': False, 'error': 'HTTP 422'})
    apply.process_research_output('update.csv', rows(), [None, *payloads()])
    saved = pd.read_csv(tmp_path / 'update.csv', keep_default_na=False)
    assert '422' in saved.loc[0, 'apply_error']


def test_dataset_checkpoint_failure_is_not_swallowed(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr(apply.puda, 'create_dataset', lambda x: {'success': True, 'uuid': 'saved'})
    monkeypatch.setattr(apply, '_append_apply_manifest_entry', Mock(side_effect=OSError('disk full')))
    with pytest.raises(OSError, match='disk full'):
        apply.process_datasets('update.csv', rows(), [{'doi': {'doi': '10.1/a'}}])


def test_write_sessions_cannot_retry_put_or_post():
    for session in [apply.session, apply.puda.session, apply.pure_researchoutputs.session]:
        retry = session.adapters['https://'].max_retries
        assert 'PUT' not in retry.allowed_methods
        assert 'POST' not in retry.allowed_methods


def test_ambiguous_create_stops_and_blocks_replay(tmp_path, monkeypatch):
    from workflow_http import FatalWorkflowError
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr(apply.pure_researchoutputs.session, 'put', Mock(side_effect=requests.Timeout('lost response')))
    frame = rows()
    with pytest.raises(FatalWorkflowError, match='unknown'):
        apply.process_research_output('update.csv', frame, payloads())
    assert pd.read_csv(tmp_path / 'update.csv').loc[0, 'apply_status'] == 'unknown'
    with pytest.raises(FatalWorkflowError, match='unknown'):
        apply.process_research_output('update.csv', frame, payloads())
    assert apply.pure_researchoutputs.session.put.call_count == 1


def test_auth_failure_stops_before_second_record(tmp_path, monkeypatch):
    from workflow_http import FatalWorkflowError
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    response = Mock(status_code=401, text='unauthorized')
    monkeypatch.setattr(apply.pure_researchoutputs.session, 'put', Mock(return_value=response))
    with pytest.raises(FatalWorkflowError, match='authorization'):
        apply.process_research_output('update.csv', rows(), payloads())
    assert apply.pure_researchoutputs.session.put.call_count == 1


def test_child_creation_survives_parent_rejection(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    record = payloads()[0]
    record['contributors'] = [{'_btp_external_person': {'first_name': 'Jane', 'last_name': 'Doe'}}]
    child = Mock(status_code=201, json=lambda: {'uuid': 'child-uuid'})
    parent = Mock(status_code=422, text='invalid title')
    monkeypatch.setattr(apply.pure_researchoutputs.session, 'put', Mock(side_effect=[child, parent]))
    apply.process_research_output('update.csv', rows().iloc[:1].copy(), [record])
    journal = [json.loads(line) for line in (tmp_path / 'apply_journal.jsonl').read_text().splitlines()]
    children = [e for e in journal if e['status'] == 'child_created']
    assert children[0]['created_external_persons'][0]['uuid'] == 'child-uuid'


def test_accepted_create_with_null_response_is_unknown(tmp_path, monkeypatch):
    from workflow_http import FatalWorkflowError
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr(apply.pure_researchoutputs.session, 'put', Mock(return_value=Mock(status_code=201, json=lambda: None)))
    with pytest.raises(FatalWorkflowError, match='unknown'):
        apply.process_research_output('update.csv', rows(), payloads())


@pytest.mark.parametrize('processor,key,payload', [
    (apply.process_internal_persons, 'PURE_UUID_PERS', {'uuid': 'p', 'identifiers': None}),
    (apply.process_external_persons, 'Pure_UUID', {'uuid': 'p'}),
    (apply.process_external_orgs, 'uuid', {'uuid': 'p'}),
])
def test_person_updates_persist_before_next_failure(tmp_path, monkeypatch, processor, key, payload):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    frame = pd.DataFrame([{'to_be_updated': 'X', 'updated': '', key: 'p', 'new_id': 'id', 'new_value': 'abc'},
                          {'to_be_updated': 'X', 'updated': '', key: 'q', 'new_id': 'id', 'new_value': 'def'}])
    def put(url, **kwargs):
        if url.endswith('/p'):
            return Mock(status_code=200, text='')
        events = [json.loads(line) for line in (tmp_path / 'apply_journal.jsonl').read_text().splitlines()]
        assert any(e['status'] == 'success' and e.get('record_uuid') == 'p' for e in events)
        raise requests.Timeout()
    monkeypatch.setattr(apply.session, 'put', put)
    from workflow_http import FatalWorkflowError
    with pytest.raises(FatalWorkflowError):
        processor('update.csv', frame, [None, payload, {'uuid': 'q'}])


def test_fulltext_failure_reason_is_durable(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    frame = pd.DataFrame([{'to_be_updated': 'X', 'updated': '', 'doi': '10.1000/a', 'pure_uuid': 'p', 'candidate_url': 'https://example/a.pdf'}])
    monkeypatch.setattr(apply, 'download_and_validate', Mock(side_effect=ValueError('not a PDF')))
    apply.process_full_text('update.csv', frame)
    saved = pd.read_csv(tmp_path / 'update.csv')
    assert 'not a PDF' in saved.loc[0, 'apply_error']


def test_failed_person_lookup_does_not_create_duplicate(tmp_path, monkeypatch):
    import pure_persons
    from workflow_http import FatalWorkflowError
    monkeypatch.setattr(pure_persons.requests, 'post', Mock(return_value=Mock(status_code=401)))
    with pytest.raises(FatalWorkflowError):
        pure_persons.find_external_person({'orcid': '0000-0000-0000-0000'})


def test_target_json_parse_failure_is_fatal(tmp_path):
    from workflow_http import FatalWorkflowError
    (tmp_path / 'payload.json').write_text('{broken')
    with pytest.raises(FatalWorkflowError, match='payload.json'):
        apply.get_json_files(str(tmp_path), {'payload.json'})


def test_partial_child_failure_blocks_replay_even_without_parent_response(tmp_path, monkeypatch):
    from workflow_http import FatalWorkflowError
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    record = payloads()[0]
    record['contributors'] = [{'_btp_external_person': {'first_name': 'A'}}, {'_btp_external_person': {'first_name': 'B'}}]
    monkeypatch.setattr(apply.pure_researchoutputs, 'create_external_person', Mock(side_effect=['child', None]))
    frame = rows().iloc[:1].copy()
    apply.process_research_output('update.csv', frame, [record])
    with pytest.raises(FatalWorkflowError, match='unknown'):
        apply.process_research_output('update.csv', frame, [record])


def test_partial_child_failure_blocks_duplicate_row_in_same_run(tmp_path, monkeypatch):
    from workflow_http import FatalWorkflowError
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    record = payloads()[0]
    record['contributors'] = [{'_btp_external_person': {'first_name': 'A'}}, {'_btp_external_person': {'first_name': 'B'}}]
    monkeypatch.setattr(apply.pure_researchoutputs, 'create_external_person', Mock(side_effect=['child', None]))
    frame = pd.concat([rows().iloc[:1], rows().iloc[:1]], ignore_index=True)
    with pytest.raises(FatalWorkflowError, match='unknown'):
        apply.process_research_output('update.csv', frame, [record])


@pytest.mark.parametrize('loader,filename', [(apply.get_csv_files, 'update.csv'), (apply.get_json_files, 'payload.json')])
def test_missing_explicit_apply_input_is_fatal(tmp_path, loader, filename):
    from workflow_http import FatalWorkflowError
    with pytest.raises(FatalWorkflowError, match=filename):
        loader(str(tmp_path), {filename})


def test_fulltext_blank_csv_licence_is_valid(tmp_path, monkeypatch):
    from io import StringIO
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    frame = pd.read_csv(StringIO('to_be_updated,updated,doi,pure_uuid,candidate_url,version,licence,access_status\nX,,10.1000/a,p,https://example/a.pdf,publishedVersion,,\n'))
    monkeypatch.setattr(apply, 'download_and_validate', lambda *args: (b'%PDF-', 'a.pdf'))
    monkeypatch.setattr(apply, 'upload_pdf', lambda *args: 'key')
    monkeypatch.setattr(apply.requests, 'get', Mock(return_value=Mock(status_code=200, json=lambda: {'uuid': 'p'})))
    monkeypatch.setattr(apply.requests, 'put', Mock(return_value=Mock(status_code=200)))
    apply.process_full_text('update.csv', frame)
    assert frame.loc[0, 'updated'] == 'x'


def test_fulltext_upload_service_failure_stops():
    import fulltext_deposit
    from workflow_http import FatalWorkflowError
    session = Mock()
    session.put.return_value = Mock(status_code=503)
    with pytest.raises(FatalWorkflowError, match='unknown'):
        fulltext_deposit.upload_pdf(session, b'%PDF-', 'a.pdf')
    assert session.put.call_count == 1
