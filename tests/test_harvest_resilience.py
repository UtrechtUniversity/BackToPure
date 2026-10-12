"""Source outages must never masquerade as an empty, successful harvest."""
from unittest.mock import Mock

import pytest
import requests

import enrich_internal_persons_with_ids as internal
import enrich_pure_external_orgs as organizations
import enrich_pure_external_persons as external
import update_datasets_from_ricgraph as datasets
import update_researchoutput_from_ricgraph as outputs


@pytest.mark.parametrize('module', [internal, external, organizations, outputs, datasets])
def test_personroot_auth_failure_stops_harvest(module, monkeypatch):
    client = module.requests if module in (internal, organizations) else module.session
    response = Mock(status_code=401)
    response.raise_for_status.side_effect = requests.HTTPError('unauthorized', response=response)
    monkeypatch.setattr(client, 'get', Mock(return_value=response))
    with pytest.raises(RuntimeError, match='401'):
        module.fetch_personroots('faculty')


@pytest.mark.parametrize('module', [internal, external, organizations, outputs, datasets])
def test_invalid_collection_is_not_empty_success(module, monkeypatch):
    client = module.requests if module in (internal, organizations) else module.session
    response = Mock(status_code=200)
    response.json.return_value = {'results': None}
    monkeypatch.setattr(client, 'get', Mock(return_value=response))
    with pytest.raises(RuntimeError, match='results'):
        module.fetch_personroots('faculty')


def test_dataset_selection_preserves_all_faculties(monkeypatch):
    monkeypatch.setattr(datasets, 'fetch_personroots', lambda faculty: [{'_key': faculty}])
    monkeypatch.setattr(datasets, 'select_datasets', lambda key: [{'_key': key + '|doi'}])
    assert datasets.select_persons_datasets(['one', 'two'], 'selection') == ['one', 'two']


def test_safe_reads_retry_transient_failure_but_stop_after_budget(monkeypatch):
    from workflow_http import FatalWorkflowError, request_read
    monkeypatch.setattr('workflow_http.time.sleep', lambda _: None)
    send = Mock(side_effect=requests.Timeout('timeout'))
    with pytest.raises(FatalWorkflowError, match='Timeout'):
        request_read(send, 'https://source.test/records')
    assert send.call_count == 3
    assert send.call_args.kwargs['timeout'] == 30


def test_safe_search_post_can_recover(monkeypatch):
    from workflow_http import request_read
    monkeypatch.setattr('workflow_http.time.sleep', lambda _: None)
    response = Mock(status_code=200)
    send = Mock(side_effect=[requests.ConnectionError('lost'), response])
    assert request_read(send, 'https://pure.test/persons/search', json={'uuids': ['one']}) is response
    assert send.call_count == 2


def test_datacite_auth_failure_is_fatal(monkeypatch):
    import datacite_utils
    response = Mock(status_code=401)
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    monkeypatch.setattr(datacite_utils.session, 'get', Mock(return_value=response))
    with pytest.raises(RuntimeError, match='401'):
        datacite_utils.fetch_data_for_doi('10.1/example')


def test_openalex_source_failure_is_not_cached_as_empty_success(monkeypatch):
    import openalex_cache
    monkeypatch.setattr(openalex_cache, '_load_json', lambda *args: {})
    monkeypatch.setattr(openalex_cache, '_save_json', Mock())
    monkeypatch.setattr(openalex_cache.time, 'sleep', lambda _: None)
    session = Mock()
    session.get.side_effect = requests.Timeout('down')
    monkeypatch.setattr(openalex_cache, '_make_session', lambda: session)
    with pytest.raises(RuntimeError, match='Timeout'):
        openalex_cache.fetch_openalex_works_cached(['10.1/example'])


def test_malformed_internal_identifiers_are_skipped_with_warning(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr(internal, 'fetch_personroots', lambda _: [{'_key': 'person'}, {}])
    monkeypatch.setattr(internal, 'fetch_person_ids', lambda _: [
        {'name': 'PURE_UUID_PERS', 'value': 'uuid'},
        {'name': 'FULL_NAME', 'value': 'Person'},
        {'name': 'ORCID', 'value': None},
    ])
    result = internal.select_persons(['faculty'], 'faculty')
    assert len(result) == 1
    assert 'invalid' in caplog.text.lower()


def test_external_person_review_write_failure_propagates(monkeypatch, tmp_path):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr('builtins.open', Mock(side_effect=OSError('disk full')))
    with pytest.raises(OSError, match='disk full'):
        external.update_externalpersons_pure([], [], 'yes')


def test_repeated_search_page_fails_instead_of_looping_forever(monkeypatch):
    response = Mock(status_code=200)
    response.json.return_value = {'items': [{'uuid': str(i)} for i in range(100)]}
    session = Mock()
    session.post.side_effect = [response, response, AssertionError('unbounded pagination')]
    monkeypatch.setattr(external, '_make_session', lambda: session)
    monkeypatch.setattr(external.time, 'sleep', lambda _: None)
    with pytest.raises(RuntimeError, match='repeated'):
        external.fetch_batch(['10.1/a'], 'https://pure.test/search', {}, 30)


def test_null_record_on_full_page_does_not_truncate_search(monkeypatch):
    first = Mock(status_code=200)
    first.json.return_value = {'items': [None] + [{'uuid': str(i)} for i in range(99)]}
    last = Mock(status_code=200)
    last.json.return_value = {'items': [{'uuid': 'last'}]}
    session = Mock()
    session.post.side_effect = [first, last]
    monkeypatch.setattr(external, '_make_session', lambda: session)
    monkeypatch.setattr(external.time, 'sleep', lambda _: None)
    items = external.fetch_batch(['10.1/a'], 'https://pure.test/search', {}, 30)
    assert items[-1]['uuid'] == 'last'


def test_fulltext_openalex_auth_failure_stops_immediately(monkeypatch):
    import harvest_fulltext_candidates as fulltext
    response = Mock(status_code=401)
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    session = Mock()
    session.get.return_value = response
    with pytest.raises(RuntimeError, match='401'):
        fulltext.examine_output({'doi': '10.1/example'}, session, Mock())
    session.get.assert_called_once()


def test_empty_successful_collection_remains_valid(monkeypatch):
    response = Mock(status_code=200)
    response.json.return_value = {'results': []}
    monkeypatch.setattr(internal.requests, 'get', Mock(return_value=response))
    assert internal.fetch_personroots('faculty') == []
