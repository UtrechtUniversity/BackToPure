import json
import multiprocessing
from unittest.mock import Mock

import pytest

from workflow_http import FatalWorkflowError
from safe_creation import Reservation, find_exact_doi, normalize_doi, tenant_key


def response(items, count=None):
    return Mock(status_code=200, json=lambda: {'count': len(items) if count is None else count, 'items': items})


def claim(path, gate, results):
    gate.wait()
    try:
        Reservation(path, 'https://pure.example/ws/api', '10.1/a', 'job', 'research_outputs').acquire()
        results.put('claimed')
    except FatalWorkflowError:
        results.put('blocked')


def test_claim_is_unique_across_processes(tmp_path):
    context = multiprocessing.get_context('fork')
    gate, results = context.Event(), context.Queue()
    processes = [context.Process(target=claim, args=(tmp_path / 'runtime.sqlite3', gate, results)) for _ in range(2)]
    for process in processes:
        process.start()
    gate.set()
    assert sorted(results.get(timeout=10) for _ in processes) == ['blocked', 'claimed']
    for process in processes:
        process.join(10)
        assert process.exitcode == 0


def test_unknown_reservation_stays_blocked(tmp_path):
    reservation = Reservation(tmp_path / 'db', 'https://pure.example/ws/api', 'HTTPS://DOI.ORG/10.1/A', 'one', 'research_outputs')
    reservation.acquire()
    reservation.finish('unknown')
    with pytest.raises(FatalWorkflowError, match='reserved'):
        Reservation(tmp_path / 'db', 'https://PURE.EXAMPLE:443/ws/api/', '10.1/a', 'two', 'datasets').acquire()
    Reservation(tmp_path / 'db', 'https://other.example/ws/api', '10.1/a', 'two', 'datasets').acquire()


def test_normalization():
    assert normalize_doi(' DOI:10.1/A ') == '10.1/a'
    assert normalize_doi('https://dx.doi.org/10.1/A') == '10.1/a'
    assert tenant_key('https://PURE.example:443/ws/api/') == 'https://pure.example/ws/api'


def test_lookup_checks_every_page_and_exact_doi():
    session = Mock()
    session.post.side_effect = [response([{'uuid': 'fuzzy', 'electronicVersions': [{'doi': '10.1/aa'}]}], 2),
                                response([{'uuid': 'exact', 'electronicVersions': [{'doi': 'https://doi.org/10.1/a'}]}], 2)]
    assert find_exact_doi(session, 'https://pure/', {}, 'research_outputs', '10.1/a')[0]['uuid'] == 'exact'
    assert session.post.call_args_list[1].kwargs['json']['offset'] == 1


@pytest.mark.parametrize('body', [{}, {'items': [], 'count': 1}, {'items': [None], 'count': 1}, {'items': [{'uuid': 'p', 'doi': 'wrong'}], 'count': 1}])
def test_malformed_lookup_fails_closed(body):
    session = Mock()
    session.post.return_value = Mock(status_code=200, json=lambda: body)
    with pytest.raises(FatalWorkflowError):
        find_exact_doi(session, 'https://pure/', {}, 'datasets', '10.1/a')


def test_existing_never_calls_create(tmp_path, monkeypatch):
    from safe_creation import create_once
    session, create = Mock(), Mock()
    session.post.side_effect = [response([]), response([{'uuid': 'existing', 'doi': {'doi': '10.1/a'}}])]
    result = create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)
    assert result['disposition'] == 'existing'
    assert result['target_tenant'] == 'https://pure'
    create.assert_not_called()


def test_ambiguous_write_blocks_other_job(monkeypatch):
    from safe_creation import create_once
    session, create = Mock(), Mock(side_effect=FatalWorkflowError('unknown'))
    session.post.return_value = response([])
    with pytest.raises(FatalWorkflowError, match='unknown'):
        create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)
    monkeypatch.setenv('BTP_JOB_ID', 'another-job')
    with pytest.raises(FatalWorkflowError, match='reserved'):
        create_once(session, 'https://pure/', {}, 'datasets', '10.1/a', create)
    assert create.call_count == 1


def test_no_implicit_cli_database(monkeypatch):
    from safe_creation import runtime_db_path
    monkeypatch.delenv('BTP_RUNTIME_DB')
    with pytest.raises(FatalWorkflowError, match='shared application database'):
        runtime_db_path()


def test_completed_claim_is_handled_but_never_replayed(monkeypatch):
    from safe_creation import create_once
    session, create = Mock(), Mock(return_value={'success': True, 'uuid': 'made'})
    session.post.return_value = response([])
    assert create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)['disposition'] == 'created'
    monkeypatch.setenv('BTP_JOB_ID', 'next-job')
    session.post.side_effect = [response([{'uuid': 'made', 'electronicVersions': [{'doi': '10.1/a'}]}]), response([])]
    assert create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)['disposition'] == 'existing'
    assert create.call_count == 1
    session.post.side_effect = None
    session.post.return_value = response([])
    with pytest.raises(FatalWorkflowError, match='no longer matches'):
        create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)
    assert create.call_count == 1


def interrupted_write(path):
    import os
    reservation = Reservation(path, 'https://pure/', '10.1/a', 'dead-job', 'research_outputs').acquire()
    reservation.finish('writing')
    os._exit(17)  # Simulates death after remote intent, before acknowledgement.


def test_dead_process_write_reservation_is_never_reclaimed(tmp_path):
    context = multiprocessing.get_context('fork')
    process = context.Process(target=interrupted_write, args=(tmp_path / 'db',))
    process.start()
    process.join(10)
    assert process.exitcode == 17
    with pytest.raises(FatalWorkflowError, match='reserved'):
        Reservation(tmp_path / 'db', 'https://pure/', '10.1/a', 'replacement', 'research_outputs').acquire()


def test_missing_uuid_after_acknowledgement_stays_reserved():
    from safe_creation import create_once
    session = Mock()
    session.post.return_value = response([])
    create = Mock(return_value={'success': True})
    with pytest.raises(FatalWorkflowError, match='unknown'):
        create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)
    with pytest.raises(FatalWorkflowError, match='reserved'):
        create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)
    assert create.call_count == 1


@pytest.mark.parametrize('second_metadata', [{'offset': 1, 'size': 100}, {'offset': 2, 'size': 100}, None])
def test_overlapping_search_pages_never_authorize_create(second_metadata):
    from safe_creation import create_once
    session, create = Mock(), Mock(return_value={'success': True, 'uuid': 'created'})
    pages = [{'count': 4, 'items': [{'uuid': 'a'}, {'uuid': 'b'}], 'pageInformation': {'offset': 0, 'size': 100}},
             {'count': 4, 'items': [{'uuid': 'b'}, {'uuid': 'c'}]}]
    if second_metadata is not None:
        pages[1]['pageInformation'] = second_metadata
    session.post.side_effect = [Mock(status_code=200, json=lambda page=page: page) for page in pages] + [response([])]
    with pytest.raises(FatalWorkflowError, match='pagination|repeated'):
        create_once(session, 'https://pure/', {}, 'research_outputs', '10.1/a', create)
    create.assert_not_called()


@pytest.mark.parametrize('metadata', [{'offset': 1}, {'offset': '0'}, {'offset': False}, {'size': 0}, {'size': -1}, {'size': True}, {'size': 101}, {'size': 1}, [], None])
def test_inconsistent_supplied_page_metadata_fails_closed(metadata):
    session = Mock()
    session.post.return_value = Mock(status_code=200, json=lambda: {
        'count': 2, 'items': [{'uuid': 'a'}, {'uuid': 'b'}], 'pageInformation': metadata})
    with pytest.raises(FatalWorkflowError, match='pagination'):
        find_exact_doi(session, 'https://pure/', {}, 'research_outputs', '10.1/a')


def test_duplicate_uuid_in_same_page_fails_closed():
    session = Mock()
    session.post.return_value = response([{'uuid': 'a'}, {'uuid': 'A'}])
    with pytest.raises(FatalWorkflowError, match='repeated'):
        find_exact_doi(session, 'https://pure/', {}, 'research_outputs', '10.1/a')


def test_consistent_supplied_pagination_metadata_reaches_exact_match():
    session = Mock()
    pages = [{'count': 4, 'items': [{'uuid': 'a'}, {'uuid': 'b'}], 'pageInformation': {'offset': 0, 'size': 100}},
             {'count': 4, 'items': [{'uuid': 'c'}, {'uuid': 'd', 'doi': {'doi': '10.1/a'}}], 'pageInformation': {'offset': 2, 'size': 100}}]
    session.post.side_effect = [Mock(status_code=200, json=lambda page=page: page) for page in pages]
    assert [item['uuid'] for item in find_exact_doi(session, 'https://pure/', {}, 'datasets', '10.1/a')] == ['d']
