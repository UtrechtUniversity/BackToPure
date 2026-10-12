"""Pure search can include null entries alongside valid person records."""
import json
from unittest.mock import Mock

import pandas as pd

import enrich_internal_persons_with_ids as workflow


def test_fetch_person_data_excludes_null_records_from_saved_results(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    valid = {'uuid': 'person-1', 'identifiers': []}
    response = Mock()
    response.json.return_value = {'items': [None, valid, None]}
    monkeypatch.setattr(workflow.requests, 'post', Mock(return_value=response))

    result = workflow.fetch_person_data(
        pd.DataFrame({'PURE_UUID_PERS': ['person-1', 'missing-1', 'missing-2']}), 100,
    )

    assert result == [valid]
    assert json.loads((tmp_path / 'datatotal.json').read_text()) == [valid]
    assert '2 invalid person records' in caplog.text


def test_update_persons_accepts_existing_results_with_null_entries(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    persons = pd.DataFrame([{
        'FULL_NAME': 'Example', 'person_id': 'root-1',
        'PURE_UUID_PERS': 'person-1', 'ORCID': '0000-0001',
    }])

    workflow.update_persons(persons, [None, {'uuid': 'person-1', 'identifiers': []}, None])

    selected = pd.read_csv(next(tmp_path.glob('personstobeupdated_*.csv')))
    assert selected['PURE_UUID_PERS'].tolist() == ['person-1']
    assert selected['new_id'].tolist() == ['orcid']
    assert selected['new_value'].tolist() == ['0000-0001']
