from unittest.mock import Mock

import pandas as pd
import pytest

import apply_updates_to_pure as apply
import enrich_pure_external_orgs as harvest

COUNTRIES = [{'uri': '/dk/atira/pure/core/countries/us', 'term': {'en_GB': 'United States'}}]


def test_name_only_country_gets_supported_uri():
    assert harvest.build_pure_country_ref({'country': 'United States'}, COUNTRIES) == COUNTRIES[0]


def test_unknown_country_is_not_fabricated():
    with pytest.raises(ValueError, match='not supported'):
        harvest.build_pure_country_ref({'country_code': 'XK', 'country': 'Kosovo'}, COUNTRIES)


def test_apply_repairs_legacy_name_only_country_and_blocks_unsupported(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    get = Mock(return_value=Mock(status_code=200, json=lambda: {'classifications': COUNTRIES}))
    put = Mock(return_value=Mock(status_code=200, text=''))
    monkeypatch.setattr(apply.session, 'get', get)
    monkeypatch.setattr(apply.session, 'put', put)
    frame = pd.DataFrame([{'uuid': key, 'to_be_updated': 'X', 'updated': ''} for key in ['a', 'b']])
    records = [{'uuid': 'a', 'address': {'country': {'term': {'en_GB': 'United States'}}}},
               {'uuid': 'b', 'address': {'country': {'uri': '/dk/atira/pure/core/countries/xk', 'term': {'en_GB': 'Kosovo'}}}}]
    apply.process_external_orgs('update.csv', frame, records)
    assert put.call_count == 1
    assert put.call_args.kwargs['json']['address']['country'] == COUNTRIES[0]
    assert frame.loc[0, 'updated'].lower() == 'x'
    assert 'not supported' in frame.loc[1, 'apply_error']
    assert get.call_count == 1
    assert get.call_args.args[0].endswith('/external-organizations/allowed-address-countries')
    assert 'uri' not in records[0]['address']['country']


def test_harvest_keeps_unsupported_country_for_review_without_invalid_payload(monkeypatch):
    monkeypatch.setattr(harvest, 'load_external_org_countries', lambda *args: COUNTRIES)
    source = {'uuid': 'a', 'identifiers': []}
    row = {'uuid': 'a', 'ror': 'https://ror.org/example', 'match_type': 'exact_display_name',
           'geo_summary': {'country': 'Kosovo', 'country_code': 'XK', 'city': 'Pristina'}}
    _, _, rows, payloads = harvest.update_externalorg_pure([row], 'yes', 0, org_index={'a': source})
    assert rows[0]['to_be_updated'] == ''
    assert 'not supported' in rows[0]['validation_error']
    assert not rows[0]['needs_geo_update']
    assert 'address' not in payloads[0]
    assert source == {'uuid': 'a', 'identifiers': []}


def test_harvest_resolves_name_only_country(monkeypatch):
    monkeypatch.setattr(harvest, 'load_external_org_countries', lambda *args: COUNTRIES)
    row = {'uuid': 'a', 'ror': 'https://ror.org/example', 'match_type': 'exact_display_name',
           'geo_summary': {'country': 'United States', 'city': 'Manhattan'}}
    _, _, rows, payloads = harvest.update_externalorg_pure([row], 'yes', 0, org_index={'a': {'uuid': 'a'}})
    assert rows[0]['to_be_updated'] == 'X'
    assert payloads[0]['address']['country'] == COUNTRIES[0]


def test_country_vocabulary_failure_stops_before_writes(tmp_path, monkeypatch):
    from workflow_http import FatalWorkflowError
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    monkeypatch.setattr(apply.session, 'get', Mock(return_value=Mock(status_code=200, json=lambda: {})))
    put = Mock()
    monkeypatch.setattr(apply.session, 'put', put)
    frame = pd.DataFrame([{'uuid': 'a', 'to_be_updated': 'X', 'updated': ''}])
    with pytest.raises(FatalWorkflowError, match='invalid address-country vocabulary'):
        apply.process_external_orgs('update.csv', frame, [{'uuid': 'a', 'address': {'country': {'term': {'en_GB': 'US'}}}}])
    put.assert_not_called()


@pytest.mark.parametrize(('source_name', 'pure_name', 'code'), [('Türkiye', 'Turkey', 'tr'), ('Vietnam', 'Viet Nam', 'vn')])
def test_country_name_variants_use_supported_uri(source_name, pure_name, code):
    countries = [{'uri': '/dk/atira/pure/core/countries/' + code, 'term': {'en_GB': pure_name}}]
    assert harvest.build_pure_country_ref({'country': source_name}, countries) == countries[0]
