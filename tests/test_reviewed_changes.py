from copy import deepcopy

import pytest

from config import OPENALEXEX_ID_URI, ORCID_ID_URI, ROR_ID_URI
from reviewed_changes import capture_apply_baseline, merge_reviewed_changes


def ident(value, uri=ORCID_ID_URI):
    return {'id': value, 'type': {'uri': uri}, 'typeDiscriminator': 'ClassifiedId'}


def source_person():
    base = {'uuid': 'u', 'version': '1', 'identifiers': []}
    source = deepcopy(base)
    capture_apply_baseline(source)
    source['identifiers'] = [ident('o'), ident('a', OPENALEXEX_ID_URI)]
    return source, base


def test_internal_selected_only_and_fresh_fields():
    source = {'uuid': 'u', 'version': '1', 'orcid': None, 'identifiers': []}
    current = {**source, 'version': '2', 'name': 'Human edit', 'identifiers': [ident('x', '/other')]}
    rows = [{'new_id': 'orcid', 'new_value': 'o'}]
    originals = deepcopy((source, rows, current))
    payload, owned = merge_reviewed_changes('internal_persons', source, rows, current)
    assert payload == {**current, 'orcid': 'o'}
    assert owned == [{'item_key': 'u|orcid|o', 'field_name': 'orcid', 'identifier_type': 'orcid', 'old_value': None, 'new_value': {'value': 'o'}}]
    assert (source, rows, current) == originals


@pytest.mark.parametrize('kind', ['internal_persons', 'external_persons', 'external_orgs'])
def test_requires_matching_uuid_and_version(kind):
    for current in ({'uuid': 'wrong', 'version': '2'}, {'uuid': 'u'}):
        with pytest.raises(ValueError):
            merge_reviewed_changes(kind, {'uuid': 'u'}, [], current)


def test_external_mixed_noop_and_actual_preserves_unrelated():
    source, current = source_person()
    current.update(version='2', identifiers=[ident('o'), ident('z', '/unrelated')])
    payload, owned = merge_reviewed_changes('external_persons', source, [{'ORCID': 'o', 'Alex_ID': 'a'}], current)
    assert payload['identifiers'] == current['identifiers'] + [ident('a', OPENALEXEX_ID_URI)]
    assert len(owned) == 1
    assert owned[0]['item_key'] == f'u|{OPENALEXEX_ID_URI}|a'
    assert owned[0]['old_value'] is None


@pytest.mark.parametrize('baseline_ids,current_ids', [([], [ident('other')]), ([ident('old')], [])])
def test_target_identifier_addition_or_deletion_conflicts(baseline_ids, current_ids):
    source, current = source_person()
    source['_btp_apply_baseline']['identifiers'] = baseline_ids
    current.update(version='2', identifiers=current_ids)
    with pytest.raises(ValueError, match='changed'):
        merge_reviewed_changes('external_persons', source, [{'ORCID': 'o'}], current)


@pytest.mark.parametrize('version,noop,allowed', [('1', False, True), ('2', False, False), ('2', True, True)])
def test_legacy_external_evidence(version, noop, allowed):
    source, current = source_person()
    del source['_btp_apply_baseline']
    current.update(version=version, identifiers=[ident('o')] if noop else [])
    if not allowed:
        with pytest.raises(ValueError, match='baseline'):
            merge_reviewed_changes('external_persons', source, [{'ORCID': 'o'}], current)
    else:
        _, owned = merge_reviewed_changes('external_persons', source, [{'ORCID': 'o'}], current)
        assert bool(owned) is not noop


def org_source():
    base = {'uuid': 'u', 'version': '1', 'identifiers': [], 'address': {'geoLocation': {'point': 'old'}}}
    source = deepcopy(base)
    capture_apply_baseline(source)
    source['address']['geoLocation']['point'] = 'new'
    source['_btp_match_metadata'] = {'pure_address_field': 'address', 'previous_address': deepcopy(base['address']), 'pure_address_payload': source['address']}
    return source, base


def test_org_merges_nested_leaf_preserving_new_human_fields():
    source, current = org_source()
    current.update(version='2')
    current['address']['geoLocation']['human'] = 'retained'
    current['address']['street'] = 'retained'
    payload, owned = merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': 'true'}], current)
    assert payload['address'] == {'street': 'retained', 'geoLocation': {'point': 'new', 'human': 'retained'}}
    assert owned[0]['old_value'] == {'field': 'address', 'value': current['address']}
    assert owned[0]['new_value'] == {'field': 'address', 'value': payload['address']}


def test_org_drift_conflict_and_legacy_previous_address():
    source, current = org_source()
    del source['_btp_apply_baseline']
    current['version'] = '2'
    payload, _ = merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': True}], current)
    assert payload['address']['geoLocation']['point'] == 'new'
    current['address']['geoLocation']['point'] = 'human'
    with pytest.raises(ValueError, match='changed'):
        merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': True}], current)


def test_baseline_capture_once_deepcopy():
    source, current = source_person()
    capture_apply_baseline(source)
    assert source['_btp_apply_baseline']['identifiers'] == []
    assert source['_btp_apply_baseline']['uuid'] == current['uuid']


def test_requires_selected_external_identifier_in_source():
    source, current = source_person()
    with pytest.raises(ValueError, match='source'):
        merge_reviewed_changes('external_persons', source, [{'ORCID': 'not-reviewed'}], current)


def test_country_resolution_only_when_changed():
    source, current = org_source()
    source['address']['country'] = {'uri': '/guess', 'term': {'en_GB': 'Country'}}
    countries = [{'uri': '/real', 'term': {'en_GB': 'Country'}}]
    payload, _ = merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': True}], current, countries=countries)
    assert payload['address']['country'] == countries[0]


def test_internal_identifier_append_records_absence_and_preserves_other_same_uri():
    source = {'uuid': 'u', 'version': '1', 'identifiers': [ident('old', '/local')]}
    current = {**deepcopy(source), 'version': '2', 'human': 'kept'}
    rows = [{'new_id': 'local', 'new_value': 'new', 'uri': '/local'}]
    payload, owned = merge_reviewed_changes('internal_persons', source, rows, current)
    assert [entry['id'] for entry in payload['identifiers']] == ['old', 'new']
    assert owned[0]['old_value'] is None
    current['identifiers'] = []
    with pytest.raises(ValueError, match='changed'):
        merge_reviewed_changes('internal_persons', source, rows, current)


def test_internal_orcid_drift_refused_but_desired_is_unowned():
    source = {'uuid': 'u', 'version': '1', 'orcid': 'old'}
    row = [{'new_id': 'orcid', 'new_value': 'desired'}]
    with pytest.raises(ValueError, match='changed'):
        merge_reviewed_changes('internal_persons', source, row, {**source, 'version': '2', 'orcid': 'human'})
    _, owned = merge_reviewed_changes('internal_persons', source, row, {**source, 'version': '2', 'orcid': 'desired'})
    assert owned == []


def test_org_ror_selected_only_and_noop_address():
    source, current = org_source()
    source['identifiers'] = [ident('r', ROR_ID_URI)]
    rows = [{'needs_ror_update': True, 'ror': 'r', 'needs_geo_update': True}]
    current['address'] = deepcopy(source['address'])
    current['identifiers'] = [ident('other', '/unrelated')]
    current['version'] = '2'
    payload, owned = merge_reviewed_changes('external_orgs', source, rows, current)
    assert len(owned) == 1
    assert owned[0]['item_key'] == f'u|{ROR_ID_URI}|r'
    assert payload['identifiers'][0] == current['identifiers'][0]
    current['identifiers'].append(ident('human', ROR_ID_URI))
    with pytest.raises(ValueError, match='changed'):
        merge_reviewed_changes('external_orgs', source, rows, current)
    _, owned = merge_reviewed_changes('external_orgs', source, [{'needs_ror_update': False, 'ror': 'r'}], current)
    assert owned == []


def test_existing_country_does_not_need_vocabulary_for_other_address_change():
    source, current = org_source()
    country = {'uri': '/existing', 'term': {'en_GB': 'Existing'}}
    source['_btp_apply_baseline']['address']['country'] = deepcopy(country)
    source['address']['country'] = deepcopy(country)
    current['address']['country'] = deepcopy(country)
    payload, _ = merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': True}], current)
    assert payload['address']['country'] == country


def test_missing_external_address_baseline_allows_only_exact_noop():
    source = {'uuid': 'u', 'version': '1', 'address': {'city': 'desired'}}
    current = {'uuid': 'u', 'version': '2', 'address': {'city': 'human'}}
    with pytest.raises(ValueError, match='baseline'):
        merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': True}], current)
    current['address'] = deepcopy(source['address'])
    _, owned = merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': True}], current)
    assert owned == []


def test_primary_identifiers_preserved_and_malformed_classified_refused():
    source, current = source_person()
    primary = {'typeDiscriminator': 'PrimaryId', 'idSource': 'scival', 'value': '508262'}
    current['identifiers'] = [primary]
    payload, _ = merge_reviewed_changes('external_persons', source, [{'ORCID': 'o'}], current)
    assert payload['identifiers'][0] == primary
    current['identifiers'] = [{'typeDiscriminator': 'ClassifiedId', 'id': 'broken'}]
    with pytest.raises(ValueError, match='Malformed'):
        merge_reviewed_changes('external_persons', source, [{'ORCID': 'o'}], current)


def test_internal_row_uuid_checked_and_nan_not_appended():
    source, current = source_person()
    with pytest.raises(ValueError, match='UUID'):
        merge_reviewed_changes('internal_persons', current, [{'PURE_UUID_PERS': 'wrong', 'new_id': 'orcid', 'new_value': 'x'}], current)
    _, owned = merge_reviewed_changes('external_persons', source, [{'ORCID': float('nan'), 'Alex_ID': 'nan'}], current)
    assert owned == []


def test_external_person_harvest_captures_baseline_before_first_of_repeated_matches(tmp_path, monkeypatch):
    import json

    import enrich_pure_external_persons as harvest
    monkeypatch.setenv('BTP_OUTPUT_DIR', str(tmp_path))
    person = {'uuid': 'u', 'version': '1', 'identifiers': []}
    rows = [{'Pure_UUID': 'u', 'ORCID': 'o', 'Alex_ID': ''}, {'Pure_UUID': 'u', 'ORCID': '', 'Alex_ID': 'a'}]
    harvest.update_externalpersons_pure(rows, [person], False)
    exported = json.loads((tmp_path / 'to_be_updated.json').read_text())
    assert exported
    for record in exported:
        assert record['_btp_apply_baseline'] == {'uuid': 'u', 'version': '1', 'identifiers': []}
        assert len(record['identifiers']) == 2


def test_conflicting_selected_orcid_intentions_rejected_before_merge():
    source = {'uuid': 'u', 'version': '1', 'orcid': None}
    rows = [{'new_id': 'orcid', 'new_value': 'first'}, {'new_id': 'orcid', 'new_value': 'second'}]
    with pytest.raises(ValueError, match='Conflicting.*ORCID'):
        merge_reviewed_changes('internal_persons', source, rows, source)


def test_country_classification_is_atomic_against_concurrent_country_addition():
    source, current = org_source()
    source['address']['country'] = {'uri': '/nl', 'term': {'en_GB': 'Netherlands'}}
    current['address']['country'] = {'term': {'nl_NL': 'Duitsland'}}
    with pytest.raises(ValueError, match='country.*changed'):
        merge_reviewed_changes('external_orgs', source, [{'needs_geo_update': True}], current, countries=[source['address']['country']])


def test_internal_orcid_normalized_value_retains_original_review_key():
    source = {'uuid': 'u', 'version': '1', 'orcid': None}
    value = 'https://orcid.org/0000-0000-0000-1234?lang=en#details'
    payload, owned = merge_reviewed_changes('internal_persons', source, [{'new_id': 'orcid', 'new_value': value}], source)
    assert payload['orcid'] == '0000-0000-0000-1234'
    assert owned[0]['item_key'] == f'u|orcid|{value}'
    assert owned[0]['new_value'] == {'value': '0000-0000-0000-1234'}


@pytest.mark.parametrize('kind', ['internal_persons', 'external_persons', 'external_orgs'])
def test_plain_id_entries_are_preserved_without_becoming_classified_identifiers(kind):
    plain = {'typeDiscriminator': 'Id', 'idSource': 'local-system', 'value': 'existing-primary'}
    current = {'uuid': 'u', 'version': 'fresh-opaque', 'identifiers': [plain]}
    source = deepcopy(current)
    capture_apply_baseline(source)
    if kind == 'internal_persons':
        rows = [{'new_id': 'other', 'new_value': 'job-added', 'uri': '/other'}]
        wanted = {'typeDiscriminator': 'ClassifiedId', 'id': 'job-added', 'type': {'uri': '/other'}}
    elif kind == 'external_persons':
        source['identifiers'].append(ident('job-added'))
        rows = [{'ORCID': 'job-added'}]
        wanted = ident('job-added')
    else:
        source['identifiers'].append(ident('job-added', ROR_ID_URI))
        rows = [{'needs_ror_update': True, 'ror': 'job-added'}]
        wanted = ident('job-added', ROR_ID_URI)
    payload, owned = merge_reviewed_changes(kind, source, rows, current)
    assert payload['identifiers'] == [plain, wanted]
    assert len(owned) == 1
    assert owned[0]['new_value']['id'] == 'job-added'
