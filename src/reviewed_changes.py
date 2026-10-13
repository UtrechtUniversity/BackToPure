"""Pure, conservative merges of reviewed intentions into fresh Pure records."""
from copy import deepcopy
from collections import Counter

from config import ORCID_ID_URI, OPENALEXEX_ID_URI, ROR_ID_URI
from pure_countries import resolve_country

_MISSING = object()


def capture_apply_baseline(record):
    """Capture once, before harvest enrichment mutates the fetched record."""
    if '_btp_apply_baseline' not in record:
        record['_btp_apply_baseline'] = deepcopy({k: v for k, v in record.items() if not k.startswith('_btp_')})


def _identifiers(record):
    values = record.get('identifiers')
    if values is None:
        values = []
    if not isinstance(values, list):
        raise ValueError('Malformed identifier evidence')
    classified = []
    for value in values:
        if not isinstance(value, dict):
            raise ValueError('Malformed identifier evidence')
        if value.get('typeDiscriminator') == 'PrimaryId':
            continue
        if not isinstance(value.get('type'), dict) or not value['type'].get('uri') or not value.get('id'):
            raise ValueError('Malformed identifier evidence')
        classified.append(value)
    return classified


def _values(record, uri):
    return Counter(str(v.get('id')) for v in _identifiers(record) if v['type'].get('uri') == uri)


def _text(value):
    text = str(value or '').strip()
    return '' if text.lower() in {'nan', 'none', '<na>'} else text


def _truth(value):
    return str(value).strip().lower() in {'true', '1', 'yes', 'x'}


def _merge_leaves(before, desired, current, path='address'):
    if before == desired:
        return deepcopy(current) if current is not _MISSING else _MISSING
    if isinstance(desired, dict) and (isinstance(before, dict) or before is _MISSING or before is None):
        if current is not _MISSING and current is not None and not isinstance(current, dict):
            raise ValueError(f'{path} changed since review')
        original = before if isinstance(before, dict) else {}
        fresh = current if isinstance(current, dict) else {}
        result = deepcopy(fresh)
        for key in original.keys() | desired.keys():
            merged = _merge_leaves(original.get(key, _MISSING), desired.get(key, _MISSING), fresh.get(key, _MISSING), f'{path}.{key}')
            if merged is _MISSING:
                result.pop(key, None)
            else:
                result[key] = merged
        return result
    if current == desired:
        return deepcopy(current) if current is not _MISSING else _MISSING
    if current != before:
        raise ValueError(f'{path} changed since review')
    return deepcopy(desired) if desired is not _MISSING else _MISSING


def merge_reviewed_changes(kind, source, selected_rows, current, *, countries=None):
    """Return a fresh payload and only changes this write would actually own.

    External legacy artifacts require equal versions unless already satisfied or
    historical previous_address metadata proves the selected address delta.
    No inputs are mutated; no network access is performed.
    """
    kind = {'internal_person': 'internal_persons', 'external_person': 'external_persons', 'external_org': 'external_orgs'}.get(kind, kind)
    if kind not in {'internal_persons', 'external_persons', 'external_orgs'}:
        raise ValueError(f'Unsupported merge kind: {kind}')
    uuid = source.get('uuid')
    if not uuid or current.get('uuid') != uuid:
        raise ValueError('Fresh Pure UUID does not match source UUID')
    if not isinstance(current.get('version'), str) or not current['version'].strip():
        raise ValueError('Fresh Pure record has no version')
    payload = deepcopy({k: v for k, v in current.items() if not k.startswith('_btp_')})
    baseline = source if kind == 'internal_persons' else source.get('_btp_apply_baseline')
    if baseline is not None and (not isinstance(baseline, dict) or baseline.get('uuid') != uuid):
        raise ValueError('Invalid apply baseline')
    if baseline is None and source.get('version') == current['version']:
        baseline = current
    owned = []

    def record(key, field, identifier_type, old, new):
        owned.append(dict(item_key=key, field_name=field, identifier_type=identifier_type, old_value=deepcopy(old), new_value=deepcopy(new)))

    def append_id(value, uri, identifier_type, key, require_source):
        if not value or not uri:
            raise ValueError('Reviewed identifier is incomplete')
        template = next((v for v in _identifiers(source) if v.get('id') == value and v['type'].get('uri') == uri), None)
        if require_source and template is None:
            raise ValueError('Reviewed identifier is missing from source')
        if value in _values(payload, uri):
            return
        if baseline is None:
            raise ValueError('Missing apply baseline; re-harvest and review this record')
        if _values(baseline, uri) != _values(current, uri):
            raise ValueError('Target identifier values changed since review')
        entry = deepcopy(template) if template else {'typeDiscriminator': 'ClassifiedId', 'id': value, 'type': {'uri': uri}}
        payload.setdefault('identifiers', [])
        if payload['identifiers'] is None:
            payload['identifiers'] = []
        payload['identifiers'].append(entry)
        record(key, 'identifier', identifier_type, None, {'id': value, 'uri': uri})

    for row in selected_rows:
        row_uuid = row.get('uuid') or row.get('UUID') or row.get('Pure_UUID') or row.get('PURE_UUID_PERS')
        if row_uuid and row_uuid != uuid:
            raise ValueError('Selected row UUID does not match source')
        if kind == 'internal_persons':
            identifier_type = str(row.get('new_id') or '').strip()
            value = _text(row.get('new_value'))
            key = f'{uuid}|{identifier_type}|{value}'
            if identifier_type == 'orcid':
                if not value:
                    raise ValueError('Reviewed ORCID is empty')
                if payload.get('orcid') == value:
                    continue
                if current.get('orcid') != baseline.get('orcid'):
                    raise ValueError('ORCID changed since review')
                record(key, 'orcid', 'orcid', current.get('orcid'), {'value': value})
                payload['orcid'] = value
            else:
                append_id(value, str(row.get('uri') or '').strip(), identifier_type, key, False)
        elif kind == 'external_persons':
            for column, uri in [('ORCID', ORCID_ID_URI), ('Alex_ID', OPENALEXEX_ID_URI)]:
                value = _text(row.get(column))
                if value:
                    append_id(value, uri, column.lower(), f'{uuid}|{uri}|{value}', True)
        else:
            if _truth(row.get('needs_ror_update')):
                value = _text(row.get('ror'))
                append_id(value, ROR_ID_URI, 'ror', f'{uuid}|{ROR_ID_URI}|{value}', True)
            if _truth(row.get('needs_geo_update')):
                metadata = source.get('_btp_match_metadata') or {}
                field = row.get('pure_address_field') or metadata.get('pure_address_field') or ('contactAddress' if 'contactAddress' in source else 'address')
                if field not in {'contactAddress', 'address'}:
                    raise ValueError('Invalid reviewed address field')
                desired = deepcopy(metadata.get('pure_address_payload', source.get(field)))
                before = baseline.get(field) if baseline is not None else metadata.get('previous_address', _MISSING)
                fresh = payload.get(field)
                if desired == fresh:
                    continue
                if before is _MISSING:
                    raise ValueError('Missing address baseline; re-harvest and review this record')
                if isinstance(desired, dict) and desired.get('country') != (before or {}).get('country') and desired.get('country') is not None:
                    if countries is None:
                        raise ValueError('Country vocabulary required for reviewed country change')
                    desired['country'] = resolve_country(desired['country'], countries)
                merged = _merge_leaves(before, desired, fresh)
                if merged != fresh:
                    record(f'{uuid}|{field}|address', field, 'address', {'field': field, 'value': fresh}, {'field': field, 'value': merged})
                    payload[field] = merged
    return payload, owned
