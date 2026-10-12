"""Resolve address countries against the target Pure installation's vocabulary."""
import copy

from workflow_http import FatalWorkflowError, json_object, request_read


def load_external_org_countries(session, base_url, headers):
    response = request_read(session.get, base_url + 'external-organizations/allowed-address-countries',
                            headers=headers)
    values = json_object(response).get('classifications')
    if not isinstance(values, list) or not values or any(
        not isinstance(value, dict) or not value.get('uri') for value in values
    ):
        raise FatalWorkflowError('Pure returned an invalid address-country vocabulary; no updates can be validated.')
    return values


def resolve_country(reference, countries):
    """Use a supported URI, or an unambiguous exact country-name match."""
    uri = reference.get('uri')
    if uri:
        matches = [country for country in countries if country['uri'] == uri]
        if len(matches) == 1:
            return copy.deepcopy(matches[0])
    names = {str(name).strip().casefold() for name in (reference.get('term') or {}).values() if name}
    # OpenAlex uses these names while older Pure vocabularies use Turkey/Viet Nam.
    aliases = {'türkiye': 'turkey', 'vietnam': 'viet nam'}
    names = {aliases.get(name, name) for name in names}
    matches = [country for country in countries
               if names & {aliases.get(str(name).strip().casefold(), str(name).strip().casefold())
                           for name in (country.get('term') or {}).values() if name}]
    if len(matches) == 1:
        return copy.deepcopy(matches[0])
    label = ', '.join(sorted(names)) or str(uri or '(missing country name and URI)')
    raise ValueError(f'Address country {label!r} is not supported unambiguously by Pure; review its country mapping.')
