"""Exercise real requests/urllib3 adapters, replacing only the socket exchange."""
import importlib
import io

import pytest
from urllib3.response import HTTPResponse

from workflow_http import FatalWorkflowError, request_read


@pytest.mark.parametrize('module_name,factory', [
    ('enrich_pure_external_orgs', None),
    ('enrich_pure_external_persons', None),
    ('enrich_pure_external_persons', '_make_session'),
    ('update_researchoutput_from_ricgraph', None),
    ('update_datasets_from_ricgraph', None),
    ('datacite_utils', None),
    ('openalex_cache', '_make_session'),
    ('pure_researchoutputs', None),
    ('pure_datasets', None),
    ('apply_updates_to_pure', None),
])
@pytest.mark.parametrize('method', ['get', 'post'])
@pytest.mark.parametrize('statuses,expected_count', [([502, 200], 2), ([502, 502, 502], 3), ([401], 1)])
def test_source_status_reaches_shared_retry_handler(monkeypatch, module_name, factory, statuses, expected_count, method):
    module = importlib.import_module(module_name)
    session = getattr(module, factory)() if factory else module.session
    calls = []

    def exchange(self, conn, method, url, **kwargs):
        status = statuses[min(len(calls), len(statuses) - 1)]
        calls.append(status)
        return HTTPResponse(body=io.BytesIO(b'{"items": []}'), status=status,
                            headers={'Content-Type': 'application/json'}, preload_content=False)

    monkeypatch.setattr('urllib3.connectionpool.HTTPSConnectionPool._make_request', exchange)
    monkeypatch.setattr('workflow_http.time.sleep', lambda _: None)
    monkeypatch.setattr(session, 'trust_env', False)
    if 200 in statuses:
        assert request_read(getattr(session, method), 'https://source.test/search').status_code == 200
    else:
        with pytest.raises(FatalWorkflowError, match=f'HTTP {statuses[-1]}'):
            request_read(getattr(session, method), 'https://source.test/search')
    assert len(calls) == expected_count
