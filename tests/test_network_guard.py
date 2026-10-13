"""Exercise the isolated-suite network boundary without opening any sockets."""
from types import SimpleNamespace

import pytest
import requests

from tests.conftest import reject_unmocked_remote_requests


def install_guard(monkeypatch, module_name, base_url):
    if base_url is None:
        monkeypatch.delenv("BTP_RICGRAPH_TEST_BASE_URL", raising=False)
    else:
        monkeypatch.setenv("BTP_RICGRAPH_TEST_BASE_URL", base_url)
    reached = []

    def adapter_send(adapter, request, **kwargs):
        reached.append((request.method, request.url))
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"results": []}'
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", adapter_send)
    reject_unmocked_remote_requests.__wrapped__(
        monkeypatch, SimpleNamespace(module=SimpleNamespace(__name__=module_name))
    )
    return reached


def test_explicit_ricgraph_smoke_optin_reaches_configured_remote_adapter(monkeypatch):
    reached = install_guard(monkeypatch, "test_ricgraph_smoke", "https://ricgraph.example/api")
    response = requests.get("https://ricgraph.example/api/organization/search?value=uu", timeout=1)
    assert response.json() == {"results": []}
    assert reached == [("GET", "https://ricgraph.example/api/organization/search?value=uu")]


@pytest.mark.parametrize(
    ("module_name", "base_url", "method", "url"),
    [
        ("test_ricgraph_smoke", None, "GET", "https://ricgraph.example/api/search"),
        ("test_jobs", "https://ricgraph.example/api", "GET", "https://ricgraph.example/api/search"),
        ("test_ricgraph_smoke", "https://ricgraph.example/api", "POST", "https://ricgraph.example/api/search"),
        ("test_ricgraph_smoke", "https://ricgraph.example/api", "GET", "https://other.example/api/search"),
        ("test_ricgraph_smoke", "https://ricgraph.example/api", "GET", "http://ricgraph.example/api/search"),
        ("test_ricgraph_smoke", "https://ricgraph.example/api", "GET", "https://ricgraph.example:444/api/search"),
        ("test_ricgraph_smoke", "https://ricgraph.example/api", "GET", "https://ricgraph.example/api-other/search"),
    ],
)
def test_unapproved_remote_request_is_blocked(monkeypatch, module_name, base_url, method, url):
    reached = install_guard(monkeypatch, module_name, base_url)
    with pytest.raises(AssertionError, match="Unmocked remote request"):
        requests.request(method, url, timeout=1)
    assert reached == []
