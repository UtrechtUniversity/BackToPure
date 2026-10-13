import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SRC_ROOT = os.path.join(PROJECT_ROOT, "src")

if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)


import pytest


@pytest.fixture(autouse=True)
def isolated_write_reservations(tmp_path, monkeypatch):
    monkeypatch.setenv('BTP_RUNTIME_DB', str(tmp_path / 'reservations.sqlite3'))
    monkeypatch.setenv('BTP_JOB_ID', 'test-job')


@pytest.fixture(autouse=True)
def reject_unmocked_remote_requests(monkeypatch, request):
    if request.module.__name__.endswith("test_retry_adapter_integration"):
        return  # This module mocks the socket exchange to exercise the real adapter.
    from urllib.parse import urlsplit

    import requests
    original = requests.adapters.HTTPAdapter.send
    smoke_base_url = os.environ.get("BTP_RICGRAPH_TEST_BASE_URL")
    smoke_target = urlsplit(smoke_base_url) if smoke_base_url else None
    opted_in_smoke = request.module.__name__.split(".")[-1] == "test_ricgraph_smoke" and smoke_target is not None
    def send(adapter, request, **kwargs):
        target = urlsplit(request.url)
        opted_in_read = (
            opted_in_smoke
            and request.method in {"GET", "HEAD"}
            and (target.scheme, target.netloc) == (smoke_target.scheme, smoke_target.netloc)
            and (
                target.path == smoke_target.path.rstrip("/")
                or target.path.startswith(smoke_target.path.rstrip("/") + "/")
            )
        )
        if target.hostname not in {'localhost', '127.0.0.1', '::1'} and not opted_in_read:
            raise AssertionError('Unmocked remote request in isolated test: ' + request.method + ' ' + urlsplit(request.url).path)
        return original(adapter, request, **kwargs)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, 'send', send)
