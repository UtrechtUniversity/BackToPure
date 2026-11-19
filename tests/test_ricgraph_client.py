import importlib
import json

import pytest
import requests

from workflow_http import FatalWorkflowError


def response(payload, status=200):
    result = requests.Response()
    result.status_code = status
    result.url = "http://localhost:3030/api/get_all_neighbor_nodes"
    result._content = json.dumps(payload).encode()
    return result


def sender(pages):
    pages = iter(pages)
    calls = []

    def send(url, **kwargs):
        calls.append((url, dict(kwargs.get("params", {}))))
        return next(pages)

    return send, calls


@pytest.mark.parametrize("module_name,function_name,transport", [
    ("enrich_internal_persons_with_ids", "fetch_personroots", "requests"),
    ("enrich_internal_persons_with_ids", "fetch_person_ids", "requests"),
    ("enrich_internal_persons_with_ids", "checkenrichement", "requests"),
    ("enrich_pure_external_persons", "fetch_personroots", "session"),
    ("enrich_pure_external_persons", "fetch_ricgraph_persons_for_output", "session"),
    ("enrich_pure_external_persons", "fetch_ricgraph_person_details", "session"),
    ("enrich_pure_external_persons", "select_researchoutputs", "session"),
    ("enrich_pure_external_orgs", "fetch_personroots", "requests"),
    ("enrich_pure_external_orgs", "fetch_ricgraph_organization_neighbors", "session"),
    ("enrich_pure_external_orgs", "select_researchoutputs", "requests"),
    ("update_datasets_from_ricgraph", "fetch_personroots", "session"),
    ("update_datasets_from_ricgraph", "select_datasets", "session"),
    ("update_researchoutput_from_ricgraph", "fetch_personroots", "session"),
    ("update_researchoutput_from_ricgraph", "select_researchoutputs", "session"),
])
def test_workflow_reads_every_cursor_page(monkeypatch, module_name, function_name, transport):
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, "CATEGORIES", "journal article", raising=False)
    send, calls = sender([
        response({"results": [{"_key": "one"}, {"_key": "two"}], "meta": {"next_cursor": "next"}}),
        response({"results": [{"_key": "three"}], "meta": {"next_cursor": ""}}),
    ])
    monkeypatch.setattr(getattr(module, transport), "get", send)

    rows = getattr(module, function_name)("faculty-or-person")

    assert [row["_key"] for row in rows] == ["one", "two", "three"]
    assert len(calls) == 2
    assert calls[1][1]["cursor"] == "next"
    assert calls[0][1]["key"] == calls[1][1]["key"] == "faculty-or-person"
    assert calls[0][1].get("category_want") == calls[1][1].get("category_want")


def reader():
    try:
        from ricgraph_client import read_results
    except ImportError:
        pytest.fail("Shared Ricgraph paging reader is not implemented")
    return read_results


def test_reader_preserves_filters_and_does_not_mutate_caller_params():
    params = {"key": "person", "category_want": "journal article", "max_nr_items": "0"}
    send, calls = sender([
        response({"results": [{"_key": "one"}], "meta": {"next_cursor": "opaque-next"}}),
        response({"results": [{"_key": "two"}], "meta": {"next_cursor": None}}),
    ])
    rows = reader()(send, "http://localhost/api/get_all_neighbor_nodes", params=params)
    assert [row["_key"] for row in rows] == ["one", "two"]
    assert params == {"key": "person", "category_want": "journal article", "max_nr_items": "0"}
    assert calls[1][1] == {**calls[0][1], "cursor": "opaque-next"}


@pytest.mark.parametrize("payload", [{"results": []}, {"results": [{"_key": "old"}]}])
def test_reader_accepts_legacy_single_page(payload):
    send, calls = sender([response(payload)])
    assert reader()(send, "http://localhost/api/advanced_search") == payload["results"]
    assert len(calls) == 1


@pytest.mark.parametrize("payload", [
    {"results": [None]},
    {"results": "invalid"},
    {"meta": {"count": 2}},
    {"results": [], "meta": {"next_cursor": "more"}},
    {"results": [], "meta": "invalid"},
    {"results": [{"_key": "one"}], "meta": {"next_cursor": 42}},
])
def test_reader_rejects_malformed_or_incomplete_page(payload):
    send, _ = sender([response(payload)])
    with pytest.raises(FatalWorkflowError):
        reader()(send, "http://localhost/api/advanced_search")


def test_reader_rejects_repeated_cursor():
    send, _ = sender([
        response({"results": [{"_key": "one"}], "meta": {"next_cursor": "same"}}),
        response({"results": [{"_key": "two"}], "meta": {"next_cursor": "same"}}),
    ])
    with pytest.raises(FatalWorkflowError):
        reader()(send, "http://localhost/api/advanced_search")


def test_reader_rejects_repeated_results_with_new_cursor():
    send, _ = sender([
        response({"results": [{"_key": "one"}], "meta": {"next_cursor": "next"}}),
        response({"results": [{"_key": "one"}], "meta": {"next_cursor": "last"}}),
    ])
    with pytest.raises(FatalWorkflowError):
        reader()(send, "http://localhost/api/advanced_search")


def test_reader_does_not_return_partial_success_when_later_page_fails():
    send, _ = sender([
        response({"results": [{"_key": "one"}], "meta": {"next_cursor": "next"}}),
        response({"error": "invalid cursor"}, status=400),
    ])
    with pytest.raises(FatalWorkflowError):
        reader()(send, "http://localhost/api/advanced_search")
