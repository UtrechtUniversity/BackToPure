import os
from urllib.parse import urljoin, urlsplit
from uuid import UUID

import pytest
import requests

BASE_URL = os.environ.get("BTP_RICGRAPH_TEST_BASE_URL")


pytestmark = pytest.mark.skipif(
    not BASE_URL,
    reason="Set BTP_RICGRAPH_TEST_BASE_URL to run optional Ricgraph smoke tests",
)


@pytest.fixture(autouse=True)
def live_probe_is_read_only(monkeypatch):
    if not BASE_URL:
        return
    target = urlsplit(BASE_URL)
    original = requests.adapters.HTTPAdapter.send

    def send(adapter, request, **kwargs):
        destination = urlsplit(request.url)
        assert request.method.upper() in {"GET", "HEAD"}, "Live probes may not write"
        assert (destination.scheme, destination.netloc) == (target.scheme, target.netloc)
        assert destination.path.startswith(target.path.rstrip("/") + "/")
        return original(adapter, request, **kwargs)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)


def _get(route, params, allowed_statuses=(200,)):
    response = requests.get(urljoin(BASE_URL.rstrip("/") + "/", route), params=params, timeout=10)
    if response.status_code not in allowed_statuses:
        response.raise_for_status()
    payload = response.json()
    assert isinstance(payload, dict)
    if response.status_code == 200:
        assert "results" in payload or "meta" in payload
    return payload


def test_ricgraph_organization_search_route_shape():
    _get("organization/search", {"value": os.environ.get("BTP_RICGRAPH_TEST_ORG_VALUE", "uu faculty")})


def test_ricgraph_personroot_route_shape():
    _get("get_all_personroot_nodes", {"key": "__btp_smoke__", "max_nr_items": "1", "page_size": "1"})


def test_ricgraph_neighbor_route_shape():
    _get("get_all_neighbor_nodes", {"key": "__btp_smoke__", "max_nr_items": "1", "page_size": "1"})


def test_ricgraph_advanced_search_route_shape():
    _get("advanced_search", {"category": "data set", "max_nr_items": "1", "page_size": "1"})


def test_ricgraph_person_enrich_route_shape():
    _get("person/enrich", {"key": "__btp_smoke__"}, allowed_statuses=(200, 400))


def complete_reference(route, params):
    """Independent traversal with a different page size from the production reader."""
    params = dict(params, page_size=137)
    result = []
    cursors = set()
    while True:
        payload = _get(route, params)
        rows = payload.get("results", [])
        assert isinstance(rows, list)
        result.extend(rows)
        cursor = payload.get("meta", {}).get("next_cursor")
        if not cursor:
            return result
        assert rows and cursor not in cursors
        cursors.add(cursor)
        params["cursor"] = cursor


def selected_faculty():
    configured = os.environ.get("BTP_RICGRAPH_TEST_FACULTY_KEY")
    if configured:
        return configured
    value = os.environ.get("BTP_RICGRAPH_TEST_ORG_VALUE", "uu faculty")
    rows = complete_reference("organization/search", {"value": value})
    matches = [row for row in rows if row.get("_key", "").lower().startswith(value.lower() + ":")]
    if not matches:
        pytest.skip("Set BTP_RICGRAPH_TEST_FACULTY_KEY to a faculty with source data")
    return matches[0]["_key"]


def test_live_internal_and_external_personroot_reads_are_complete(monkeypatch):
    import enrich_internal_persons_with_ids as internal
    import enrich_pure_external_persons as external

    faculty = selected_faculty()
    reference = complete_reference("get_all_personroot_nodes", {"key": faculty})
    expected = {row["_key"] for row in reference}
    for module in (internal, external):
        monkeypatch.setattr(module, "RIC_BASE_URL", BASE_URL.rstrip("/") + "/")
        actual = module.fetch_personroots(faculty)
        assert len(actual) == len(reference)
        assert {row["_key"] for row in actual} == expected


def test_live_large_publication_author_read_is_complete(monkeypatch):
    import enrich_pure_external_persons as external

    key = os.environ.get("BTP_RICGRAPH_TEST_OUTPUT_KEY")
    if not key:
        pytest.skip("Set BTP_RICGRAPH_TEST_OUTPUT_KEY to a publication with more than 50 authors")
    reference = complete_reference("get_all_neighbor_nodes", {"key": key, "category_want": "person"})
    assert len(reference) > 50, "Choose a publication that exercises the old first-page limit"
    monkeypatch.setattr(external, "RIC_BASE_URL", BASE_URL.rstrip("/") + "/")
    actual = external.fetch_ricgraph_persons_for_output(key)
    assert len(actual) == len(reference)
    assert {row["_key"] for row in actual} == {row["_key"] for row in reference}


def test_live_books_resolve_typed_uuids_and_url_lists(monkeypatch):
    import enrich_pure_external_persons as external

    rows = complete_reference("advanced_search", {"category": "book"})
    tested = 0
    for row in rows:
        if row.get("name") == "PURE_ID_RESOUT":
            assert external.extract_researchoutput_pure_uuid(row) == str(UUID(row["value"]))
            tested += 1
        urls = row.get("url_other") or []
        if isinstance(urls, list) and len(urls) == 1 and "/publications/" in urls[0]:
            expected = str(UUID(urlsplit(urls[0]).path.rstrip("/").split("/")[-1]))
            assert external.extract_researchoutput_pure_uuid(row) == expected
            tested += 1
    if not tested:
        pytest.skip("No suitable Pure book records in the selected graph")


def test_live_doi_less_book_survives_a_scoped_selector(monkeypatch):
    import enrich_pure_external_persons as external

    books = complete_reference("advanced_search", {"category": "book"})
    for book in books:
        if book.get("name") != "PURE_ID_RESOUT":
            continue
        people = _get("get_all_neighbor_nodes", {"key": book["_key"], "category_want": "person", "page_size": 1}).get("results", [])
        if not people:
            continue
        monkeypatch.setattr(external, "RIC_BASE_URL", BASE_URL.rstrip("/") + "/")
        monkeypatch.setattr(external, "CATEGORIES", "book")
        # Scope the run to one real author; output retrieval and selection remain real.
        monkeypatch.setattr(external, "fetch_personroots", lambda _: [people[0]])
        result = external.select_persons_researchoutput(["bounded-live-probe"])
        matching = [row for row in result if row["researchoutput_key"] == book["_key"]]
        assert matching == [{"doi": None, "pure_uuid": str(UUID(book["value"])), "researchoutput_key": book["_key"]}]
        return
    pytest.skip("No DOI-less book with a linked author in the selected graph")
