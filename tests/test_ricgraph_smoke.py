import os
from urllib.parse import urljoin

import pytest
import requests


BASE_URL = os.environ.get("BTP_RICGRAPH_TEST_BASE_URL")


pytestmark = pytest.mark.skipif(
    not BASE_URL,
    reason="Set BTP_RICGRAPH_TEST_BASE_URL to run optional Ricgraph smoke tests",
)


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
    _get("get_all_personroot_nodes", {"key": "__btp_smoke__", "max_nr_items": "1"})


def test_ricgraph_neighbor_route_shape():
    _get("get_all_neighbor_nodes", {"key": "__btp_smoke__", "max_nr_items": "1"})


def test_ricgraph_advanced_search_route_shape():
    _get("advanced_search", {"category": "data set", "max_nr_items": "1"})


def test_ricgraph_person_enrich_route_shape():
    _get("person/enrich", {"key": "__btp_smoke__"}, allowed_statuses=(200, 400))
