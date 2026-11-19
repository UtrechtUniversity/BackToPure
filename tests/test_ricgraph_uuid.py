import pytest

import enrich_pure_external_persons as external

UUID = "0ccdc114-1897-4ae6-9f03-b575272da691"
OTHER_UUID = "943124c1-7184-4101-82f9-abdefdff84a5"
URL = "https://pure.example/en/publications/" + UUID


@pytest.mark.parametrize("field", ["url_main", "url_other"])
@pytest.mark.parametrize("value", [URL, [URL], ["https://openalex.org/W123", URL], [None, URL + "/?lang=en#record"]])
def test_publication_urls_support_strings_and_lists(field, value):
    assert external.extract_researchoutput_pure_uuid({field: value}) == UUID


@pytest.mark.parametrize("node", [
    {"name": "PURE_ID_RESOUT", "value": UUID},
    {"name": "pure_id_resout", "value": UUID.upper()},
    {"_key": UUID + "|pure_id_resout"},
    {"name": "PURE_ID_RESOUT", "value": UUID, "url_main": [URL], "url_other": []},
])
def test_typed_pure_output_resolves_uuid_without_a_doi(node):
    assert external.extract_researchoutput_doi(node) is None
    assert external.extract_researchoutput_pure_uuid(node) == UUID


@pytest.mark.parametrize("field", ["pure_uuid", "uuid", "PURE_UUID", "pureId"])
def test_existing_explicit_uuid_fields_take_precedence(field):
    assert external.extract_researchoutput_pure_uuid({field: OTHER_UUID, "url_other": [URL]}) == OTHER_UUID


@pytest.mark.parametrize("node", [
    {"name": "DOI", "value": UUID},
    {"name": "PURE_ID_PERS", "value": UUID},
    {"_key": UUID + "|pure_id_pers"},
    {"name": "PURE_ID_RESOUT", "value": "not-a-uuid"},
    {"pure_uuid": "not-a-uuid"},
    {"url_main": ["https://other.example/persons/" + UUID]},
    {"url_other": ["https://pure.example/publications/not-a-uuid"]},
    {"url_other": ["https://other.example/?redirect=publications/" + UUID]},
    {"url_other": ["ftp://pure.example/publications/" + UUID]},
    {"url_other": [None, 123, {}]},
    {"url_other": None},
])
def test_unrelated_or_malformed_identifiers_do_not_become_output_uuids(node):
    assert external.extract_researchoutput_pure_uuid(node) is None


def test_doi_less_output_survives_actual_workflow_selector(monkeypatch):
    monkeypatch.setattr(external, "fetch_personroots", lambda _: [{"_key": "person"}])
    node = {"name": "PURE_ID_RESOUT", "value": UUID, "_key": UUID + "|pure_id_resout", "url_main": [URL], "url_other": []}
    monkeypatch.setattr(external, "select_researchoutputs", lambda _: [node])

    result = external.select_persons_researchoutput(["faculty"])

    assert result == [{"doi": None, "pure_uuid": UUID, "researchoutput_key": node["_key"]}]
