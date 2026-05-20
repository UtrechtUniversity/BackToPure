import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import enrich_pure_external_orgs as external_orgs
import snapshot_openalex_institutions
from config import ROR_ID_URI
from enrich_internal_persons_with_ids import (
    REQUEST_TIMEOUT,
    _normalize_identifier,
    _resolve_pure_person_uuid_column,
    check_new_ids,
    fetch_person_ids,
    fetch_personroots,
    update_persons,
)
from enrich_pure_external_orgs import (
    _has_identifier,
    _points_equal,
    address_needs_update,
    dedupe_records_by_uuid,
    get_ext_orgdata_pure,
    match_organizations,
)
from enrich_pure_external_persons import (
    REQUEST_TIMEOUT as EXTERNAL_REQUEST_TIMEOUT,
)
from enrich_pure_external_persons import (
    extract_openalex_id,
    extract_orcid_id,
    extract_researchoutput_doi,
    fetch_openalex_works,
    fetch_pure_researchoutputs,
    match_ricgraph_persons,
    normalize_doi,
    select_faculties as select_external_person_faculties,
    select_persons_researchoutput,
    select_researchoutputs,
)


class InternalPersonsTests(unittest.TestCase):
    def test_normalize_identifier_strips_orcid_query_suffixes(self):
        self.assertEqual("0000-0003-2472-6589", _normalize_identifier("0000-0003-2472-6589?LANG=EN"))
        self.assertEqual("0000-0003-2472-6589", _normalize_identifier("https://orcid.org/0000-0003-2472-6589?lang=en"))

    def test_check_new_ids_returns_normalized_orcid_for_review_csv(self):
        row = pd.Series({"ORCID": "0000-0003-2472-6589?LANG=EN"})
        new_ids, data, orcidchange, orcid = check_new_ids(row, {"uuid": "pers-1", "identifiers": []})

        self.assertEqual([], new_ids)
        self.assertEqual("X", orcidchange)
        self.assertEqual("0000-0003-2472-6589", orcid)
        self.assertEqual("0000-0003-2472-6589", data["orcid"])

    def test_check_new_ids_supports_openalex_id_pers_alias(self):
        row = pd.Series({"OPENALEX_ID_PERS": "https://openalex.org/A1234567890?foo=bar"})

        new_ids, data, orcidchange, orcid = check_new_ids(row, {"uuid": "pers-1", "identifiers": []})

        self.assertEqual(
            [{"id": "A1234567890", "uri": "/dk/atira/pure/person/personsources/open_alex_id"}],
            new_ids,
        )
        self.assertEqual("", orcidchange)
        self.assertTrue(pd.isna(orcid))
        self.assertNotIn("orcid", data)

    def test_check_new_ids_skips_openalex_id_pers_when_identifier_already_exists(self):
        row = pd.Series({"OPENALEX_ID_PERS": "https://openalex.org/A1234567890"})
        data = {
            "uuid": "pers-1",
            "identifiers": [
                {
                    "id": "A1234567890",
                    "type": {"uri": "/dk/atira/pure/person/personsources/open_alex_id"},
                }
            ],
        }

        new_ids, _data, orcidchange, orcid = check_new_ids(row, data)

        self.assertEqual([], new_ids)
        self.assertEqual("", orcidchange)
        self.assertTrue(pd.isna(orcid))

    def test_resolve_pure_uuid_uses_pure_id_when_uuid_missing(self):
        frame = pd.DataFrame(
            {
                "person_id": ["person-1", "person-2", "person-3"],
                "PURE_UUID_PERS": [pd.NA, "", "uuid-3"],
                "PURE_ID_PERS": ["uuid-1", "uuid-2", "pure-id-3"],
            }
        )

        resolved = _resolve_pure_person_uuid_column(frame.copy())

        self.assertEqual(["uuid-1", "uuid-2", "uuid-3"], resolved["PURE_UUID_PERS"].tolist())

    def test_resolve_pure_uuid_keeps_missing_when_no_fallback_exists(self):
        frame = pd.DataFrame(
            {
                "person_id": ["person-1"],
                "PURE_UUID_PERS": [pd.NA],
            }
        )

        resolved = _resolve_pure_person_uuid_column(frame.copy())

        self.assertTrue(pd.isna(resolved.loc[0, "PURE_UUID_PERS"]))

    @patch("enrich_internal_persons_with_ids.requests.get")
    def test_fetch_personroots_uses_timeout_and_raise_for_status(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"results": [{"_key": "p1"}]}
        mock_get.return_value = response

        result = fetch_personroots("faculty-1")

        self.assertEqual([{"_key": "p1"}], result)
        mock_get.assert_called_once()
        self.assertEqual(REQUEST_TIMEOUT, mock_get.call_args.kwargs["timeout"])
        response.raise_for_status.assert_called_once()

    @patch("enrich_internal_persons_with_ids.requests.get")
    def test_fetch_person_ids_uses_timeout_and_raise_for_status(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"results": [{"name": "PURE_ID_PERS", "value": "uuid-1"}]}
        mock_get.return_value = response

        result = fetch_person_ids("person-root-1")

        self.assertEqual([{"name": "PURE_ID_PERS", "value": "uuid-1"}], result)
        self.assertEqual(REQUEST_TIMEOUT, mock_get.call_args.kwargs["timeout"])
        response.raise_for_status.assert_called_once()

    @patch("enrich_pure_external_persons.session.get")
    @patch("enrich_pure_external_persons.CATEGORIES", "journal article, book chapter")
    def test_external_select_researchoutputs_uses_configured_categories_timeout_and_raise_for_status(self, mock_get):
        response = MagicMock()
        response.json.side_effect = [
            {"results": [{"_key": "doi-1"}]},
            {"results": [{"_key": "doi-2"}]},
        ]
        mock_get.return_value = response

        result = select_researchoutputs("person-root-1")

        self.assertEqual([{"_key": "doi-1"}, {"_key": "doi-2"}], result)
        self.assertEqual(EXTERNAL_REQUEST_TIMEOUT, mock_get.call_args.kwargs["timeout"])
        self.assertEqual(2, response.raise_for_status.call_count)
        self.assertEqual(
            ["journal article", "book chapter"],
            [call.kwargs["params"]["category_want"] for call in mock_get.call_args_list],
        )

    def test_external_identifier_normalization_strips_query_suffixes(self):
        self.assertEqual("0000-0003-2472-6589", extract_orcid_id("https://orcid.org/0000-0003-2472-6589?lang=en"))
        self.assertEqual("A1234567890", extract_openalex_id("https://openalex.org/A1234567890?foo=bar"))

    @patch("enrich_pure_external_persons.session.get")
    def test_external_select_faculties_excludes_research_organisations(self, mock_get):
        response = MagicMock()
        response.json.return_value = {
            "results": [
                {"_key": "uu faculty: faculteit test|organization_name"},
                {"_key": "uu faculty research: faculteit test|organization_name"},
            ]
        }
        mock_get.return_value = response

        result = select_external_person_faculties("all")

        self.assertEqual(["uu faculty: faculteit test|organization_name"], result)

    @patch("enrich_pure_external_persons.session.get")
    def test_external_select_faculties_rejects_direct_research_organisation(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"results": []}
        mock_get.return_value = response

        result = select_external_person_faculties("uu faculty research: faculteit test|organization_name")

        self.assertEqual([], result)

    def test_external_doi_normalization_rejects_ricgraph_uuid_keys(self):
        self.assertEqual("10.1234/alpha", normalize_doi("https://doi.org/10.1234/Alpha."))
        self.assertIsNone(normalize_doi("455d8d8f-fe6a-4e66-ac51-39b354bb1754"))

    def test_external_researchoutput_doi_extraction_uses_valid_doi_from_key_parts(self):
        self.assertEqual(
            "10.1234/alpha",
            extract_researchoutput_doi({"_key": "455d8d8f-fe6a-4e66-ac51-39b354bb1754|10.1234/Alpha"}),
        )
        self.assertIsNone(extract_researchoutput_doi({"_key": "455d8d8f-fe6a-4e66-ac51-39b354bb1754"}))

    @patch("enrich_pure_external_persons.fetch_personroots")
    @patch("enrich_pure_external_persons.select_researchoutputs")
    def test_external_select_persons_researchoutput_does_not_treat_uuid_as_doi(self, mock_select_outputs, mock_roots):
        mock_roots.return_value = [{"_key": "person-root-1"}]
        mock_select_outputs.return_value = [
            {"_key": "455d8d8f-fe6a-4e66-ac51-39b354bb1754", "url_other": "https://pure.example/publications/pure-1"},
            {"_key": "10.1234/alpha|journal article"},
            {"_key": "not-a-doi"},
        ]

        result = select_persons_researchoutput(["faculty-1"])

        self.assertEqual(
            [
                {
                    "doi": None,
                    "pure_uuid": "pure-1",
                    "researchoutput_key": "455d8d8f-fe6a-4e66-ac51-39b354bb1754",
                },
                {
                    "doi": "10.1234/alpha",
                    "pure_uuid": None,
                    "researchoutput_key": "10.1234/alpha|journal article",
                },
            ],
            result,
        )

    def test_external_ricgraph_matching_supports_researchoutput_uuid_without_doi(self):
        pure_article = {
            "uuid": "pure-output-1",
            "contributors": [
                {
                    "externalPerson": {"uuid": "pure-person-1"},
                    "name": {"firstName": "Jane", "lastName": "Doe"},
                }
            ],
        }

        result = match_ricgraph_persons(
            [
                {
                    "Name": "Jane Doe",
                    "Alex_ID": "A123",
                    "ORCID": "",
                    "doi": None,
                    "researchoutput_key": "ricgraph-output-1",
                    "researchoutput_pure_uuid": "pure-output-1",
                }
            ],
            {"results": [pure_article], "by_doi": {}, "by_uuid": {"pure-output-1": pure_article}},
        )

        self.assertEqual(1, len(result))
        self.assertEqual("pure-person-1", result[0]["Pure_UUID"])

    @patch("enrich_pure_external_persons.time.sleep")
    @patch("enrich_pure_external_persons.fetch_batch")
    @patch("enrich_pure_external_persons.fetch_pure_researchoutput_by_doi")
    def test_external_pure_researchoutputs_uses_bounded_exact_doi_lookup(
        self,
        mock_fetch_by_doi,
        mock_fetch_batch,
        _mock_sleep,
    ):
        mock_fetch_by_doi.side_effect = [
            [
                {
                    "uuid": "output-1",
                    "electronicVersions": [{"doi": "https://doi.org/10.1234/alpha"}],
                    "additionalLinks": [],
                }
            ],
            [],
        ]

        result = fetch_pure_researchoutputs(
            [
                {"doi": "10.1234/alpha"},
                {"doi": "10.1234/beta"},
            ],
            batch_size=50,
        )

        mock_fetch_batch.assert_not_called()
        self.assertEqual(
            [("10.1234/alpha",), ("10.1234/beta",)],
            [call.args for call in mock_fetch_by_doi.call_args_list],
        )
        self.assertEqual(1, len(result["results"]))
        self.assertEqual("output-1", result["by_doi"]["10.1234/alpha"]["uuid"])
        self.assertNotIn("10.1234/beta", result["by_doi"])

    @patch("enrich_pure_external_persons.time.sleep")
    @patch("enrich_pure_external_persons._make_session")
    @patch("enrich_pure_external_persons.fetch_pure_researchoutput_by_doi")
    def test_external_pure_researchoutputs_does_not_doi_fetch_uuid_backed_doi(
        self,
        mock_fetch_by_doi,
        mock_make_session,
        _mock_sleep,
    ):
        response = MagicMock()
        response.json.return_value = {
            "items": [
                {
                    "uuid": "output-uuid",
                    "electronicVersions": [{"doi": "https://doi.org/10.1234/alpha"}],
                    "additionalLinks": [],
                }
            ]
        }
        response.raise_for_status.return_value = None
        session = MagicMock()
        session.post.return_value = response
        mock_make_session.return_value = session

        result = fetch_pure_researchoutputs(
            [
                {"doi": "10.1234/alpha", "pure_uuid": "output-uuid"},
                {"doi": "10.1234/alpha"},
            ],
            batch_size=50,
        )

        session.post.assert_called_once()
        mock_fetch_by_doi.assert_not_called()
        self.assertEqual(1, len(result["results"]))
        self.assertEqual("output-uuid", result["by_doi"]["10.1234/alpha"]["uuid"])

    @patch("enrich_pure_external_persons.time.sleep")
    @patch("enrich_pure_external_persons._make_session")
    @patch("enrich_pure_external_persons.fetch_pure_researchoutput_by_doi")
    def test_external_pure_researchoutputs_can_disable_doi_fallback(
        self,
        mock_fetch_by_doi,
        mock_make_session,
        _mock_sleep,
    ):
        response = MagicMock()
        response.json.return_value = {"items": []}
        response.raise_for_status.return_value = None
        session = MagicMock()
        session.post.return_value = response
        mock_make_session.return_value = session

        fetch_pure_researchoutputs(
            [
                {"doi": "10.1234/alpha", "pure_uuid": "output-uuid"},
                {"doi": "10.1234/beta"},
            ],
            batch_size=50,
            allow_doi_fallback=False,
        )

        session.post.assert_called_once()
        mock_fetch_by_doi.assert_not_called()

    @patch.dict(os.environ, {"OPENALEX_WORKS_BATCH_SIZE": "30", "OPENALEX_WORKS_REQUEST_DELAY": "0.25"})
    @patch("enrich_pure_external_persons.fetch_openalex_works_cached")
    def test_external_openalex_fetch_uses_configurable_batch_settings(self, mock_fetch_cached):
        mock_fetch_cached.return_value = {"results": [], "by_doi": {}}

        fetch_openalex_works(["10.123/alpha"])

        mock_fetch_cached.assert_called_once_with(
            ["10.123/alpha"],
            batch_size=30,
            request_delay=0.25,
            force_refresh=False,
        )

    def test_external_org_dedupes_rows_by_uuid(self):
        records = [
            {"uuid": "org-1", "name": "Alpha"},
            {"uuid": "org-1", "name": "Alpha newer"},
            {"uuid": "org-2", "name": "Beta"},
        ]

        result = dedupe_records_by_uuid(records, uuid_keys=("uuid",))

        self.assertEqual(2, len(result))
        self.assertEqual("Alpha newer", next(item for item in result if item["uuid"] == "org-1")["name"])

    def test_external_org_identifier_check_ignores_extra_pure_fields(self):
        identifiers = [
            {
                "typeDiscriminator": "ClassifiedId",
                "pureId": 275806459,
                "id": "https://ror.org/01xtthb56",
                "type": {"uri": "/dk/atira/pure/ueoexternalorganisation/ueoexternalorganisationsources/ror_id"},
            }
        ]

        self.assertTrue(
            _has_identifier(
                identifiers,
                "https://ror.org/01xtthb56",
                "/dk/atira/pure/ueoexternalorganisation/ueoexternalorganisationsources/ror_id",
            )
        )

    def test_external_org_address_check_ignores_geopoint_whitespace(self):
        existing_address = {
            "city": "Oslo",
            "country": {"uri": "/dk/atira/pure/core/countries/no"},
            "geoLocation": {"point": "59.91273, 10.74609"},
        }
        desired_address = {
            "city": "Oslo",
            "country": {"uri": "/dk/atira/pure/core/countries/no"},
            "geoLocation": {"point": "59.91273,10.74609"},
        }

        self.assertFalse(address_needs_update(existing_address, desired_address))

    def test_external_org_point_comparison_ignores_tiny_float_precision_difference(self):
        self.assertTrue(
            _points_equal(
                "50.8650016784668, -0.0850000008940696",
                "50.8650016784668,-0.08500000089406967",
            )
        )
        self.assertTrue(
            _points_equal(
                "48.20848846435547, 16.372079849243164",
                "48.20849,16.37208",
            )
        )

    def test_external_org_with_existing_ror_is_not_queued_for_update(self):
        pure_orgs = [
            {
                "uuid": "org-1",
                "name": "Center for Economic and Policy Research",
                "identifiers": [
                    {
                        "id": "https://ror.org/04jzmdh37",
                        "type": {"uri": ROR_ID_URI},
                    }
                ],
            }
        ]
        openalex_orgs = [
            {
                "openalex_id": "https://openalex.org/I1279858714",
                "ror": "https://ror.org/01vqjmx11",
                "display_name": "Center for Economic and Policy Research",
                "display_name_alternatives": [],
                "geo": {"city": "Washington D.C.", "country_code": "US", "country": "United States"},
            }
        ]

        orgs_to_update, orgs_with_ror_in_pure, _no_name_match, _ambiguous = match_organizations(pure_orgs, openalex_orgs)

        self.assertEqual([], orgs_to_update)
        self.assertEqual(1, len(orgs_with_ror_in_pure))

    def test_get_ext_orgdata_pure_preserves_identifier_type_uri_for_ror_checks(self):
        pure_org_data = {
            "results": [
                {
                    "uuid": "org-1",
                    "name": {"en_GB": "Center for Economic and Policy Research"},
                    "identifiers": [
                        {
                            "id": "https://ror.org/04jzmdh37",
                            "type": {
                                "uri": ROR_ID_URI,
                                "term": {"en_GB": "ROR ID"},
                            },
                        }
                    ],
                }
            ]
        }
        openalex_orgs = [
            {
                "openalex_id": "https://openalex.org/I1279858714",
                "ror": "https://ror.org/01vqjmx11",
                "display_name": "Center for Economic and Policy Research",
                "display_name_alternatives": [],
                "geo": {"city": "Washington D.C.", "country_code": "US", "country": "United States"},
            }
        ]

        pure_orgs = get_ext_orgdata_pure(["org-1"], pure_org_data)
        orgs_to_update, orgs_with_ror_in_pure, _no_name_match, _ambiguous = match_organizations(pure_orgs, openalex_orgs)

        self.assertEqual([], orgs_to_update)
        self.assertEqual(1, len(orgs_with_ror_in_pure))
        self.assertEqual(ROR_ID_URI, pure_orgs[0]["identifiers"][0]["type"]["uri"])

    def test_external_org_duplicate_openalex_candidates_do_not_create_ambiguity(self):
        pure_orgs = [{"uuid": "org-1", "name": "Example University", "identifiers": []}]
        openalex_orgs = [
            {
                "openalex_id": "https://openalex.org/I1",
                "ror": "https://ror.org/123",
                "display_name": "Example University",
                "display_name_alternatives": [],
                "geo": {"city": "Utrecht", "country_code": "NL", "country": "Netherlands"},
            },
            {
                "openalex_id": "https://openalex.org/I1",
                "ror": "https://ror.org/123",
                "display_name": "Example University",
                "display_name_alternatives": ["Example Univ"],
                "geo": {"city": "Utrecht", "country_code": "NL", "country": "Netherlands"},
            },
        ]

        orgs_to_update, _orgs_with_ror_in_pure, _no_name_match, ambiguous = match_organizations(pure_orgs, openalex_orgs)

        self.assertEqual([], ambiguous)
        self.assertEqual(1, len(orgs_to_update))

    def test_external_org_main_uses_local_snapshot_without_live_openalex_calls(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(os.environ, {"BTP_OUTPUT_DIR": tmpdir}):
            with patch.object(external_orgs, "select_faculties", return_value=["uu faculty: test|organization_name"]), patch.object(
                external_orgs.enrich,
                "select_persons_researchoutput",
                return_value=[],
            ) as select_outputs, patch.object(
                external_orgs.enrich,
                "fetch_pure_researchoutputs",
                return_value={"results": [], "by_doi": {}, "by_uuid": {}},
            ), patch.object(
                external_orgs,
                "load_openalex_institutions_lookup",
                return_value={},
            ), patch.object(external_orgs.enrich, "fetch_openalex_works") as fetch_openalex_works, patch.object(
                external_orgs,
                "fetch_pure_extorgs",
                return_value={"results": []},
            ):
                external_orgs.main("all", "yes")

            select_outputs.assert_called_once()
            fetch_openalex_works.assert_not_called()
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "external_orgs_to_update.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "external_orgs_updates.json")))

    def test_openalex_institution_snapshot_compacts_records_by_ror(self):
        record = {
            "id": "https://openalex.org/I1",
            "display_name": "Example University",
            "display_name_alternatives": ["Example Univ"],
            "country_code": "NL",
            "ids": {
                "openalex": "https://openalex.org/I1",
                "ror": "https://ror.org/123",
                "wikidata": "https://www.wikidata.org/wiki/Q1",
            },
            "geo": {"city": "Utrecht", "country_code": "NL"},
        }

        ror, compact = snapshot_openalex_institutions.compact_institution(record)

        self.assertEqual("https://ror.org/123", ror)
        self.assertEqual("Example University", compact["display_name"])
        self.assertEqual("https://openalex.org/I1", compact["ids"]["openalex"])
        self.assertEqual({"city": "Utrecht", "country_code": "NL"}, compact["geo"])

    def test_external_org_uses_dedicated_snapshot_lookup_path(self):
        self.assertEqual(
            "output/openalex_cache/openalex_institutions_snapshot_by_ror.json",
            external_orgs.OPENALEX_INSTITUTIONS_LOOKUP_PATH,
        )
        self.assertEqual(
            snapshot_openalex_institutions.DEFAULT_OUTPUT_PATH.as_posix(),
            external_orgs.OPENALEX_INSTITUTIONS_LOOKUP_PATH,
        )

    def test_openalex_s3_listing_parser_extracts_gz_keys_and_continuation(self):
        payload = b"""<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <IsTruncated>true</IsTruncated>
            <Contents>
                <Key>data/institutions/updated_date=2026-05-01/part_000.gz</Key>
                <Size>123</Size>
            </Contents>
            <Contents>
                <Key>data/institutions/manifest</Key>
                <Size>10</Size>
            </Contents>
            <NextContinuationToken>token-1</NextContinuationToken>
        </ListBucketResult>"""

        keys, next_token = snapshot_openalex_institutions.parse_s3_listing(payload)

        self.assertEqual(
            [
                {"key": "data/institutions/updated_date=2026-05-01/part_000.gz", "size": 123},
                {"key": "data/institutions/manifest", "size": 10},
            ],
            keys,
        )
        self.assertEqual("token-1", next_token)

    def test_openalex_snapshot_downloader_skips_unchanged_files(self):
        item = {
            "key": "data/institutions/updated_date=2026-05-01/part_000.gz",
            "size": 4,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_file = os.path.join(tmpdir, "updated_date=2026-05-01", "part_000.gz")
            os.makedirs(os.path.dirname(snapshot_file), exist_ok=True)
            with open(snapshot_file, "wb") as handle:
                handle.write(b"same")

            with patch.object(snapshot_openalex_institutions, "list_openalex_institution_objects", return_value=[item]), patch.object(
                snapshot_openalex_institutions, "download_file"
            ) as download_file:
                snapshot_openalex_institutions.download_institutions_snapshot(tmpdir)

            download_file.assert_not_called()

    def test_external_orgdata_enriches_ricgraph_org_names_from_snapshot(self):
        institution_snapshot = {
            "https://ror.org/123": {
                "id": "https://openalex.org/I1",
                "display_name": "Snapshot University",
                "display_name_alternatives": ["Snapshot Univ"],
                "ids": {"openalex": "https://openalex.org/I1", "ror": "https://ror.org/123"},
                "geo": {"city": "Utrecht"},
            }
        }
        name_index = external_orgs.build_institution_name_index(institution_snapshot)

        orgs = external_orgs.get_ext_orgdata_from_ricgraph(
            [{"_key": "snapshot university|organization_name", "value": "Snapshot University"}],
            name_index,
        )

        self.assertEqual(1, len(orgs))
        self.assertEqual("Snapshot University", orgs[0]["display_name"])
        self.assertEqual("https://ror.org/123", orgs[0]["ror"])

    def test_external_org_collects_article_orgs_from_ricgraph_person_organisations(self):
        researchoutputs = [
            {
                "doi": "10.123/alpha",
                "pure_uuid": "output-1",
                "researchoutput_key": "10.123/alpha|doi",
            }
        ]
        purejsons = {
            "by_uuid": {
                "output-1": {
                    "contributors": [
                        {"externalOrganizations": [{"uuid": "org-1"}]},
                    ],
                }
            }
        }

        with patch.object(
            external_orgs.enrich,
            "fetch_ricgraph_persons_for_output",
            return_value=[{"_key": "person-1|person-root"}],
        ), patch.object(
            external_orgs,
            "fetch_ricgraph_organization_neighbors",
            return_value=[{"_key": "snapshot university|organization_name", "value": "Snapshot University"}],
        ):
            article_orgs, uuid_groups = external_orgs.collect_ricgraph_article_orgs(researchoutputs, purejsons)

        self.assertEqual(["org-1"], uuid_groups[0])
        self.assertEqual("Snapshot University", article_orgs[0]["ricgraph_organizations"][0]["value"])

    def test_update_persons_writes_selected_and_no_new_id_files(self):
        person_df = pd.DataFrame(
            [
                {
                    "FULL_NAME": "Alpha",
                    "person_id": "person-1",
                    "PURE_UUID_PERS": "uuid-1",
                    "ORCID": "0000-0001",
                    "SCOPUS_AUTHOR_ID": pd.NA,
                },
                {
                    "FULL_NAME": "Beta",
                    "person_id": "person-2",
                    "PURE_UUID_PERS": "uuid-2",
                    "ORCID": pd.NA,
                    "SCOPUS_AUTHOR_ID": pd.NA,
                },
            ]
        )
        datatotal = [
            {"uuid": "uuid-1", "identifiers": []},
            {"uuid": "uuid-2", "identifiers": []},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                update_persons(person_df, datatotal)

                output_dir = os.path.join("output", "internal_persons")
                selected_matches = [
                    name for name in os.listdir(output_dir) if name.startswith("personstobeupdated_") and name.endswith(".csv")
                ]
                skipped_matches = [
                    name for name in os.listdir(output_dir) if name.startswith("persons_without_newids_") and name.endswith(".csv")
                ]
                self.assertEqual(1, len(selected_matches))
                self.assertEqual(1, len(skipped_matches))
                selected_path = os.path.join(output_dir, selected_matches[0])
                skipped_path = os.path.join(output_dir, skipped_matches[0])

                self.assertTrue(os.path.exists(selected_path))
                self.assertTrue(os.path.exists(skipped_path))

                selected = pd.read_csv(selected_path)
                skipped = pd.read_csv(skipped_path)

                self.assertEqual(["uuid-1"], selected["PURE_UUID_PERS"].dropna().unique().tolist())
                self.assertEqual(["uuid-2"], skipped["PURE_UUID_PERS"].dropna().unique().tolist())
            finally:
                os.chdir(previous_cwd)


if __name__ == "__main__":
    unittest.main()
