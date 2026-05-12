import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from enrich_pure_external_persons import (
    REQUEST_TIMEOUT as EXTERNAL_REQUEST_TIMEOUT,
    extract_openalex_id,
    extract_orcid_id,
    select_researchoutputs,
)
from enrich_pure_external_orgs import (
    address_needs_update,
    dedupe_records_by_uuid,
    get_ext_orgdata_pure,
    match_organizations,
    _has_identifier,
    _points_equal,
)
from config import ROR_ID_URI
from enrich_internal_persons_with_ids import (
    REQUEST_TIMEOUT,
    _normalize_identifier,
    check_new_ids,
    _resolve_pure_person_uuid_column,
    fetch_person_ids,
    fetch_personroots,
    update_persons,
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
    def test_external_select_researchoutputs_uses_timeout_and_raise_for_status(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"results": [{"_key": "doi-1"}]}
        mock_get.return_value = response

        result = select_researchoutputs("person-root-1")

        self.assertEqual([{"_key": "doi-1"}], result)
        self.assertEqual(EXTERNAL_REQUEST_TIMEOUT, mock_get.call_args.kwargs["timeout"])
        response.raise_for_status.assert_called_once()

    def test_external_identifier_normalization_strips_query_suffixes(self):
        self.assertEqual("0000-0003-2472-6589", extract_orcid_id("https://orcid.org/0000-0003-2472-6589?lang=en"))
        self.assertEqual("A1234567890", extract_openalex_id("https://openalex.org/A1234567890?foo=bar"))

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
