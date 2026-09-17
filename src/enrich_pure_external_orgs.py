# ########################################################################
# Script: enrich_pure_external_orgs.py
#
# Description:
# This script enriches external organization records in Pure using data from
# Ricgraph and OpenAlex. The main task is to ensure that external organizations
# in Pure have up-to-date information, such as ROR IDs and geographic data.
#
# The script includes:
# - Fetching research outputs and associated organizations.
# - Matching organizations between Pure and OpenAlex.
# - Updating external organizations in Pure with ROR IDs if missing.
#
# Important:
# This script relies on external APIs (Pure and OpenAlex) and should be run
# with necessary configurations in place.
# This script is meant to be invoked by the BackToPure web interface.
#
# Dependencies:
# - requests, pandas, json, logging, urllib3, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################

import time
import csv
import re
import difflib
import unicodedata
import pandas as pd
import logging
from logging_config import setup_logging
import requests
import enrich_pure_external_persons as enrich
import json
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import os
from config import (
    FACULTY_PREFIX,
    OPENALEX_HEADERS,
    ORCID_ID_URI,
    PURE_API_KEY,
    PURE_BASE_URL,
    PURE_HEADERS,
    RIC_BASE_URL,
    ROR_ID_URI,
)

logger = setup_logging('btp', level=logging.INFO)


headers = {
    'Accept': 'application/json',
    'api-key': PURE_API_KEY,
}
# Set up a single session for all requests
session = requests.Session()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "PUT", "POST"],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FUZZY_MATCH_THRESHOLD = 0.88
AMBIGUOUS_MATCH_GAP = 0.02
OPENALEX_INSTITUTIONS_LOOKUP_PATH = "output/openalex_cache/openalex_institutions_snapshot_by_ror.json"
EXTERNAL_ORG_UPDATE_COLUMNS = [
    "to_be_updated",
    "updated",
    "uuid",
    "pure_name",
    "needs_ror_update",
    "needs_geo_update",
    "ror",
    "openalex_id",
    "matched_openalex_name",
    "openalex_display_name",
    "match_type",
    "match_score",
    "country",
    "country_code",
    "region",
    "city",
    "latitude",
    "longitude",
    "geonames_city_id",
    "spatial_point",
    "spatial_point_latitude",
    "spatial_point_longitude",
    "pure_address_field",
    "pure_geo_point",
]


def resolve_output_dir():
    return os.environ.get("BTP_OUTPUT_DIR", "output/external_orgs")


def resolve_openalex_institutions_lookup_path():
    return os.environ.get("BTP_OPENALEX_INSTITUTIONS_LOOKUP", OPENALEX_INSTITUTIONS_LOOKUP_PATH)


def write_external_org_outputs(rows_to_update, json_updates, ambiguous_matches, no_name_matches):
    output_dir = resolve_output_dir()
    os.makedirs(output_dir, exist_ok=True)

    deduped_rows = dedupe_records_by_uuid(rows_to_update, uuid_keys=("uuid",))
    deduped_json_updates = dedupe_records_by_uuid(json_updates)
    duplicate_count = len(rows_to_update) - len(deduped_rows)
    if duplicate_count > 0:
        logger.info(f"Collapsed {duplicate_count} duplicate external org proposal row(s) to unique organisation UUIDs")

    df = pd.DataFrame(deduped_rows, columns=EXTERNAL_ORG_UPDATE_COLUMNS)
    df.to_csv(os.path.join(output_dir, "external_orgs_to_update.csv"), index=False)

    with open(os.path.join(output_dir, "external_orgs_updates.json"), 'w') as json_file:
        json.dump(deduped_json_updates, json_file, indent=4)

    with open(os.path.join(output_dir, "external_orgs_ambiguous_matches.json"), 'w') as json_file:
        json.dump(ambiguous_matches, json_file, indent=4)

    with open(os.path.join(output_dir, "external_orgs_no_name_match.json"), 'w') as json_file:
        json.dump(no_name_matches, json_file, indent=4)

    with open(os.path.join(output_dir, 'output.csv'), mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows([])

    return deduped_rows, deduped_json_updates


def normalize_ror(value):
    if not value:
        return None
    normalized = str(value).strip()
    return normalized or None


def load_json_file(path, default):
    if not os.path.exists(path):
        logger.warning(f"Lookup file not found: {path}")
        return default
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"Could not read lookup file {path}: {exc}")
        return default
    return payload


def load_openalex_institutions_lookup():
    lookup_path = resolve_openalex_institutions_lookup_path()
    if not os.path.exists(lookup_path):
        raise FileNotFoundError(
            f"OpenAlex institution snapshot lookup is missing: {lookup_path}. "
            "Run `.venv/bin/python src/snapshot_openalex_institutions.py --download` once "
            "to download and compact the OpenAlex institutions snapshot, then rerun this job. "
            "Use BTP_OPENALEX_INSTITUTIONS_LOOKUP to point to a different compact snapshot file."
        )
    by_ror = load_json_file(lookup_path, {})
    if not isinstance(by_ror, dict):
        by_ror = {}
    logger.info(f"Loaded {len(by_ror)} local OpenAlex institution snapshot record(s)")
    return by_ror


def build_institution_name_index(institution_snapshot):
    index = {}
    for record in institution_snapshot.values():
        if not isinstance(record, dict):
            continue
        names = [record.get("display_name")]
        names.extend(record.get("display_name_alternatives") or [])
        names.extend(record.get("display_name_acronyms") or [])
        for name in names:
            normalized = normalize_org_name(name)
            if normalized and record not in index.setdefault(normalized, []):
                index[normalized].append(record)
    logger.info(f"Built local institution name index with {len(index)} name key(s)")
    return index


def snapshot_record_to_org(record, fallback_name=None):
    record = record or {}
    ids = record.get("ids") if isinstance(record.get("ids"), dict) else {}
    return {
        "openalex_id": ids.get("openalex") or record.get("id"),
        "ror": ids.get("ror") or record.get("ror"),
        "display_name": record.get("display_name") or fallback_name,
        "display_name_alternatives": record.get("display_name_alternatives") or [],
        "geo": record.get("geo") or {},
    }


def get_ext_orgdata_from_ricgraph(ricgraph_orgs, institution_name_index):
    organization_details = []
    seen_names = set()
    for org in ricgraph_orgs:
        org_name = org.get("value") or org.get("display_name")
        normalized_name = normalize_org_name(org_name)
        if not normalized_name or normalized_name in seen_names:
            continue
        seen_names.add(normalized_name)
        snapshot_records = institution_name_index.get(normalized_name) or []
        if snapshot_records:
            for record in snapshot_records:
                organization_details.append(snapshot_record_to_org(record, fallback_name=org_name))
        else:
            organization_details.append(
                {
                    "openalex_id": None,
                    "ror": None,
                    "display_name": org_name,
                    "display_name_alternatives": [],
                    "geo": {},
                }
            )
    return organization_details


def dedupe_records_by_uuid(records, uuid_keys=("uuid", "UUID")):
    deduped = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        record_uuid = None
        for key in uuid_keys:
            value = record.get(key)
            if value:
                record_uuid = str(value)
                break
        if not record_uuid:
            continue
        deduped[record_uuid] = record
    return list(deduped.values())


def flatten_external_org_uuid_groups(uuid_groups):
    flat_uuids = []
    for item in uuid_groups:
        if isinstance(item, (list, tuple, set)):
            flat_uuids.extend(str(uuid) for uuid in item if uuid)
        elif item:
            flat_uuids.append(str(item))
    return list(dict.fromkeys(flat_uuids))


def normalize_org_name(name):
    if not name:
        return ""

    normalized = unicodedata.normalize("NFKD", str(name))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.lower().strip()
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"\bdept\b", "department", normalized)
    normalized = re.sub(r"\binst\b", "institute", normalized)
    normalized = re.sub(r"\buniv\b", "university", normalized)
    normalized = normalized.replace("centre", "center")
    normalized = re.sub(r"[^\w\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def build_geo_summary(geo):
    geo = geo or {}
    spatial_point = geo.get("spatial_point") or {}
    return {
        "country": geo.get("country"),
        "country_code": geo.get("country_code"),
        "region": geo.get("region"),
        "city": geo.get("city"),
        "latitude": geo.get("latitude"),
        "longitude": geo.get("longitude"),
        "geonames_city_id": geo.get("geonames_city_id"),
        "spatial_point": spatial_point,
        "spatial_point_latitude": spatial_point.get("latitude"),
        "spatial_point_longitude": spatial_point.get("longitude"),
    }


def _has_geo_payload(geo_summary):
    geo_summary = geo_summary or {}
    return any([
        geo_summary.get("city"),
        geo_summary.get("country_code"),
        geo_summary.get("country"),
        geo_summary.get("spatial_point_latitude") is not None and geo_summary.get("spatial_point_longitude") is not None,
        geo_summary.get("latitude") is not None and geo_summary.get("longitude") is not None,
    ])


def build_pure_geopoint(geo_summary):
    geo_summary = geo_summary or {}
    latitude = geo_summary.get("spatial_point_latitude")
    longitude = geo_summary.get("spatial_point_longitude")
    if latitude is None or longitude is None:
        latitude = geo_summary.get("latitude")
        longitude = geo_summary.get("longitude")
    if latitude is None or longitude is None:
        return None
    return f"{latitude},{longitude}"


def _normalize_point(point):
    if point is None:
        return None
    return ",".join(part.strip() for part in str(point).split(","))


def _points_equal(left_point, right_point, tolerance=1e-5):
    left = _normalize_point(left_point)
    right = _normalize_point(right_point)
    if left == right:
        return True
    if not left or not right:
        return False
    try:
        left_lat, left_lon = (float(part) for part in left.split(",", 1))
        right_lat, right_lon = (float(part) for part in right.split(",", 1))
    except (TypeError, ValueError):
        return False
    return abs(left_lat - right_lat) <= tolerance and abs(left_lon - right_lon) <= tolerance


def _has_identifier(identifiers, identifier_id, identifier_uri):
    for identifier in identifiers or []:
        if not isinstance(identifier, dict):
            continue
        if identifier.get("id") == identifier_id and ((identifier.get("type") or {}).get("uri")) == identifier_uri:
            return True
    return False


def build_pure_country_ref(geo_summary):
    geo_summary = geo_summary or {}
    country_code = geo_summary.get("country_code")
    country_name = geo_summary.get("country")
    if not country_code and not country_name:
        return None

    country_ref = {}
    if country_code:
        country_ref["uri"] = f"/dk/atira/pure/core/countries/{str(country_code).lower()}"
    if country_name:
        country_ref["term"] = {"en_GB": country_name}
    return country_ref or None


def build_pure_address_payload(geo_summary, existing_address=None):
    geo_summary = geo_summary or {}
    existing_address = existing_address if isinstance(existing_address, dict) else {}
    address = dict(existing_address)

    city = geo_summary.get("city")
    if city and not address.get("city"):
        address["city"] = city

    country_ref = build_pure_country_ref(geo_summary)
    if country_ref and not address.get("country"):
        address["country"] = country_ref

    point = build_pure_geopoint(geo_summary)
    if point and not (address.get("geoLocation") or {}).get("point"):
        geo_location = dict(address.get("geoLocation") or {})
        geo_location["point"] = point
        address["geoLocation"] = geo_location

    return address if address else None


def address_needs_update(existing_address, desired_address):
    if not desired_address:
        return False

    existing_address = existing_address if isinstance(existing_address, dict) else {}
    existing_city = existing_address.get("city")
    desired_city = desired_address.get("city")
    if desired_city and existing_city != desired_city:
        return True

    existing_country_uri = (existing_address.get("country") or {}).get("uri")
    desired_country_uri = (desired_address.get("country") or {}).get("uri")
    if desired_country_uri and existing_country_uri != desired_country_uri:
        return True

    existing_point = (existing_address.get("geoLocation") or {}).get("point")
    desired_point = (desired_address.get("geoLocation") or {}).get("point")
    if desired_point and not _points_equal(existing_point, desired_point):
        return True

    return False


def score_openalex_candidate(pure_name, openalex_org):
    pure_normalized = normalize_org_name(pure_name)
    primary_name = openalex_org.get("display_name") or ""
    primary_normalized = normalize_org_name(primary_name)
    alternative_names = openalex_org.get("display_name_alternatives", []) or []

    candidates = [(primary_name, primary_normalized, "display_name")]
    candidates.extend(
        (alt_name, normalize_org_name(alt_name), "display_name_alternative")
        for alt_name in alternative_names
    )

    best_score = 0.0
    best_name = ""
    best_match_type = ""

    for candidate_name, candidate_normalized, match_source in candidates:
        if not candidate_normalized:
            continue
        if pure_normalized == candidate_normalized:
            score = 1.0 if match_source == "display_name" else 0.95
            match_type = f"exact_{match_source}"
        else:
            score = difflib.SequenceMatcher(None, pure_normalized, candidate_normalized).ratio()
            match_type = f"fuzzy_{match_source}"

        if score > best_score:
            best_score = score
            best_name = candidate_name
            best_match_type = match_type

    return {
        "score": best_score,
        "matched_name": best_name,
        "match_type": best_match_type,
        "pure_normalized_name": pure_normalized,
    }


def _openalex_candidate_identity(scored_candidate):
    openalex_org = scored_candidate.get("openalex_org") or {}
    return (
        openalex_org.get("ror")
        or openalex_org.get("openalex_id")
        or normalize_org_name(openalex_org.get("display_name"))
    )


def dedupe_scored_candidates(scored_candidates):
    deduped = {}
    for candidate in scored_candidates:
        identity = _openalex_candidate_identity(candidate)
        if not identity:
            continue
        previous = deduped.get(identity)
        if previous is None or candidate["score"] > previous["score"]:
            deduped[identity] = candidate
    return sorted(deduped.values(), key=lambda item: item["score"], reverse=True)


def match_organizations(pure_orgs, openalex_orgs):
    orgs_to_update = []
    orgs_with_ror_in_pure = []
    orgs_with_no_name_match = []
    orgs_with_ambiguous_match = []

    for pure_org in pure_orgs:
        scored_candidates = []
        for openalex_org in openalex_orgs:
            candidate_score = score_openalex_candidate(pure_org['name'], openalex_org)
            if candidate_score["score"] >= FUZZY_MATCH_THRESHOLD:
                scored_candidates.append({
                    "openalex_org": openalex_org,
                    **candidate_score,
                })

        scored_candidates = dedupe_scored_candidates(scored_candidates)

        if not scored_candidates:
            orgs_with_no_name_match.append(pure_org)
            logger.info(f"No match found for Pure org '{pure_org['name']}' in OpenAlex organizations.")
            continue

        best_candidate = scored_candidates[0]
        runner_up = scored_candidates[1] if len(scored_candidates) > 1 else None
        if runner_up and best_candidate["score"] - runner_up["score"] <= AMBIGUOUS_MATCH_GAP:
            ambiguous_match = {
                "uuid": pure_org["uuid"],
                "name": pure_org["name"],
                "match_score": round(best_candidate["score"], 4),
                "matched_openalex_name": best_candidate["matched_name"],
                "match_type": best_candidate["match_type"],
                "candidate_names": [
                    best_candidate["openalex_org"].get("display_name"),
                    runner_up["openalex_org"].get("display_name"),
                ],
            }
            orgs_with_ambiguous_match.append(ambiguous_match)
            logger.warning(
                f"Ambiguous OpenAlex match for Pure org '{pure_org['name']}': "
                f"{ambiguous_match['candidate_names']}"
            )
            continue

        openalex_org = best_candidate["openalex_org"]
        logger.debug(
            f"Matched Pure '{pure_org['name']}' to OpenAlex '{best_candidate['matched_name']}' "
            f"with {best_candidate['match_type']} score={best_candidate['score']:.4f}"
        )

        pure_ror_ids = {
            identifier.get('id')
            for identifier in pure_org.get('identifiers', [])
            if isinstance(identifier, dict)
            and ((identifier.get('type') or {}).get('uri')) == ROR_ID_URI
            and identifier.get('id')
        }

        match_metadata = {
            'uuid': pure_org['uuid'],
            'pure_name': pure_org['name'],
            'openalex_id': openalex_org['openalex_id'],
            'matched_openalex_name': best_candidate['matched_name'],
            'openalex_display_name': openalex_org.get('display_name'),
            'match_type': best_candidate['match_type'],
            'match_score': round(best_candidate['score'], 4),
            'ror': openalex_org['ror'],
            'geo': openalex_org['geo'],
            'geo_summary': build_geo_summary(openalex_org['geo']),
        }

        pure_has_ror = bool(pure_ror_ids)
        if pure_has_ror:
            org_with_ror = dict(pure_org)
            org_with_ror.update(match_metadata)
            orgs_with_ror_in_pure.append(org_with_ror)
            logger.info(
                f"Pure org '{pure_org['name']}' already has a ROR in Pure; skipping candidate ROR {openalex_org['ror']} "
                f"(match={best_candidate['match_type']}, score={best_candidate['score']:.4f})"
            )
        elif _has_geo_payload(match_metadata['geo_summary']):
            orgs_to_update.append(match_metadata)
            logger.info(
                f"Prepared update proposal for Pure org '{pure_org['name']}' with ROR {openalex_org['ror']} "
                f"(match={best_candidate['match_type']}, score={best_candidate['score']:.4f})"
            )

    orgs_with_ror_in_pure = list({org['uuid']: org for org in orgs_with_ror_in_pure}.values())

    return orgs_to_update, orgs_with_ror_in_pure, orgs_with_no_name_match, orgs_with_ambiguous_match

def identifier_exists(identifiers, new_id, id_type_uri):

    for identifier in identifiers:
        if 'type' in identifier and identifier['type']['uri'] == id_type_uri and identifier['id'] == new_id:
            return True
    return False
def update_externalorg_pure(orgs, test_choice, update):
    inpure = False
    # Initialize a list to store rows for the DataFrame
    rows_to_update = []

    # Initialize a list to store JSON objects
    json_updates = []

    for row in orgs:
        url = PURE_BASE_URL + 'external-organizations/' + row['uuid']
        try:
            response = session.get(url, headers=headers, verify=False, timeout=30)
        except requests.exceptions.RequestException as exc:
            logger.error(f"Failed to fetch external organization {row['uuid']}: {exc}")
            continue
        logging.debug(f"get org data {row['uuid']}. responsecode = {response.status_code}")
        if response.status_code != 200:
            logger.error(f"Failed to fetch external organization {row['uuid']}: status {response.status_code}")
            continue
        try:
            data = response.json()  # Parse JSON response
        except ValueError:
            logger.error(f"Invalid JSON response while fetching external organization {row['uuid']}")
            continue
        address_field = 'contactAddress' if 'contactAddress' in data else 'address'
        existing_address = data.get(address_field) or {}
        desired_address = build_pure_address_payload(row.get('geo_summary'), existing_address)
        needs_geo_update = address_needs_update(existing_address, desired_address)
        new_ror = None

        if row['ror']:
            new_ror = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['ror'],
                "type": {
                    "uri": ROR_ID_URI,
                    "term": {
                        "en_GB": "ROR ID"
                    }
                }
            }

        if 'identifiers' not in data:
            data['identifiers'] = []

        needs_ror_update = bool(new_ror and not _has_identifier(data['identifiers'], new_ror['id'], ROR_ID_URI))

        if needs_ror_update or needs_geo_update:
            auto_select = str(row.get('match_type') or '').startswith('exact_')
            # Mark as "to be updated"
            rows_to_update.append({
                'to_be_updated': 'X' if auto_select else '',
                'updated': ' ',
                'uuid': row['uuid'],
                'pure_name': row.get('pure_name'),
                'needs_ror_update': needs_ror_update,
                'needs_geo_update': needs_geo_update,
                'ror': row['ror'],
                'openalex_id': row.get('openalex_id'),
                'matched_openalex_name': row.get('matched_openalex_name'),
                'openalex_display_name': row.get('openalex_display_name'),
                'match_type': row.get('match_type'),
                'match_score': row.get('match_score'),
                'country': row.get('geo_summary', {}).get('country'),
                'country_code': row.get('geo_summary', {}).get('country_code'),
                'region': row.get('geo_summary', {}).get('region'),
                'city': row.get('geo_summary', {}).get('city'),
                'latitude': row.get('geo_summary', {}).get('latitude'),
                'longitude': row.get('geo_summary', {}).get('longitude'),
                'geonames_city_id': row.get('geo_summary', {}).get('geonames_city_id'),
                'spatial_point': json.dumps(row.get('geo_summary', {}).get('spatial_point') or {}),
                'spatial_point_latitude': row.get('geo_summary', {}).get('spatial_point_latitude'),
                'spatial_point_longitude': row.get('geo_summary', {}).get('spatial_point_longitude'),
                'pure_address_field': address_field,
                'pure_geo_point': (desired_address or {}).get('geoLocation', {}).get('point'),
            })
            # Add the JSON to the big JSON list
            if needs_ror_update:
                data['identifiers'].append(new_ror)
            if needs_geo_update and desired_address:
                data[address_field] = desired_address
            data['_btp_match_metadata'] = {
                'pure_name': row.get('pure_name'),
                'openalex_id': row.get('openalex_id'),
                'matched_openalex_name': row.get('matched_openalex_name'),
                'openalex_display_name': row.get('openalex_display_name'),
                'match_type': row.get('match_type'),
                'match_score': row.get('match_score'),
                'geo': row.get('geo'),
                'geo_summary': row.get('geo_summary'),
                'pure_address_field': address_field,
                'needs_ror_update': needs_ror_update,
                'needs_geo_update': needs_geo_update,
                'previous_address': existing_address,
                'pure_address_payload': desired_address,
            }
            json_updates.append(data)
        else:
            inpure = True


    # for row in orgs:
    #     session = requests.Session()
    #     session.mount("https://", adapter)
    #
    #     headers = {
    #         'Accept': 'application/json',
    #         'api-key': PURE_API_KEY,
    #     }
    #
    #     url = PURE_BASE_URL +  'external-organizations/' + row['uuid']
    #     response = session.get(url, headers=headers, verify=False)
    #     logging.debug(f"get org data {row['uuid']}. responsecode = {response.status_code}")
    #     data = response.json()  # Directly parse JSON response
    #     new_ror = None
    #     if row['ror']:
    #         new_ror = {
    #             "typeDiscriminator": "ClassifiedId",
    #             "id": row['ror'],
    #             "type": {
    #                 "uri": ROR_ID_URI,
    #                 "term": {
    #                     "en_GB": "ROR ID"
    #                 }
    #             }
    #         }
    #
    #     if 'identifiers' not in data:
    #         data['identifiers'] = []
    #     # Add the new ror if it does not already exist and the ID is not empty
    #     if new_ror:
    #         data['identifiers'].append(new_ror)
    #         logger.debug(f"update of uuid {row['uuid']}, ror, {new_ror}")
    #         update += 1
    #         if test_choice == 'no':
    #             response = session.put(url, headers=headers, json=data, verify=False)
    #             if response.status_code != 200:
    #                 logger.debug(f"Failed to update data for UUID {row['uuid']}: {response.text}")
    #             else:
    #                 logger.debug(f"Successfully updated data for UUID {row['uuid']}, ror, {new_ror}")
    #             session.close()


    return update, inpure, rows_to_update, json_updates



def select_faculties(faculty_choice, test_choice):
    logging.info(f"start fetching person-roots for {faculty_choice}")
    logging.info(f"Test run =  {test_choice}")
    params = {
        'value': FACULTY_PREFIX,
    }
    url = RIC_BASE_URL + 'organization/search'
    response = requests.get(url, params=params)
    data = response.json()
    if faculty_choice.lower() == 'all':
        selected_faculties = [item['_key'] for item in data["results"] if enrich.is_primary_faculty_key(item.get('_key'))]
    else:
        selected_faculties = [faculty_choice] if enrich.is_primary_faculty_key(faculty_choice) else []

    return selected_faculties

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '0'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = requests.get(url, params=params)
        # response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []


def fetch_ricgraph_organization_neighbors(personroot_key):
    try:
        params = {'key': personroot_key, 'category_want': 'organization', 'max_nr_items': '0'}
        response = session.get(RIC_BASE_URL + 'get_all_neighbor_nodes', params=params, timeout=30)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching organization neighbors for person-root {personroot_key}: {e}")
        return []


def select_researchoutputs(persoonroot_key):
    """Fetch person IDs for a given person-ro    ot."""
    try:
        params = {'key': persoonroot_key, 'category_want': 'journal article'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = requests.get(url, params=params)

        # response.raise_for_status()

        return response.json().get("results", [])

    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []


def select_persons_researchoutput(selected_faculties):
    persons = []
    new_data = []

    for faculty in selected_faculties:
        logging.info(f"Processing faculty: {faculty}")
        personroots = fetch_personroots(faculty)
        for personroot in personroots:
            if not personroot['_key'] == None:
                personroot_key = personroot['_key']
                outputs = select_researchoutputs(personroot_key)
                for output in outputs:
                    doi = output["_key"].split("|")[0]

                    new_data.append(doi)
                    # print(doi)
                    # if 'Pure-uu' in output["_source"] and 'OpenAlex-uu' in output["_source"]:
                    #     new_data.append(doi)
                    #
                    # else:
                    #     print(output["_key"], 'not in both systems')


    num_elements = len(new_data)
    logging.info(f"total research output with external persons selected:  {num_elements}")
    return new_data


def extract_external_organization_uuids(pure_article):
    external_organization_uuids = set()
    if not pure_article:
        return []

    for contributor in pure_article.get('contributors', []) or []:
        for ext_org in contributor.get('externalOrganizations', []) or []:
            uuid = ext_org.get('uuid')
            if uuid:
                external_organization_uuids.add(uuid)

    for ext_org in pure_article.get('externalOrganizations', []) or []:
        uuid = ext_org.get('uuid')
        if uuid:
            external_organization_uuids.add(uuid)

    return list(external_organization_uuids)


def collect_ricgraph_article_orgs(researchoutputs, purejsons):
    article_orgs = []
    all_external_org_uuid_groups = []
    organization_cache = {}
    total_person_nodes = 0
    total_org_nodes = 0

    for idx, output in enumerate(researchoutputs, start=1):
        doi = output.get("doi")
        pure_uuid = output.get("pure_uuid")
        researchoutput_key = output.get("researchoutput_key") or (f"{doi}|doi" if doi else "")
        pure_article = (
            enrich.get_ro_from_pure_uuid(pure_uuid, purejsons)
            if pure_uuid
            else enrich.get_ro_from_pure(doi, purejsons)
        )
        external_organization_uuids = extract_external_organization_uuids(pure_article)
        if not researchoutput_key or not external_organization_uuids:
            continue

        person_nodes = enrich.fetch_ricgraph_persons_for_output(researchoutput_key)
        total_person_nodes += len(person_nodes)
        orgs_by_key = {}
        for person_node in person_nodes:
            person_key = person_node.get("_key")
            if not person_key:
                continue
            if person_key not in organization_cache:
                organization_cache[person_key] = fetch_ricgraph_organization_neighbors(person_key)
            for org_node in organization_cache[person_key]:
                org_key = org_node.get("_key")
                if org_key:
                    orgs_by_key[org_key] = org_node

        total_org_nodes += len(orgs_by_key)
        article_orgs.append(
            {
                "doi": doi,
                "pure_uuid": pure_uuid,
                "external_organization_uuids": external_organization_uuids,
                "ricgraph_organizations": list(orgs_by_key.values()),
            }
        )
        all_external_org_uuid_groups.append(external_organization_uuids)

        if idx % 500 == 0 or idx == len(researchoutputs):
            logger.info(
                f"Ricgraph organisation harvest progress: {idx}/{len(researchoutputs)} output(s), "
                f"{total_person_nodes} person node(s), {total_org_nodes} organisation node link(s)"
            )

    logger.info(
        f"Collected Ricgraph organisations for {len(article_orgs)} publication(s), "
        f"{total_person_nodes} person node(s), {total_org_nodes} organisation node link(s)"
    )
    return article_orgs, all_external_org_uuid_groups

def fetch_pure_extorgs(uuids):
    logger.info(f"start fetching external orgs from pure")
    url = PURE_BASE_URL + 'external-organizations/search'

    # Function to split the list into batches of size n
    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    batch_size = 10

    flat_uuids = flatten_external_org_uuid_groups(uuids)
    if not flat_uuids:
        logger.info("No external organization UUIDs found to fetch from Pure")
        return {"results": []}
    batches = list(split_into_batches(flat_uuids, batch_size))

    # Initialize an empty list to hold all the research outputs
    all_orgs = []
    total_dois = set()  # Initialize a set to hold all DOIs
    # Optional: set a delay between requests to avoid hitting rate limits
    request_delay = 0.1  # seconds
    total_items = 0
    # Loop over each batch and make a request
    for batch_index, batch in enumerate(batches):
        pipe_separated_uuids = "|".join(batch)
        logger.info(
            f"Finding ext orgs for batch {batch_index + 1}/{len(batches)}, {len(batch)} UUID(s).")
        json_data = {
            'size': 100,  # Set size to batch size
            'searchString': pipe_separated_uuids,
        }
        try:
            # Make the API request using the pre-configured session
            response = session.post(
                url,
                headers=headers,
                json=json_data,
                timeout=100
            )
            response.raise_for_status()  # Raises an HTTPError for bad responses

            # Parse the response JSON
            data = response.json()
            returned_items = data.get('count')
            logger.debug(f"Total items found for batch {batch_index + 1}: {data.get('count', 0)}")
            total_items += returned_items
            orgs = data.get("items", [])  # Extract the list of items

            # Add the retrieved works to the list
            all_orgs.extend(orgs)
            logger.debug(f"Batch {batch_index + 1}/{len(batches)}: Retrieved {len(orgs)} items.")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error occurred while processing batch {batch_index + 1}: {e}")
   # logger.info(f"Total matching research outputs found: {len(pureworks['results'])}")
        # Optional: Add a delay between requests to avoid hitting rate limits
        time.sleep(request_delay)

    # Combine all works into one JSON object
    orgs = {"results": all_orgs}
    # logger.info(f"Total external orgs found in pure: {str(total_items)}")

    return orgs


def get_ext_orgdata_pure(external_organization_uuids, pure_org_data, pure_org_index=None):
    # List to store the results
    def get_result_by_uuid(data, target_uuid):
        # Iterate through all results
        for result in data.get('results', []):
            # Check if the uuid matches the target uuid
            if result.get('uuid') == target_uuid:
                return result
        return None
    organization_details = []

    if pure_org_index is None:
        pure_org_index = {result.get('uuid'): result for result in pure_org_data.get('results', []) if result.get('uuid')}

    for uuid in external_organization_uuids:
        data = pure_org_index.get(uuid)
        if data is None:
            data = get_result_by_uuid(pure_org_data, uuid)


        if data:
            # Extract the required information
            org_name = data.get('name', {}).get('en_GB', '')  # English name
            org_uuid = data.get('uuid', '')  # UUID
            # Preserve identifier semantics so downstream ROR checks can still
            # inspect the Pure identifier type URI instead of only the label.
            identifiers = []
            for identifier in data.get('identifiers', []):
                id_value = identifier.get('id') or identifier.get('value')
                identifier_type = identifier.get('type') if isinstance(identifier.get('type'), dict) else {}
                id_name = identifier_type.get('term', {}).get('en_GB') or identifier.get('idSource')
                id_uri = identifier_type.get('uri')

                if id_name and id_value:
                    identifiers.append(
                        {
                            'name': id_name,
                            'id': id_value,
                            'type': {'uri': id_uri, 'term': identifier_type.get('term', {})},
                        }
                    )

            # Append the extracted information to the results list
            organization_details.append({
                'uuid': org_uuid,
                'name': org_name,
                'identifiers': identifiers
            })

        else:
            logger.warning(f"Failed to retrieve data for UUID {uuid}.")
    return organization_details


def main(faculty_choice, test_choice):
    logger.info("Script to update external organisations in pure from ricgraph has started")

    faculties = select_faculties(faculty_choice, test_choice)
    logger.info(f"Selected {len(faculties)} faculty key(s)")
    institution_snapshot = load_openalex_institutions_lookup()
    institution_name_index = build_institution_name_index(institution_snapshot)
    researchoutputs = enrich.select_persons_researchoutput(faculties)

    purejsons = enrich.fetch_pure_researchoutputs(researchoutputs, allow_doi_fallback=False)
    article_orgs, uuids = collect_ricgraph_article_orgs(researchoutputs, purejsons)

    pure_orgsjsons = fetch_pure_extorgs(uuids)
    unique_external_org_uuids = flatten_external_org_uuid_groups(uuids)
    logger.info(
        f"External org funnel: {len(researchoutputs)} Ricgraph research output selection row(s), "
        f"{len(purejsons.get('results', []))} Pure research output(s), "
        f"{len(article_orgs)} publication(s) with Ricgraph organisations, "
        f"{sum(len(article['external_organization_uuids']) for article in article_orgs)} external org link(s), "
        f"{len(unique_external_org_uuids)} unique external org UUID(s), "
        f"{len(pure_orgsjsons.get('results', []))} Pure external org record(s) fetched"
    )
    pure_org_index = {result.get('uuid'): result for result in pure_orgsjsons.get('results', []) if result.get('uuid')}
    all_rows_toupdate = []
    all_jsons_update = []
    all_orgs_with_ror = []
    all_no_name_match = []
    all_ambiguous_matches = []

    for count, article in enumerate(article_orgs, start=1):
        if count % 25 == 0:
            logger.info(f"Processed {str(count)} batch")
        pure_org_details = get_ext_orgdata_pure(article['external_organization_uuids'], pure_orgsjsons, pure_org_index)
        snapshot_org_details = get_ext_orgdata_from_ricgraph(
            article['ricgraph_organizations'],
            institution_name_index,
        )

        orgs_to_update, orgs_with_ror_in_pure, orgs_with_no_name_match, orgs_with_ambiguous_match = match_organizations(
            pure_org_details,
            snapshot_org_details
        )
        all_orgs_with_ror.extend(orgs_with_ror_in_pure)
        all_no_name_match.extend(orgs_with_no_name_match)
        all_ambiguous_matches.extend(orgs_with_ambiguous_match)

        _update, _inpure, rows_to_update, json_updates = update_externalorg_pure(orgs_to_update, test_choice, 0)
        all_rows_toupdate.extend(rows_to_update)
        all_jsons_update.extend(json_updates)

    deduped_rows, _deduped_json_updates = write_external_org_outputs(
        all_rows_toupdate,
        all_jsons_update,
        all_ambiguous_matches,
        all_no_name_match,
    )

    logger.info(
        f"Total external orgs processed: {sum(len(article['external_organization_uuids']) for article in article_orgs)}")
    exact_proposals = sum(1 for row in deduped_rows if str(row.get('match_type') or '').startswith('exact_'))
    fuzzy_proposals = sum(1 for row in deduped_rows if str(row.get('match_type') or '').startswith('fuzzy_'))
    logger.info(
        f"External org funnel results: {exact_proposals} exact proposal(s), {fuzzy_proposals} fuzzy proposal(s), "
        f"{len({row.get('uuid') for row in all_orgs_with_ror if row.get('uuid')})} unique org(s) already with ROR, "
        f"{len({row.get('uuid') for row in all_no_name_match if row.get('uuid')})} unique no-name-match org(s), "
        f"{len({row.get('uuid') for row in all_ambiguous_matches if row.get('uuid')})} unique ambiguous org(s)"
    )
    logger.info(f"nr of ext orgs that can be updated: {len(deduped_rows)}")
    logger.info(f"nr of ext orgs that already have a ROR in Pure: {len(all_orgs_with_ror)}")
    logger.info(f"nr of ext orgs with no name match in institution snapshot: {len(all_no_name_match)}")
    logger.info(f"nr of ext orgs with ambiguous institution snapshot matches: {len(all_ambiguous_matches)}")

# ########################################################################
# MAIN
# ########################################################################
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update external persons from Ricgraph')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='uu faculty: faculteit rebo|organization_name',
                        # default='uu faculty: information & technology services|organization_name',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='no', help='Run in test mode ("yes" or "no")')

    args = parser.parse_args()

    main(args.faculty_choice, args.test_choice)
