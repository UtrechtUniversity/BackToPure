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

from itertools import chain
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
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ROR_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS
from openalex_cache import fetch_openalex_institutions_cached

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


def resolve_output_dir():
    return os.environ.get("BTP_OUTPUT_DIR", "output/external_orgs")


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
    if city:
        address["city"] = city

    country_ref = build_pure_country_ref(geo_summary)
    if country_ref:
        address["country"] = country_ref

    point = build_pure_geopoint(geo_summary)
    if point:
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

        scored_candidates.sort(key=lambda item: item["score"], reverse=True)

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
                f"Queued Pure org '{pure_org['name']}' for update with ROR {openalex_org['ror']} "
                f"(match={best_candidate['match_type']}, score={best_candidate['score']:.4f})"
            )

    orgs_with_ror_in_pure = list({org['uuid']: org for org in orgs_with_ror_in_pure}.values())

    return orgs_to_update, orgs_with_ror_in_pure, orgs_with_no_name_match, orgs_with_ambiguous_match

def match_orgs_oa_pure(oa_article, pure_article, article_orgs):
    # Initialize a dictionary to store unique institutions
    oa_unique_institutions = {}
    oa_ids = []
    uuids =[]
    # Iterate over the authorships to extract institutions
    if oa_article['authorships'] is not None:
        for authorship in oa_article['authorships']:
            institutions = authorship.get('institutions', [])
            for institution in institutions:
                inst_id = institution.get('id')
                display_name = institution.get('display_name')
                ror = institution.get('ror')

                oa_ids.append(ror)

                # Check if the institution is already added using its OpenAlex ID
                if inst_id and inst_id not in oa_unique_institutions:
                    oa_unique_institutions[inst_id] = {
                        'openalex_id': inst_id,
                        'display_name': display_name,
                        'ror': ror
                    }

    # Initialize a set to store unique external organization UUIDs
    external_organization_uuids = set()

    contributors = pure_article.get('contributors', [])

    if not contributors:
        logger.info("No contributors found in the item.")
    else:

        # Loop through each contributor
        for contributor in contributors:
            # Check for external organizations associated with the contributor
            external_orgs = contributor.get('externalOrganizations', [])
            for ext_org in external_orgs:
                uuid = ext_org.get('uuid')
                if uuid:
                    external_organization_uuids.add(uuid)

    # Extract from top-level 'externalOrganizations' section
    top_level_external_orgs = pure_article.get('externalOrganizations', [])
    for ext_org in top_level_external_orgs:
        uuid = ext_org.get('uuid')
        if uuid:
            external_organization_uuids.add(uuid)

    # Convert set to a list
    external_organization_uuids = list(external_organization_uuids)
    data_entry = {
        'doi': oa_article['doi'],
        'external_organization_uuids': external_organization_uuids,
        'unique_institutions': oa_unique_institutions
    }
    article_orgs.append(data_entry)
    uuids.append(external_organization_uuids)

    return article_orgs, uuids, oa_ids

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
            # Mark as "to be updated"
            rows_to_update.append({
                'to_be_updated': 'X',
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
        'value': 'uu faculty',
    }
    url = RIC_BASE_URL + 'organization/search'
    response = requests.get(url, params=params)
    data = response.json()
    if faculty_choice.lower() == 'all':
        selected_faculties = [item['_key'] for item in data["results"]]
    else:
        selected_faculties = [faculty_choice]

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


def mainproces(doi, pure, open_alex, article_orgs):
    logging.debug(f"start fetching organizations for {doi}")
    uuids = []
    oa_ids = []
    oa_article = enrich.get_ro_from_openalex(doi, open_alex)
    pure_article = enrich.get_ro_from_pure(doi, pure)
    if oa_article and pure_article:
        article_orgs, uuids, oa_ids = match_orgs_oa_pure(oa_article, pure_article, article_orgs)

    return article_orgs, uuids, oa_ids

# Function to chunk a list into smaller parts
def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Function to get institution data from OpenAlex API using a session
def fetch_openalex_rors(rors, chunk_size=20):
    logger.debug("start fetching organizations in open alex")
    flattened_rors = []
    for item in rors:
        if isinstance(item, list):
            flattened_rors.extend(ror for ror in item if ror)
        elif item:
            flattened_rors.append(item)
    all_results = fetch_openalex_institutions_cached(
        flattened_rors,
        chunk_size=chunk_size,
        request_delay=1.0,
        force_refresh=False
    )
    logger.debug(f"Fetched {len(all_results.get('results', []))} unique institutions from OpenAlex")
    logger.debug("end fetching organizations in open alex")
    return all_results


def fetch_pure_extorgs(uuids):
    logger.info(f"start fetching external orgs from pure")
    url = PURE_BASE_URL + 'external-organizations/search'

    # Function to split the list into batches of size n
    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Define batch size for testing
    batch_size = 10

    # Fully flatten list of lists of UUIDs
    uuids = list(chain.from_iterable(u for u in uuids if isinstance(u, list)))
    # Flatten the list
    flat_uuids = [uuid for sublist in uuids for uuid in sublist]
    flat_uuids = list(dict.fromkeys(flat_uuids))
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
        pipe_separated_dois = "|".join(batch)
        logger.info(
            f"Finding ext orgs for batch {batch_index + 1}/{len(batches)}, {batch_size} DOIs per batch.")
        json_data = {
            'size': 100,  # Set size to batch size
            'searchString': pipe_separated_dois,
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


def get_ext_orgdata_openalex(oa_unique_institutions, oa_orgsjsons, openalex_org_index=None):
    organization_details = []

    def get_result_by_id(data, target_uuid):
        # Iterate through all results
        for result in data.get('results', []):
            # Check if the uuid matches the target uuid
            if result.get('id') == target_uuid:
                return result
        return None
    if openalex_org_index is None:
        openalex_org_index = {result.get('id'): result for result in oa_orgsjsons.get('results', []) if result.get('id')}

    for institute in oa_unique_institutions:
        data = openalex_org_index.get(institute)
        if data is None:
            data = get_result_by_id(oa_orgsjsons, institute)


        if data:

            # Extract the required fields
            openalex_id = data['ids'].get('openalex')
            ror = data['ids'].get('ror')
            display_name = data.get('display_name')
            display_name_alternatives = data.get('display_name_alternatives', [])
            geo = data.get('geo', {})

            # Create a list with the extracted information
            extracted_info = {
                "openalex_id": openalex_id,
                "ror": ror,
                "display_name": display_name,
                "display_name_alternatives": display_name_alternatives,
                "geo": geo
            }

            # Append the extracted information to the results list
            organization_details.append(extracted_info)

    return organization_details


def main(faculty_choice, test_choice):
    logger.info("Script to update external organisations in pure from ricgraph has started")

    faculties = select_faculties(faculty_choice, test_choice)

    researchoutputs = enrich.select_persons_researchoutput(faculties)

    purejsons = enrich.fetch_pure_researchoutputs(researchoutputs)
    dois = [entry["doi"] for entry in researchoutputs if entry.get("doi")]
    openalexjsons = enrich.fetch_openalex_works(dois)
    rorsuiids =[]
    update = 0
    article_orgs = []
    # Initialize sets for unique UUIDs and unique institutions
    uuids = []
    oa_ids = []
    orgs = []

    for doi in dois:
        orgs_out, new_uuids, new_oa_ids = mainproces(doi, purejsons, openalexjsons, article_orgs)
        for org in orgs_out:
            if org not in orgs:
                orgs.append(org)

        uuids.append(new_uuids)
        oa_ids.append(new_oa_ids)

    pure_orgsjsons = fetch_pure_extorgs(uuids)
    notupdate = 0

    openalex_orgjsons = fetch_openalex_rors(oa_ids)
    pure_org_index = {result.get('uuid'): result for result in pure_orgsjsons.get('results', []) if result.get('uuid')}
    openalex_org_index = {result.get('id'): result for result in openalex_orgjsons.get('results', []) if result.get('id')}
    all_rows_toupdate = []
    all_jsons_update =[]

    all_orgs_to_update = []
    all_orgs_with_ror = []
    all_no_name_match = []
    all_ambiguous_matches = []

    count = 0
    for article in article_orgs:
         count += 1
         if count % 25 == 0:
            logger.info(f"Processed {str(count)} batch")
         pure_org_details = get_ext_orgdata_pure(article['external_organization_uuids'], pure_orgsjsons, pure_org_index)
         oa_org_details = get_ext_orgdata_openalex(article['unique_institutions'], openalex_orgjsons, openalex_org_index)

         orgs_to_update, orgs_with_ror_in_pure, orgs_with_no_name_match, orgs_with_ambiguous_match = match_organizations(
             pure_org_details,
             oa_org_details
         )
         all_orgs_to_update.extend(orgs_to_update)
         all_orgs_with_ror.extend(orgs_with_ror_in_pure)
         all_no_name_match.extend(orgs_with_no_name_match)
         all_ambiguous_matches.extend(orgs_with_ambiguous_match)

         update, inpure, rows_to_update, json_updates  = update_externalorg_pure(orgs_to_update, test_choice, update)
         all_rows_toupdate.extend(rows_to_update)
         all_jsons_update.extend(json_updates)

         if inpure == True:
             notupdate = notupdate  +1


    # Save the DataFrame
    # Directory to save the output files
    output_dir = resolve_output_dir()
    os.makedirs(output_dir, exist_ok=True)
    deduped_rows = dedupe_records_by_uuid(all_rows_toupdate, uuid_keys=("uuid",))
    deduped_json_updates = dedupe_records_by_uuid(all_jsons_update)
    duplicate_count = len(all_rows_toupdate) - len(deduped_rows)
    if duplicate_count > 0:
        logger.info(f"Collapsed {duplicate_count} duplicate external org proposal row(s) to unique organisation UUIDs")

    df = pd.DataFrame(deduped_rows)
    df.to_csv(os.path.join(output_dir, "external_orgs_to_update.csv"), index=False)

    # Save the big JSON file
    with open(os.path.join(output_dir, "external_orgs_updates.json"), 'w') as json_file:
        json.dump(deduped_json_updates, json_file, indent=4)

    with open(os.path.join(output_dir, "external_orgs_ambiguous_matches.json"), 'w') as json_file:
        json.dump(all_ambiguous_matches, json_file, indent=4)

    with open(os.path.join(output_dir, "external_orgs_no_name_match.json"), 'w') as json_file:
        json.dump(all_no_name_match, json_file, indent=4)

    logger.info(
        f"Total external orgs processed: {sum(len(article['external_organization_uuids']) for article in article_orgs)}")

    logger.info(f"nr of ext orgs that can be updated: {len(deduped_rows)}")
    logger.info(f"nr of ext orgs that already have a ROR in Pure: {len(all_orgs_with_ror)}")
    logger.info(f"nr of ext orgs with no name match in OpenAlex: {len(all_no_name_match)}")
    logger.info(f"nr of ext orgs with ambiguous OpenAlex matches: {len(all_ambiguous_matches)}")

    unique_rorsuiids = list(set(rorsuiids))
    with open(os.path.join(output_dir, 'output.csv'), mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(unique_rorsuiids)



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
