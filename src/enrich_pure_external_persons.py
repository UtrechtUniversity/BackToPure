# ########################################################################
# Script: enrich_pure_external_persons.py
#
# Description:
# This script updates external person records in Pure by integrating data from
# Ricgraph and OpenAlex. The goal is to enrich external person profiles with
# additional identifiers such as ORCID and OpenAlex IDs.
#
# The script includes:
# - Fetching research outputs associated with external persons.
# - Matching authors between Pure and OpenAlex based on identifiers.
# - Retrieving external person records from Pure and updating them with missing IDs.
# - Logging and error handling.
#
# Important:
# This script relies on external APIs (Pure and OpenAlex) and should be run
# with necessary configurations in place.
# This script is meant to be invoked by the BackToPure web interface.
#
# Dependencies:
# - requests, pandas, logging, urllib3, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import re
import os
import time
import pandas as pd
import logging
from logging_config import setup_logging
import requests
import json
import argparse
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from config import PURE_BASE_URL, PURE_API_KEY, EMAIL, RIC_BASE_URL, OPENALEXEX_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS
from typing import List, Dict
import sys
from urllib.parse import quote
from openalex_cache import fetch_openalex_works_cached
logger = setup_logging('btp', level=logging.INFO)
datetimetoday = datetime.now().strftime('%Y%m%d')
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
session.mount("http://", adapter)
# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
REQUEST_TIMEOUT = 30


def _output_dir():
    return os.environ.get("BTP_OUTPUT_DIR", "output/external_persons")


def _normalize_identifier(value):
    if pd.isna(value):
        return ""
    normalized = str(value).strip()
    if not normalized or normalized.lower() == "nan":
        return ""
    if normalized.lower().startswith("https://orcid.org/"):
        normalized = normalized.split("/")[-1]
    if normalized.lower().startswith("https://openalex.org/"):
        normalized = normalized.split("/")[-1]
    if "?" in normalized:
        normalized = normalized.split("?", 1)[0]
    if "#" in normalized:
        normalized = normalized.split("#", 1)[0]
    return normalized

def timestamp(seconds: bool = False) -> str:
    """Get a timestamp only consisting of a time.

    :param seconds: If True, also show seconds in the timestamp.
    :return: the timestamp.
    """
    now = datetime.now()
    if seconds:
        time_stamp = now.strftime("%H:%M:%S")
    else:
        time_stamp = now.strftime("%H:%M")
    return time_stamp
def extract_orcid_id(orcid):
    return _normalize_identifier(orcid)

def extract_openalex_id(openalex):
    return _normalize_identifier(openalex)

def normalize_doi(doi_value):
    if not doi_value:
        return None
    return str(doi_value).replace("https://doi.org/", "").strip().lower()

def get_ro_from_openalex(item, openalexworks):
    normalized = normalize_doi(item)
    if not normalized:
        return None
    by_doi = openalexworks.get("by_doi", {})
    if by_doi:
        return by_doi.get(normalized)

    doi = 'https://doi.org/' + item
    for work in openalexworks.get("results", []):
        if work.get("doi") == doi:
            return work
    return None


def get_ro_from_pure(target_doi, pureworks):
    """
    Retrieves the first research output from the Pure API results that matches the provided DOI.

    Parameters:
    pureworks (dict): The combined JSON object containing all research outputs.
    target_doi (str): The DOI of the research output to retrieve.

    Returns:
    dict: The first research output that corresponds to the provided DOI. Returns None if none are found.
    """

    normalized_doi = normalize_doi(target_doi)
    if not normalized_doi:
        return None
    by_doi = pureworks.get("by_doi", {})
    if by_doi:
        return by_doi.get(normalized_doi)

    for work in pureworks.get("results", []):
        # Check in 'electronicVersions' for the DOI
        if 'electronicVersions' in work:
            for version in work['electronicVersions']:
                if 'doi' in version:
                    # Normalize the DOI in the data
                    normalized_version_doi = version['doi'].replace("https://doi.org/", "").lower()
                    if normalized_doi == normalized_version_doi:
                        return work  # Return the first matching work

        # Check in 'additionalLinks' for the DOI if not already found
        if 'additionalLinks' in work:
            for link in work['additionalLinks']:
                if 'url' in link:
                    # Normalize the DOI in the link
                    normalized_link_doi = link['url'].replace("https://doi.org/", "").lower()
                    if normalized_doi == normalized_link_doi:
                        return work  # Return the first matching work

    return None  # Return None if no match is found


def check_name_match(alex_name, pure_authors):
    # Check for exact full name match
    if alex_name in pure_authors:
        return pure_authors[alex_name]

    # Split the alex_name into first and last names
    alex_parts = alex_name.split(' ')
    if len(alex_parts) < 2:
        return None  # Not enough parts to compare

    alex_first_name, alex_last_name = alex_parts[0], alex_parts[-1]

    for pure_name in pure_authors:
        pure_parts = pure_name.split(' ')
        if len(pure_parts) < 2:
            continue  # Skip if the name format is not as expected

        pure_first_name, pure_last_name = pure_parts[0], pure_parts[-1]

        # Check if last names match and if first letter of first names match
        if alex_last_name == pure_last_name and alex_first_name and pure_first_name and alex_first_name[0] == \
                pure_first_name[0]:
            return pure_authors[pure_name]
    return None
def match_persons_oa_pure(oa_article, pure_article):
    # Extract authors from the alex1.json dataset
    # Extract authors from the alex1.json dataset with ORCID if available
    alex_authors = {}

    for author in oa_article.get('authorships', []):
        author_info = author.get('author')
        if author_info and isinstance(author_info, dict):
            display_name = author_info.get('display_name')
            if display_name:
                alex_authors[display_name] = {
                    'alex_id': author_info.get('id'),
                    'orcid': author_info.get('orcid', None)
                }

    # Extract authors from the pure1.json dataset
    # Correcting the extraction of UUIDs for contributors and ensuring Pure_UUID is not a list

    pure_authors = {}  # Initialize the dictionary if not already initialized

    for contributor in pure_article.get('contributors', []):
        if 'externalPerson' in contributor:  # Check for externalPerson first
            # Extract UUID
            uuid = contributor['externalPerson'].get('uuid', "")

            # Extract first and last names
            name_info = contributor.get('name', {})
            first_name = name_info.get('firstName', "")
            last_name = name_info.get('lastName', "")

            if first_name or last_name:
                name = f"{first_name} {last_name}".strip()
                # Store in pure_authors dictionary
                pure_authors[name] = uuid
            else:
                logger.info(f"Missing first or last name for contributor: {contributor}")
        # else:
        #     logger.info(f"Contributor is not an external person: {contributor}")

    # Find common authors based on names and create the list with names, all IDs, and ORCID if available
    common_authors_list = []
    for name, ids in alex_authors.items():

        pure_uuid = check_name_match(name, pure_authors)
        if pure_uuid and name:

            common_authors_list.append({
                'Name': name,
                'Alex_ID': extract_openalex_id(ids['alex_id']),
                'Pure_UUID': pure_uuid,
                'ORCID': extract_orcid_id(ids['orcid']),
                'Source': 'openalex'
            })




    # Display the common authors

    # output_path = "common_authors.xlsx"
    # common_authors_df.to_excel(output_path, index=False)
    return common_authors_list

def identifier_exists(identifiers, new_id, id_type_uri):

    for identifier in identifiers:
        if 'type' in identifier and identifier['type']['uri'] == id_type_uri and identifier['id'] == new_id:
            return True
    return False
def update_externalpersons_pure(persons, matched_personsjson, test_choice):
    logger.info(f"Starting update preparation for {len(persons)} matched persons")
    data_to_save = []  # List to store JSON objects for saving
    rows_to_update = []  # List to store rows for the DataFrame
    persons_by_uuid = {person.get('uuid'): person for person in matched_personsjson if person.get('uuid')}
    logger.info(f"Loaded {len(persons_by_uuid)} external person records from Pure for comparison")

    ro, matched_persons, updated_persons, already_ids = 0, 0, 0, 0
    for row in persons:
        uuid = row['Pure_UUID']
        matched_person = persons_by_uuid.get(uuid)

        if matched_person is None:
            logger.debug(f"Matched person not found for UUID {uuid}")
            continue

        # Initialize identifiers if not already present
        if 'identifiers' not in matched_person:
            matched_person['identifiers'] = []

        new_openalexid = None
        new_orcid = None

        # Create new ORCID object if available
        if row['ORCID']:
            new_orcid = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['ORCID'],
                "type": {
                    "uri": ORCID_ID_URI,
                    "term": {
                        "en_GB": "ORCID"
                    }
                }
            }

        # Create new OpenAlex ID object if available
        if row['Alex_ID']:
            new_openalexid = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['Alex_ID'],
                "type": {
                    "uri": OPENALEXEX_ID_URI,
                    "term": {
                        "en_GB": "Open Alex id"
                    }
                }
            }

        # Check if the new ORCID and OpenAlex ID already exist
        orcid_exists = (
            new_orcid and
            identifier_exists(matched_person['identifiers'], new_orcid['id'], ORCID_ID_URI)
        )

        openalexid_exists = (
            new_openalexid and
            identifier_exists(matched_person['identifiers'], new_openalexid['id'], OPENALEXEX_ID_URI)
        )

        # Add new identifiers if they don't already exist
        identifiers_updated = False
        if new_orcid and not orcid_exists:
            matched_person['identifiers'].append(new_orcid)
            identifiers_updated = True

        if new_openalexid and not openalexid_exists:
            matched_person['identifiers'].append(new_openalexid)
            identifiers_updated = True

        # Update person data in Pure if identifiers were added
        # if identifiers_updated:
        #     updated_persons += 1
        #     if test_choice == 'no':
        #         url = PURE_BASE_URL + 'external-persons/' + uuid
        #         try:
        #             response = session.put(url, headers=headers, json=matched_person, verify=False)
        #             if response.status_code != 200:
        #                 logger.debug(f"Failed to update data for UUID {uuid}: {response.text}")
        #             else:
        #                 logger.debug(f"Successfully updated data for UUID {uuid} with ORCID {new_orcid}, OpenAlex {new_openalexid}")
        #
        #         except Exception as e:
        #             logger.error(f"Error updating UUID {uuid}: {e}")
        #         time.sleep(0.1)  # Adjust the sleep time based on rate limits
        #     else:
        #
        #         logger.debug(f"Test mode: would update UUID {uuid} with ORCID {new_orcid}, OpenAlex {new_openalexid}")
        # else:
        #     already_ids += 1
        # Save person data to file if identifiers were added
        if identifiers_updated:

            updated_persons += 1
            matched_person['UUID'] = uuid  # Optionally include UUID for reference
            matched_person['to_be_updated'] = 'X'
            matched_person['updated'] = ' '
            data_to_save.append(
                matched_person)  # Add to the list of JSON objects
            row['to_be_updated'] = 'X'
            row['updated'] = ' '
            rows_to_update.append(row)  # Add the current row to the list for the DataFrame
        else:
            already_ids += 1
        if (updated_persons + already_ids) % 500 == 0:
            logger.info(f"Processed {updated_persons + already_ids} external persons for update eligibility")

    # Save all collected JSON objects to the file
    output_folder = _output_dir()
    output_file = os.path.join(output_folder, "to_be_updated.json")  # Full file path
    csv_output_file = os.path.join(output_folder, "ext_pers_update.csv")  # CSV file path

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    try:
        with open(output_file, 'w') as file:
            json.dump(data_to_save, file, indent=4)
        logger.info(f"Saved {len(data_to_save)} persons to {output_file}")
    except Exception as e:
        logger.error(f"Error saving JSON data to file: {e}")

    try:
        desired_order = ['to_be_updated', 'updated', 'Name', 'Alex_ID', 'Pure_UUID', 'ORCID', 'Source']
        ext_pers_update = pd.DataFrame(rows_to_update, columns=desired_order)
        ext_pers_update.to_csv(csv_output_file, index=False)
        if rows_to_update:
            logger.info(f"Saved {len(rows_to_update)} rows to {csv_output_file}")
        else:
            logger.info(f"No rows to update. Created empty CSV with headers at {csv_output_file}.")
    except Exception as e:
        logger.error(f"Error saving DataFrame to CSV: {e}")


    logger.info(f"total external persons that can be updated: {updated_persons}")
    logger.info(f"total external persons that cannot be updated (already has ids, or no ids found): {already_ids}")




def get_external_persons_data(persons):
    """
    Retrieves the data for all external persons based on their UUIDs using the POST /external-persons/search endpoint,
    paging through until no more items are returned.
    """
    all_person_data = []
    page_size = 500  # Pure API max page size
    logger.info(f"Fetching external person records for {len(persons)} candidate persons")

    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Split unique UUIDs into batches of up to page_size
    uids = list(dict.fromkeys(
        person['Pure_UUID'] for person in persons if person.get('Pure_UUID')
    ))
    logger.info(f"Reduced external person fetch to {len(uids)} unique UUID(s)")
    batches = list(split_into_batches(uids, page_size))
    logger.info(f"External persons fetch split into {len(batches)} batch(es)")
    for batch_idx, batch in enumerate(batches, 1):
        logger.debug(f"Fetching external persons for batch {batch_idx}/{len(batches)}")

        offset = 0
        while True:
            json_body = {
                "uuids": batch,
                "size": page_size,
                "offset": offset
            }

            try:
                resp = session.post(
                    PURE_BASE_URL + 'external-persons/search',
                    json=json_body,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                    verify=False
                )
                time.sleep(0.5)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching batch {batch_idx} (offset {offset}): {e}")
                break

            data = resp.json()
            items = data.get("items", [])
            if not items:
                # no more pages
                break

            all_person_data.extend(items)
            logger.debug(f"  Retrieved {len(items)} items (offset {offset})")

            # if we got fewer than page_size, that was the last page
            if len(items) < page_size:
                break

            offset += page_size

    return all_person_data
    logger.debug(f"end fetching pure")
    logger.debug(f"Matching external persons found: {len(all_person_data)}")

    #
    # output_folder = 'output/external_persons'
    # os.makedirs(output_folder, exist_ok=True)
    # ext_perons_json = os.path.join(output_folder, f'ext_personstobeupdated_{datetimetoday}.csv')
    #
    # # Write the data to the JSON file
    # with open(ext_perons_json, 'w') as json_file:
    #     json.dump(all_person_data, json_file, indent=4)


    return all_person_data

def select_faculties(faculty_choice):
    # the text for logger is wrong, it actually gets person roots, then research output, then external persons. but seems to complicated to inform the user
    logger.info(f"start fetching itemsfor {faculty_choice}")

    params = {
        'value': 'uu faculty',
    }
    url = RIC_BASE_URL + 'organization/search'
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching faculties from Ricgraph: {e}")
        return []
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
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_researchoutputs(persoonroot_key):
    """Fetch person IDs for a given person-ro    ot."""
    # categories = CATEGORIES
    categories =  ['journal article']
    all_results = []

    for categorie in categories:
        logger.debug(f"fetching {categorie}")
        try:
            params = {'key': persoonroot_key, 'category_want': categorie}
            url = RIC_BASE_URL + 'get_all_neighbor_nodes'
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            results = response.json().get("results", [])
            all_results.extend(results)
        except requests.RequestException as e:
            logger.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")

    return all_results



def select_persons_researchoutput(selected_faculties):
    """
    Fetches all unique DOIs and Pure UUIDs for research outputs linked to external persons
    in the selected faculties.

    Returns:
        list: List of dicts with "doi" and optional "pure_uuid"
    """
    all_outputs = []
    max_workers = 10

    def process_personroot(personroot):
        if not personroot.get('_key'):
            return []
        outputs = select_researchoutputs(personroot['_key'])
        result = []
        for output in outputs:
            doi = output["_key"].split("|")[0]
            pure_uuid = output.get("url_other", "").split("/")[-1] \
                if "publications/" in output.get("url_other", "") else None
            result.append({
                "doi": doi,
                "pure_uuid": pure_uuid,
                "researchoutput_key": output.get("_key"),
            })
        return result

    for faculty in selected_faculties:
        logging.info(f"Processing faculty: {faculty}")
        try:
            personroots = fetch_personroots(faculty)
            if not personroots:
                logging.warning(f"No personroots found for faculty {faculty}")
                continue

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_personroot, pr) for pr in personroots]

                for future in as_completed(futures):
                    try:
                        outputs = future.result()
                        all_outputs.extend(outputs)
                    except Exception as e:
                        logging.error(f"Error processing personroot: {e}")

        except Exception as e:
            logging.error(f"Error fetching personroots for faculty {faculty}: {e}")

    deduped_outputs = []
    seen_dois = set()
    for output in all_outputs:
        doi = output.get("doi")
        if not doi or doi in seen_dois:
            continue
        seen_dois.add(doi)
        deduped_outputs.append(output)

    logger.info(f"Total unique DOIs found in Ricgraph: {len(seen_dois)}")
    return deduped_outputs


def fetch_ricgraph_persons_for_output(researchoutput_key):
    if not researchoutput_key:
        return []

    try:
        params = {
            'key': researchoutput_key,
            'category_want': 'person',
            'max_nr_items': '0',
        }
        response = session.get(RIC_BASE_URL + 'get_all_neighbor_nodes', params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching Ricgraph person neighbors for output {researchoutput_key}: {e}")
        return []


def fetch_ricgraph_person_details(person_key):
    if not person_key:
        return []

    try:
        params = {
            'key': person_key,
            'category_want': 'person',
            'max_nr_items': '0',
        }
        response = session.get(RIC_BASE_URL + 'get_all_neighbor_nodes', params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching Ricgraph person details for {person_key}: {e}")
        return []


def parse_ricgraph_person_details(person_fields):
    parsed = {
        "Name": "",
        "Alex_ID": "",
        "Pure_UUID": "",
        "ORCID": "",
        "Source": "ricgraph",
    }

    for field in person_fields:
        field_name = field.get('name')
        value = field.get('value')
        if field_name == 'FULL_NAME':
            if value:
                name = value.split("#", 1)[0].strip()
                if "," in name:
                    surname, given = [part.strip() for part in name.split(",", 1)]
                    if given and surname:
                        name = f"{given} {surname}"
                parsed["Name"] = re.sub(r"\s+", " ", name).strip() or parsed["Name"]
        elif field_name == 'PURE_UUID_PERS':
            parsed["Pure_UUID"] = value or parsed["Pure_UUID"]
        elif field_name in {'OPENALEX', 'OPENALEX_ID_PERS'}:
            parsed["Alex_ID"] = extract_openalex_id(value)
        elif field_name == 'ORCID':
            parsed["ORCID"] = extract_orcid_id(value)

    return parsed


def match_persons_to_pure(source_persons, pure_article, source_name):
    pure_authors = {}

    for contributor in pure_article.get('contributors', []):
        if 'externalPerson' not in contributor:
            continue

        uuid = contributor['externalPerson'].get('uuid', "")
        name_info = contributor.get('name', {})
        first_name = name_info.get('firstName', "")
        last_name = name_info.get('lastName', "")
        if first_name or last_name:
            name = f"{first_name} {last_name}".strip()
            pure_authors[name] = uuid

    common_authors_list = []
    for person in source_persons:
        name = person.get("Name", "")
        if not name:
            continue

        pure_uuid = check_name_match(name, pure_authors)
        if pure_uuid:
            common_authors_list.append({
                'Name': name,
                'Alex_ID': person.get('Alex_ID', ''),
                'Pure_UUID': pure_uuid,
                'ORCID': person.get('ORCID', ''),
                'Source': source_name,
                'doi': person.get('doi', ''),
                'researchoutput_key': person.get('researchoutput_key', ''),
            })

    return common_authors_list


def collect_ricgraph_person_matches(outputs, max_workers=10):
    logger.info(f"Collecting external-person identifiers directly from Ricgraph for {len(outputs)} research output(s)")
    collected_matches = []
    output_keys_with_person_nodes = set()
    output_keys_with_complete_ids = set()
    total_person_nodes = 0
    missing_identifier_values = 0

    def process_output(output):
        researchoutput_key = output.get("researchoutput_key")
        if not researchoutput_key:
            return [], None, 0, 0

        person_nodes = fetch_ricgraph_persons_for_output(researchoutput_key)
        output_matches = []
        output_has_usable_persons = False
        output_missing_identifier_values = 0
        for person_node in person_nodes:
            details = fetch_ricgraph_person_details(person_node.get('_key'))
            parsed = parse_ricgraph_person_details(details)
            if parsed.get("Name") and (parsed.get("Alex_ID") or parsed.get("ORCID")):
                parsed["doi"] = output.get("doi", "")
                parsed["researchoutput_key"] = researchoutput_key
                output_matches.append(parsed)
                output_has_usable_persons = True
            else:
                output_missing_identifier_values += 1

        return (
            output_matches,
            researchoutput_key if person_nodes else None,
            len(person_nodes),
            output_missing_identifier_values,
            researchoutput_key if output_has_usable_persons else None,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_output, output) for output in outputs]
        for future in as_completed(futures):
            try:
                (
                    output_matches,
                    output_key_with_person_nodes,
                    person_node_count,
                    output_missing_identifier_values,
                    output_key_with_complete_ids,
                ) = future.result()
            except Exception as e:
                logger.error(f"Error collecting Ricgraph person identifiers: {e}")
                continue

            collected_matches.extend(output_matches)
            total_person_nodes += person_node_count
            missing_identifier_values += output_missing_identifier_values
            if output_key_with_person_nodes:
                output_keys_with_person_nodes.add(output_key_with_person_nodes)
            if output_key_with_complete_ids:
                output_keys_with_complete_ids.add(output_key_with_complete_ids)

    logger.info(
        f"Ricgraph yielded {len(collected_matches)} external-person candidate row(s) "
        f"from {total_person_nodes} person node(s) across {len(output_keys_with_person_nodes)} research output(s)"
    )
    logger.info(
        f"Ricgraph harvest diagnostics: outputs_with_person_nodes={len(output_keys_with_person_nodes)}, "
        f"outputs_with_complete_ids={len(output_keys_with_complete_ids)}, "
        f"missing_identifier_values={missing_identifier_values}"
    )
    return collected_matches, output_keys_with_complete_ids


def match_ricgraph_persons(ricgraph_persons, purejsons):
    if not ricgraph_persons:
        return []

    matched_persons = []
    persons_by_doi = {}
    missing_pure_articles = []
    for row in ricgraph_persons:
        doi = normalize_doi(row.get("doi"))
        if not doi:
            continue
        persons_by_doi.setdefault(doi, []).append(row)

    for doi, source_persons in persons_by_doi.items():
        pure_article = get_ro_from_pure(doi, purejsons)
        if not pure_article:
            missing_pure_articles.append(doi)
            logger.info(f"Skipping DOI {doi}: not found in Pure")
            continue
        matched_persons.extend(match_persons_to_pure(source_persons, pure_article, "ricgraph"))

    if missing_pure_articles:
        logger.info(
            f"Ricgraph publication matching skipped {len(missing_pure_articles)} DOI(s) not found in Pure"
        )

    return matched_persons




def match_persons(doi, openalexjsons, purejsons):
    persons = []
    oa_article = get_ro_from_openalex(doi, openalexjsons)
    pure_article = get_ro_from_pure(doi, purejsons)

    if oa_article and pure_article:
        persons = match_persons_oa_pure(oa_article, pure_article)
        # updated_persons, already_ids = update_externalpersons_pure(persons, test_choice, updated_persons, already_ids)

    if persons:
        return persons

import requests
import math
import time
from typing import List, Dict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

def _make_session(retries: int = 3, backoff: float = 0.3) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_batch(batch: List[str], url: str, headers: Dict[str, str], timeout: int) -> List[Dict]:
    """Fetch research outputs by DOI using searchString (pipe-separated), with pagination."""
    session = _make_session()
    pipe_separated_dois = "|".join(batch)
    size = 100
    offset = 0
    all_items = []

    while True:
        json_data = {
            'size': size,
            'offset': offset,
            'searchString': pipe_separated_dois,
        }

        logger.info(f"Requesting Pure page offset {offset} for {len(batch)} DOI(s)")

        try:
            response = session.post(url, headers=headers, json=json_data, timeout=timeout, verify=False)
            response.raise_for_status()

            data = response.json()
            items = data.get("items", [])
            all_items.extend(items)

            total_results = data.get("totalElements", None)
            logger.debug(f"Fetched {len(items)} items; total results: {total_results}")

            if len(items) < size:
                break  # Last page
            offset += size

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching batch at offset {offset}: {e}")
            break  # Exit loop on error

        time.sleep(0.1)

    return all_items


def fetch_pure_researchoutput_by_doi(doi: str) -> List[Dict]:
    normalized_doi = normalize_doi(doi)
    if not normalized_doi:
        return []

    api_url = PURE_BASE_URL.rstrip("/") + "/research-outputs/search/"
    data = {"searchString": normalized_doi}

    try:
        response = session.post(api_url, headers=headers, json=data, timeout=60, verify=False)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Pure research output for DOI {normalized_doi}: {e}")
        return []

    items = response.json().get("items", [])
    matches = []
    for item in items:
        for version in item.get("electronicVersions", []):
            if normalize_doi(version.get("doi")) == normalized_doi:
                matches.append(item)
                break
        else:
            for link in item.get("additionalLinks", []):
                if normalize_doi(link.get("url")) == normalized_doi:
                    matches.append(item)
                    break

    if not matches and items:
        logger.info(
            f"Pure DOI search returned {len(items)} candidate item(s) for {normalized_doi} "
            f"but none matched exactly"
        )

    return matches

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_pure_researchoutputs(outputs: List[Dict], batch_size: int = 500) -> Dict:
    """
    Fetch research outputs from Pure using batched UUIDs when available and DOI search as fallback.
    Outputs without a Pure UUID are looked up by DOI. If a DOI is not found in Pure, it is skipped.
    Returns: {"results": [...], "by_doi": {...}} list of Pure outputs indexed by normalized DOI.
    """
    logger.debug("Start batched fetch of research outputs from Pure by UUID and DOI fallback")
    session = _make_session()
    url = PURE_BASE_URL.rstrip("/") + "/research-outputs/search"
    timeout = 60
    results = []

    uuid_list = [entry["pure_uuid"] for entry in outputs if entry.get("pure_uuid")]
    doi_list = [normalize_doi(entry.get("doi")) for entry in outputs if not entry.get("pure_uuid") and normalize_doi(entry.get("doi"))]

    uuid_batches = [uuid_list[i:i + batch_size] for i in range(0, len(uuid_list), batch_size)]
    doi_batch_size = min(50, batch_size)
    doi_batches = [doi_list[i:i + doi_batch_size] for i in range(0, len(doi_list), doi_batch_size)]

    for idx, batch in enumerate(uuid_batches, start=1):
        logger.info(f"Fetching Pure research outputs batch {idx}/{len(uuid_batches)} with {len(batch)} UUIDs")
        json_data = {"uuids": batch, "size": batch_size, "offset": 0}
        try:
            resp = session.post(url, headers=headers, json=json_data, timeout=timeout, verify=False)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            logger.info(f"UUID batch {idx} returned {len(items)} results")
            results.extend(items)
        except Exception as e:
            logger.error(f"Failed to fetch UUID batch {idx}: {e}")
        time.sleep(0.1)

    for idx, batch in enumerate(doi_batches, start=1):
        logger.info(
            f"Fetching Pure research outputs DOI batch {idx}/{len(doi_batches)} "
            f"with {len(batch)} DOI(s)"
        )
        batch_results = []
        for doi in batch:
            items = fetch_pure_researchoutput_by_doi(doi)
            if items:
                batch_results.extend(items)
            else:
                logger.info(f"Pure DOI lookup returned 0 exact match(es) for {doi}")
        logger.info(f"DOI batch {idx} returned {len(batch_results)} results")
        results.extend(batch_results)

    by_doi = {}
    for work in results:
        for version in work.get('electronicVersions', []):
            version_doi = normalize_doi(version.get('doi'))
            if version_doi and version_doi not in by_doi:
                by_doi[version_doi] = work
        for link in work.get('additionalLinks', []):
            link_doi = normalize_doi(link.get('url'))
            if link_doi and link_doi not in by_doi:
                by_doi[link_doi] = work

    requested_dois = {normalize_doi(entry.get("doi")) for entry in outputs if normalize_doi(entry.get("doi"))}
    found_dois = set(by_doi.keys())
    missing_dois = sorted(requested_dois - found_dois)
    if missing_dois:
        logger.info(
            f"Pure lookup skipped {len(missing_dois)} DOI(s) not found in Pure: "
            f"{', '.join(missing_dois[:10])}{' ...' if len(missing_dois) > 10 else ''}"
        )

    logger.info(f"Total research outputs fetched from Pure: {len(results)}")
    logger.info(f"Indexed {len(by_doi)} DOI(s) from Pure research outputs")
    return {"results": results, "by_doi": by_doi}




from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import logging
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from config import EMAIL  # You already have this in your project

def fetch_openalex_works(dois):
    logger.info(f"Starting OpenAlex fetch for {len(dois)} DOI entries")
    return fetch_openalex_works_cached(dois, batch_size=10, request_delay=1.5, force_refresh=False)

def match_all_persons(researchoutputs, openalexjsons, purejsons):
    all_persons = []
    logger.info(f"Starting person matching across {len(researchoutputs)} DOI(s)")

    for idx, doi in enumerate(researchoutputs, start=1):

        persons = match_persons(doi, openalexjsons, purejsons)
        if persons:
            all_persons = all_persons + persons
        if idx % 1000 == 0 or idx == len(researchoutputs):
            logger.info(f"Matching progress: {idx}/{len(researchoutputs)} DOI(s)")

    logger.info(f"total external persons found: {len(all_persons)}")
    return all_persons


def consolidate_person_matches(persons):
    """
    Collapse multiple matches for the same Pure external person into a single row.

    The same external person can appear on many publications. For the update flow
    we only need one candidate row per Pure UUID. If conflicting IDs are found
    across publications, skip that person and surface it in the logs.
    """
    consolidated = {}
    conflicts = {}

    for row in persons:
        pure_uuid = row.get("Pure_UUID")
        if not pure_uuid:
            continue

        existing = consolidated.get(pure_uuid)
        if existing is None:
            consolidated[pure_uuid] = {
                "Name": row.get("Name", ""),
                "Alex_ID": row.get("Alex_ID", ""),
                "Pure_UUID": pure_uuid,
                "ORCID": row.get("ORCID", ""),
                "Source": row.get("Source", ""),
            }
            continue

        conflict_fields = []
        if existing.get("Alex_ID") and row.get("Alex_ID") and existing["Alex_ID"] != row["Alex_ID"]:
            conflict_fields.append("Alex_ID")
        if existing.get("ORCID") and row.get("ORCID") and existing["ORCID"] != row["ORCID"]:
            conflict_fields.append("ORCID")

        if conflict_fields:
            existing_source = existing.get("Source")
            incoming_source = row.get("Source")
            if existing_source == "ricgraph" and incoming_source != "ricgraph":
                continue
            if incoming_source == "ricgraph" and existing_source != "ricgraph":
                for field in conflict_fields:
                    existing[field] = row.get(field, existing.get(field, ""))
                existing["Source"] = "ricgraph"
                if not existing.get("Name") and row.get("Name"):
                    existing["Name"] = row["Name"]
                continue
            conflicts.setdefault(pure_uuid, []).append({
                "existing": existing.copy(),
                "incoming": {
                    "Name": row.get("Name", ""),
                    "Alex_ID": row.get("Alex_ID", ""),
                    "Pure_UUID": pure_uuid,
                    "ORCID": row.get("ORCID", ""),
                    "Source": row.get("Source", ""),
                },
                "fields": conflict_fields,
            })
            continue

        if not existing.get("Name") and row.get("Name"):
            existing["Name"] = row["Name"]
        if not existing.get("Alex_ID") and row.get("Alex_ID"):
            existing["Alex_ID"] = row["Alex_ID"]
        if not existing.get("ORCID") and row.get("ORCID"):
            existing["ORCID"] = row["ORCID"]
        if not existing.get("Source") and row.get("Source"):
            existing["Source"] = row["Source"]

    for pure_uuid in conflicts:
        consolidated.pop(pure_uuid, None)
        logger.warning(f"Skipping Pure UUID {pure_uuid} due to conflicting matched IDs across publications")

    consolidated_list = list(consolidated.values())
    logger.info(
        f"Consolidated external person matches from {len(persons)} row(s) to "
        f"{len(consolidated_list)} unique Pure UUID(s)"
    )
    if conflicts:
        logger.warning(f"Excluded {len(conflicts)} Pure UUID(s) from update candidates because of conflicting ORCID/OpenAlex matches")
    return consolidated_list

def main(faculty_choice, test_choice, use_openalex_fallback='yes'):
    logger.info("Script to update external persons in pure from ricgraph has started")

    logger.info("The script performs the following steps:\n"
     "1. Retrieve internal persons from Ricgraph for the selected faculty.\n"
    "2. Gather their research outputs (e.g. publications).\n"
    "3. Find external co-authors linked to those outputs.\n"
    "4. Check Pure for missing ORCID/OpenAlex IDs on each external person.\n"
    "5. Export a CSV for manual review and deselect unwanted updates.\n"
    "6. press the button to apply the approved updates back to Pure.\n"
)

    faculties = select_faculties(faculty_choice)
    logger.info(f"Selected {len(faculties)} faculty key(s)")

    researchoutputs = select_persons_researchoutput(faculties)
    logger.info(f"Collected {len(researchoutputs)} research output candidate(s) from Ricgraph")

    ricgraph_persons, _ = collect_ricgraph_person_matches(researchoutputs)
    logger.info(f"Ricgraph person harvest produced {len(ricgraph_persons)} source row(s)")

    purejsons = fetch_pure_researchoutputs(researchoutputs)
    logger.info(f"Fetched {len(purejsons.get('results', []))} Pure research output record(s) for Ricgraph matching")

    ricgraph_persons = match_ricgraph_persons(ricgraph_persons, purejsons)
    logger.info(f"Ricgraph publication-level matching produced {len(ricgraph_persons)} candidate row(s)")
    output_keys_with_ricgraph_ids = {
        entry.get("researchoutput_key")
        for entry in ricgraph_persons
        if entry.get("researchoutput_key")
    }

    use_openalex_fallback = str(use_openalex_fallback).lower()
    fallback_enabled = use_openalex_fallback in {'yes', 'true', '1'}
    fallback_outputs = []
    if fallback_enabled:
        fallback_outputs = [
            entry for entry in researchoutputs
            if entry.get("researchoutput_key") not in output_keys_with_ricgraph_ids
        ]
        logger.info(
            f"OpenAlex fallback enabled; still needed for {len(fallback_outputs)} research output(s) "
            f"without usable Ricgraph person identifiers"
        )
    else:
        logger.info("OpenAlex fallback disabled; using Ricgraph-only external person identifiers")

    dois = [entry["doi"] for entry in fallback_outputs if entry.get("doi")]
    logger.info(f"Prepared {len(dois)} DOI(s) for OpenAlex matching")

    openalexjsons = {"results": [], "by_doi": {}}
    openalex_by_doi = {}
    if dois:
        openalexjsons = fetch_openalex_works(dois)
        openalex_by_doi = openalexjsons.get("by_doi", {})

    researchoutputs_with_openalex = [
        entry for entry in fallback_outputs
        if normalize_doi(entry.get("doi")) in openalex_by_doi
    ]
    logger.info(
        f"Filtered research outputs to {len(researchoutputs_with_openalex)} DOI(s) "
        f"with OpenAlex metadata"
    )

    purejsons_openalex = fetch_pure_researchoutputs(researchoutputs_with_openalex)

    filtered_dois = [entry["doi"] for entry in researchoutputs_with_openalex if entry.get("doi")]
    openalex_persons = match_all_persons(filtered_dois, openalexjsons, purejsons_openalex) if filtered_dois else []
    all_persons = ricgraph_persons + openalex_persons
    logger.info(
        f"Total matched external person candidate rows: {len(all_persons)} "
        f"(Ricgraph={len(ricgraph_persons)}, OpenAlex fallback={len(openalex_persons)})"
    )

    unique_persons = consolidate_person_matches(all_persons)
    matched_personsjson = get_external_persons_data(unique_persons)
    logger.info(f"Retrieved {len(matched_personsjson)} external person record(s) from Pure")

    update_externalpersons_pure(unique_persons, matched_personsjson, test_choice)
    logger.info(f"Script Update external persons part 1 has ended, ")

# ########################################################################
# MAIN
# ########################################################################
if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Update external persons from Ricgraph')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        # default='uu faculty: information & technology services|organization_name',
                        default='uu faculty: geosciences|organization_name',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='yes', help='Run in test mode ("yes" or "no")')
    parser.add_argument(
        'use_openalex_fallback',
        type=str,
        nargs='?',
        default='yes',
        help='Use OpenAlex as fallback when Ricgraph lacks external person IDs ("yes" or "no")'
    )
    args = parser.parse_args()
    main(args.faculty_choice, args.test_choice, args.use_openalex_fallback)
