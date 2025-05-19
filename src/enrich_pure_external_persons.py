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
# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    # Check if the ORCID is in URL format
    if orcid and orcid.startswith('https://orcid.org/'):
        # Extract just the ID part
        return orcid.split('/')[-1]
    elif orcid:
        # Return the ORCID as is, assuming it's already in the correct format
        return orcid
    else:
        # Return an empty string if orcid is None
        return ''

def extract_openalex_id(openalex):
    # Check if the openalex is in URL format
    if openalex and openalex.startswith('https://openalex.org/'):
        # Extract just the ID part
        return openalex.split('/')[-1]
    elif openalex:
        # Return the openalex as is, assuming it's already in the correct format
        return openalex
    else:
        # Return an empty string if orcid is None
        return ''

def get_ro_from_openalex(item, openalexworks):
    doi = 'https://doi.org/' + item
    url = 'https://api.openalex.org/works/' + doi
    # try:
    #     response = session.get(url, headers=OPENALEX_HEADERS)
    #     if response.status_code == 200:
    #         data = response.json()  # Directly parse JSON response
    #         return data
    # except:
    #     pass

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

    # Normalize the target DOI to ensure matching works correctly
    normalized_doi = target_doi.replace("https://doi.org/", "").lower()

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
                'ORCID': extract_orcid_id(ids['orcid'])
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
    def get_person_by_uuid(matched_personsjson, target_uuid):
        """
        Retrieves a specific person from the matched_personsjson list based on the provided uuid.

        Parameters:
        matched_personsjson (list): The list of persons from which to retrieve the specific person.
        target_uuid (str): The UUID of the person to retrieve.

        Returns:
        dict: The dictionary of the person that matches the provided uuid. Returns None if no match is found.
        """
        for person in matched_personsjson:
            if person['uuid'] == target_uuid:
                return person
        return None

    data_to_save = []  # List to store JSON objects for saving
    rows_to_update = []  # List to store rows for the DataFrame

    ro, matched_persons, updated_persons, already_ids = 0, 0, 0, 0
    for row in persons:
        uuid = row['Pure_UUID']
        matched_person = get_person_by_uuid(matched_personsjson, uuid)

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

    # Save all collected JSON objects to the file
    output_folder = "output/external_persons"  # Folder path
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
        if rows_to_update:
            ext_pers_update = pd.DataFrame(rows_to_update)
            desired_order = ['to_be_updated', 'updated', 'Name', 'Alex_ID', 'Pure_UUID', 'ORCID']
            ext_pers_update = ext_pers_update[desired_order]

            ext_pers_update.to_csv(csv_output_file, index=False)
            # logger.info(f"Saved {len(rows_to_update)} rows to {csv_output_file}")
        else:
            logger.info("No rows to save to CSV.")
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

    def split_into_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Split the UUIDs into batches of up to page_size
    uids = [person['Pure_UUID'] for person in persons]
    batches = list(split_into_batches(uids, page_size))
    session = requests.Session()

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
            response = requests.get(url, params=params)

            # response.raise_for_status()
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
    seen_dois = set()
    max_workers = 10

    def process_personroot(personroot):
        if not personroot.get('_key'):
            return []
        outputs = select_researchoutputs(personroot['_key'])
        result = []
        for output in outputs:
            doi = output["_key"].split("|")[0]
            if doi in seen_dois:
                continue
            seen_dois.add(doi)
            pure_uuid = output.get("url_other", "").split("/")[-1] \
                if "publications/" in output.get("url_other", "") else None
            result.append({
                "doi": doi,
                "pure_uuid": pure_uuid
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

    logger.info(f"Total unique DOIs found in Ricgraph: {len(seen_dois)}")
    return all_outputs




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
        allowed_methods=["POST"]
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

        logger.debug(f"Requesting batch with offset {offset}: {json_data}")

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

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_pure_researchoutputs(outputs: List[Dict], batch_size: int = 500) -> Dict:
    """
    Fetch research outputs from Pure using batched UUIDs via POST /research-outputs/search.
    Skips entries without a Pure UUID.
    Returns: {"results": [...]} list of Pure outputs.
    """
    logger.debug("Start batched fetch of research outputs from Pure by UUID")
    session = _make_session()
    url = PURE_BASE_URL.rstrip("/") + "/research-outputs/search"
    timeout = 30
    results = []

    # Filter UUIDs
    uuid_list = [entry["pure_uuid"] for entry in outputs if entry.get("pure_uuid")]
    batches = [uuid_list[i:i + batch_size] for i in range(0, len(uuid_list), batch_size)]

    for idx, batch in enumerate(batches, start=1):
        logger.info(f"Fetching batch {idx}/{len(batches)} with {len(batch)} UUIDs")

        json_data = {
            "uuids": batch,
            "size": batch_size,
            "offset": 0  # no pagination needed — we provide the full list
        }

        try:
            resp = session.post(url, headers=headers, json=json_data, timeout=timeout, verify=False)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            logger.info(f"Batch {idx} returned {len(items)} results")
            results.extend(items)
        except Exception as e:
            logger.error(f"Failed to fetch batch {idx}: {e}")

        time.sleep(0.1)

    logger.info(f"Total research outputs fetched from Pure: {len(results)}")
    return {"results": results}




from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import logging
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from config import EMAIL  # You already have this in your project

def fetch_openalex_works(dois):
    """
    Fetches works metadata from OpenAlex API for a list of DOIs.
    Uses parallel requests, retries on errors, and rate limiting.
    """
    max_workers = 1  # Safe with backoff
    batch_size = 10  # Lowered to avoid 403 errors
    request_delay = 1.2  # Throttle to avoid 429s

    headers = {
        "Accept": "application/json",
        "User-Agent": f"mailto:{EMAIL}"
    }

    def clean_dois(dois):
        return [doi.strip().rstrip(",") for doi in dois if doi]

    # Before batching
    dois = list(set(clean_dois(dois)))


    def split_batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    @retry(wait=wait_exponential(multiplier=1, min=2, max=60),
           stop=stop_after_attempt(5),
           retry=retry_if_exception_type(requests.exceptions.HTTPError))
    def fetch_batch(batch):
        pipe_dois = "|".join(batch)
        url = f"https://api.openalex.org/works?filter=doi:{pipe_dois}&per-page=50"
        try:

            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                logging.warning(f"Rate limit hit. Retrying batch starting with DOI: {batch[0]}")
                raise requests.exceptions.HTTPError("Rate limit hit (429)")
            if response.status_code == 403:
                logging.error(f"Forbidden. Likely too many DOIs in batch: {batch}")
                return []
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            # Handle pagination (rare in this use case)
            while 'next' in data.get('meta', {}):
                next_url = data['meta']['next']
                time.sleep(request_delay)
                next_resp = requests.get(next_url, headers=headers)
                next_resp.raise_for_status()
                data = next_resp.json()
                results.extend(data.get("results", []))
            return results
        except Exception as e:
            logging.error(f"Error fetching batch starting with {batch[0]}: {e}")
            return []

    all_works = []
    batches = list(split_batches(list(set(dois)), batch_size))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_batch, batch) for batch in batches]
        for future in as_completed(futures):
            results = future.result()
            all_works.extend(results)
            time.sleep(request_delay)  # Slow down between batches

    # logger.info(f"Total works found in OpenAlex: {len(all_works)}")
    return {"results": all_works}

def match_all_persons(researchoutputs, openalexjsons, purejsons):
    all_persons = []

    for doi in researchoutputs:

        persons = match_persons(doi, openalexjsons, purejsons)
        if persons:
            all_persons = all_persons + persons

    logger.info(f"total external persons found: {len(all_persons)}")
    return all_persons

def main(faculty_choice, test_choice):
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

    researchoutputs = select_persons_researchoutput(faculties)

    purejsons = fetch_pure_researchoutputs(researchoutputs)

    dois = [entry["doi"] for entry in researchoutputs if entry.get("doi")]

    openalexjsons = fetch_openalex_works(dois)

    all_persons = match_all_persons(dois, openalexjsons, purejsons)
    matched_personsjson = get_external_persons_data(all_persons)

    update_externalpersons_pure(all_persons, matched_personsjson, test_choice)
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
    args = parser.parse_args()
    main(args.faculty_choice, args.test_choice)
