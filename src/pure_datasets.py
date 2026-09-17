# ########################################################################
#
# pure datasets - import module that uses a json export of YODA or dataset-dois
# and datacite to import datasets in Pure
#
# ########################################################################
#
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################
#
# This file contains example code for Ricgraph.
#
# With this code, you can harvest persons and research outputs from OpenAlex.
# You have to set some parameters in ricgraph.ini.
# Also, you can set a number of parameters in the code following the "import" statements below.
#
# Original version David Grote Beverborg, april 2024
#
# ########################################################################
#
# Usage
#
# Options:
#   --source options <Yoda|Ricgraph>
#
#
# ########################################################################
import pandas as pd
import json
import requests
import configparser
import os
import logging
import pure_persons
import yoda_utils
import datacite_utils
import logging.handlers
from pathlib import Path
from datetime import datetime
from config import PURE_BASE_URL, DEFAULTS, ORCID_ID_URI, PURE_HEADERS, RIC_BASE_URL, OPENALEX_HEADERS, OPENALEX_BASE_URL, PURE_HEADERS, TYPE_URI
from logging_config import setup_logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = setup_logging('btp', level=logging.INFO)
session = requests.Session()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "PUT"],
    backoff_factor=1,
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))

def get_headers(api_key):
    """Constructs the header required for API requests."""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api-key": api_key
    }
def split_name(name):
    """Splits a full name into first name and last name.

    Args:
        name (str): The full name to split.

    Returns:
        tuple: A tuple containing the first name and last name.
    """
    if not name or not isinstance(name, str):
        return None, None  # or raise ValueError("Invalid name")

    name = name.strip()
    parts = name.split(' ', 1)

    if len(parts) == 1:
        return parts[0], ''  # or handle as a special case
    else:
        return parts[0], parts[1]
def format_doi(doi):
    """
    Format a DOI to always start with 'doi.org/' and not include 'https://'.
    Only formats strings that contain 'doi'.
    """
    if doi is None:
        return None
    if 'doi' in doi.lower():
        if doi.startswith('https://doi.org/'):
            doi = doi.replace('https://doi.org/', '')
        if doi.startswith('doi.org/'):
            doi = doi.replace('doi.org/', '')
    return doi

def request_dataset_by_uuid(uuid):
    """Request dataset details by UUID."""
    api_url = f"{PURE_BASE_URL}data-sets/{uuid}"
    response = session.get(api_url, headers=PURE_HEADERS, timeout=30)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to fetch dataset by UUID {uuid}: {response.status_code} - {response.text}")
        return None
def search_dataset_by_string(search_string):
    """Search for datasets using a search string."""
    search_string = format_doi(search_string)
    data = {"searchString": search_string}
    json_data = json.dumps(data)
    api_url = f"{PURE_BASE_URL}data-sets/search/"
    response = session.post(api_url, headers=PURE_HEADERS, data=json_data, timeout=30)
    if response.status_code == 200:
        return response.json().get('items', [])
    else:
        logger.error(f"Failed to search datasets by string {search_string}: {response.status_code} - {response.text}")
        return []
def find_dataset(uuid, search_string):
    """Finds a single dataset in the pure system by UUID or search string."""
    if uuid:
        data = request_dataset_by_uuid(uuid)
        if data:
            return [data['uuid']]

    if not uuid and search_string:
        items = search_dataset_by_string(search_string)
        return [item['uuid'] for item in items if 'uuid' in item]


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
def create_external_person(first_name, last_name, orcid):
    """
    Creates an external person in the Pure system.
    :param first_name, last_name:  first and last names.
    :return: UUID of the newly created external person.
    """
    api_url = PURE_BASE_URL + 'external-persons/'
    data = {"name": {"firstName": first_name, "lastName": last_name}}



    # Create new ORCID object if available
    if orcid:
        data['identifiers'] = []
        orcid  = extract_orcid_id(orcid)
        new_orcid = {
            "typeDiscriminator": "ClassifiedId",
            "id": orcid,
            "type": {
                "uri": ORCID_ID_URI,
                "term": {
                    "en_GB": "ORCID"
                }
            }
        }
        data['identifiers'].append(new_orcid)
    json_data = json.dumps(data)

    try:
        response = session.put(api_url, headers=PURE_HEADERS, data=json_data, timeout=30)
        if response.status_code in [200, 201]:
            external_person = response.json()
            return external_person.get('uuid')
        else:
            logger.error(f"Error creating external person: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logger.error(f"An error occurred while creating external person: {e}")

    return None


def _build_pending_external_person(first_name, last_name, orcid, openalex):
    return {
        "first_name": first_name,
        "last_name": last_name,
        "orcid": orcid or "",
        "openalex": openalex or "",
    }
def get_contributors_details(contributors, ref_date):
    persons = {}
    found_internal_person = False

    # First pass: Check for internal persons and mark if any are found
    for contributor in contributors:
        contributor['name'] = contributor['first_name'] + ' ' + contributor['last_name']
        contributor_id = contributor['name']
        person_details = pure_persons.find_person(contributor, contributor['person_ids'], ref_date, contributor['type'])

        if person_details:
            person_details['type'] = contributor['type']
            persons[contributor_id] = person_details
            found_internal_person = True
        else:
            # Mark as None for now
            persons[contributor_id] = None
            # Second pass: Find/Create external persons only if an internal person is found
    if found_internal_person:
        for contributor in contributors:
            contributor_id = contributor['name']
            if persons[contributor_id] is None:  # This contributor needs an external person
                # check if external persons already exists based on id's
                # if so add uuid of ext pers to external_person_uuid
                # if not create external person
                # same for affiliations of external person
                person_details = {}
                external_person_uuid, orcid, openalex = pure_persons.find_external_person(contributor['person_ids'])

                person_details['type'] = contributor['type']

                # externalorg = pure_persons.find_extenal_orgs(contributor['affiliations'])
                externalorg = None

                persons[contributor_id] = {
                    "external_person_extorgui": externalorg,
                    "external_person_uuid": external_person_uuid,
                    "external_person_first_name": contributor['first_name'],
                    "external_person_last_name": contributor['last_name'],
                    "person_details": person_details
                }
                if not external_person_uuid:
                    persons[contributor_id]["_btp_external_person"] = _build_pending_external_person(
                        contributor['first_name'],
                        contributor['last_name'],
                        orcid,
                        openalex,
                    )
    else:
        persons ={}
        logger.debug("No internal contributors found in Pure for the dataset.")
        return None

    return persons
def format_contributors(contributors_data):
    formatted_contributors = []
    # removing duplicate uuid's (sometimes a pure person has two affils with same org id)

    for name, details in contributors_data.items():
        logger.debug(f"Processing {name}")
        if details and 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):

            unique_uuids = set()
            unique_association_dicts = []

            for assoc in details['associationsUUIDs']:
                if assoc['uuid'] not in unique_uuids:
                    unique_uuids.add(assoc['uuid'])
                    unique_association_dicts.append(assoc)

            details['associationsUUIDs'] = unique_association_dicts
        else:
            logger.debug(f"No associations found for {name}")

    for name, details in contributors_data.items():
        logger.debug(f"Processing {name}")
        if details is not None and 'uuid' in details:
            type_uri = TYPE_URI[details['type']]
            contributor = {
                "typeDiscriminator": "InternalDataSetPersonAssociation",
                "name": {
                    "firstName": details['firstName'],
                    "lastName": details['lastName']
                },
                "role": {
                    "uri": type_uri,
                    # "term": {"en_GB": details['type']}
                },
                "person": {
                    "systemName": "Person",
                    "uuid": details['uuid']
                },
                "organizations": [
                    {"systemName": "Organization", "uuid": org['uuid']} for org in details['associationsUUIDs']
                ]

            }
        else:  # External Contributor
            type_uri = '/dk/atira/pure/dataset/roles/dataset/contributor'
            contributor = {
                "typeDiscriminator": "ExternalDataSetPersonAssociation",
                "name": {
                    "firstName": details['external_person_first_name'],
                    "lastName": details['external_person_last_name']
                },
                "role": {
                    "uri": type_uri,
                    # "term": {"en_GB": details['type']}
                }
            }
            if details.get('external_person_uuid'):
                contributor["externalPerson"] = {
                    "systemName": "ExternalPerson",
                    "uuid": details['external_person_uuid']
                }
            if details.get("_btp_external_person") and not details.get('external_person_uuid'):
                contributor["_btp_external_person"] = details["_btp_external_person"]

        formatted_contributors.append(contributor)

    return formatted_contributors


def _ensure_dataset_external_people(dataset_json):
    created_external_people = []
    for contributor in dataset_json.get("persons", []):
        if contributor.get("externalPerson", {}).get("uuid"):
            contributor.pop("_btp_external_person", None)
            continue

        pending = contributor.pop("_btp_external_person", None)
        if not pending:
            continue

        person_ids = {}
        if pending.get("orcid"):
            person_ids["orcid"] = pending["orcid"]
        if pending.get("openalex"):
            person_ids["openalex"] = pending["openalex"]

        external_person_uuid = None
        if person_ids:
            external_person_uuid, _, _ = pure_persons.find_external_person(person_ids)
        if not external_person_uuid:
            external_person_uuid = create_external_person(
                pending.get("first_name", ""),
                pending.get("last_name", ""),
                pending.get("orcid", ""),
            )
            if external_person_uuid:
                created_external_people.append(
                    {
                        "uuid": external_person_uuid,
                        "first_name": pending.get("first_name", ""),
                        "last_name": pending.get("last_name", ""),
                        "orcid": pending.get("orcid", ""),
                        "openalex": pending.get("openalex", ""),
                    }
                )

        if not external_person_uuid:
            raise RuntimeError(
                f"Could not resolve or create external person for "
                f"{pending.get('first_name', '')} {pending.get('last_name', '')}".strip()
            )

        contributor["externalPerson"] = {
            "systemName": "ExternalPerson",
            "uuid": external_person_uuid,
        }

    return created_external_people
def format_organizations_from_contributors(contributors):
    """
       Extracts and formats organization UUIDs from contributors' details.
       Includes a default organization UUID if no others are found.
       :param contributors: List of contributors with their details, including association UUIDs.
       :param default_uuid: The default organization UUID to use if no others are found.
       :return: A list of dictionaries, each representing an organization.
       """
    organization_uuids = set()
    default_uuid = DEFAULTS['university']
    managing_org = None
    for name, details in contributors.items():
        logger.debug(f"Processing {name}")
        # Set managing_org only for the first contributor
        if managing_org is None and 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'],
                                                                                  list) and details['associationsUUIDs']:
            managing_org = details['associationsUUIDs'][0]['uuid']
            logger.debug(f"Managing organization for {name}: {managing_org}")

        # Check if 'associationsUUIDs' is in details and is a list
        if 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):
            # Extract the uuids from the list of dictionaries
            association_uuids = [assoc['uuid'] for assoc in details['associationsUUIDs']]
            organization_uuids.update(association_uuids)
            logger.debug(f"Found associations for {name}: {association_uuids}")
        else:
            logger.debug(f"No associations found for {name}")  # Debugging

    if not organization_uuids:
        logger.debug("No organization UUIDs found, adding default")  # Debugging print
        organization_uuids.add(default_uuid)

    # Remove duplicate UUIDs from organization_uuids
    unique_organization_uuids = list(set(organization_uuids))

    formatted_organizations = [{"systemName": "Organization", "uuid": uuid} for uuid in unique_organization_uuids]
    if not managing_org:
        managing_org = default_uuid
    return formatted_organizations, managing_org

def find_publisher(publisher):
    data = {"searchString": publisher}
    publisher_id =  None
    json_data = json.dumps(data)
    api_url = PURE_BASE_URL + 'publishers/search/'
    try:
        response = session.post(api_url, headers=PURE_HEADERS, data=json_data, timeout=30)

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if items:
                for item in items:
                    uuid = item.get('uuid')
                    name = item.get('name')
                    if name == publisher:
                        publisher_uuid = uuid
                        return publisher_uuid
                if not publisher_id:
                    publisher_uuid = DEFAULTS['publisher']
            else:
                publisher_uuid = DEFAULTS['publisher']
            return publisher_uuid

        else:
    #         default publisher
            publisher_uuid = DEFAULTS['publisher']
            return publisher_uuid

    except requests.RequestException as e:
        logging.error(f"An error occurred while searching for publisher: {publisher}: {e}")
def format_description(description):

    description_object = {

        "value": {
            # Provide string values for each submission locale. Replace 'en', 'fr', etc. with actual locales
            "en_GB": description
        },
        "type": {
            "uri": "/dk/atira/pure/dataset/descriptions/datasetdescription",  # Replace with actual classification URI
            "term": {

                "en_GB": "Description",

            }
        }
    }

    return description_object
def construct_dataset_json(row):
    
    publisher_uuid = find_publisher(row['publisher'])
    description = format_description(row['description'])
    date_str = row['created'][:10]
    year, month, day = date_str.split("-")
    if not row['doi']:
        row['doi'] = 'n/a'
    dataset = {
         "title": {"en_GB": row['title']},
         "descriptions": [description],
         "doi": {"doi": row['doi']},
         "type": {"uri": TYPE_URI['type_dataset']},
         "publisher": {"systemName": "Publisher", "uuid": publisher_uuid},
         "publicationAvailableDate": {"year": year, "month": month, "day": day},
         "managingOrganization": {"systemName": "Organization", "uuid": row['managing_org']},
         "persons": row['parsed_contributors'],
         "organizations": row['parsed_organizations'],
         "visibility": {"key": DEFAULTS['visibility_key']}
        }
    return dataset
def create_dataset(dataset_json):
    url = PURE_BASE_URL + 'data-sets'
    payload = json.loads(json.dumps(dataset_json))
    created_external_people = _ensure_dataset_external_people(payload)
    json_data = json.dumps(payload)

    response = session.put(url, headers=PURE_HEADERS, data=json_data, timeout=30)
    if response.status_code in [200, 201]:
        data = response.json()
        logger.info(f"created dataset: {response.status_code} - {data['uuid']}")
        return {
            "success": True,
            "uuid": data['uuid'],
            "created_external_persons": created_external_people,
        }
    else:
        logging.error(f"Error creating dataset {response.status_code} - {response.text}")
        # Print the entire response to see all available details
        return {
            "success": False,
            "uuid": None,
            "created_external_persons": created_external_people,
        }
        try:
            response_json = response.json()  # If response is JSON

        except json.JSONDecodeError:
            print(response.text)  # If response is not JSON, print raw text

def user_choice():
    """
        Prompts the user to choose the data source (1 for DOIs, 2 for JSON file) and returns the respective DataFrame.
        Continues to prompt until a valid choice is made.
        """
    while True:  # Loop until a valid input is received
        choice = input("Choose the data source:\n1 - Load from Rickgraph DOIs\n2 - Load from YODA JSON file\nEnter 1 or 2: ")
        if choice.strip() == '1':
            datasets = ['10.6084/M9.FIGSHARE.21829182', '10.5061/dryad.tn70pf1', 'DOI3']
            df = datacite_utils.get_df_from_datacite(datasets)
            break  # Exit loop after successful operation
        elif choice.strip() == '2':
            file_path = 'source_files/export.json'
            df = yoda_utils.get_df_from_yoda(file_path)
            break  # Exit loop after successful operation
        else:
            print("Invalid choice. Please enter 1 or 2.")  # Prompt again if input is not valid

    return df
def main():
    df = user_choice()
#     df = user_choice()
#     created = 0
#     ignored = 0
#     for _, row in df.iterrows():
#
#         already_in_pure = find_dataset(None, row['doi'])
#
#         if already_in_pure:
#             logging.info(f"dataset with doi: {row['doi']}, already in pure")
#             print("skipped ", row['doi'], ' , is already in pure')
#             ignored += 1
#         else:
#            print(row['persons'])
#            contributors_details = get_contributors_details(row['persons'], row['publication_year'], row['title'])
#            if contributors_details and not already_in_pure:
#                 row['parsed_contributors'] = format_contributors(contributors_details)
#                 row['parsed_organizations'], row['managing_org'] = format_organizations_from_contributors(
#                     contributors_details)
#
#                 # Construct the dataset JSON
#                 dataset_json = construct_dataset_json(row)
#                 uuid_ds = create_dataset(dataset_json)
#                 if not uuid_ds == 'error':
#                     print ('created dataset: ', uuid_ds)
#                     created += 1
#                 else:
#                     ignored += 1
#                     print('error creating dataset: ', row['title'])
#            else:
#                 ignored += 1
#
#     # summary
#     logging.info(f"Process completed. created datasets: {created}, ignored: {ignored}")
#     print (f"Process completed. created datasets: {created}, skipped: {ignored}")
#
#
# # Load configuration from file
# config_path = 'config.ini'
# if not os.path.exists(config_path):
#     raise FileNotFoundError(f"The configuration file {config_path} does not exist.")
#
# config = configparser.ConfigParser()
# config.read('config.ini')
#
# BASE_URL = config['API']['BaseURL']
# API_KEY = config['API']['APIKey']
# BASE_ORG = config['DEFAULTS']['university']
# # Setup logging
# logger = setup_logging()
# # Headers for API requests
# headers = get_headers(API_KEY)

if __name__ == '__main__':
    main()
