# ########################################################################
# Script: pure_researchoutputs.py
#
# Description:
# This script contains **main functions** for importing research outputs into
# Pure using metadata from OpenAlex. It is intended to be used as a module
# by the `update_researchoutput_from_ricgraph.py` script and should not be
# run independently.
#
# Functions include:
# - Retrieving research outputs and journals from Pure.
# - Creating external persons and organizations.
# - Formatting research output metadata into the required JSON structure.
# - Constructing and submitting the JSON payload to the Pure API.
#
# Important:
# Do not run this script standalone. It is called as a module by
# `update_researchoutput_from_ricgraph.py`.
#
# Dependencies:
# - pandas, json, requests, configparser, dateutil, logging, etc.
#
# Author: David Grote Beverborg
# Created: April 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import pandas as pd
import json
import requests
from datetime import datetime
import configparser
import os
import logging
import pure_persons
from logging_config import setup_logging
import logging.handlers
from dateutil import parser
from config import PURE_BASE_URL, PURE_API_KEY, OPENALEXEX_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS, OPENALEX_BASE_URL, PURE_HEADERS
import time
import sys
logger = setup_logging('btp', level=logging.INFO)
def get_researchoutput(uuid):
    headers = PURE_HEADERS
    api_url = PURE_BASE_URL + 'research-outputs/' + uuid
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        logger.error(f"Error searching for research output {uuid}: {response.status_code} - {response.text}")

def create_external_person(first_name, last_name, orcid, openalex):
    """
    Creates an external person in the Pure system.

    :param first_name, last_name:  first and last names.
    :return: UUID of the newly created external person.
    """
    api_url = PURE_BASE_URL + 'external-persons/'
    url = "https://staging.research-portal.uu.nl/ws/api/external-persons"

    data = {"name": {"firstName": first_name, "lastName": last_name}}
    if openalex:
        openalexid = {
            "typeDiscriminator": "ClassifiedId",
            "id": openalex,
            "type": {
                "uri": OPENALEXEX_ID_URI,
                "term": {
                    "en_GB": "Open Alex id"
                }
            }
        }

    if orcid:
        orcidid = {
            "typeDiscriminator": "ClassifiedId",
            "id": orcid,
            "type": {
                "uri": ORCID_ID_URI,
                "term": {
                    "en_GB": "ORCID"
                }
            }
        }
    if orcid or openalex:
        data['identifiers'] = []
        if orcid:
            data['identifiers'].append(orcidid)
        if openalex:
            data['identifiers'].append(openalexid)

    json_data = json.dumps(data)

    try:
        response = requests.put(api_url, headers=PURE_HEADERS, data=json_data)

        if response.status_code in [200, 201]:
            external_person = response.json()
            return external_person.get('uuid')
        else:
            logger.error(f"Error creating external person: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logger.error(f"An error occurred while creating external person: {e}")

    return None


def find_external_person(person_ids):
    orcid = None
    openalex = None
    first_uuid = None

    # Extract ORCID and OpenAlex before the API calls
    for id_type, id_value in person_ids.items():
        if id_type.lower() == 'orcid':
            orcid = pure_persons.extract_orcid(id_value)
        elif id_type.lower() == 'openalex':
            openalex = id_value.replace("https://openalex.org/", "")

    # Perform API searches
    for id_type, id_value in person_ids.items():
        if id_type.lower() == 'orcid' and orcid:
            id_value = orcid
        elif id_type.lower() == 'openalex' and openalex:
            id_value = openalex

        data = {"searchString": id_value}
        api_url = PURE_BASE_URL + 'external-persons/search/'

        logger.debug(f"Searching for {id_type}: {id_value} with payload: {data}")

        try:
            response = requests.post(api_url, headers=PURE_HEADERS, json=data)  # Using json=data
            logger.debug(f"Response status code: {response.status_code}")

            if response.status_code == 200:
                response_data = response.json()
                items = response_data.get('items', [])

                if items:
                    first_uuid = items[0].get('uuid')
                    logger.debug(f"Person found with {id_type}: {id_value}, UUID: {first_uuid}")

                    # Stop looping if UUID is found but ensure ORCID and OpenAlex IDs are processed
                    if first_uuid:
                        break  # Exit loop since we found the UUID
            else:
                logger.debug(f"Error searching for {id_type}: {response.status_code} - {response.text}")

        except requests.RequestException as e:
            logger.debug(f"An error occurred while searching for {id_type}: {e}")

    return first_uuid, orcid, openalex





def find_extenal_orgs(affiliations):
    # 'affiliations': {'OpenAlex': 'https://openalex.org/I193662353', 'ROR': 'https://ror.org/04pp8hn57'}}
    ror = affiliations.get('ROR')

    first_uuid = None
    if ror:
        data = {"searchString": ror}
        json_data = json.dumps(data)
        api_url = PURE_BASE_URL + 'external-organizations/search/'
        try:
            response = requests.post(api_url, headers=PURE_HEADERS, data=json_data)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if items:
                    if len(items) == 1:

                        # Extract the UUID of the first item in the "items" list
                        first_uuid = data['items'][0]['uuid']
                        logger.debug(f"Person found with {ror}")

                    else:
                        logger.debug(f"Multiple or no orgs found for {ror}")
            else:
                logger.error(f"Error searching for {ror}")
        except requests.RequestException as e:
            logger.error(f"An error occurred while searching for {ror}: {e}")

    return first_uuid


def get_contributors_details(contributors, ref_date):
    persons = {}
    found_internal_person = False

    # First pass: Check for internal persons and mark if any are found
    for contributor in contributors:
        contributor_id = contributor['name']
        person_details = pure_persons.find_person(contributor, contributor['ids'], ref_date, None)
        if person_details:
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
                external_person_uuid, orcid, openalex = find_external_person(contributor['ids'])
                externalorg = find_extenal_orgs(contributor['affiliations'])

                if not external_person_uuid:

                    external_person_uuid = create_external_person(contributor['first_name'],contributor['last_name'], orcid, openalex)
                if external_person_uuid:
                    logger.debug(f'Created external person: {external_person_uuid}')
                    persons[contributor_id] = {
                        "external_person_extorgui": externalorg,
                        "external_person_uuid": external_person_uuid,
                        "external_person_first_name": contributor['first_name'],
                        "external_person_last_name": contributor['last_name']
                    }
                else:
                    logger.error(f"Failed to create external person for {contributor_id}")
    else:
        logger.debug("No internal contributors found in Pure for the research output.")
        return None

    return persons

def parse_keywords(keywords):
    data = [
            {
                "typeDiscriminator": "FreeKeywordsKeywordGroup",
                "pureId": 191008212,
                "logicalName": "keywordContainers",
                "name": {
                    "en_GB": "Keywords"
                },
                "keywords": [

                    {
                        "pureId": 191008215,
                        "locale": "en_GB",
                        "freeKeywords": keywords
                    }
                ]
            }
        ]

    return data

def get_journal_uuid(issn):
    # url = "https://staging.research-portal.uu.nl/ws/api/journals/search/"
    url = PURE_BASE_URL + '/journals/search/'
    data = {"searchString": issn}
    json_data = json.dumps(data)
    headers = PURE_HEADERS
    response = requests.post(url, headers=headers, data=json_data)
    data = response.json()
    items = data.get('items', [])

    if items:
        for item in items:

            journal_uuid = item['uuid']

        if not journal_uuid:
            journal_uuid = None
    else:
        journal_uuid = None


    return journal_uuid


def construct_research_output_json(row):
    """
    Constructs the JSON structure for a research output using data from the 'row'.
    :param row: A dictionary containing all the necessary data fields.
    :return: A dictionary representing the research output in the defined JSON format.
    """

    research_output = {
        "typeDiscriminator": "ContributionToJournal",
        "peerReview": row['peer_review'],

        "title": {"value": row['title']},
        "type": {"uri": "/dk/atira/pure/researchoutput/researchoutputtypes/contributiontojournal/article"},
        "category": {"uri": "/dk/atira/pure/researchoutput/category/academic"},
        "publicationStatuses": [{
            "current": True,
            "publicationStatus": {"uri": "/dk/atira/pure/researchoutput/status/published"},
            "publicationDate": {"year": row['publication_year'], "month": row['publication_month']}
        }],
        "language": {"uri": row['language_uri']},
        "contributors": row['parsed_contributors'],
        "organizations": row['parsed_organizations'],
        "totalNumberOfContributors": len(row['contributors']),
        "managingOrganization": {"systemName": "Organization", "uuid": row['managing_org']},
        "electronicVersions": [{
            "typeDiscriminator": "DoiElectronicVersion",
            "accessType": {"uri": "/dk/atira/pure/core/openaccesspermission/unknown"},
            "doi": row['doi'],
            "versionType": {"uri": "/dk/atira/pure/researchoutput/electronicversion/versiontype/publishersversion"}
        }],
        "keywordGroups":  row['keywords'],
        # "links": [{"url": f"{row['doi']}"}],
        "visibility": {"key": row['visibility_key']},
        "workflow": {"step": row['workflow_step']},
        "identifiers": [
            # Include any identifiers as required
        ],
        "journalAssociation": {
            "journal": {"systemName": "Journal", "uuid": row['journal']}
        },
        "systemName": "ResearchOutput"
    }

    # Conditionally add 'externalOrganizations' if 'ext_organization_uuids' exists and is not empty
    if 'formatted_ext_organizations' in row and row['formatted_ext_organizations']:
        research_output["externalOrganizations"] = row['formatted_ext_organizations']
    return research_output



def format_organizations_from_contributors(contributors):
    """
       Extracts and formats organization UUIDs from contributors' details.
       Includes a default organization UUID if no others are found.
       :param contributors: List of contributors with their details, including association UUIDs.
       :param default_uuid: The default organization UUID to use if no others are found.
       :return: A list of dictionaries, each representing an organization.
       """
    organization_uuids = set()
    ext_organization_uuids = set()
    default_uuid = 'UU_uuid'
    managing_org = None

    for name, details in contributors.items():
        logger.debug(f"Processing {name}")

        # Set managing_org only for the first contributor
        if details:
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

            # Handle external organizations

            if 'external_person_extorgui' in details and details['external_person_extorgui']:
                # Add the entire UUID string as one element to the set
                ext_organization_uuids.add(details['external_person_extorgui'])

                logger.debug(f"Found external organizations for {details['external_person_extorgui']}")


    # if not organization_uuids:
    #     logger.info("No organization UUIDs found, adding default")  # Debugging print
    #     organization_uuids.add(default_uuid)

    formatted_organizations = [{"systemName": "Organization", "uuid": uuid} for uuid in organization_uuids]
    formatted_ext_organizations = [{"systemName": "ExternalOrganization", "uuid": uuid} for uuid in ext_organization_uuids]

    if not managing_org:

        managing_org = None
    return formatted_organizations, formatted_ext_organizations, managing_org

def format_contributors(contributors_data):
    formatted_contributors = []
    # removing duplicate uuid's (that might be there to multiple aa
    for name, details in contributors_data.items():
        logger.debug(f"Processing {name}")

        if details is not None and 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):
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

        if details is not None:

            if 'uuid' in details:  # Internal Contributor

                contributor = {
                    "typeDiscriminator": "InternalContributorAssociation",
                    "hidden": False,
                    "correspondingAuthor": False,  # Set appropriately if information available
                    "name": {
                        "firstName": details['firstName'],
                        "lastName": details['lastName']
                    },
                    "role": {
                        "uri": "/dk/atira/pure/researchoutput/roles/contributiontojournal/author",
                        "term": {"en_GB": "Author"}
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
                if details["external_person_extorgui"]:
                    contributor = {
                        "typeDiscriminator": "ExternalContributorAssociation",
                        "externalOrganizations": [
                            {
                                "systemName": "ExternalOrganization",
                                "uuid": details["external_person_extorgui"]
                            }
                        ],

                        "name": {
                            "firstName": details['external_person_first_name'],
                            "lastName": details['external_person_last_name']
                        },
                        "role": {
                            "uri": "/dk/atira/pure/researchoutput/roles/contributiontojournal/author",
                            "term": {"en_GB": "Author"}
                        },
                        "externalPerson": {
                            "systemName": "ExternalPerson",
                            "uuid": details['external_person_uuid']
                        }
                    }
                else:
                    contributor = {
                        "typeDiscriminator": "ExternalContributorAssociation",

                        "name": {
                            "firstName": details['external_person_first_name'],
                            "lastName": details['external_person_last_name']
                        },
                        "role": {
                            "uri": "/dk/atira/pure/researchoutput/roles/contributiontojournal/author",
                            "term": {"en_GB": "Author"}
                        },
                        "externalPerson": {
                            "systemName": "ExternalPerson",
                            "uuid": details['external_person_uuid']
                        }
                    }


            formatted_contributors.append(contributor)

    return formatted_contributors


def create_research_output(research_output_json):
    url = " https://staging.research-portal.uu.nl/ws/api/research-outputs"
    json_data = json.dumps(research_output_json)

    # Open a file for writing
    with open('test123.json', 'w') as file:
        json.dump(json_data, file, indent=4)
    # Make the put request
    headers = PURE_HEADERS
    response = requests.put(url, headers=headers, data=json_data)
    if response.status_code in [200, 201]:
        logger.debug(f"created researchoutput: {response.status_code} ")
    else:
        output_file = "research_output.jsonerror"

        # Write the dictionary to a JSON file
        with open(output_file, 'w') as json_file:
            json.dump(research_output_json, json_file, indent=4)
        logger.error(f"Error creating research output: {response.status_code} - {response.text}")


    return 'test'


def get_supervisors(supervisors, ref_date):
    persons = {}
    found_internal_person = False
    # First pass: Check for internal persons and mark if any are found
    for supervisor in supervisors:

        supervisor_id = supervisor['name']
        person_details = pure_persons.find_person(supervisor['name'], supervisor['ids'], ref_date, None)

        if person_details:
            persons[supervisor_id] = person_details
            person_details['role'] = supervisor['role']
            found_internal_person = True
        else:
            # Mark as None for now
            persons[supervisor_id] = None

            # Second pass: Create external persons only if an internal person is found
    if found_internal_person:
        for supervisor in supervisors:
            supervisor_id = supervisor['name']
            if persons[supervisor_id] is None:  # This contributor needs an external person
                logger.debug(f"Creating external person for {supervisor_id}.")
                external_person_uuid = create_external_person(supervisor['first_name'], supervisor['last_name'])

                if external_person_uuid:
                    logger.debug(f'Created external person: {external_person_uuid}')
                    persons[supervisor_id] = {
                        "external_person_uuid": external_person_uuid,
                        "external_person_first_name": supervisor['first_name'],
                        "external_person_last_name": supervisor['last_name'],
                        "external_person_role": supervisor['role']
                    }
                else:
                    logger.error(f"Failed to create external person for {supervisor_id}")
    else:
        logger.error("No internal contributors found in Pure for the research output.")
        return None

    return persons

    pass


def format_supervisors(param):
    pass


def unique_fields_per_type(row):
    error = False

    reason = row['type']
    if row['type'] == 'article':

        if row['journal_issn'] or row['journal_issn'] == None:
            if row['journal_issn'] == 'No ISSN' or  row['journal_issn'] == '':
                error = True
                reason = f"No ISSN for {row['title']}"
                logger.debug(f"No ISSN for {row['title']}")
            else:
                # Process article type

                row['journal'] = get_journal_uuid(row['journal_issn'])
                if row['journal'] == None:
                    error = True
                    reason = f"No ISSN for {row['title']}"
                    logger.debug(f"No ISSN for {row['title']}")
        else:
            error = True
            reason = f"No ISSN for {row['title']}"
            logger.debug(f"No ISSN for {row['title']}")

    elif row['type'] == 'dissertation':
        # Process dissertation type
        row['award_data'] == '2'
        row['supervisors'] == get_supervisors(row['supervisors'], row['publication_date'])
        row['parsed_supervisors'] == format_supervisors(row['supervisors'])
        pass
    elif row['type'] == 'book':
        # Process book type
        pass
    elif row['type'] == 'conference proceeding':
        # Process conference proceeding type
        pass
    else:
        # Handle other types or unexpected values
        pass

    return row, error, reason


def format_rest(row):
    row['keywords'] = parse_keywords(row['keywords'])

    # doi
    row['electronic_version'] = {
        "typeDiscriminator": "DoiElectronicVersion",
        "accessType": {
            "uri": "/dk/atira/pure/core/openaccesspermission/unknown",
            "term": {
                "en_GB": "unknown"
            }
        },
        "visibleOnPortalDate": row['publication_date'],
        "doi": row['doi'],
        "versionType": {
            "uri": "/dk/atira/pure/researchoutput/electronicversion/versiontype/publishersversion",
            "term": {
                "en_GB": "Final published version"
            }
        }
    }

    return row


def check_research_in_pure(doi):

    exists_in_pure =  False
    headers = PURE_HEADERS
    doi = doi.split("org/")[-1]
    data = {"searchString": doi}

    json_data = json.dumps(data)
    api_url = PURE_BASE_URL + 'research-outputs/search/'

    response = requests.post(api_url, headers=headers, data=json_data)

    if response.status_code == 200:
        data = response.json()

        items = data.get('items', [])

        for item in items:
            electronic_versions = item.get('electronicVersions', [])
            for version in electronic_versions:
                if 'doi' in version:
                    full_doi_url = version['doi']
                    if doi == full_doi_url:
                        exists_in_pure = True
                        logger.debug(f"{doi}: already in pure")

    return exists_in_pure


def df_to_pure(df):
    # Initialize a list to collect all research output JSONs
    research_output_collection = []
    success = 0
    error = 0
    inpure = 0

    # Initialize a list to hold rows for the "to be updated" DataFrame
    to_be_updated_rows = []
    logger.info('Formatting the output in Pure needed JSON format. This is a slow process, you might want to get some coffee...')
    for index, row in df.iterrows():
        if index % 25 == 0:  # Print progress every 5 iterations
            logger.info(f"Processing: {index}")
            time.sleep(0.1)  # Simulate work
        try:
            logger.debug('Processing research output: %s', row['title'])

            # Check if the research output exists in Pure
            exists_in_pure = check_research_in_pure(row['doi'])

            if not exists_in_pure:
                # Get contributor details
                contributors_details = get_contributors_details(row['contributors'], row['publication_date'])
                if contributors_details:

                    # Format and enrich row data
                    row['parsed_contributors'] = format_contributors(contributors_details)
                    parsed_orgs, formatted_ext_orgs, managing_org = format_organizations_from_contributors(
                        contributors_details)
                    row['parsed_organizations'] = parsed_orgs
                    row['formatted_ext_organizations'] = formatted_ext_orgs


                    # Additional formatting and validation
                    enriched_row = format_rest(row)
                    enriched_row, error_flag, reason = unique_fields_per_type(enriched_row)

                    if managing_org:
                        row['managing_org'] = managing_org
                    else:
                        error_flag = True
                        reason =  'no managing organization found'

                    if not error_flag:
                        research_output_json = construct_research_output_json(enriched_row)
                        research_output_collection.append(research_output_json)  # Add to the collection

                        # Add data to 'to be updated' list
                        to_be_updated_rows.append({
                            'to_be_updated': 'x',
                            'updated': ' ',
                            'doi': row['doi'],
                            'title': row['title']
                        })

                        success += 1
                    else:
                        logger.warning(f"Skipped research output {row['research_output_id']}. {reason}")
                        error += 1
                else:
                    logger.warning(
                        f"Skipped research output {row['research_output_id']} due to missing contributor details.")
                    error += 1
            else:
                inpure += 1
                logger.debug(f"already in pure {row['doi']}.")
        except Exception as e:
            logger.info(f"Error processing row {index}: {e}")
            error += 1

    # Save the collected research outputs to a JSON file
    output_dir = 'output/research_output'
    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists
    output_file = os.path.join(output_dir, 'output_to_be_updated.json')

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(research_output_collection, f, ensure_ascii=False, indent=4)
        logger.debug(f"Successfully saved research outputs to {output_file}.")
    except Exception as e:
        logger.error(f"Failed to save research outputs: {e}")

    # Create and save the "to be updated" DataFrame
    to_be_updated_df = pd.DataFrame(to_be_updated_rows)
    csv_output_file = os.path.join(output_dir, 'to_be_updated.csv')

    logger.info(f"{error} items cannot be imported in pure, see reasons above")
    logger.info(f"{inpure} items are already in pure")
    logger.info(f"{success} items can be updated")
    print(" ")
    logger.info(f"Research output that can be imported are in file: {csv_output_file}")
    # logger.info(f"Please open that file to check if you want them all to be updated")
    # logger.info(f"if not, please remove the 'X' for that row in the column 'to_be_updated'")



    try:
        to_be_updated_df.to_csv(csv_output_file, index=False, encoding='utf-8')
        logger.debug(f"Successfully saved 'to be updated' DataFrame to {csv_output_file}.")
    except Exception as e:
        logger.error(f"Failed to save 'to be updated' DataFrame: {e}")







