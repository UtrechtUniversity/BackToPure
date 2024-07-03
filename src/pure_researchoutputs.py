# ########################################################################
#
# pure research outputs - import module that uses a an export of dois of
# Ricgraph to import research outputs in Pure. The metadata is imported
# from open alex
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
from datetime import datetime
import configparser
import os
import logging
import pure_persons
import openalex_utils
import logging.handlers
from dateutil import parser
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, OPENALEX_HEADERS, OPENALEX_BASE_URL, PURE_HEADERS

def get_researchoutput(uuid):
    headers = PURE_HEADERS
    api_url = PURE_BASE_URL + 'research-outputs/' + uuid
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        logging.error(f"Error searching for research output {uuid}: {response.status_code} - {response.text}")

def create_external_person(first_name, last_name):
    """
    Creates an external person in the Pure system.

    :param first_name, last_name:  first and last names.
    :return: UUID of the newly created external person.
    """
    api_url = PURE_BASE_URL + 'external-persons/'
    url = "https://staging.research-portal.uu.nl/ws/api/external-persons"

    data = {"name": {"firstName": first_name, "lastName": last_name}}
    json_data = json.dumps(data)

    try:
        response = requests.put(api_url, headers=PURE_HEADERS, data=json_data)

        if response.status_code in [200, 201]:
            external_person = response.json()
            return external_person.get('uuid')
        else:
            logging.error(f"Error creating external person: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logging.error(f"An error occurred while creating external person: {e}")

    return None


def get_contributors_details(contributors, ref_date, test):
    persons = {}
    found_internal_person = False

    # First pass: Check for internal persons and mark if any are found
    for contributor in contributors:

        contributor_id = contributor['name']
        person_details = pure_persons.find_person(contributor['name'], contributor['ids'], ref_date)

        if person_details:
            persons[contributor_id] = person_details
            found_internal_person = True
        else:
            # Mark as None for now
            persons[contributor_id] = None


            # Second pass: Create external persons only if an internal person is found
    if found_internal_person:
        for contributor in contributors:
            contributor_id = contributor['name']
            if persons[contributor_id] is None:  # This contributor needs an external person
                logging.debug(f"Creating external person for {contributor_id}.")
                if test == 'no':
                    external_person_uuid = create_external_person(contributor['first_name'],contributor['last_name'])

                    if external_person_uuid:
                        logging.debug(f'Created external person: {external_person_uuid}')
                        persons[contributor_id] = {
                            "external_person_uuid": external_person_uuid,
                            "external_person_first_name": contributor['first_name'],
                            "external_person_last_name": contributor['last_name']
                        }
                    else:
                        logging.error(f"Failed to create external person for {contributor_id}")
    else:
        logging.error("No internal contributors found in Pure for the research output.")
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
    url = "https://staging.research-portal.uu.nl/ws/api/journals/search/"
    # url = BASE_URL + '/journals/search/'
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
        "links": [{"url": f"https://doi.org/{row['doi']}"}],
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
    default_uuid = 'UU_uuid'
    managing_org = None
    for name, details in contributors.items():
        logging.info(f"Processing {name}")

        # Set managing_org only for the first contributor
        if details:
            if managing_org is None and 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'],
                                                                                      list) and details['associationsUUIDs']:
                managing_org = details['associationsUUIDs'][0]['uuid']

                logging.info(f"Managing organization for {name}: {managing_org}")

            # Check if 'associationsUUIDs' is in details and is a list
            if 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):
                # Extract the uuids from the list of dictionaries
                association_uuids = [assoc['uuid'] for assoc in details['associationsUUIDs']]

                organization_uuids.update(association_uuids)

                logging.info(f"Found associations for {name}: {association_uuids}")

            else:
                logging.info(f"No associations found for {name}")  # Debugging


    # if not organization_uuids:
    #     logging.info("No organization UUIDs found, adding default")  # Debugging print
    #     organization_uuids.add(default_uuid)

    formatted_organizations = [{"systemName": "Organization", "uuid": uuid} for uuid in organization_uuids]
    if not managing_org:
        managing_org = None
    return formatted_organizations, managing_org

def format_contributors(contributors_data):
    formatted_contributors = []
    # removing duplicate uuid's (that might be there to multiple aa
    for name, details in contributors_data.items():
        logging.info(f"Processing {name}")

        if details is not None and 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):
            unique_uuids = set()
            unique_association_dicts = []

            for assoc in details['associationsUUIDs']:
                if assoc['uuid'] not in unique_uuids:
                    unique_uuids.add(assoc['uuid'])
                    unique_association_dicts.append(assoc)

            details['associationsUUIDs'] = unique_association_dicts
            logging.info(f"Deduplicated associations for {name}: {details['associationsUUIDs']}")
        else:
            logging.info(f"No associations found for {name}")

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
                contributor = {
                    "typeDiscriminator": "ExternalContributorAssociation",
                    # Assuming pureId and country details are available, or else set default or fetch
                    # "externalOrganizations": [],  # Placeholder: Populate if organization data available
                    # "country": {
                    #     "uri": "/dk/atira/pure/core/countries/de",  # Placeholder: Replace with actual country URI
                    #     "term": {"en_GB": "Germany"}  # Placeholder: Replace with actual country

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
        logging.info(f"created researchoutput: {response.status_code} - {response.text}")
    else:
        logging.error(f"Error creating research output: {response.status_code} - {response.text}")


    return 'test'


def get_supervisors(supervisors, ref_date):
    persons = {}
    found_internal_person = False
    # First pass: Check for internal persons and mark if any are found
    for supervisor in supervisors:

        supervisor_id = supervisor['name']
        person_details = pure_persons.find_person(supervisor['name'], supervisor['ids'], ref_date)

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
                logging.info(f"Creating external person for {supervisor_id}.")
                external_person_uuid = create_external_person(supervisor['first_name'], supervisor['last_name'])

                if external_person_uuid:
                    logging.info(f'Created external person: {external_person_uuid}')
                    persons[supervisor_id] = {
                        "external_person_uuid": external_person_uuid,
                        "external_person_first_name": supervisor['first_name'],
                        "external_person_last_name": supervisor['last_name'],
                        "external_person_role": supervisor['role']
                    }
                else:
                    logging.error(f"Failed to create external person for {supervisor_id}")
    else:
        logging.error("No internal contributors found in Pure for the research output.")
        return None

    return persons

    pass


def format_supervisors(param):
    pass


def unique_fields_per_type(row):
    error = False
    if row['type'] == 'article':

        if row['journal_issn'] == 'No ISSN':
            error = True
            logging.error(f"No ISSN for {row['title']}")
        else:
            # Process article type

            row['journal'] = get_journal_uuid(row['journal_issn'])
            if row['journal'] == None:
                error = True
                logging.error(f"No ISSN for {row['title']}")

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

    return row, error


def format_rest(row):


    row['keywords'] = parse_keywords(row['keywords'])


    return row


def df_to_pure(df, test):
    error = 0
    succes = 0
    for _, row in df.iterrows():
        print('processing', row['title'])
        contributors_details = get_contributors_details(row['contributors'], row['publication_date'], test)

        if contributors_details:
            row['parsed_contributors'] = format_contributors(contributors_details)
            row['parsed_organizations'], row['managing_org'] = format_organizations_from_contributors(
                contributors_details)
            row = format_rest(row)
            row, error = unique_fields_per_type(row)
            if error == False:
                # Construct the research output JSON
                research_output_json = construct_research_output_json(row)
                succes = succes +1
                if test == 'no':
                    uuid_ro = create_research_output(research_output_json)
                else:
                    error = error + 1



        else:
            logging.warning(f"skipped research output {row['research_output_id']}.")
            error = error + 1
    print(error, ' errors, see log for more info')
    print(succes, ' successes')


def main():
    OPENALEX_HEADERS = {'Accept': 'application/json',
                       # The following will be read in __main__
                       'User-Agent': 'mailto:d.h.j.grotebeverborg@uu.nl'
                       }
    OPENALEX_MAX_RECS_TO_HARVEST = 3

    # List of DOIs
    # dois = ['doi.org/10.1002/ijc.34742', 'doi.org/10.1038/s41598-024-51595-6', 'doi.org/10.1002/yet-another-doi']
    dois = ['doi.org/10.1038/s41598-024-51595-6']
    # List to hold all responses
    all_openalex_data = []

    # Loop through each DOI and make a request
    for doi in dois:
        url = 'https://api.openalex.org/works/' + doi
        response = requests.get(url, headers=OPENALEX_HEADERS)

        if response.status_code == 200:
            openalex_data = response.json()
            all_openalex_data.append(openalex_data)
        else:
            print(f"Failed to retrieve data for DOI: {doi}")

    df, errors = openalex_utils.transform_openalex_to_df(all_openalex_data)
    df_to_pure(df)

if __name__ == '__main__':
    main()


