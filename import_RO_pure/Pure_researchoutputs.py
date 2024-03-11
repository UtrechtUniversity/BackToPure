import pandas as pd
import json
import requests
from datetime import datetime
import configparser
import os
import logging
import pure_persons
import openalex_utils
from dateutil import parser

config_path = 'config.ini'
if not os.path.exists(config_path):
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read('config.ini')
BASE_URL = config['API']['BaseURL']
API_KEY = config['API']['APIKey']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='pure_utilities.log',
)

headers = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": API_KEY
}

def get_researchoutput(uuid):

    api_url = BASE_URL + 'research-outputs/' + uuid
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
    api_url = BASE_URL + 'external-persons/'
    url = "https://staging.research-portal.uu.nl/ws/api/external-persons"

    data = {"name": {"firstName": first_name, "lastName": last_name}}
    json_data = json.dumps(data)

    try:
        response = requests.put(api_url, headers=headers, data=json_data)

        if response.status_code in [200, 201]:
            external_person = response.json()
            return external_person.get('uuid')
        else:
            logging.error(f"Error creating external person: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logging.error(f"An error occurred while creating external person: {e}")

    return None


def get_contributors_details(contributors, ref_date):
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
                logging.info(f"Creating external person for {contributor_id}.")
                # external_person_uuid = create_external_person(contributor['first_name'],contributor['last_name'])
                external_person_uuid = '11ww'
                if external_person_uuid:
                    logging.info(f'Created external person: {external_person_uuid}')
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
    if keywords:
        transformed_data = {
            "keywordGroups": [
                {
                    "typeDiscriminator": "FreeKeywordsKeywordGroup",
                    "logicalName": "keywordContainers",
                    "name": {
                        "en_GB": "Keywords"
                    },
                    "keywords": [
                        {
                            "locale": "en_GB",
                            "freeKeywords": keywords
                        }
                    ]
                }
            ]
        }
    else:
        transformed_data = None
    return transformed_data

def create_base_json(title, subTitle, type, category, publicationStatuses, language, contributors, organizations, totalNumberOfContributors, managingOrganization, visibility, workflow, customDefinedFields, systemName, externalOrganizations, abstract):
    """Creates the base structure for a research output JSON file."""
    return {
        'title': title,
        'subTitle': subTitle,
        'type': type,
        'category': category,
        'publicationStatuses': publicationStatuses,
        'language': language,
        'contributors': contributors,
        'organizations': organizations,
        'totalNumberOfContributors': totalNumberOfContributors,
        'managingOrganization': managingOrganization,
        'visibility': visibility,
        'workflow': workflow,
        'customDefinedFields': customDefinedFields,
        'systemName': systemName,
        'externalOrganizations': externalOrganizations,
        'abstract': abstract
    }

def create_conference_paper_json(unique_fields, base_fields):
    """Creates a JSON file for a conference paper."""
    json_data = create_base_json(**base_fields)
    json_data.update(unique_fields)
    return json_data


OPENALEX_HEADERS = {'Accept': 'application/json',
                    # The following will be read in __main__
                    'User-Agent': 'mailto:d.h.j.grotebeverborg@uu.nl'
                    }
OPENALEX_MAX_RECS_TO_HARVEST = 3

 # List of DOIs
dois = ['doi.org/10.1002/ijc.34742', 'doi.org/10.1038/s41598-024-51595-6', 'doi.org/10.1002/yet-another-doi']

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


def get_journal_uuid(issn):
    url = "https://staging.research-portal.uu.nl/ws/api/journals/search/"
    # url = BASE_URL + '/journals/search/'
    data = {"searchString": issn}
    json_data = json.dumps(data)
    response = requests.post(url, headers=headers, data=json_data)
    data = response.json()
    items = data.get('items', [])
    for item in items:
        journal_uuid = item['uuid']

    if not journal_uuid:
        journal_uuid = None

    return journal_uuid


def construct_research_output_json(row):
    print('test')

    pass


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
    for name, details in contributors.items():
        print(f"Processing {name}")  # Debugging print
        print(details)
        # Check if 'associationsUUIDs' is in details and is a list
        if 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):
            association_uuids = details['associationsUUIDs']
            print(association_uuids)
            managing_org = details['associationsUUIDs']
            organization_uuids.update(association_uuids)
            print(f"Found associations for {name}: {association_uuids}")  # Debugging print
        else:
            print(f"No associations found for {name}")  # Debugging print

    if not organization_uuids:
        print("No organization UUIDs found, adding default")  # Debugging print
        organization_uuids.add(default_uuid)

    formatted_organizations = [{"systemName": "Organization", "uuid": uuid} for uuid in organization_uuids]
    if not managing_org:
        managing_org = None
    return formatted_organizations, managing_org


for _, row in df.iterrows():
    contributors_details = get_contributors_details(row['contributors'], row['publication_date'])

    if contributors_details:
        row['parsed_organizations'], row['managing_org'] = format_organizations_from_contributors(contributors_details)
        row['journal'] = get_journal_uuid(row['journal_issn'])
        row['contributors_details'] = contributors_details

        # Construct the research output JSON
        research_output_json = construct_research_output_json(row)

    else:
        logging.warning(f"skipped research output {row['research_output_id']}.")




