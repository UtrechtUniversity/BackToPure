import pandas as pd
import json
import requests
from datetime import datetime
import configparser
import os
import logging
import pure_persons
import test
import openalex_utils
import logging.handlers
from dateutil import parser

config_path = 'config.ini'
if not os.path.exists(config_path):
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read('config.ini')
BASE_URL = config['API']['BaseURL']
API_KEY = config['API']['APIKey']

# Ensure the logs directory exists
log_directory = "logs"
os.makedirs(log_directory, exist_ok=True)

# Configure logging
log_file_path = os.path.join(log_directory, "pure_utilities.log")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a handler that writes log messages to a file, rotating the log file at midnight every day
handler = logging.handlers.TimedRotatingFileHandler(
    log_file_path, when="midnight", interval=1, backupCount=7
)
handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

logger.addHandler(handler)

headers = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": API_KEY
}

#
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


def get_contributors_details(contributors, param1):
    persons = {}
    parsed_contributors = []
    found_internal_person = False
    for contributor in contributors:
        ids_dict = {}
        contributor_id = contributor

        person_details = pure_persons.find_person(contributor, ids_dict, None)

        if person_details:
            persons[contributor_id] = person_details
            found_internal_person = True
        else:
            # Mark as None for now
            persons[contributor_id] = None

            # Second pass: Create external persons only if an internal person is found
    if found_internal_person:
        for contributor in contributors:
            contributor_id = contributor
            if persons[contributor_id] is None:  # This contributor needs an external person
                logging.info(f"Creating external person for {contributor_id}.")
                # Splitting the name on the comma
                parts = contributor.split(',')

                # Trimming whitespace and assigning
                last_name = parts[0].strip()
                first_name = parts[1].strip() if len(parts) > 1 else ''
                external_person_uuid = create_external_person(first_name, last_name)

                if external_person_uuid:
                    logging.info(f'Created external person: {external_person_uuid}')
                    persons[contributor_id] = {
                        "external_person_uuid": external_person_uuid,
                        "external_person_first_name": first_name,
                        "external_person_last_name": last_name
                    }
                else:
                    logging.error(f"Failed to create external person for {contributor_id}")
    else:
        logging.error("No internal contributors found in Pure for the research output.")
        return None

    return persons


def format_contributors(contributors_data):
    formatted_contributors = []
    # removing duplicate uuid's (that might be there to multiple aa
    for name, details in contributors_data.items():
        logging.info(f"Processing {name}")

        if 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):
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
        if 'uuid' in details:  # Internal Contributor
            contributor = {
                "typeDiscriminator": "InternalDataSetPersonAssociation",
                "name": {
                    "firstName": details['firstName'],
                    "lastName": details['lastName']
                },
                "role": {
                    "uri": "/dk/atira/pure/dataset/roles/dataset/creator",
                    "term": {"en_GB": "Creator"}
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
                "typeDiscriminator": "ExternalDataSetPersonAssociation",
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
                    "uri": "/dk/atira/pure/dataset/roles/dataset/contributor",
                    "term": {"en_GB": "Contributor"}
                },
                "externalPerson": {
                    "systemName": "ExternalPerson",
                    "uuid": details['external_person_uuid']
                }
            }

        formatted_contributors.append(contributor)

    return formatted_contributors
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

response = requests.get('https://api.datacite.org/dois/10.6084/M9.FIGSHARE.21829182')

single_dataset = response.json()
data = single_dataset['data']['attributes']
title = data['titles'][0]['title']
persons = [creator['name'] for creator in data['creators']]
publisher = data['publisher']
doi = data['doi']
publication_year = data['publicationYear']
publication_month = 1
publication_day = 1
subjects = [subject['subject'] for subject in data['subjects']]

# Creating a DataFrame
df = pd.DataFrame([{
    'title': title,
    'persons': persons,
    'publisher': publisher,
    'DOI': doi,
    'managing_org': '0bafdc8f-1117-40a4-b555-64ac03d78b05',
    'publication_year': publication_year,
    'publication_month': publication_month,
    'publication_day': publication_day,
    'publication_date': "2024-03-25",
    'subject': subjects
}])

print(df)


def construct_dataset_json(row):
    dataset = {
         "title": {"en_GB": row['title']},
         "type": {"uri": "/dk/atira/pure/dataset/datasettypes/dataset/dataset"},
         "publisher": {"systemName": "Publisher", "uuid": 'beb3e811-1f2c-4725-ad70-7f1f38692aec'},
         "publicationAvailableDate": {"year": row['publication_year'], "month": row['publication_month'], "day": row['publication_month']},
         "managingOrganization": {"systemName": "Organization", "uuid": row['managing_org']},
         "persons": row['parsed_contributors'],
         "organizations": row['parsed_organizations'],
         "visibility": {"key": "FREE"}

    }
    return dataset


def create_dataset(dataset_json):
    url = " https://staging.research-portal.uu.nl/ws/api/data-sets"
    json_data = json.dumps(dataset_json)
    # Write to a new file
    with open('datasssa.json', 'w') as file:
        file.write(json_data)
    # Make the put request
    response = requests.put(url, headers=headers, data=json_data)
    if response.status_code in [200, 201]:
        logging.info(f"created dataset: {response.status_code} - {response.text}")
    else:
        logging.error(f"Error creating dataset {response.status_code} - {response.text}")
        # Print the entire response to see all available details
        try:
            response_json = response.json()  # If response is JSON
            print(json.dumps(response_json, indent=4))  # Print formatted JSON
        except json.JSONDecodeError:
            print(response.text)  # If response is not JSON, print raw text
    return 'test'

for _, row in df.iterrows():

    contributors_details = get_contributors_details(row['persons'], row['publication_year'])
    if contributors_details:
        row['parsed_contributors'] = format_contributors(contributors_details)
        row['parsed_organizations'], row['managing_org'] = format_organizations_from_contributors(
            contributors_details)
        print(row['parsed_organizations'] )
        # Construct the dataset JSON
        dataset_json = construct_dataset_json(row)
        uuid_ds = create_dataset(dataset_json)
        print(dataset_json)

