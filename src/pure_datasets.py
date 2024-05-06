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

def load_config():
    """Loads the configuration from the config.ini file."""
    config_path = Path(__file__).resolve().parent.parent / 'config.ini'
    if not config_path.exists():
        raise FileNotFoundError(f"The configuration file {config_path} does not exist.")
    config = configparser.ConfigParser()
    config.read(config_path)
    return config
def setup_logging():
    """Sets up the logging configuration."""
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_name = f"pure_utilities_{current_time}.log"
    log_file_path = os.path.join(log_directory, log_file_name)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file_path)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
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
    api_url = f"{BASE_URL}data-sets/{uuid}"
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to fetch dataset by UUID {uuid}: {response.status_code} - {response.text}")
        return None
def search_dataset_by_string(search_string):
    """Search for datasets using a search string."""
    search_string = format_doi(search_string)
    data = {"searchString": search_string}
    json_data = json.dumps(data)
    api_url = f"{BASE_URL}data-sets/search/"
    response = requests.post(api_url, headers=headers, data=json_data)
    if response.status_code == 200:
        return response.json().get('items', [])
    else:
        logging.error(f"Failed to search datasets by string {search_string}: {response.status_code} - {response.text}")
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

def create_external_person(first_name, last_name):
    """
    Creates an external person in the Pure system.
    :param first_name, last_name:  first and last names.
    :return: UUID of the newly created external person.
    """
    api_url = BASE_URL + 'external-persons/'
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
def get_contributors_details(contributors, date, title):
    """
       Processes a list of contributors to find detailed information about each.
       For each contributor, the function attempts to find them as internal persons
       using their identifiers. If an internal person is not found, it creates an
       external person entry.

       Each contributor is expected to be a dictionary with at least 'name' and
       'person_ids' keys. The function returns a dictionary with contributor names
       as keys and their details (either internal or external) as values.

       If any contributor is found as an internal person, the function will also
       process external persons, creating new entries for them if necessary.

       Parameters:
       - contributors (list of dict): A list where each element is a dictionary
         representing a contributor. Each dictionary must have a 'name' key and
         a 'person_ids' key.

       Returns:
       - dict: A dictionary where keys are contributor names and values are dictionaries
         of their details. Returns None if no internal contributors are found.

       Note:
       - The function logs an error and returns None if no internal contributors are
         found. It also logs information when external persons are created and logs
         an error if it fails to create an external person.
       """
    persons = {}
    found_internal_person = False

    for contributor in contributors:

        # Assuming contributor has 'name' and 'person_ids'
        name = contributor['name']
        ids_dict = {id_info['id']: id_info['value'] for id_info in contributor['person_ids']}

        person_details = pure_persons.find_person(name, ids_dict, date)


        if person_details:
            person_details['type'] = contributor['type']
            persons[name] = person_details
            found_internal_person = True
        else:
            logging.info(f"No internal person found for {name}")


    if found_internal_person:
        # Process external persons
        for contributor in contributors:
            contributor_id = contributor['name']

            if persons.get(contributor_id) is None:# This contributor needs an external person
                logging.info(f"Creating external person for {contributor_id}.")
                first_name, last_name = split_name(contributor['name'])
                external_person_uuid = external_person_uuid = create_external_person(first_name, last_name)

                if external_person_uuid:
                    logging.info(f'Created external person: {external_person_uuid}')
                    persons[contributor_id] = {
                        "external_person_uuid": external_person_uuid,
                        "external_person_first_name": first_name,
                        "external_person_last_name": last_name,
                        "type": contributor['type']
                    }

    else:
        logging.error("No internal contributors found in Pure for the dataset.")
        print ("skipped ",  title, ", No internal contributors found in Pure for the dataset")
        return None

    return persons
def format_contributors(contributors_data):
    formatted_contributors = []
    # removing duplicate uuid's (that might be there to multiple aa
    for name, details in contributors_data.items():

        logging.info(f"Processing {name}")
        type_uri = config['URI'][details['type']]

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
                    "uri": type_uri,
                    "term": {"en_GB": details['type']}
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
                "name": {
                    "firstName": details['external_person_first_name'],
                    "lastName": details['external_person_last_name']
                },
                "role": {
                    "uri": type_uri,
                    "term": {"en_GB": details['type']}
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
    default_uuid = BASE_ORG
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


    if not organization_uuids:
        logging.info("No organization UUIDs found, adding default")  # Debugging print
        organization_uuids.add(default_uuid)

    formatted_organizations = [{"systemName": "Organization", "uuid": uuid} for uuid in organization_uuids]
    if not managing_org:
        managing_org = default_uuid
    return formatted_organizations, managing_org
def find_publisher(publisher):
    data = {"searchString": publisher}
    publisher_id =  None
    json_data = json.dumps(data)
    api_url = BASE_URL + 'publishers/search/'
    try:
        response = requests.post(api_url, headers=headers, data=json_data)

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
                    publisher_uuid = config['DEFAULTS']['publisher']
            else:
                publisher_uuid = config['DEFAULTS']['publisher']
            return publisher_uuid

        else:
    #         default publisher
            publisher_uuid = config['DEFAULTS']['publisher']
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
    if not row['doi']:
        row['doi'] = 'n/a'
    dataset = {
         "title": {"en_GB": row['title']},
         "descriptions": [description],
         "doi": {"doi": row['doi']},
         "type": {"uri": config['URI']['type_dataset']},
         "publisher": {"systemName": "Publisher", "uuid": publisher_uuid},
         "publicationAvailableDate": {"year": row['publication_year'], "month": row['publication_month'], "day": row['publication_month']},
         "managingOrganization": {"systemName": "Organization", "uuid": row['managing_org']},
         "persons": row['parsed_contributors'],
         "organizations": row['parsed_organizations'],
         "visibility": {"key": config['DEFAULTS']['visibility_key']}
        }
    return dataset
def create_dataset(dataset_json):
    url = BASE_URL + 'data-sets'
    json_data = json.dumps(dataset_json)
    # Write to a new file
    # with open('datasssa.json', 'w') as file:
    #     file.write(json_data)
    # Make the put request
    response = requests.put(url, headers=headers, data=json_data)
    if response.status_code in [200, 201]:
        data = response.json()
        logging.info(f"created dataset: {response.status_code} - {data['uuid']}")
        return data['uuid']
    else:
        logging.error(f"Error creating dataset {response.status_code} - {response.text}")
        # Print the entire response to see all available details
        return 'error'
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
            file_path = 'other_files/test2.json'
            df = yoda_utils.get_df_from_yoda(file_path)
            break  # Exit loop after successful operation
        else:
            print("Invalid choice. Please enter 1 or 2.")  # Prompt again if input is not valid

    return df
def main():

    df = user_choice()
    created = 0
    ignored = 0
    for _, row in df.iterrows():

        already_in_pure = find_dataset(None, row['doi'])

        if already_in_pure:
            logging.info(f"dataset with doi: {row['doi']}, already in pure")
            print("skipped ", row['doi'], ' , is already in pure')
            ignored += 1
        else:
           contributors_details = get_contributors_details(row['persons'], row['publication_year'], row['title'])
           if contributors_details and not already_in_pure:
                row['parsed_contributors'] = format_contributors(contributors_details)
                row['parsed_organizations'], row['managing_org'] = format_organizations_from_contributors(
                    contributors_details)

                # Construct the dataset JSON
                dataset_json = construct_dataset_json(row)
                uuid_ds = create_dataset(dataset_json)
                if not uuid_ds == 'error':
                    print ('created dataset: ', uuid_ds)
                    created += 1
                else:
                    ignored += 1
                    print('error creating dataset: ', row['title'])
           else:
                ignored += 1

    # summary
    logging.info(f"Process completed. created datasets: {created}, ignored: {ignored}")
    print (f"Process completed. created datasets: {created}, skipped: {ignored}")


# Load configuration from file
config = load_config()
BASE_URL = config['API']['BaseURL']
API_KEY = config['API']['APIKey']
BASE_ORG = config['DEFAULTS']['university']
# Setup logging
logger = setup_logging()
# Headers for API requests
headers = get_headers(API_KEY)

if __name__ == '__main__':
    main()
