# Refactoring and optimizing the provided script based on the suggestions.

import pandas as pd
import json
import requests
from datetime import datetime
import configparser
import os
import logging
from dateutil import parser

# Config file handling
def load_config(config_path='config.ini'):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"The configuration file {config_path} does not exist.")
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['API']['BaseURL'], config['API']['APIKey']

# Logging setup
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='pure_utilities.log',
    )

# Parse date using dateutil
def parse_date(date_string):
    return parser.parse(date_string, default=None)

# Construct person detail from API data
def construct_person_detail(data, ref_date=None):
    associations = data.get('staffOrganizationAssociations', [])
    associationsUUIDs = [
        {
            "uuid": assoc.get('organization', {}).get('uuid'),
            "startDate": assoc.get('period', {}).get('startDate'),
            "endDate": assoc.get('period', {}).get('endDate', '9999-12-31')
        }
        for assoc in associations
        if not ref_date or (parse_date(assoc.get('period', {}).get('startDate')) <= ref_date <= parse_date(assoc.get('period', {}).get('endDate', '9999-12-31')))
    ]

    return {
        "uuid": data.get('uuid'),
        "firstName": data.get('name', {}).get('firstName'),
        "lastName": data.get('name', {}).get('lastName'),
        "associationsUUIDs": associationsUUIDs,
    }

# Function to get person details from API
def get_person_from_api(api_url, headers):
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json(), None
    else:
        return None, f"Error {response.status_code}: {response.text}"

# Find person by UUID or other identifiers
def find_person(name, person_ids, date, base_url, headers):
    ref_date = parse_date(date) if date else None
    person_detail = None

    # Search by UUID
    if person_ids and 'uuid' in person_ids:
        uuid = person_ids['uuid']
        api_url = f"{base_url}persons/{uuid}"
        data, error = get_person_from_api(api_url, headers)
        if data:
            person_detail = construct_person_detail(data, ref_date)
            logging.info(f"Person found with UUID: {uuid}")
        if error:
            logging.error(error)

    # Search by other identifiers
    if not person_detail and person_ids:
        for id_type, id_value in person_ids.items():
            data = {"searchString": id_value}
            json_data = json.dumps(data)
            api_url = f"{base_url}persons/search/"
            try:
                response = requests.post(api_url, headers=headers, data=json_data)
                if response.status_code == 200:
                    items = response.json().get('items', [])
                    if len(items) == 1:
                        person_detail = construct_person_detail(items[0], ref_date)
                        logging.info(f"Person found with {id_type}: {id_value}")
                    else:
                        logging.warning(f"Multiple or no persons found for {id_type}, {id_value}")
                else:
                    logging.error(f"Error searching for {id_type}: {response.status_code} - {response.text}")
            except requests.RequestException as e:
                logging.error(f"An error occurred while searching for {id_type}: {e}")

    # Search by name
    if not person_detail and name:
        data = {"searchString": name}
        json_data = json.dumps(data)
        api_url = f"{base_url}persons/search/"
        try:
            response = requests.post(api_url, headers=headers, data=json_data)
            if response.status_code == 200:
                items = response.json().get('items', [])
                if len(items) == 1:
                    person_detail = construct_person_detail(items[0], ref_date)
                    logging.info(f"Person found with name: {name}")
                else:
                    logging.warning(f"Multiple persons found for name: {name}")
            else:
                logging.error(f"Error searching for {name}: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logging.error(f"An error occurred while searching for {name}: {e}")

    return person_detail

# Get active associations based on a reference date
def get_active_associations(person_details, ref_date_str):
    if not person_details or not ref_date_str:
        logging.warning("No person_details or date for get_active_associations")
        return person_details

    ref_date = parse_date(ref_date_str)
    active_associations = [
        assoc for assoc in person_details.get('associationsUUIDs', [])
        if parse_date(assoc.get('startDate')) <= ref_date <= parse_date(assoc.get('endDate', '9999-12-31'))
    ]

    person_details['associationsUUIDs'] = active_associations
    return person_details

# Main function to find person and get active associations
def main(name, person_ids, date):
    base_url, api_key = load_config()
    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
        "api-key": api_key
    }

    setup_logging()

    person_details = find_person(name, person_ids, date, base_url, headers)
    if person_details:
        active_person_details = get_active_associations(person_details, date)
        return active_person_details
    else:
        logging.info("No person details found.")
        return None

# Uncomment the following line to run the main function with example parameters
main('John Doe', {'uuid': '1234-5678'}, '2020-01-01')
