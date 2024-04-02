import pandas as pd
import json
import requests
from datetime import datetime
import configparser
import os
import logging

from dateutil import parser
from pathlib import Path

# Calculate the path to the config.ini file
# Path(__file__).resolve() gets the absolute path of the current script
# .parent gets the directory containing the script (src)
# .parent again moves up to the project root directory
config_path = Path(__file__).resolve().parent.parent / 'config.ini'
# config_path = 'config.ini'
if not config_path.exists():
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read(config_path)
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



def parse_date(date_string):
    try:
        return parser.parse(date_string)
    except (ValueError, TypeError):
        return None  # or some default date

def extract_orcid(orcid_full_url):
    # Split the string by '/' and return the last part
    return orcid_full_url.split('/')[-1]

# Function to construct person_detail from API response data
def construct_person_detail(data, ref_date=None):
    """
       Constructs a dictionary of person details from API response data.
       (Note that the header of the api-call contain the apikey that is loaded in the top of this script)

       Parameters:
       - data (dict): The JSON data returned from the API.
       - ref_date (datetime, optional): A reference date used to filter associations.
         Only associations active on this date are included. If None, all associations are included.

       Returns:
       - dict: A dictionary containing the person's UUID, first name, last name,
               and a list of associations with their UUIDs and active dates.
       """
    associations = data.get('staffOrganizationAssociations', [])

    associationsUUIDs = []
    for assoc in associations:
        assoc_start_date = assoc.get('period', {}).get('startDate')
        assoc_end_date = assoc.get('period', {}).get('endDate', '9999-12-31')

        # Convert string dates to datetime objects for comparison
        assoc_start_datetime = datetime.strptime(assoc_start_date, "%Y-%m-%d") if assoc_start_date else None
        assoc_end_datetime = datetime.strptime(assoc_end_date, "%Y-%m-%d") if assoc_end_date else None

        # Check if the association falls within the ref_date, if provided
        if ref_date:
            if assoc_start_datetime <= ref_date <= assoc_end_datetime:
                associationsUUIDs.append({
                    "uuid": assoc.get('organization', {}).get('uuid'),
                    "startDate": assoc_start_date,
                    "endDate": assoc_end_date
                })
        else:
            associationsUUIDs.append({
                "uuid": assoc.get('organization', {}).get('uuid'),
                "startDate": assoc_start_date,
                "endDate": assoc_end_date
            })

    return {
        "uuid": data.get('uuid'),
        "firstName": data.get('name', {}).get('firstName'),
        "lastName": data.get('name', {}).get('lastName'),
        "associationsUUIDs": associationsUUIDs,
    }


def find_person(name, person_ids, date):
    """
    Searches for and retrieves detailed information about a person from an API.

    Parameters:
    - name (str): The name of the person to be searched. if none => the module will not try to find person on name
    - person_ids (dict): A dictionary of identifiers for the person (e.g., UUID, other IDs).
    - date (str): A date string used for filtering data. if None => all association ids will be collected
    - apikey (str): API key for authentication with the API.
      (Note that the header of the api-call contain the apikey that is loaded in the top of this script)


    Returns:
    - dict: A dictionary containing detailed information about the person if a unique match is found.
    - str: A message indicating no unique person was found if no match or multiple matches are found.
    """
    ref_date = None
    if date:
        # ref_date = datetime.strptime(date, "%Y-%m-%d")

        ref_date = parse_date(date)

    person_detail = None
    if person_ids and 'uuid' in person_ids:

        uuid = person_ids['uuid']
        api_url = BASE_URL + 'persons/' + uuid

        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            person_detail = construct_person_detail(data, ref_date)
            if person_detail:
                logging.info(f"Person found with UUID: {person_ids['uuid']}")
                return person_detail

    if person_ids:
        for id_type, id_value in person_ids.items():
                    if id_type.lower() == 'orcid':
                        id_value = extract_orcid(id_value)
                    data = {"searchString": id_value}
                    json_data = json.dumps(data)
                    api_url = BASE_URL + 'persons/search/'
                    try:
                        response = requests.post(api_url, headers=headers, data=json_data)
                        if response.status_code == 200:
                            data = response.json()
                            items = data.get('items', [])
                            if items:
                                if len(items) == 1:
                                    item = items[0]
                                    person_detail = construct_person_detail(item, ref_date)
                                    logging.info(f"Person found with {id_type}: {id_value}")
                                    return person_detail
                                else:
                                    logging.warning(f"Multiple or no persons found for {id_type}, {id_value}")
                        else:
                            logging.error(f"Error searching for {id_type}: {response.status_code} - {response.text}")
                    except requests.RequestException as e:
                        logging.error(f"An error occurred while searching for {id_type}: {e}")

    if not person_detail and name is not None:

        data = {"searchString": name}
        json_data = json.dumps(data)
        api_url = BASE_URL + 'persons/search/'
        try:
            response = requests.post(api_url, headers=headers, data=json_data)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if items:
                    if len(items) == 1:
                        item = items[0]
                        person_detail = construct_person_detail(item, ref_date)
                        logging.info(f"Person found with name: {name}")
                        return person_detail
                    else:
                        logging.warning(f"Multiple persons found for name: {name}")
                else:
                    logging.warning(f"no persons found for name: {name}")



            else:
                logging.error(f"Error searching for {name}: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logging.error(f"An error occurred while searching for {name}: {e}")

    return (person_detail)


from datetime import datetime

def get_active_associations(person_details, ref_date_str):
    """
    Updates person details to include only active associations based on a reference date.

    Parameters:
    - person_details (dict): A dictionary containing person's details including associations.
    - ref_date_str (str): The reference date in string format (e.g., 'YYYY-MM-DD').

    Returns:
    - dict: Updated person_details with only active associations for the given reference date.
    """
    if not person_details or not ref_date_str:
        logging.warning(f"no person_details or date for get_active_associations")
        return

    active_associations = []
    ref_date = None
    if ref_date_str:

        ref_date = parse_date(ref_date_str)

    if 'associationsUUIDs' in person_details:
        for association in person_details['associationsUUIDs']:
            start_date_str = association.get('startDate')
            end_date_str = association.get('endDate', '9999-12-31')

            start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None

            if start_date and end_date and start_date <= ref_date <= end_date:
                active_associations.append(association)

        # Update the person_details with only active associations
    person_details['associationsUUIDs'] = active_associations
    return person_details


# person_info = {
#     'name': 'menno Straataaaaasma',
#     'first_name': 'Jan',
#     'last_name': 'Test',
#     'ids': {'ORCID': '0000-0000-0000-0000', 'ScopusID': '123456'}
# }
#
# # Extract the full name and IDs
# full_name = person_info['name']  # Or construct from first_name and last_name if needed
# person_ids = person_info['ids']
#
# person_details = find_person(full_name, person_ids, None)
#
# person_details = get_active_associations(person_details, '01-01-2019')

