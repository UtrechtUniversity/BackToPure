import requests
import json
from datetime import datetime
import logging
import configparser
from requests.structures import CaseInsensitiveDict

# Setting up basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration settings from config.ini
config = configparser.ConfigParser()
config.read('config.ini')
BASE_URL = config['DEFAULT']['BaseURL']
API_KEY = config['DEFAULT']['APIKey']


def get_pure_person_details(contributor, headers):
    """
    Retrieves details of a person from Pure based on various IDs.

    :param contributor: A dictionary containing contributor's IDs and names.
    :param headers: Headers to be used for the API request.
    :return: A dictionary of person details if found, else None.
    """
    api_url = BASE_URL + 'persons/search/'
    # api_url = "https://staging.research-portal.uu.nl/ws/api/persons/search/"
    specific_date = datetime.strptime('2023-01-01', "%Y-%m-%d")

    for id_type, id_value in contributor['ids'].items():
        data = {"searchString": id_value}
        json_data = json.dumps(data)

        try:
            response = requests.post(api_url, headers=headers, data=json_data)

            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                if items:
                    # Assuming the first item is the desired person
                    item = items[0]
                    person_detail = {
                        "id_type": id_type,
                        "id_value": id_value,
                        "uuid": item.get('uuid'),
                        "firstName": item.get('name', {}).get('firstName'),
                        "lastName": item.get('name', {}).get('lastName'),
                        "associationsUUIDs": [assoc.get('organization', {}).get('uuid') for assoc in
                                              item.get('staffOrganizationAssociations', []) if
                                              datetime.strptime(assoc.get('period', {}).get('startDate'),
                                                                "%Y-%m-%d") <= specific_date <= datetime.strptime(
                                                  assoc.get('period', {}).get('endDate', '9999-12-31'), "%Y-%m-%d")]
                    }
                    return person_detail

            else:
                logging.error(f"Error searching for {id_type}: {response.status_code} - {response.text}")

        except requests.RequestException as e:
            logging.error(f"An error occurred while searching for {id_type}: {e}")

    return None  # Person not found for any ID


def create_external_person(contributor, headers):
    """
    Creates an external person in the Pure system.

    :param contributor: A dictionary containing contributor's first and last names.
    :param headers: Headers to be used for the API request.
    :return: UUID of the newly created external person.
    """
    url = "https://staging.research-portal.uu.nl/ws/api/external-persons"

    data = {"name": {"firstName": contributor['first_name'], "lastName": contributor['last_name']}}
    json_data = json.dumps(data)

    try:
        response = requests.put(url, headers=headers, data=json_data)

        if response.status_code in [200, 201]:
            external_person = response.json()
            return external_person.get('uuid')
        else:
            logging.error(f"Error creating external person: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logging.error(f"An error occurred while creating external person: {e}")

    return None


def get_contributors_details(contributors, headers):
    """
    Processes a list of contributors, retrieves or creates their details in Pure.

    :param contributors: A list of dictionaries, each representing a contributor.
    :param headers: Headers to be used for the API requests.
    :return: A dictionary of contributors with their Pure details.
    """
    persons = {}

    for contributor in contributors:
        # Constructing a unique ID for each contributor
        contributor_id = '-'.join([contributor['first_name'], contributor['last_name'], *contributor['ids'].values()])
        person_details = get_pure_person_details(contributor, headers)

        if person_details:
            persons[contributor_id] = person_details
        else:
            logging.info(f"UUID not found for {contributor_id}, creating external person.")
            external_person_uuid = create_external_person(contributor, headers)

            if external_person_uuid:
                logging.info(f'Created external person: {external_person_uuid}')
                person_details = {
                    "external_person_uuid": external_person_uuid,
                    "external_person_first_name": contributor['first_name'],
                    "external_person_last_name": contributor['last_name']
                }
                persons[contributor_id] = person_details
            else:
                logging.error(f"Failed to create external person for {contributor_id}")

    return persons


# Example usage
# headers
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"
headers["accept"] = "application/json"
headers["api-key"] = API_KEY


contributors = [
    {'ids': {'orcid': '0000-0002-0014-8625', 'scopus_id': '123456'}, 'first_name': 'Laurens', 'last_name': 'Bloem'},
    {'ids': {'orcid': '0000-0002-8393-460d', 'scopus_id': '654321'}, 'first_name': 'David', 'last_name': 'Bowie'},
    # Add more contributors as needed
]

persons = get_contributors_details(contributors, headers)
print(persons)
