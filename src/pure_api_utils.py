import requests
import json
import logging
from datetime import datetime
import configparser
import os
# Load configuration settings from config.ini
config_path = 'config.ini'
if not os.path.exists(config_path):
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read('config.ini')
BASE_URL = config['API']['BaseURL']
API_KEY = config['API']['APIKey']
def construct_research_output_json(research_output_id, title, contributors, journal, publication_year, publication_month, language_uri, language_term,
                                   peer_review, submission_year, doi, visibility_key, workflow_step):
    """
    Constructs the JSON structure for a research output.
    :param research_output_id: The ID of the research output.
    :param title: The title of the research output.
    :param contributors: List of contributor details.
    :param journal: Details of the journal.
    :param publication_year: Year of publication.
    :param publication_month: Month of publication.
    :param language_uri: URI for the language.
    :param language_term: Term for the language.
    :param peer_review: Boolean indicating if peer-reviewed.
    :param submission_year: Year of submission.
    :param doi: DOI of the research output.
    :param issn: ISSN of the journal.
    :param visibility_key: Key for visibility setting.
    :param workflow_step: Current step in the workflow.
    :return: A dictionary representing the research output in the defined JSON format.
    """

    print('kom ik hier dan')
    parsed_contributors = format_contributors(contributors)
    parsed_organizations = format_organizations_from_contributors(contributors)

    print('gg' + language_uri)
    research_output = {
        "typeDiscriminator": "ContributionToJournal",
        "peerReview": peer_review,
        "title": {"value": title},
        "type": {"uri": "/dk/atira/pure/researchoutput/researchoutputtypes/contributiontojournal/article"},
        "category": {"uri": "/dk/atira/pure/researchoutput/category/academic"},
        "publicationStatuses": [{
            "current": True,
            "publicationStatus": {
                "uri": "/dk/atira/pure/researchoutput/status/published",
            },
            "publicationDate": {"year": publication_year, "month": publication_month}
        }],
        "language": {"uri": language_uri},
        "contributors": parsed_contributors,
        "organizations": parsed_organizations,
        "totalNumberOfContributors": len(contributors),
        "managingOrganization": {
            "systemName": "Organization",
            "uuid": "360f34ba-b6ea-4bf0-8067-c85994209e8b"
        },
        # "submissionYear": submission_year,
        "electronicVersions": [{
            "typeDiscriminator": "DoiElectronicVersion",
            "accessType": {
                "uri": "/dk/atira/pure/core/openaccesspermission/unknown",
            },
            "doi": doi,
            "versionType": {
                "uri": "/dk/atira/pure/researchoutput/electronicversion/versiontype/publishersversion",
            }
        }],
        "links": [{"url": f"https://doi.org/{doi}"}],  # Placeholder for links
        "visibility": {"key": visibility_key},
        "workflow": {"step": workflow_step},
        "identifiers": [
            {
                "typeDiscriminator": "PrimaryId",
                "idSource": "ORCID",
                "value": "/0000-0002-5899-0663/work/81690395"
            },
            {
                "typeDiscriminator": "Id",

                "idSource": "Scopus",
                "value": "85092593111"
            }
        ],
        # "customDefinedFields": {},
        "journalAssociation": {
            "journal": {
                "systemName": "Journal",
                "uuid": journal
            }
        },
        "systemName": "ResearchOutput"
    }

    print(json.dumps(research_output, indent=4))
    return research_output



def get_pure_person_details(contributor, headers):
    """
    Retrieves details of a person from Pure based on various IDs.

    :param contributor: A dictionary containing contributor's IDs and names.
    :param headers: Headers to be used for the API request.
    :return: A dictionary of person details if found, else None.
    """

    # api_url = BASE_URL + 'persons/search/'
    api_url = "https://staging.research-portal.uu.nl/ws/api/persons/search/"
    specific_date = datetime.strptime('2023-01-01', "%Y-%m-%d")

    for id_type, id_value in contributor['ids'].items():
        data = {"searchString": id_value}
        json_data = json.dumps(data)

        try:
            # print(json_data)
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
    # url = BASE_URL + '/external-persons'

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


def get_contributors_details(contributors, headers, ref_date):

    persons = {}
    found_internal_person = False

    # First pass: Check for internal persons and mark if any are found
    for contributor in contributors:
        print(contributor)
        contributor_id = contributor['name']
        person_details = get_pure_person_details(contributor, headers)

        if person_details:
            persons[contributor_id] = person_details
            found_internal_person = True
        else:
            # Mark as None for now
            persons[contributor_id] = None

    # Second pass: Create external persons only if an internal person is found
        print(found_internal_person)
    if found_internal_person:
        for contributor in contributors:
            contributor_id = contributor['name']
            if persons[contributor_id] is None:  # This contributor needs an external person
                logging.info(f"Creating external person for {contributor_id}.")
                external_person_uuid = create_external_person(contributor, headers)

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


def parse_contributors(contributors_str):
    # Split the string into individual contributors
    contributors_list = contributors_str.split('), ')
    parsed_contributors = []

    for contributor in contributors_list:
        parts = contributor.rstrip(')').split(' (')  # Split name and IDs
        name = parts[0]
        first_name, last_name = name.split(' ')
        ids = parts[1] if len(parts) > 1 else ''

        # Extract ORCID and ScopusID
        id_parts = ids.split(', ')
        ids_dict = {}
        for id_part in id_parts:
            id_type, id_value = id_part.split(': ')
            ids_dict[id_type] = id_value

        parsed_contributors.append({
            'name': name,
            'first_name': first_name,
            'last_name': last_name,
            'ids': ids_dict
        })

    return parsed_contributors


def get_journal_uuid(issn, headers):
    print('get journal id')
    url = "https://staging.research-portal.uu.nl/ws/api/journals/search/"
    # url = BASE_URL + '/journals/search/'
    data = {"searchString": issn}
    json_data = json.dumps(data)

    response = requests.post(url, headers=headers, data=json_data)

    data = response.json()
    items = data.get('items', [])
    for item in items:
        journal_uuid = item['uuid']

    return journal_uuid

def create_research_output(ro, headers):

    print('create research output')
    url = " https://staging.research-portal.uu.nl/ws/api/research-outputs"
    json_data = json.dumps(ro)
    print(json_data)

    # Make the put request
    response = requests.put(url, headers=headers, data=json_data)
    print(response.status_code)
    return 'test'

def format_contributors(contributors_data):
    formatted_contributors = []

    for name, details in contributors_data.items():
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
                    {"systemName": "Organization", "uuid": org_uuid} for org_uuid in details['associationsUUIDs']
                ]
            }
        else:  # External Contributor
            contributor = {
                "typeDiscriminator": "ExternalContributorAssociation",
                # Assuming pureId and country details are available, or else set default or fetch
                "pureId": 0,  # Placeholder: Replace with actual pureId if available
                "externalOrganizations": [],  # Placeholder: Populate if organization data available
                "hidden": False,
                "country": {
                    "uri": "/dk/atira/pure/core/countries/de",  # Placeholder: Replace with actual country URI
                    "term": {"en_GB": "Germany"}  # Placeholder: Replace with actual country
                },
                "correspondingAuthor": False,  # Set appropriately if information available
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


def format_organizations_from_contributors(contributors, default_uuid="cdd6493c-70ab-40f8-8246-b8be95f27e71"):
    """
    Extracts and formats organization UUIDs from contributors' details.
    Includes a default organization UUID if no others are found.
    :param contributors: List of contributors with their details, including association UUIDs.
    :param default_uuid: The default organization UUID to use if no others are found.
    :return: A list of dictionaries, each representing an organization.
    """
    organization_uuids = set()

    for name, details in contributors.items():
        print(f"Processing {name}")  # Debugging print

        # Check if 'associationsUUIDs' is in details and is a list
        if 'associationsUUIDs' in details and isinstance(details['associationsUUIDs'], list):
            association_uuids = details['associationsUUIDs']
            organization_uuids.update(association_uuids)
            print(f"Found associations for {name}: {association_uuids}")  # Debugging print
        else:
            print(f"No associations found for {name}")  # Debugging print

    if not organization_uuids:
        print("No organization UUIDs found, adding default")  # Debugging print
        organization_uuids.add(default_uuid)

    formatted_organizations = [{"systemName": "Organization", "uuid": uuid} for uuid in organization_uuids]
    return formatted_organizations

