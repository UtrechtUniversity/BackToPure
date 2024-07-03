import time
import pandas as pd
import logging
from logging_config import setup_logging
import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ID_URI, OPENALEX_HEADERS
logger = setup_logging('test', level=logging.INFO)
logger.info("Script to update researchoutput in pure from ricgraph has started")

headers = {
    'Accept': 'application/json',
    'api-key': PURE_API_KEY,
}

# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



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

def extract_openalex_id(openalex):
    # Check if the openalex is in URL format
    if openalex and openalex.startswith('https://openalex.org/'):
        # Extract just the ID part
        return openalex.split('/')[-1]
    elif openalex:
        # Return the openalex as is, assuming it's already in the correct format
        return openalex
    else:
        # Return an empty string if orcid is None
        return ''

def get_ro_from_openalex(item):
    doi = 'doi.org/' + item
    url = 'https://api.openalex.org/works/' + doi
    try:
        response = requests.get(url, headers=OPENALEX_HEADERS)
        if response.status_code == 200:
            data = response.json()  # Directly parse JSON response
            return data
    except:
        pass
def get_ro_from_pure(item):
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "PUT"],
        backoff_factor=1
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    url = "https://staging.research-portal.uu.nl/ws/api/research-outputs/search/"
    session = requests.Session()
    session.mount("https://", adapter)
    # url = BASE_URL + '/journals/search/'
    data = {"searchString": item}
    json_data = json.dumps(data)
    headers = PURE_HEADERS
    # response = session.get(url, headers=headers,  data=json_data, verify=False)
    response = session.post(url, headers=headers, data=json_data)
    time.sleep(1)  # Adjust the sleep time based on rate limits
    # Close the session
    session.close()
    data = response.json()
    print(response.status_code)
    return data


def check_name_match(alex_name, pure_authors):
    # Check for exact full name match
    if alex_name in pure_authors:
        return pure_authors[alex_name]

    # Check for match based on first letter of first name and full last name
    alex_first_name, alex_last_name = alex_name.split(' ')[0], alex_name.split(' ')[-1]
    for pure_name in pure_authors:
        pure_first_name, pure_last_name = pure_name.split(' ')[0], pure_name.split(' ')[-1]
        if alex_last_name == pure_last_name and alex_first_name[0] == pure_first_name[0]:
            return pure_authors[pure_name]

    return None
def match_persons_oa_pure(oa_article, pure_article):
    # Extract authors from the alex1.json dataset
    # Extract authors from the alex1.json dataset with ORCID if available
    alex_authors = {author['author']['display_name']: {'alex_id': author['author']['id'],
                                                       'orcid': author['author'].get('orcid', None)} for author in
                    oa_article['authorships']}

    # Extract authors from the pure1.json dataset
    # Correcting the extraction of UUIDs for contributors and ensuring Pure_UUID is not a list
    pure_authors = {}
    for item in pure_article['items']:
        for contributor in item['contributors']:
            if 'name' in contributor:
                first_name = contributor['name'].get('firstName')
                last_name = contributor['name'].get('lastName')
                if first_name and last_name:
                    name = first_name + ' ' + last_name
                    if 'person' in contributor:
                        intuuid = contributor['person']['uuid']
                    else:
                        uuid = contributor['externalPerson']['uuid']
                        pure_authors[name] = uuid
                else:
                    print(f"Missing first or last name for contributor: {contributor}")
            else:
                print(f"Missing name for contributor: {contributor}")
    # Find common authors based on names and create the list with names, all IDs, and ORCID if available
    common_authors_list = []
    for name, ids in alex_authors.items():
        pure_uuid = check_name_match(name, pure_authors)
        if pure_uuid:
            common_authors_list.append({
                'Name': name,
                'Alex_ID': extract_openalex_id(ids['alex_id']),
                'Pure_UUID': pure_uuid,
                'ORCID': extract_orcid_id(ids['orcid'])
            })
    # Create a DataFrame to display the common authors
    common_authors_df = pd.DataFrame(common_authors_list)

    # Create a DataFrame to display the common authors
    common_authors_df = pd.DataFrame(common_authors_list)

    # Display the common authors

    # output_path = "common_authors.xlsx"
    # common_authors_df.to_excel(output_path, index=False)
    return common_authors_df

def identifier_exists(identifiers, new_id, id_type_uri):

    for identifier in identifiers:
        if 'type' in identifier and identifier['type']['uri'] == id_type_uri and identifier['id'] == new_id:
            return True
    return False
def update_externalpersons_pure(persons):
    # Set up retry strategy
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "PUT"],
        backoff_factor=1
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)


    for index, row in persons.iterrows():
        session = requests.Session()
        session.mount("https://", adapter)
        uuid = row['Pure_UUID']
        headers = {
            'Accept': 'application/json',
            'api-key': '0dac149f-5630-4044-8276-c1ed2e5cd0f0',
        }

        api_url = 'https://staging.research-portal.uu.nl/ws/api/' + 'external-persons/' + uuid

        response = session.get(api_url, headers=headers, verify=False)
        data = response.json()  # Directly parse JSON response

        new_openalexid = None
        new_orcid = None

        if row['Alex_ID']:
            new_openalexid = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['Alex_ID'],
                "type": {
                    "uri": "/dk/atira/pure/externalperson/externalpersonsources/open_alex_id",
                    "term": {
                        "en_GB": "Open Alex id"
                    }
                }
            }

        if row['ORCID']:
            new_orcid = {
                "typeDiscriminator": "ClassifiedId",
                "id": row['ORCID'],
                "type": {
                    "uri": "/dk/atira/pure/externalperson/externalpersonsources/orcid",
                    "term": {
                        "en_GB": "ORCID"
                    }
                }
            }

        # Check if the new ORCID and OpenAlex ID already exist
        orcid_exists = new_orcid and identifier_exists(data.get('identifiers', []), new_orcid['id'],
                                                       new_orcid['type']['uri'])
        openalexid_exists = new_openalexid and identifier_exists(data.get('identifiers', []), new_openalexid['id'],
                                                                 new_openalexid['type']['uri'])



        # Initialize identifiers if not already present
        if 'identifiers' not in data:
            data['identifiers'] = []

        # Add the new ORCID if it does not already exist and the ID is not empty
        if new_orcid and not orcid_exists:
            data['identifiers'].append(new_orcid)

        # Add the new OpenAlex ID if it does not already exist
        if new_openalexid and not openalexid_exists:
            data['identifiers'].append(new_openalexid)

        if (new_orcid and not orcid_exists) or (new_openalexid and not openalexid_exists):
            if data['identifiers']:  # Ensure identifiers array is not empty
                api_url = 'https://staging.research-portal.uu.nl/ws/api/' + 'external-persons/' + uuid

                response = session.put(api_url, headers=headers, json=data, verify=False)
                if response.status_code != 200:
                    print(f"Failed to update data for UUID {uuid}: {response.text}")
                else:
                    print(f"Successfully updated data for UUID {uuid}")
            else:
                print(f"No valid identifiers to update for UUID {uuid}")            # Respect rate limits
                time.sleep(1)  # Adjust the sleep time based on rate limits
                # Close the session
                session.close()

                    # response = requests.put(api_url, headers=headers, json=data)

def print_faculty_list(faculty_list):
    for idx, faculty in enumerate(faculty_list, start=1):
        print(f"{idx}. {faculty['value']}")
    print("all. All Faculties")
def select_faculties():
    params = {
        'value': 'uu faculty',
    }
    url = RIC_BASE_URL + 'organization/search'

    response = requests.get(url, params=params)
    data = response.json()

    faculties = []
    faculty_person_data = {}
    print(
        "Please choose one of the following faculties (or choose all):")
    print_faculty_list(data["results"])

    # Get user input
    choice = input("Enter the number of your choice, or 'all' to select all faculties: ")

    if choice.lower() == 'all':
        selected_faculties = data["results"]
    else:
        selected_faculties = [data["results"][int(choice) - 1]]

    return selected_faculties

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '100'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_researchoutputs(persoonroot_key):
    """Fetch person IDs for a given person-ro    ot."""
    try:
        params = {'key': persoonroot_key, 'category_want': 'journal article'}
        response = requests.get('http://127.0.0.1:3030/api/get_all_neighbor_nodes', params=params)
        # response.raise_for_status()
        return response.json().get("results", [])

    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []


def select_persons_researchoutput(selected_faculties):
    persons = []
    print("You have selected:")

    for faculty in selected_faculties:
        print(faculty['_key'])
        faculty_key = faculty['_key']
        logging.info(f"Processing faculty: {faculty_key}")

        personroots = fetch_personroots(faculty_key)
        new_data = []

        for personroot in personroots:
            logging.info(f"RICgraph - Processing person: {personroot}")

            if not personroot['_key'] == None:
                personroot_key = personroot['_key']

                outputs = select_researchoutputs(personroot_key)
                for output in outputs:
                    doi = 'doi.org/' + output["_key"].split("|")[0]
                    logging.info(f"RICgraph - Processing publication: {doi}")
                    if 'Pure-uu' in output["_source"] and 'OpenAlex-uu' in output["_source"]:
                        new_data.append(doi)
                        print(output["_key"], output["_source"])
                    else:
                        print(output["_key"], 'not in both systems')
        return new_data


def mainproces(doi):
    oa_article = get_ro_from_openalex(doi)
    pure_article = get_ro_from_pure(doi)

    persons = match_persons_oa_pure(oa_article, pure_article)
    update_externalpersons_pure(persons)



def main():
    # Set logging level to INFO for this script
    logger = setup_logging('update_externalpersonids_from_ricgraph', level=logging.INFO)
    logger.info("Script to update ids from externalpersons in pure from ricgraph has started")
    faculties = select_faculties()
    researchoutputs = select_persons_researchoutput(faculties)
    # test = test_or_not(researchoutputs)


    # Loop through each DOI and make a check if persons can be updated
    # Get the number of entries
    # Get the number of elements in the list
    num_elements = len(researchoutputs)
    print(f"Number of researchoutput: {num_elements}")
    number  = 0
    for doi in researchoutputs:
        number = number + 1
        print(number, doi)
        mainproces(doi)

main()
#
# file = 'pure1.json'
# with open(file, "w") as f:
#     json.dump(pure_article, f, indent=4)