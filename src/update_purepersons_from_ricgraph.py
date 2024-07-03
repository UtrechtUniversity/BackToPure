import configparser
import logging
import math
import os
import pandas as pd
import requests
import json
from logging_config import setup_logging
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ID_URI
# steps:
# - get list of faculties
# - user choose faculty (or all)
# - get persons from faculty
# - get list of ids from all persons
# - check if id is in pure for all persons
# - update person pure if not present (and predefined)

logger = setup_logging('update_researchoutput_from_ricgraph', level=logging.INFO)
def print_faculty_list(faculty_list):
    for idx, faculty in enumerate(faculty_list, start=1):
        print(f"{idx}. {faculty['value']}")
    print("all. All Faculties")

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '0'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def fetch_person_ids(persoonroot_key):
    """Fetch person IDs for a given person-ro    ot."""
    try:
        params = {'key': persoonroot_key, 'category_want': 'person'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []

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
        "This script will export all ids's of all persons of a given faculty. Please choose one of the following faculties (or choose all):")
    print_faculty_list(data["results"])

    # Get user input
    choice = input("Enter the number of your choice, or 'all' to select all faculties: ")

    if choice.lower() == 'all':
        selected_faculties = data["results"]
    else:
        selected_faculties = [data["results"][int(choice) - 1]]

    return selected_faculties


# Function to check if a value is NaN
def is_nan(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


def checkenrichement(persoonroot_key):

    try:
        params = {'key': persoonroot_key, 'source_system': 'pure uu', 'max_nr_items': 1}
        url = RIC_BASE_URL + 'person/enrich'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []


def select_persons(selected_faculties):
    persons = []
    print("You have selected:")

    for faculty in selected_faculties:
        print(faculty['_key'])
        faculty_key = faculty['_key']
        logging.info(f"Processing faculty: {faculty_key}")

        personroots = fetch_personroots(faculty_key)

        for persoonroot in personroots:
            persoonroot_key = persoonroot['_key']
            enrich = checkenrichement(persoonroot_key)
            print(enrich)
            personids = fetch_person_ids(persoonroot_key)

            persons.extend([
                [persoonroot_key, personid['name'], personid['value']]
                for personid in personids
            ])

    # Convert the list into a DataFrame
    df = pd.DataFrame(persons, columns=['person_id', 'id_name', 'id_value'])

    # Group by `person_id` and `id_name` and aggregate `id_value` by joining them if duplicates exist
    df_aggregated = df.groupby(['person_id', 'id_name'], as_index=False).agg(lambda x: ' | '.join(x))

    # Pivot the DataFrame to have a row per person-root and id_name as columns
    persondf = df_aggregated.pivot(index='person_id', columns='id_name', values='id_value').reset_index()

    # Define the file path
    print("downloaded all ids to excel in person_ids.xlsx")
    file_path = "person_ids.xlsx"

    # Save the dataframe to an Excel file
    persondf.to_excel(file_path, index=False)

    return persondf


def update_person(new_ids, data, api_url):

    for new in new_ids:
        new_identifier = {
            'typeDiscriminator': 'ClassifiedId',
            'id': new['id'],
            'type': {
                'uri': new['uri'],

            }
        }
        # Append the new identifier to the existing list of identifiers
        if 'identifiers' in data:
            data['identifiers'].append(new_identifier)
        else:
            data['identifiers'] = [new_identifier]

    response2 = requests.put(api_url, headers=PURE_HEADERS, json=data)
    print(response2.status_code)




def check_new_ids(row, data):
    # Extract the identifiers from the data

    identifiers = data.get('identifiers', [])
    # Convert identifiers to a dictionary for quick lookup
    existing_ids = {entry.get('id') or entry.get('value'): entry for entry in identifiers}

    # Create lists for new IDs and different IDs
    new_ids = []
    different_ids_values = []
    orcid = ''
    orcidchange = ''
    # Check each ID in the DataFrame row against the existing IDs

    for key, value in row.items():

        if key == 'ORCID':
            orcid = value

        if key in ID_URI and not is_nan(value):
            id_type_uri = ID_URI[key]
            found = False
            for entry in identifiers:
                if entry.get('type', {}).get('uri') == id_type_uri:
                    found = True
                    if (entry.get('id') or entry.get('value')) != value:

                        different_ids_values.append({
                            'id': value,
                            'uri': id_type_uri,
                            'existing_id': entry.get('id') or entry.get('value')
                        })
            if not found:
                new_ids.append({'id': value, 'uri': id_type_uri})

    # check orchid:

    if 'orcid' not in data:
        if isinstance(orcid, float) and math.isnan(orcid):
            pass
        else:

            data['orcid'] = orcid
            orcidchange = 'X'


    return new_ids, data, orcidchange

def find_item_by_uuid(data, target_uuid):

    items = data.get('items', [])

    for item in items:

        if item.get('uuid') == target_uuid:
            return item
    return None


def test_or_not(persons):
    for person in persons:
        print(person)
    print(f"above persons with id's are not in pure but are in ricgraph")
    print("Would you like to do a test run? (id's will not be inserted in pure, but the script will check if all needed info is there")
    choice = input("enter yes or no ")
    return choice


def main():
    selected_faculties = select_faculties()
    person_df = select_persons(selected_faculties)
    id_list = person_df['PURE_UUID_PERS'].tolist()


    json_data = {
        'uuids': id_list,
        'size': 999999,
        'offset': 0,

    }
    headers = PURE_HEADERS
    response = requests.post('https://staging.research-portal.uu.nl/ws/api/persons/search', headers=headers,
                             json=json_data)
    datatotal = response.json()
    print("total persons selected: ", datatotal["count"])
    test = test_or_not(id_list)
    file = 'datatotal.json'
    with open(file, "w") as f:
        json.dump(datatotal, f, indent=4)

    counter = 0
    # now per person get id's from pure, and check if id is filled. if not. it should update the person
    for index, row in person_df.iterrows():

        data = find_item_by_uuid(datatotal, row['PURE_UUID_PERS'])

        if data:
            new_ids, data, orcidchange = check_new_ids(row, data)

            if new_ids or orcidchange:

                for id in new_ids:
                    print (id)
                counter = counter + 1
                api_url = PURE_BASE_URL + 'persons/' + row['PURE_UUID_PERS']
                if test == 'no':
                    update_person(new_ids, data, api_url)

    print('total persons updated = ', counter)

# ########################################################################
# MAIN
# ########################################################################

if __name__ == '__main__':
    main()
