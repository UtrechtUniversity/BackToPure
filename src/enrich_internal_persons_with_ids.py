# ########################################################################
# Script: enrich_internal_persons_with_ids.py
#
# Description:
# This script enriches internal person profiles in Pure with additional identifiers
# such as ORCID and ResearcherID using data from Ricgraph.
# It fetches person-root nodes, checks for missing identifiers, and updates
# the Pure system if necessary. The script is designed to run through the
# BackToPure web interface, not as a standalone command-line script.
#
# The script includes:
# - Fetching person-root nodes and identifiers from Ricgraph.
# - Checking for missing identifiers in Pure.
# - Creating JSON files for updating person data.
# - Logging the enrichment progress.
#
# Important:
# This script is meant to be invoked by the BackToPure web interface.
#
# Dependencies:
# - requests, pandas, argparse, logging, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import argparse
import logging
import requests
import pandas as pd
import math
import json
import csv
import sys
import btp
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ID_URI, FACULTY_PREFIX
from logging_config import setup_logging

import os
from datetime import datetime


logger = setup_logging('btp', level=logging.INFO)
logger.handlers[0].stream.flush = lambda: sys.stdout.flush()
datetimetoday = datetime.now().strftime('%Y%m%d')

def is_nan(value):
    """
       Checks if a given value is NaN (Not a Number).
    """
    return value is None or (isinstance(value, float) and math.isnan(value))

def fetch_personroots(faculty_key):

    try:
        params = {'key': faculty_key, 'max_nr_items': '0'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = requests.get(url, params=params)
        # print(response, faculty_key)
        # response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def fetch_person_ids(persoonroot_key):
    try:
        params = {'key': persoonroot_key, 'category_want': 'person'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = requests.get(url, params=params)
        # response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []

def checkenrichement(persoonroot_key):
    try:
        params = {'key': persoonroot_key, 'source_system': 'pure uu', 'max_nr_items': 0}
        url = RIC_BASE_URL + 'person/enrich'
        response = requests.get(url, params=params)
        # response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []

def select_persons(faculties, faculty_choice):
    persons = []
    count = 0

    for faculty in faculties:

        logger.info(f"Processing faculty: {faculty}")


        personroots = fetch_personroots(faculty)
        logger.info(f"Processing {str(len(personroots))} persons in ricgraph, this can be slow...")

        for personroot in personroots:
            count += 1
            if count % 250 == 0:
                logger.debug(f"Processed {str(count)} persons in ricgraph")
            personids = fetch_person_ids(personroot['_key'])
            persons.extend([
                [personroot['_key'], personid['name'], personid['value']]
                for personid in personids
            ])


    df = pd.DataFrame(persons, columns=['person_id', 'id_name', 'id_value'])
    df_aggregated = df.groupby(['person_id', 'id_name'], as_index=False).agg(lambda x: ' | '.join(x))
    persondf = df_aggregated.pivot(index='person_id', columns='id_name', values='id_value').reset_index()

    # Ensure PURE_UUID_PERS is added or handled properly
    if 'PURE_UUID_PERS' not in persondf.columns:
        persondf['PURE_UUID_PERS'] = pd.NA

    file_path = f"output/internal_persons/allpersons_{datetimetoday}.csv"
    # persondf.to_csv(file_path, index=False)
    return persondf

def update_person(new_ids, data, api_url):
    for new in new_ids:
        new_identifier = {
            'typeDiscriminator': 'ClassifiedId',
            'id': new['id'],
            'type': {'uri': new['uri']}
        }
        if 'identifiers' in data:
            data['identifiers'].append(new_identifier)
        else:
            data['identifiers'] = [new_identifier]
    response2 = requests.put(api_url, headers=PURE_HEADERS, json=data)


def check_new_ids(row, data):
    identifiers = data.get('identifiers', [])
    existing_ids = {entry.get('id') or entry.get('value'): entry for entry in identifiers}
    new_ids = []
    different_ids_values = []
    orcid = ''
    orcidchange = ''
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
    if 'orcid' not in data:
        if isinstance(orcid, float) and math.isnan(orcid):
            pass
        else:
            if orcid:
                data['orcid'] = orcid
                orcidchange = 'X'
    return new_ids, data, orcidchange, orcid

def find_item_by_uuid(data, target_uuid):

    for item in data:
        if item.get('uuid') == target_uuid:

            return item
    return None

def fetch_person_data(person_df, batch_size):
    """
    Fetch person data from the Pure API in batches and combine the results.

    Parameters:
    - person_df: DataFrame containing the person data with 'PURE_UUID_PERS' column.
    - headers: Headers required for the API requests.
    - base_url: Base URL of the Pure API.
    - batch_size: Number of records to fetch per batch.

    Returns:
    - datatotal: List containing all the combined data from each API call.
    """
    # Extract the list of UUIDs from the DataFrame
    id_list = person_df['PURE_UUID_PERS'].tolist()
    total_records = len(id_list)
    datatotal = []  # List to collect all data from the API responses
    total_found = 0  # Counter to keep track of the total number of items found
    # Loop through the list in batches

    for offset in range(0, total_records, batch_size):
        batch = id_list[offset:offset + batch_size]
        # Clean the batch by removing NaN values
        batch = [uuid for uuid in id_list[offset:offset + batch_size] if
                 isinstance(uuid, str) and uuid.lower() != 'nan' and not (isinstance(uuid, float) and math.isnan(uuid))]

        json_data = {'uuids': batch, 'size': len(batch), 'offset': 0}
        url = PURE_BASE_URL + 'persons/search/'

        try:
            response = requests.post(url, headers=PURE_HEADERS, json=json_data)
            response.raise_for_status()
            response_data = response.json()
            batch_data = response_data.get('items', [])
            datatotal.extend(batch_data)  # Append batch response to datatotal
            total_found += response_data['count']
            logger.debug(f"processing personnodes  {str(response_data['count'])}, for {batch_size} uuids")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for offset {offset}: {e}")

    logger.info(f"Total persons found in pure:  {str(total_found)}")
    # Return the combined data
    with open('output/internal_persons/datatotal.json', 'w') as file:
        json.dump(datatotal, file, indent=4)
    return datatotal

def update_persons(person_df, datatotal):
    count = 0
    succes = 0

    result_list = []
    # Create DataFrames to store rows for new IDs and no new IDs
    new_df = pd.DataFrame()
    no_new_ids_df = pd.DataFrame()

    for index, row in person_df.iterrows():
        count += 1
        if count % 250 == 0:
            logger.debug(f"Processed {str(count)} persons in Pure")

        logger.debug(f"Checking person: {row['PURE_UUID_PERS']}")
        data = find_item_by_uuid(datatotal, row['PURE_UUID_PERS'])

        if data:
            new_ids, data, orcidchange, orcid = check_new_ids(row, data)
            # Case when there are new IDs or an ORCID change
            if orcidchange:
                # Append to the result list as a new row
                result_list.append({
                    'to_be_updated': 'X',
                    'updated': '',
                    'FULL_NAME': row['FULL_NAME'],
                    'person_id': row['person_id'],
                    'PURE_UUID_PERS': row['PURE_UUID_PERS'],
                    'new_id': 'orcid',
                    'new_value': orcid,

                })
            if new_ids or orcidchange:
                for item in new_ids:
                    new_id = item['uri'].split('/')[-1]  # Extract the last part after '/'
                    new_value = item['id']

                    # Append to the result list as a new row
                    result_list.append({
                        'to_be_updated': 'X',
                        'updated': '',
                        'FULL_NAME':row['FULL_NAME'],
                        'person_id': row['person_id'],
                        'PURE_UUID_PERS': row['PURE_UUID_PERS'],
                        'new_id': new_id,
                        'new_value': new_value,
                        'uri': item['uri']
                    })

                succes += 1
                new_df = pd.DataFrame(result_list)
                logger.debug(f"New IDs found: {new_ids} {orcid}")
                api_url = PURE_BASE_URL + 'persons/' + row['PURE_UUID_PERS']


            else:
                # No new IDs or ORCID change, add to no_new_ids_df
                no_new_ids_df = pd.concat([no_new_ids_df, pd.DataFrame([row])], ignore_index=True)
                logger.debug(f"No new IDs for {row['PURE_UUID_PERS']}")
        else:
            # no_new_ids_df = pd.concat([no_new_ids_df, pd.DataFrame([row])], ignore_index=True)
            logger.debug(f"{row['PURE_UUID_PERS']} not found in Pure")

    logger.info(f"Total persons that can be updated: {succes}")

    # Define output folder and save CSV files
    output_folder = 'output/internal_persons'
    os.makedirs(output_folder, exist_ok=True)
    new_ids_filename = os.path.join(output_folder, f'personstobeupdated_{datetimetoday}.csv')
    no_new_ids_filename = os.path.join(output_folder, f'persons_without_newids_{datetimetoday}.csv')

    # Save the DataFrames to CSV

    new_df.to_csv(new_ids_filename, index=False)
    # no_new_ids_df.to_csv(no_new_ids_filename, index=False)
    logger.info(f"Persons without new IDs saved to {no_new_ids_filename}")


    logger.info(f"Persons that can be updated are in file: {new_ids_filename}")
    logger.info(f"Please open that file to check if you want them all to be updated")
    logger.info(f"if not, please remove the 'X' for that row in the column 'to_be_updated'")


def main(faculty_choice):
    logger.info(f"Script enrich persons has started")

    logger.info("The script performs the following steps:\n"
                 "1. **Person Root Node Retrieval**: Retrieves all person-root nodes from Ricgraph for the selected faculty and fetches the associated person IDs.\n"
                 "2. **Enrichment Check**: Checks if each person already has the required identifiers in Pure. Prepares to update missing information if needed.\n"
                 "3. **Update file**: Produces a file with all of the persons that can be updated, with the new ids. after the first part is finished, you can access that file. You must check that file, and remove unwanted updates\n"
                 "4. **Person Data Update**: After you have checked the file a new button appears that sends the updates to pure.\n\n"
                 "**Note:** The process may take a while before log items appear on the screen, especially if a large faculty is chosen.")

    btp.checks_before_start(faculty_choice)
    faculties = btp.select_faculties(faculty_choice)
    person_df = select_persons(faculties, faculty_choice)

    if not person_df.empty:
       datatotal = fetch_person_data(person_df, 100)
       update_persons(person_df, datatotal)
    logger.info(f"Script enrich persons part 1 has ended")



# ########################################################################
# MAIN
# ########################################################################
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Enrich Internal Persons with IDs.')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='uu faculty: faculteit rebo|organization_name',
                        # default='all',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='yes', help='Run in test mode ("yes" or "no")')

    args = parser.parse_args()

    main(args.faculty_choice)


