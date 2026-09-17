# ########################################################################
# Script: update_datasets_from_ricgraph.py
#
# Description:
# This script retrieves dataset DOIs from Ricgraph and compares them with
# datasets in the Pure system. If a dataset is missing in Pure, metadata is
# retrieved, formatted, and saved for import. The script supports running in
# test mode.
#
# The script includes:
# - Fetching a list of faculties and selecting datasets by faculty.
# - Fetching datasets associated with person nodes from Ricgraph.
# - Comparing datasets between Ricgraph and Pure.
# - Creating JSON files for datasets that can be imported into Pure.
#
# Steps:
# 1. Retrieve a list of faculties.
# 2. Fetch persons and their associated datasets by faculty.
# 3. Compare datasets in Ricgraph and Pure.
# 4. Create a JSON file for import and a CSV for manual verification.
#
# Important:
# This script calls utility modules such as `datacite_utils.py` and `pure_datasets.py`.
# Do not run this script without the dependencies.
#
# Dependencies:
# - requests, pandas, datacite_utils, pure_datasets, argparse, logging, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################

import logging
import requests
import datacite_utils
import time
import pure_datasets as puda
import pandas as pd
import os
import argparse
import enrich_pure_external_persons as enrich
from config import FACULTY_PREFIX, RIC_BASE_URL
from logging_config import setup_logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = setup_logging('dataset', level=logging.INFO)
import json
# steps:
# - get list of faculties
# - user choose faculty (or all)
# - get persons from faculty
# - get datasets from persons
# - check pure for presence of these datasets
# - create datasets in pure

session = requests.Session()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    backoff_factor=1,
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))

def print_faculty_list(faculty_list):
    for idx, faculty in enumerate(faculty_list, start=1):
        print(f"{idx}. {faculty['value']}")
    print("all. All Faculties")

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '9999'}
        url = RIC_BASE_URL + 'get_all_personroot_nodes'
        response = session.get(url, params=params, timeout=30)

        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_faculties(faculty_choice):
    # Set logging level to INFO for this script
    logger = setup_logging('dataset', level=logging.INFO)
    logger.info("Script to update datasets in pure from ricgraph has started")
    params = {
        'value': FACULTY_PREFIX,
    }
    try:
        url = RIC_BASE_URL + 'organization/search'
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching faculties from Ricgraph: {e}")
        return []

    if faculty_choice.lower() == 'all':
        selected_faculties = [item['_key'] for item in data["results"] if enrich.is_primary_faculty_key(item.get('_key'))]
    else:
        selected_faculties = [faculty_choice] if enrich.is_primary_faculty_key(faculty_choice) else []

    return selected_faculties

def select_persons_datasets(faculties, faculty_choice):
    persons = []
    data = []
    if faculty_choice == 'all':

        params = {
            'category': 'data set',
            'max_nr_items': '0',
        }
        data = []
        url = RIC_BASE_URL + 'advanced_search'
        response = session.get(url, params=params, timeout=30)
        datasets = response.json().get("results", [])
        for set in datasets:
            doi = set["_key"].split("|")[0]
            data.append(doi)
    else:
        for faculty in faculties:
            logger.info(f"Processing faculty: {faculty}")

            personroots = fetch_personroots(faculty)
            data = []
            for persoonroot in personroots:
                if not persoonroot['_key'] == None:
                    persoonroot_key = persoonroot['_key']
                    datasets = select_datasets(persoonroot_key)
                    for set in datasets:
                        doi = set["_key"].split("|")[0]
                        data.append(doi)
    data = list(dict.fromkeys(data))
    logger.info("datasets found in ricgraph: " + str(len(data)))
    return data


def select_datasets(persoonroot_key):

    """Fetch IDs for a given person-ro    ot."""
    try:
        params = {'key': persoonroot_key, 'category_want': 'data set'}
        url = RIC_BASE_URL + 'get_all_neighbor_nodes'
        response = session.get(url, params=params, timeout=30)

        return response.json().get("results", [])
    except requests.RequestException as e:
        logger.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []
def test_or_not(datasets):
    number_of_datasets = len(datasets)
    print(f"{number_of_datasets} are not in pure but are in ricgraph")
    print("Would you like to do a test run? (datsets will not be inserted in pure, but the script will check if all needed info is there")
    choice = input("enter yes or no ")
    return choice


def df_to_pure(df, created, ignored, no_internal):
    dataset_collection = []
    to_be_updated_rows = []
    logger.info(
        'Formatting the output in Pure needed JSON format. This is a slow process, you might want to get some coffee...')
    for _, row in df.iterrows():

        if _ % 10 == 0:  # Print progress every 5 iterations

            logger.info(f"Processing: {_}")

        already_in_pure = puda.find_dataset(None, row['doi'])
        if already_in_pure:
            logger.debug(f"dataset with doi: {row['doi']}, already in pure")
            ignored += 1
        else:
            contributors_details = puda.get_contributors_details(row['persons'], row['created'])

            if contributors_details is not None:
                row['parsed_contributors'] = puda.format_contributors(contributors_details)
                row['parsed_organizations'], row['managing_org'] = puda.format_organizations_from_contributors(
                    contributors_details)

                dataset_json = puda.construct_dataset_json(row)
                if dataset_json:
                    dataset_collection.append(dataset_json)
                    # Add data to 'to be updated' list
                    to_be_updated_rows.append({
                        'to_be_updated': 'x',
                        'updated': ' ',
                        'doi': row['doi'],
                        'title': row['title']
                    })
                    created += 1

                    # uuid_ds = puda.create_dataset(dataset_json)
                    # if not uuid_ds == 'error':
                    #    created += 1
                    # else:
                    #     ignored += 1
                    #     logger.debug('error creating dataset: ' + row['title'])

            else:
                no_internal += 1

    output_dir = os.environ.get('BTP_OUTPUT_DIR', 'output/datasets')
    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists
    output_file = os.path.join(output_dir, 'datasets_to_be_updated.json')
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(dataset_collection, f, ensure_ascii=False, indent=4)
        logger.debug(f"Successfully saved datasets to {output_file}.")
    except Exception as e:
        logger.error(f"Failed to save datasets: {e}")

    # Create and save the "to be updated" DataFrame
    to_be_updated_df = pd.DataFrame(to_be_updated_rows)
    csv_output_file = os.path.join(output_dir, 'to_be_updated.csv')

    try:
        to_be_updated_df.to_csv(csv_output_file, index=False, encoding='utf-8')
        logger.debug(f"Successfully saved 'to be updated' DataFrame to {csv_output_file}.")
    except Exception as e:
        logger.error(f"Failed to save 'to be updated' DataFrame: {e}")

    return created, ignored, no_internal


def main(faculty_choice):
    logger.debug("Starting datasets import flow")
    faculties = select_faculties(faculty_choice)
    if not faculties:
        logger.error("No faculties found or Ricgraph unavailable; stopping dataset import.")
        return
    datasets = select_persons_datasets(faculties, faculty_choice)

    df = datacite_utils.get_df_from_datacite(datasets)


    created = 0
    ignored = 0
    no_internal = 0
    created, ignored, no_internal = df_to_pure(df, created, ignored, no_internal)

    logger.info(f"Process completed. datasets that can be imported: {created}")
    logger.info(f"Process completed. datasets that are already in pure: {ignored}")
    logger.info(f"Process completed. datasets that have no internal persons: {no_internal}")
    logger.info("Script part 1 to import datasets in pure from ricgraph has ended")
    logger.info("Please look at the update file and uncheck items you do not want to be imported, then proceed to import them in pure via *Apply Update to Pure*")

# ########################################################################
# MAIN
# ########################################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import Datasets from Ricgraph')
    parser.add_argument('faculty_choice', type=str, nargs='?',
                        default='all',
                        help='Faculty choice or "all"')
    parser.add_argument('test_choice', type=str, nargs='?', default='no', help='Run in test mode ("yes" or "no")')

    args = parser.parse_args()

    main(args.faculty_choice)
