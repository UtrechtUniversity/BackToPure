import configparser
import logging
from logging_config import setup_logging
import math
import os
import pandas as pd
import requests
import datacite_utils
import pure_datasets as puda
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, OPENALEX_HEADERS, OPENALEX_BASE_URL

import json
# steps:
# - get list of faculties
# - user choose faculty (or all)
# - get persons from faculty
# - get datasets from persons
# - check pure for presence of these datasets
# - create datasets in pure


def print_faculty_list(faculty_list):
    for idx, faculty in enumerate(faculty_list, start=1):
        print(f"{idx}. {faculty['value']}")
    print("all. All Faculties")

def fetch_personroots(faculty_key):
    """Fetch person-root nodes for a given faculty."""
    try:
        params = {'key': faculty_key, 'max_nr_items': '9999'}
        response = requests.get('http://127.0.0.1:3030/api/get_all_personroot_nodes', params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person-roots for faculty {faculty_key}: {e}")
        return []

def select_faculties():
    params = {
        'value': 'uu faculty',
    }

    response = requests.get('http://127.0.0.1:3030/api/organization/search', params=params)
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

def select_persons_datasets(selected_faculties):
    persons = []
    print("You have selected:")

    for faculty in selected_faculties:
        print(faculty['_key'])
        faculty_key = faculty['_key']
        logging.info(f"Processing faculty: {faculty_key}")

        personroots = fetch_personroots(faculty_key)
        data = []
        for persoonroot in personroots:
            if not persoonroot['_key'] == None:
                persoonroot_key = persoonroot['_key']
                datasets = select_datasets(persoonroot_key)

                for set in datasets:

                    print(set["_key"], set["_source"])
                    doi = set["_key"].split("|")[0]
                    data.append(doi)

        return data

            # persons.extend([
            #     [persoonroot_key, personid['name'], personid['value']]
            #     for personid in personids
            # ])


def select_datasets(persoonroot_key):


    """Fetch person IDs for a given person-ro    ot."""
    try:

        params = {'key': persoonroot_key, 'category_want': 'data set'}

        response = requests.get('http://127.0.0.1:3030/api/get_all_neighbor_nodes', params=params)

        # response.raise_for_status()
        return response.json().get("results", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching person IDs for person-root {persoonroot_key}: {e}")
        return []
def test_or_not(datasets):
    number_of_datasets = len(datasets)
    print(f"{number_of_datasets} are not in pure but are in ricgraph")
    print("Would you like to do a test run? (datsets will not be inserted in pure, but the script will check if all needed info is there")
    choice = input("enter yes or no ")
    return choice


def main():
    # Set logging level to INFO for this script
    logger = setup_logging('update_datasets_from_ricgraph', level=logging.INFO)
    logger.info("Script to update datasets in pure from ricgraph has started")
    faculties = select_faculties()
    datasets = select_persons_datasets(faculties)
    # datasets = select_datasets(persons)
    print(datasets)
    test = test_or_not(datasets)
    df = datacite_utils.get_df_from_datacite(datasets)

    # Define the file path
    print("downloaded all datasets to excel in datasets.xlsx")
    file_path = "datasets.xlsx"
    # Save the dataframe to an Excel file
    df.to_excel(file_path, index=False)

    created = 0
    ignored = 0
    for _, row in df.iterrows():

        already_in_pure = puda.find_dataset(None, row['doi'])

        if already_in_pure:
            logging.info(f"dataset with doi: {row['doi']}, already in pure")
            print("skipped ", row['doi'], ' , is already in pure')
            ignored += 1
        else:
            print(row['persons'])
            contributors_details = puda.get_contributors_details(row['persons'], row['publication_year'], row['title'], test)
            if contributors_details and not already_in_pure:
                row['parsed_contributors'] = puda.format_contributors(contributors_details)
                row['parsed_organizations'], row['managing_org'] = puda.format_organizations_from_contributors(
                    contributors_details)

                # Construct the dataset JSON
                dataset_json = puda.construct_dataset_json(row)
                if test == 'no':
                    uuid_ds = puda.create_dataset(dataset_json)
                    if not uuid_ds == 'error':
                        print('created dataset: ', uuid_ds)
                        created += 1
                    else:
                        ignored += 1
                        print('error creating dataset: ', row['title'])
            else:
                ignored += 1
    print(f"Process completed. created datasets: {created}, skipped: {ignored}")

# ########################################################################
# MAIN
# ########################################################################

if __name__ == '__main__':
    main()
