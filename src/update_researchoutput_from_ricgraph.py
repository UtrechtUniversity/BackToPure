import configparser
import logging
import math
import os
import openalex_utils
import pandas as pd
import requests
import datacite_utils
import pure_researchoutputs as pure
import json
import datetime
from logging_config import setup_logging
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, OPENALEX_HEADERS, OPENALEX_BASE_URL

# steps:
# - get list of faculties
# - user choose faculty (or all)
# - get persons from faculty
# - get researchoutput from persons
# - check pure for presence of these researchoutput
# - create researchoutput in pure


def print_faculty_list(faculty_list):
    for idx, faculty in enumerate(faculty_list, start=1):
        print(f"{idx}. {faculty['value']}")
    print("all. All Faculties")

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
                    if 'Pure-uu' not in output["_source"]:
                        new_data.append(doi)
                        print(output["_key"], output["_source"])
                    else:
                        print(output["_key"], ' already in pure')
        return new_data


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


def test_or_not(researchoutputs):
    number_of_researchoutput = len(researchoutputs)
    print(f"{number_of_researchoutput} are not in pure but are in ricgraph")
    print("Would you like to do a test run? (publications will not be inserted in pure, but the script will check if all needed info is there")
    choice = input("enter yes or no ")
    return choice

def main():
    # Set logging level to INFO for this script
    logger = setup_logging('update_researchoutput_from_ricgraph', level=logging.INFO)
    logger.info("Script to update researchoutput in pure from ricgraph has started")

    faculties = select_faculties()
    researchoutputs = select_persons_researchoutput(faculties)
    test = test_or_not(researchoutputs)

    all_openalex_data = []

    # Loop through each DOI and make a request
    for doi in researchoutputs:
        url = OPENALEX_BASE_URL + doi
        response = requests.get(url, headers=OPENALEX_HEADERS)

        if response.status_code == 200:
            openalex_data = response.json()
            all_openalex_data.append(openalex_data)
        else:
            print(f"Failed to retrieve data for DOI: {doi}")

    df, errors = openalex_utils.transform_openalex_to_df(all_openalex_data)

    # Define the file path
    print("downloaded all publications in ro.xlsx")
    file_path = "ro.xlsx"
    # Save the dataframe to an Excel file
    df.to_excel(file_path, index=False)

    pure.df_to_pure(df, test)
    logger.info("Script to update researchoutput in pure from ricgraph has ended")

# ########################################################################
# MAIN
# ########################################################################

if __name__ == '__main__':
    main()
