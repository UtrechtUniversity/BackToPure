# ########################################################################
# Script: datacite_utils.py
#
# Description:
# This script provides **main functions** for interacting with the DataCite API,
# fetching metadata for datasets based on DOIs, and formatting the data into
# pandas DataFrames. It is meant to be imported as a module by other scripts and
# should not be executed standalone.
#
# Functions include:
# - Fetching metadata for a single DOI and parsing it.
# - Fetching metadata for multiple DOIs using multi-threading.
# - Formatting parsed data into a pandas DataFrame.
#
# Important:
# This script is a utility module and is intended to be used by other scripts.
#
# Dependencies:
# - requests, pandas, json, concurrent.futures, logging, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import requests

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
import logging
from logging_config import setup_logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
logger = setup_logging('dataset', level=logging.INFO)

session = requests.Session()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    backoff_factor=1,
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
NOT_FOUND_DOI_COUNT = 0
NOT_FOUND_DOI_SAMPLES = []

def get_first_affiliation_name(affiliations):
    if isinstance(affiliations, list) and affiliations:
        # Assume each item in the list is a dictionary with a 'name' key
        first_affiliation = affiliations[0]
        if isinstance(first_affiliation, dict):
            return first_affiliation.get('name', 'None')
        elif isinstance(first_affiliation, str):
            return first_affiliation  # Assuming the string itself is the name
    # Default case if 'affiliations' is not list-like or is empty
    return 'None'

def fetch_data_for_doi(doi):
    """Fetch and parse data for a single DOI."""
    global NOT_FOUND_DOI_COUNT, NOT_FOUND_DOI_SAMPLES
    wait_seconds = 1.5
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(f'https://api.datacite.org/dois/{doi}', timeout=30)
        except requests.RequestException as e:
            logger.info(f"Failed to fetch data for DOI: {doi} ({e})")
            time.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2, 30)
            continue

        if response.status_code == 200:
            try:
                data = response.json()['data']['attributes']
                return parse_datacite_response(data, doi)
            except Exception as e:
                logger.info(f"Failed to parse DataCite response for DOI: {doi} ({e})")
                return None

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and str(retry_after).isdigit():
                wait_seconds = max(wait_seconds, int(retry_after))
            logger.info(f"DataCite rate limit for DOI {doi} (attempt {attempt}/{max_attempts}), waiting {wait_seconds:.1f}s")
            time.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2, 30)
            continue

        if response.status_code == 404:
            NOT_FOUND_DOI_COUNT += 1
            if len(NOT_FOUND_DOI_SAMPLES) < 10:
                NOT_FOUND_DOI_SAMPLES.append(doi)
            return None

        logger.info(f"Failed to fetch data for DOI: {doi} (status {response.status_code})")
        return None

    logger.info(f"Failed to fetch data for DOI: {doi} after {max_attempts} attempts")
    return None

def parse_datacite_response(data, doi):
    """Parse the response from DataCite API and return structured data."""

    title = data['titles'][0]['title']
    persons = []
    description = ''
    for creator in data['creators']:

        if not 'givenName' in creator and not 'familyName' in creator:

            if 'name' in creator:
                # Split the name into parts
                name_parts = creator['name'].split(",")
                if len(name_parts) != 2:
                    name_parts = creator['name'].split(" ")
                creator['givenName'] = name_parts[0]
                creator['familyName'] = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        if 'givenName' in creator and  'familyName' in creator:
            affiliations = creator.get('affiliation', [])
            first_affiliation_name = get_first_affiliation_name(affiliations)
            person_ids = {
                ni.get('nameIdentifierScheme'): ni.get('nameIdentifier')
                for ni in creator.get('nameIdentifiers', [])
                if ni.get('nameIdentifier')  # Ensure only valid entries are included
            }
            creator_info = {
                # 'name': creator['name'],
                'first_name': creator['givenName'],
                'last_name': creator['familyName'],
                'type': 'creator',
                'affiliations': first_affiliation_name,
                'person_ids': person_ids

            }
            persons.append(creator_info)

    subjects = [subject['subject'] for subject in data.get('subjects', [])]

    if 'descriptions' in data and isinstance(data['descriptions'], list):
        for desc in data['descriptions']:
            if desc.get('descriptionType', '').lower() == 'abstract':
                description = desc.get('description', '') or ''
                break

    return {
        'title': title,
        'description': description,
        'persons': persons,
        'publisher': data['publisher'],
        'doi': doi,
        'publication_year': data['publicationYear'],
        'created': str(datetime.strptime(data['created'], '%Y-%m-%dT%H:%M:%S.%fZ')),
        'subjects': subjects
    }

def get_df_from_datacite(datasets):
    """Fetch data for multiple DOIs and return a DataFrame."""
    global NOT_FOUND_DOI_COUNT, NOT_FOUND_DOI_SAMPLES
    NOT_FOUND_DOI_COUNT = 0
    NOT_FOUND_DOI_SAMPLES = []

    unique_datasets = list(dict.fromkeys(datasets))
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(fetch_data_for_doi, unique_datasets))

    # Filter out None results in case of failed fetches
    valid_results = [result for result in results if result]

    # """Fetch data for multiple DOIs and return a DataFrame."""
    # results = [fetch_data_for_doi(doi) for doi in datasets]
    #
    # # Filter out None results in case of failed fetches
    # valid_results = [result for result in results if result is not None]


    df = pd.DataFrame(valid_results)
    if NOT_FOUND_DOI_COUNT > 0:
        sample_text = ", ".join(NOT_FOUND_DOI_SAMPLES)
        logger.info(f"DataCite 404 for {NOT_FOUND_DOI_COUNT} DOI(s). Sample: {sample_text}")
    # logger.info("datasets found in open alex: " + str(df.shape[0]))
    # file_path = "datasets.xlsx"
    # logger.info("downloaded datasets in: " + file_path)
    # # Save the dataframe to an Excel file
    # df.to_excel(file_path, index=False)
    return df

def main():
    return
    # # Usage example, assuming 'datasets' is a list of DOI strings
    # datasets = ['10.6084/M9.FIGSHARE.21829182', '10.5061/dryad.tn70pf1', 'DOI3']
    # df = get_df_from_datacite2(datasets)
    # print(df)

if __name__ == '__main__':
    main()
