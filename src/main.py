# ########################################################################
#
# Ricgraph to Pure
#
# ########################################################################
#
# MIT License
#
# main line of work:
# (to do) get publications from ricgraph (for example all publications that are not in pure):
#     - get dois
#     - get persons of publications with their id's
#
# get publication information from source (for now only open_alex as source)
# check if publication should be in pure:
# - at least one internal person
# - check on possible duplicates (names)  (to do)
# - check if journal exists in pure  (to do)

# if all is ok:
# - get all persons of publication (check if exists in pure => internal persons, if not make new external person)
# - get persons id's and org id's of publication
# - get journal id of publication
# - fill jsonfile with metadata
# - make research output in pure

import configparser
import logging
import requests
import json
from requests.structures import CaseInsensitiveDict
from pure_api_utils import get_contributors_details, get_journal_uuid, construct_research_output_json, parse_contributors, create_research_output
import csv
import openalex_utils
import os
__author__ = 'David Grote Beverborg'
__copyright__ = 'Copyright (c) 2023 David Grote Beverborg'
__email__ = ''
__license__ = 'MIT License'
__package__ = 'Ricgraph'
__version__ = ''


def get_dois_from_csv(filename):
    dois = []
    # Read from the CSV file
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        for row in reader:
            # Each row is a list, where the first element is the DOI
            if row:  # Check if row is not empty
                dois.append(row[0])
    return dois

def get_jsons_from_open_alex(dois):
    OPENALEX_HEADERS = {'Accept': 'application/json',
                        # The following will be read in __main__
                        'User-Agent': 'mailto:d.h.j.grotebeverborg@uu.nl'
                        }
    OPENALEX_MAX_RECS_TO_HARVEST = 1
    all_responses = []


    for item in dois:
        doi = 'doi.org/'  +  item
        url = 'https://api.openalex.org/works/' + doi
        try:
            response = requests.get(url, headers=OPENALEX_HEADERS)
            if response.status_code == 200:
                all_responses.append(response.json())
            else:
                print(f"Failed to retrieve data for DOI {item}. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data for DOI {item}: {e}")

    # Write all responses to a JSON file
    with open('all_responses.json', 'w', encoding='utf-8') as f:
        json.dump(all_responses, f, ensure_ascii=False, indent=4)

    return all_responses

def create_pubs_in_pure(df):
    # Process each research output
    for _, row in df.iterrows():
        try:
            # Fetch contributors details

            contributors = row['contributors']

            contributors_details = get_contributors_details(contributors, headers)

            if contributors_details:

                # Fetch journal UUID
                journal = get_journal_uuid(row['journal_issn'], headers)

                # Construct the research output JSON
                research_output_json = construct_research_output_json(
                    row['research_output_id'],

                    contributors_details,
                    journal,
                    row['title'],
                    row['publication_year'],
                    row['publication_month'],
                    row['language_uri'],
                    row['language_term'],
                    row['peer_review'],
                    row['submission_year'],
                    row['doi'],
                    row['visibility_key'],
                    row['workflow_step']
                )

                uuid_ro = create_research_output(research_output_json, headers)
                # Here, you can process the research_output_json as needed
                logging.info(f"Processed research output {row['research_output_id']} successfully.")
            else:
                logging.info(f"skipped research output {row['research_output_id']}.")

        except Exception as e:
            logging.error(f"Error processing research output {row['research_output_id']}: {e}")
    return df

config = configparser.ConfigParser()
config.read('config.ini')
BASE_URL = config['API']['BaseURL']
API_KEY = config['API']['APIKey']
# headers
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"
headers["accept"] = "application/json"
headers["api-key"] = API_KEY
# Ensure the log directory exists
log_directory = "logs"
# Configure logging
logging.basicConfig(filename=os.path.join(log_directory, 'output.log'),
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')



dois = get_dois_from_csv('output.csv')

responses = get_jsons_from_open_alex(dois)

df, errors = openalex_utils.get_df_from_openalex(responses)

df = create_pubs_in_pure(df)

