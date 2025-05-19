
import logging
import time
import sys
import enrich_internal_persons_with_ids as ipersons
import pure_researchoutputs
import pure_datasets as puda
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS
from logging_config import setup_logging
from datetime import datetime
import json
import pandas as pd
import os
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
#Setup logger

logger = setup_logging('btp', level=logging.INFO)
logger.handlers[0].stream.flush = lambda: sys.stdout.flush()
datetimetoday = datetime.now().strftime('%Y%m%d')
# Set up a single session for all requests
session = requests.Session()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "PUT", "POST"],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
headers = {
    'Accept': 'application/json',
    'api-key': PURE_API_KEY,
}

def get_csv_files(directory):
    """
    Reads all CSV files from the given directory that contain 'update' in the filename and returns a list of DataFrames.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        list: A list of pandas DataFrames for each CSV file found.
    """



    csv_files = {}
    for filename in os.listdir(directory):
        if filename.endswith(".csv") and 'update' in filename:
            file_path = os.path.join(directory, filename)
            try:
                df = pd.read_csv(file_path)
                if df.empty:
                    logger.warning(f"File {filename} is empty. Skipping...")
                    continue
                csv_files[filename] = df
                logger.info(f"Successfully loaded file {filename}.")
            except pd.errors.EmptyDataError:
                logger.warning(f"File {filename} is empty or malformed. Skipping...")
            except Exception as e:
                logger.error(f"Error reading {filename}: {e}")
    return csv_files


def get_json_files(directory):
    """
    Reads all JSON files from the given directory and returns a list of JSON objects.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        list: A list of JSON objects for each JSON file found.
    """
    json_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r") as json_file:
                json_data = json.load(json_file)
                json_files.append(json_data)
    return json_files


def read_directory_files(directory):
    """
    Reads all CSV and JSON files from the given directory, storing CSV files as DataFrames
    and JSON files as JSON objects.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        tuple: A tuple containing two lists - one for CSV DataFrames and one for JSON objects.
    """
    csv_files = get_csv_files(directory)
    json_files = get_json_files(directory)
    return csv_files, json_files


def process_internal_persons(filename, csv_file, json_data):
    """
    Processes a CSV file by looping over each row and retrieving an entry from the big JSON file
    for each 'personuuid' in the CSV file.

    Args:
        csv_file (pd.DataFrame): The CSV file as a DataFrame.
        json_data (list): The big JSON list containing the data to look up.
    """

    # Filter the DataFrame to only consider rows where 'to_be_updated' is 'X'
    filtered_csv = csv_file[csv_file['to_be_updated'] == 'X']

    # Grouping the DataFrame by 'PURE_UUID_PERS' to collect updates for the same person
    grouped = filtered_csv.groupby('PURE_UUID_PERS')
    logger.info(f"{len(grouped)} persons will be updated")
    success = 0
    errors = 0
    for person_uuid, group in grouped:
        # Retrieve the corresponding entry from the JSON data
        entry = next((item for item in json_data if item['uuid'] == person_uuid), None)

        if entry:
            # Iterate through all rows in the group to update the entry
            for _, row in group.iterrows():
                if row['new_id'] == 'orcid':
                    entry['orcid'] = row['new_value']
                else:

                    new_identifier = {
                        'typeDiscriminator': 'ClassifiedId',
                        'id': row['new_value'],
                        'type': {'uri': row.get('uri', '')}  # Use .get() to avoid KeyError
                    }
                    if 'identifiers' in entry:
                        entry['identifiers'].append(new_identifier)
                    else:
                        entry['identifiers'] = [new_identifier]

            # After processing all updates for the person, call the API once
            api_url = PURE_BASE_URL + 'persons/' + person_uuid
            response = requests.put(api_url, headers=PURE_HEADERS, json=entry)
            # response = type('MockResponse', (), {'status_code': 200})()
            # If the API call is successful, mark all rows for the person as updated and clear 'to_be_updated'
            if response.status_code == 200:
                success = success + 1
                csv_file['updated'] = csv_file['updated'].astype(str)
                csv_file.loc[group.index, 'updated'] = 'X'
                csv_file.loc[group.index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
            else:
                errors = errors + 1
                logger.error(f'Failed to update person UUID: {person_uuid}, Response: {response.text}')
        else:
            logger.info(f'Not found for person UUID: {person_uuid}')

    logger.info(f"{success} persons have been updated")
    if errors > 0:
        logger.info(f"{errors} updates have resulted in an error , check te log for more info")

    # Reorder the columns to make 'updated' the second column
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))
    csv_file = csv_file[cols]

    # Save the updated DataFrame to 'output/updated.csv'
    file = 'output/internal_persons/' + filename
    os.makedirs('output', exist_ok=True)  # Ensure the 'output' directory exists
    csv_file.to_csv(file, index=False)

    logger.info(f"Updated DataFrame saved to {file}.")


# Define a function to match and extract the JSON object
def find_json_by_uuid(pure_uuid, json_data):
    for record in json_data:
        if record.get('UUID') == pure_uuid:
            return record
        if record.get('uuid') == pure_uuid:
            return record
    return None

def process_external_persons(filename, csv_file, big_json_data):
    # Filter the DataFrame to only consider rows where 'to_be_updated' is 'X'
    filtered_csv = csv_file[csv_file['to_be_updated'] == 'X']
    logger.info(f"{len(filtered_csv)} persons will be updated")
    success = 0
    for index, row in filtered_csv.iterrows():
        if index % 250 == 0 and index != 0:
            logger.info(f"processing {index} items")
        uuid = row['Pure_UUID']
        matched_record = find_json_by_uuid(uuid, big_json_data)
        if matched_record:
            url = PURE_BASE_URL + 'external-persons/' + row['Pure_UUID']
            try:
                response = session.put(url, headers=headers, json=matched_record, verify=False)
                if response.status_code != 200:
                    logger.warning(f"Failed to update data for UUID {uuid}: {response.text}")
                else:
                    csv_file.loc[index, 'updated'] = 'X'
                    csv_file.loc[index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
                    success = success + 1
                    logger.debug(f"Successfully updated data for UUID {uuid}")
            except Exception as e:
                logger.error(f"Error updating UUID {uuid}: {e}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits

    # Reorder the columns to make 'updated' the second column
    logger.info(f"{success} persons have been updated")
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))
    csv_file = csv_file[cols]

    # Save the updated DataFrame to 'output/updated.csv'
    file = 'output/external_persons/' + filename
    os.makedirs('output', exist_ok=True)  # Ensure the 'output' directory exists
    csv_file.to_csv(file, index=False)



def process_research_output(filename, csv_file, big_json_data):
    filtered_csv = csv_file[csv_file['to_be_updated'] == 'x']

    for index, row in filtered_csv.iterrows():
        doi_item = next((item for item in big_json_data if any(
            version.get("doi", "").endswith(row['doi']) for version in item.get("electronicVersions", []))), None)

        # Output the result
        if doi_item:

            pure_researchoutputs.create_research_output(doi_item)
            csv_file.loc[index, 'updated'] = 'x'
            csv_file.loc[index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
        else:
            logger.debug(f"No item found with DOI: {row['doi']}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits

        # Reorder the columns to make 'updated' the second column
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))
    csv_file = csv_file[cols]

    # Save the updated DataFrame
    file = 'output/research_output/' + filename
    os.makedirs('output/research_output', exist_ok=True)  # Ensure the 'output' directory exists
    csv_file.to_csv(file, index=False)



def process_datasets(filename, csv_file, big_json_data):
    filtered_csv = csv_file[csv_file['to_be_updated'] == 'x']
    # Create a lookup dictionary for faster DOI-based access
    doi_to_dataset = {dataset.get("doi", {}).get("doi"): dataset for dataset in big_json_data}

    for index, row in filtered_csv.iterrows():

        dataset = doi_to_dataset.get(row['doi'])

        # Output the result
        if dataset:

            puda.create_dataset(dataset)
            csv_file.loc[index, 'updated'] = 'x'
            csv_file.loc[index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
        else:
            logger.debug(f"No item found with DOI: {row['doi']}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits

        # Reorder the columns to make 'updated' the second column
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))

    csv_file = csv_file[cols]

    # Save the updated DataFrame
    file = 'output/datasets/' + filename
    os.makedirs('output/datasets', exist_ok=True)  # Ensure the 'output' directory exists
    csv_file.to_csv(file, index=False)



def process_external_orgs(filename, csv_file, big_json_data):
    # Filter the DataFrame to only consider rows where 'to_be_updated' is 'X'
    filtered_csv = csv_file[csv_file['to_be_updated'] == 'X']
    logger.info(f" {len(filtered_csv)} are selected to be updated")
    succes = 0
    for index, row in filtered_csv.iterrows():
        uuid = row['uuid']

        # print(big_json_data)
        matched_record = find_json_by_uuid(uuid, big_json_data)
        # print('test', matched_record)
        if matched_record:

            url = PURE_BASE_URL + 'external-organizations/' + row['uuid']

            try:
                response = session.put(url, headers=headers, json=matched_record, verify=False)

                if response.status_code != 200:
                    logger.info(f"Failed to update data for UUID {uuid}: {response.text}")

                else:
                    csv_file.loc[index, 'updated'] = 'X'
                    csv_file.loc[index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
                    logger.debug(f"Successfully updated data for UUID {uuid}")
                    succes +=1
            except Exception as e:
                logger.error(f"Error updating UUID {uuid}: {e}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits

    # Reorder the columns to make 'updated' the second column
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))
    csv_file = csv_file[cols]

    # Save the updated DataFrame to 'output/updated.csv'uccessfully updated data for UUID
    file = 'output/external_orgs/' + filename
    os.makedirs('output', exist_ok=True)  # Ensure the 'output' directory exists
    csv_file.to_csv(file, index=False)
    logger.info(f" {succes} external persons are updated.. check pure for the results")


if __name__ == "__main__":

    # Step 1: Retrieve the REFERER_PAGE environment variable
    referer_page = os.environ.get('REFERER_PAGE', 'unknown')

  
    logger.debug(f"Script called from page: {referer_page}")

    # Step 2: Execute specific logic based on the Referer
    if 'enrich_external_persons' in referer_page:
        directory = "output/external_persons"
    elif 'enrich_internal_persons_with_ids' in referer_page:
        directory = "output/internal_persons"
    elif 'enrich_external_orgs' in referer_page:
        directory = "output/external_orgs"
    elif 'import_datasets' in referer_page:
        directory = "output/datasets"
    elif 'import_research_output' in referer_page:
        directory = "output/research_output"

    csv_files, json_files = read_directory_files(directory)
    if json_files:
        big_json_data = json_files[0]
        for filename, csv_file in csv_files.items():
            if 'enrich_external_persons' in referer_page:
                process_external_persons(filename, csv_file, big_json_data)
            elif 'enrich_internal_persons_with_ids' in referer_page:
                process_internal_persons(filename, csv_file, big_json_data)
            elif 'import_research_outputs' in referer_page:
                process_research_output(filename, csv_file, big_json_data)
            elif 'import_datasets' in referer_page:
                process_datasets(filename, csv_file, big_json_data)
            elif 'enrich_external_orgs' in referer_page:
                process_external_orgs(filename, csv_file, big_json_data)
    logger.info(f"script to update Pure has ended")
