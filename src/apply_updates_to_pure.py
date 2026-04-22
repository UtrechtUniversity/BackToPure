
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
session.mount("http://", adapter)
# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
headers = {
    'Accept': 'application/json',
    'api-key': PURE_API_KEY,
}


def _resolve_output_directory(referer_page):
    explicit_directory = os.environ.get("BTP_OUTPUT_DIR")
    if explicit_directory:
        return explicit_directory

    if 'enrich_external_persons' in referer_page:
        return "output/external_persons"
    if 'enrich_internal_persons_with_ids' in referer_page:
        return "output/internal_persons"
    if 'enrich_external_orgs' in referer_page:
        return "output/external_orgs"
    if 'import_datasets' in referer_page:
        return "output/datasets"
    if 'import_research_output' in referer_page:
        return "output/research_output"
    if 'import_research_outputs' in referer_page:
        return "output/research_output"
    return None


def _selected_for_update(csv_file):
    """Return rows marked for update, accepting x/X with optional whitespace."""
    markers = csv_file['to_be_updated'].fillna('').astype(str).str.strip().str.lower()
    return csv_file[markers == 'x']


def _parse_target_filenames(raw_value):
    if not raw_value:
        return None
    return {item.strip() for item in raw_value.split(",") if item.strip()}


def _append_apply_manifest_entry(entry):
    manifest_path = os.environ.get("BTP_APPLY_MANIFEST_FILE")
    if not manifest_path:
        return
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def get_csv_files(directory, allowed_filenames=None):
    """
    Reads all CSV files from the given directory that contain 'update' in the filename and returns a list of DataFrames.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        list: A list of pandas DataFrames for each CSV file found.
    """



    csv_files = {}
    if not os.path.isdir(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return csv_files
    for filename in sorted(os.listdir(directory)):
        if allowed_filenames is not None and filename not in allowed_filenames:
            continue
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
    if allowed_filenames:
        missing_files = sorted(set(allowed_filenames) - set(csv_files))
        for filename in missing_files:
            file_path = os.path.join(directory, filename)
            if os.path.exists(file_path):
                logger.warning(f"Target CSV file {filename} was present but could not be loaded.")
            else:
                logger.warning(f"Target CSV file {filename} was not found in {directory}.")
    return csv_files


def get_json_files(directory, allowed_filenames=None):
    """
    Reads all JSON files from the given directory and returns a list of JSON objects.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        list: A list of JSON objects for each JSON file found.
    """
    json_files = []
    if not os.path.isdir(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return json_files
    for filename in sorted(os.listdir(directory)):
        if allowed_filenames is not None and filename not in allowed_filenames:
            continue
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, "r") as json_file:
                    json_data = json.load(json_file)
                    json_files.append(json_data)
            except Exception as e:
                logger.error(f"Error reading JSON file {filename}: {e}")
    if allowed_filenames:
        loaded_names = {
            filename for filename in sorted(os.listdir(directory))
            if filename.endswith(".json") and (allowed_filenames is None or filename in allowed_filenames)
        }
        missing_files = sorted(set(allowed_filenames) - loaded_names)
        for filename in missing_files:
            logger.warning(f"Target JSON file {filename} was not found in {directory}.")
    return json_files


def read_directory_files(directory, allowed_csv_filenames=None, allowed_json_filenames=None):
    """
    Reads all CSV and JSON files from the given directory, storing CSV files as DataFrames
    and JSON files as JSON objects.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        tuple: A tuple containing two lists - one for CSV DataFrames and one for JSON objects.
    """
    csv_files = get_csv_files(directory, allowed_filenames=allowed_csv_filenames)
    json_files = get_json_files(directory, allowed_filenames=allowed_json_filenames)
    return csv_files, json_files


def process_internal_persons(filename, csv_file, json_data):
    """
    Processes a CSV file by looping over each row and retrieving an entry from the big JSON file
    for each 'personuuid' in the CSV file.

    Args:
        csv_file (pd.DataFrame): The CSV file as a DataFrame.
        json_data (list): The big JSON list containing the data to look up.
    """

    required_columns = {'to_be_updated', 'PURE_UUID_PERS', 'new_id', 'new_value', 'updated'}
    missing_columns = required_columns - set(csv_file.columns)
    if missing_columns:
        logger.error(f"CSV file {filename} is missing required columns: {sorted(missing_columns)}")
        return

    # Filter the DataFrame to only consider rows where 'to_be_updated' is 'X'
    filtered_csv = _selected_for_update(csv_file)
    logger.info(f"{len(filtered_csv)} persons are selected to be updated")

    # Grouping the DataFrame by 'PURE_UUID_PERS' to collect updates for the same person
    grouped = filtered_csv.groupby('PURE_UUID_PERS')
    logger.info(f"{len(grouped)} persons will be updated")
    success = 0
    errors = 0
    json_by_uuid = {}
    for item in json_data:
        item_uuid = item.get('uuid')
        if not item_uuid:
            continue
        if item_uuid not in json_by_uuid:
            json_by_uuid[item_uuid] = item
    for person_uuid, group in grouped:
        # Retrieve the corresponding entry from the JSON data
        entry = json_by_uuid.get(person_uuid)

        if entry:
            # Iterate through all rows in the group to update the entry
            for _, row in group.iterrows():
                if row['new_id'] == 'orcid':
                    entry['orcid'] = ipersons._normalize_identifier(row['new_value'])
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
            try:
                response = session.put(api_url, headers=PURE_HEADERS, json=entry, timeout=30)
            except requests.exceptions.RequestException as e:
                errors = errors + 1
                logger.error(f"Failed to update person UUID: {person_uuid}, Request error: {e}")
                continue
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
    output_directory = _resolve_output_directory(os.environ.get('REFERER_PAGE', 'unknown')) or 'output/internal_persons'
    file = os.path.join(output_directory, filename)
    os.makedirs(output_directory, exist_ok=True)
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
    required_columns = {'to_be_updated', 'Pure_UUID', 'updated'}
    missing_columns = required_columns - set(csv_file.columns)
    if missing_columns:
        logger.error(f"CSV file {filename} is missing required columns: {sorted(missing_columns)}")
        return

    # Filter the DataFrame to only consider rows where 'to_be_updated' is 'X'
    filtered_csv = _selected_for_update(csv_file)
    grouped = filtered_csv.groupby('Pure_UUID')
    logger.info(f"{len(grouped)} persons will be updated")
    success = 0
    json_by_uuid = {}
    for record in big_json_data:
        record_uuid = record.get('UUID') or record.get('uuid')
        if record_uuid and record_uuid not in json_by_uuid:
            json_by_uuid[record_uuid] = record
    for group_index, (uuid, group) in enumerate(grouped, start=1):
        if group_index % 250 == 0:
            logger.info(f"processing {group_index} items")
        matched_record = json_by_uuid.get(uuid)
        if matched_record:
            url = PURE_BASE_URL + 'external-persons/' + uuid
            try:
                response = session.put(url, headers=headers, json=matched_record, timeout=30, verify=False)
                if response.status_code != 200:
                    logger.warning(f"Failed to update data for UUID {uuid}: {response.text}")
                else:
                    csv_file.loc[group.index, 'updated'] = 'X'
                    csv_file.loc[group.index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
                    success = success + 1
                    logger.debug(f"Successfully updated data for UUID {uuid}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error updating UUID {uuid}: {e}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits
        else:
            logger.warning(f"Not found for external person UUID: {uuid}")

    # Reorder the columns to make 'updated' the second column
    logger.info(f"{success} persons have been updated")
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))
    csv_file = csv_file[cols]

    output_directory = _resolve_output_directory(os.environ.get('REFERER_PAGE', 'unknown')) or 'output/external_persons'
    file = os.path.join(output_directory, filename)
    os.makedirs(output_directory, exist_ok=True)
    csv_file.to_csv(file, index=False)
    logger.info(f"Updated DataFrame saved to {file}.")



def process_research_output(filename, csv_file, big_json_data):
    required_columns = {'to_be_updated', 'doi', 'updated'}
    missing_columns = required_columns - set(csv_file.columns)
    if missing_columns:
        logger.error(f"CSV file {filename} is missing required columns: {sorted(missing_columns)}")
        return

    def normalize_doi(doi_value):
        if not doi_value:
            return ""
        return str(doi_value).replace("https://doi.org/", "").replace("http://doi.org/", "").strip().lower()

    filtered_csv = _selected_for_update(csv_file)
    logger.info(f"{len(filtered_csv)} research outputs are selected to be updated")
    doi_to_item = {}
    for item in big_json_data:
        for version in item.get("electronicVersions", []):
            norm = normalize_doi(version.get("doi", ""))
            if norm and norm not in doi_to_item:
                doi_to_item[norm] = item

    for index, row in filtered_csv.iterrows():
        doi_item = doi_to_item.get(normalize_doi(row['doi']))

        # Output the result
        if doi_item:
            result = pure_researchoutputs.create_research_output(doi_item)
            if result.get("success"):
                csv_file.loc[index, 'updated'] = 'x'
                csv_file.loc[index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
                _append_apply_manifest_entry(
                    {
                        "job_type": "research_outputs",
                        "item_key": normalize_doi(row['doi']),
                        "doi": normalize_doi(row['doi']),
                        "record_uuid": result.get("uuid"),
                        "created_external_persons": result.get("created_external_persons", []),
                    }
                )
            else:
                logger.error(f"Failed to create research output for DOI {row['doi']}")
        else:
            logger.debug(f"No item found with DOI: {row['doi']}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits

        # Reorder the columns to make 'updated' the second column
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))
    csv_file = csv_file[cols]

    # Save the updated DataFrame
    output_directory = _resolve_output_directory(os.environ.get('REFERER_PAGE', 'unknown')) or 'output/research_output'
    os.makedirs(output_directory, exist_ok=True)
    file = os.path.join(output_directory, filename)
    csv_file.to_csv(file, index=False)
    logger.info(f"Updated DataFrame saved to {file}.")



def process_datasets(filename, csv_file, big_json_data):
    required_columns = {'to_be_updated', 'doi', 'updated'}
    missing_columns = required_columns - set(csv_file.columns)
    if missing_columns:
        logger.error(f"CSV file {filename} is missing required columns: {sorted(missing_columns)}")
        return

    def normalize_doi(doi_value):
        if not doi_value:
            return ""
        return str(doi_value).replace("https://doi.org/", "").replace("http://doi.org/", "").strip().lower()

    filtered_csv = _selected_for_update(csv_file)
    logger.info(f"{len(filtered_csv)} datasets are selected to be updated")
    # Create a lookup dictionary for faster DOI-based access
    doi_to_dataset = {}
    for dataset in big_json_data:
        norm = normalize_doi(dataset.get("doi", {}).get("doi"))
        if norm and norm not in doi_to_dataset:
            doi_to_dataset[norm] = dataset

    for index, row in filtered_csv.iterrows():

        dataset = doi_to_dataset.get(normalize_doi(row['doi']))

        # Output the result
        if dataset:

            try:
                result = puda.create_dataset(dataset)
                if result.get("success"):
                    csv_file.loc[index, 'updated'] = 'x'
                    csv_file.loc[index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
                    _append_apply_manifest_entry(
                        {
                            "job_type": "datasets",
                            "item_key": normalize_doi(row['doi']),
                            "doi": normalize_doi(row['doi']),
                            "record_uuid": result.get("uuid"),
                            "created_external_persons": result.get("created_external_persons", []),
                        }
                    )
                else:
                    logger.error(f"Error creating dataset for DOI {row['doi']}: create_dataset returned failure")
            except Exception as e:
                logger.error(f"Error creating dataset for DOI {row['doi']}: {e}")
        else:
            logger.debug(f"No item found with DOI: {row['doi']}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits

        # Reorder the columns to make 'updated' the second column
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))

    csv_file = csv_file[cols]

    # Save the updated DataFrame
    output_directory = _resolve_output_directory(os.environ.get('REFERER_PAGE', 'unknown')) or 'output/datasets'
    os.makedirs(output_directory, exist_ok=True)
    file = os.path.join(output_directory, filename)
    csv_file.to_csv(file, index=False)
    logger.info(f"Updated DataFrame saved to {file}.")



def process_external_orgs(filename, csv_file, big_json_data):
    required_columns = {'to_be_updated', 'uuid', 'updated'}
    missing_columns = required_columns - set(csv_file.columns)
    if missing_columns:
        logger.error(f"CSV file {filename} is missing required columns: {sorted(missing_columns)}")
        return

    # Filter the DataFrame to only consider rows where 'to_be_updated' is 'X'
    filtered_csv = _selected_for_update(csv_file)
    grouped = filtered_csv.groupby('uuid')
    logger.info(f" {len(grouped)} external organizations are selected to be updated")
    succes = 0
    json_by_uuid = {}
    for record in big_json_data:
        record_uuid = record.get('UUID') or record.get('uuid')
        if record_uuid and record_uuid not in json_by_uuid:
            json_by_uuid[record_uuid] = record

    for uuid, group in grouped:
        matched_record = json_by_uuid.get(uuid)
        if matched_record:
            payload = {
                key: value for key, value in matched_record.items()
                if not str(key).startswith('_')
            }

            url = PURE_BASE_URL + 'external-organizations/' + uuid

            try:
                response = session.put(url, headers=headers, json=payload, verify=False, timeout=30)

                if response.status_code != 200:
                    logger.info(f"Failed to update data for UUID {uuid}: {response.text}")

                else:
                    csv_file.loc[group.index, 'updated'] = 'X'
                    csv_file.loc[group.index, 'to_be_updated'] = ''  # Clear 'to_be_updated' for successfully updated rows
                    logger.debug(f"Successfully updated data for UUID {uuid}")
                    succes +=1
            except requests.exceptions.RequestException as e:
                logger.error(f"Error updating UUID {uuid}: {e}")
            time.sleep(0.1)  # Adjust the sleep time based on rate limits

    # Reorder the columns to make 'updated' the second column
    cols = list(csv_file.columns)
    cols.insert(1, cols.pop(cols.index('updated')))
    csv_file = csv_file[cols]

    # Save the updated DataFrame to 'output/updated.csv'uccessfully updated data for UUID
    output_directory = _resolve_output_directory(os.environ.get('REFERER_PAGE', 'unknown')) or 'output/external_orgs'
    file = os.path.join(output_directory, filename)
    os.makedirs(output_directory, exist_ok=True)
    csv_file.to_csv(file, index=False)
    logger.info(f"Updated DataFrame saved to {file}.")
    logger.info(f" {succes} external organizations are updated.. check pure for the results")


if __name__ == "__main__":

    # Step 1: Retrieve the REFERER_PAGE environment variable
    referer_page = os.environ.get('REFERER_PAGE', 'unknown')

  
    logger.debug(f"Script called from page: {referer_page}")

    # Step 2: Execute specific logic based on the Referer
    directory = _resolve_output_directory(referer_page)

    if not directory:
        logger.error(f"Unknown referer page '{referer_page}'. Cannot determine update directory.")
        sys.exit(1)

    allowed_csv_filenames = _parse_target_filenames(os.environ.get('BTP_TARGET_CSV_FILES'))
    allowed_json_filenames = _parse_target_filenames(os.environ.get('BTP_TARGET_JSON_FILES'))

    csv_files, json_files = read_directory_files(
        directory,
        allowed_csv_filenames=allowed_csv_filenames,
        allowed_json_filenames=allowed_json_filenames,
    )
    if not csv_files:
        logger.info(f"No update CSV files found in {directory}")
        logger.info(f"script to update Pure has ended")
        sys.exit(0)

    if len(json_files) > 1:
        logger.warning(f"Multiple JSON files found in {directory}; using the first one after sorting.")

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
