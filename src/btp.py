import logging
import sys
from logging_config import setup_logging
import requests
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, ID_URI, FACULTY_PREFIX

logger = setup_logging('btp', level=logging.INFO)
# logger.handlers[0].stream.flush = lambda: sys.stdout.flush()

def checks_before_start(faculty):

    if not PURE_BASE_URL or not PURE_API_KEY or not PURE_HEADERS or not RIC_BASE_URL:
        logger.error(
            "One or more required variables are empty: PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL")
        sys.exit("Program terminated due to missing configuration.")


    params = {'key': faculty, 'max_nr_items': '1'}
    url = RIC_BASE_URL + 'get_all_personroot_nodes'
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # This will raise an HTTPError for bad responses (4xx, 5xx)
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"Failed to connect to ricgraph: {e}")

    try:
        url = PURE_BASE_URL + 'persons/'
        params = {
            'size': '1',
            'offset': '1',
        }
        response = requests.get(url, headers = PURE_HEADERS, params=params)
        response.raise_for_status()  # This will raise an HTTPError for bad responses (4xx, 5xx)
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for pure: {e}")

        raise SystemExit(f"Failed to connect to Pure: {e}")

def select_faculties(faculty_choice):
    params = {'value': FACULTY_PREFIX}
    url = f"{RIC_BASE_URL}organization/search"

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
    except requests.RequestException as e:
        logger.error(f"Error conntecting to Ricgraph: {e}")
        raise SystemExit()

    try:
        data = response.json()
    except ValueError:
        raise SystemExit("Failed to decode JSON from response.")

    # Extract faculties or use the provided choice
    if faculty_choice.lower() == 'all':
        selected_faculties = [item.get('_key') for item in data.get("results", [])]
    else:
        selected_faculties = [faculty_choice]

    return selected_faculties