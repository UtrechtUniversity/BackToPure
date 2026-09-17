
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, OPENALEXEX_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS
import requests
import json
import logging
from logging_config import setup_logging
def select_researchoutputs():
    """Fetch person IDs for a given person-ro    ot."""

    try:
        params = {
            'category': 'journal article',
            'max_nr_items': '100',
        }
        url = RIC_BASE_URL + 'advanced_search'
        response = requests.get(url, params=params)
        # response.raise_for_status()
        return response.json().get("results", [])

    except requests.RequestException as e:
        logging.error(f"Error fetching researchputs: {e}")
        return []


def get_all_personsfromdoi(key):
    params = {
        'key': key,
        'category_want': 'person',
        'max_nr_items': '0',
    }
    url = RIC_BASE_URL + 'get_all_neighbor_nodes'

    response = requests.get(url, params=params)
    return response.json().get("results", [])

def get_allpersoninfo(key):
    params = {
        'key': key,
        'name_want': [
            'FULL_NAME',
            'PURE_UUID_PERS',
            'OPENALEX',
            'ORCID'

        ],
        'category_want': 'person',
        'max_nr_items': '0',
    }
    url = RIC_BASE_URL + 'get_all_neighbor_nodes'
    response = requests.get(url, params=params)
    return response.json().get("results", [])

def get_idsandname(personfields):
    ids = {}
    full_name = ''
    for fields in personfields:

        if fields['name'] == 'FULL_NAME':
            full_name = fields['value']
        if fields['name'] == 'ORCID':
            ids['ORCID'] = fields['value']
        if fields['name'] == 'OPENALEX':
            ids['OPENALEX'] = fields['value']
        if fields['name'] == 'PURE_UUID_PERS':
            ids['PURE_UUID_PERS'] = fields['value']



    return ids, full_name



logger = setup_logging('external persons', level=logging.INFO)
logging.info(f"script started")
outputs = select_researchoutputs()
export  = []
#
# for output in outputs:
#     # print(output)
#     persons = get_all_personsfromdoi(output['_key'])
#     for person in persons:
#         personfields = get_allpersoninfo(person['_key'])
#         ids, full_name = get_idsandname(personfields)
#         export.append({
#             'pubkey': output['_key'],
#             'personroot': person['_key'],
#             'full_name': full_name,
#             'ids': ids})


# for item in export:
#     # print(item['full_name'])

logging.info(f"script ended")
