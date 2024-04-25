# ########################################################################
#
# Datacite utilities - function modules for getting datasets from datacite
#
# ########################################################################
#
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

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
    response = requests.get(f'https://api.datacite.org/dois/{doi}')
    if response.status_code == 200:
        data = response.json()['data']['attributes']

        return parse_datacite_response(data, doi)
    else:
        print(f"Failed to fetch data for DOI: {doi}")
        return None

def parse_datacite_response(data, doi):
    """Parse the response from DataCite API and return structured data."""
    title = data['titles'][0]['title']
    persons = []
    for creator in data['creators']:
        affiliations = creator.get('affiliation', [])
        first_affiliation_name = get_first_affiliation_name(affiliations)

        creator_info = {
            'name': creator['name'],
            'type': 'creator',
            'affiliation': first_affiliation_name,
            'person_ids': [
                {'id': ni.get('nameIdentifierScheme'), 'value': ni.get('nameIdentifier')}
                for ni in creator.get('nameIdentifiers', [])
            ]
        }
        persons.append(creator_info)

    subjects = [subject['subject'] for subject in data.get('subjects', [])]
    description = data.get('descriptions', [{'description': 'No description available'}])[0]['description']

    return {
        'title': title,
        'description': description,
        'persons': persons,
        'publisher': data['publisher'],
        'doi': doi,
        'publication_year': data['publicationYear'],
        'publication_month': 1,  # Assuming January for all
        'publication_day': 1,  # Assuming the 1st for all
        'subjects': subjects
    }

def get_df_from_datacite(datasets):
    """Fetch data for multiple DOIs and return a DataFrame."""
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_data_for_doi, datasets))

    # Filter out None results in case of failed fetches
    valid_results = [result for result in results if result]
    return pd.DataFrame(valid_results)

def main():
    # Usage example, assuming 'datasets' is a list of DOI strings
    datasets = ['10.6084/M9.FIGSHARE.21829182', '10.5061/dryad.tn70pf1', 'DOI3']
    df = get_df_from_datacite2(datasets)
    print(df)

if __name__ == '__main__':
    main()
