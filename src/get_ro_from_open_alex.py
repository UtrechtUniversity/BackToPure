import requests
import json

import json
import requests

def get_openalex_data(api_url):
    """ Fetch data from OpenAlex API """
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from OpenAlex: {response.status_code}")

def extract_contributors(authorships):
    """ Extract contributor details from OpenAlex authorships """
    contributors = []
    for author in authorships:
        contributor = {
            'firstName': author.get('author', {}).get('display_name', '').split()[0],  # Assuming first word as first name
            'lastName': ' '.join(author.get('author', {}).get('display_name', '').split()[1:]),  # Rest of the words as last name
            'uuid': author.get('author', {}).get('id', ''),  # Assuming OpenAlex author ID can be used as UUID
            'associationsUUIDs': []  # Placeholder for organization UUIDs
        }
        contributors.append(contributor)
    return contributors

def transform_openalex_to_pure(openalex_data):
    """ Transform OpenAlex JSON data into Pure-compatible structure """
    publication_year = openalex_data.get('publication_year', '')
    publication_month = openalex_data.get('publication_date', '').split('-')[1] if openalex_data.get('publication_date') else ''

    transformed_data = {
        'research_output_id': openalex_data.get('id', ''),
        'title': openalex_data.get('display_name', ''),
        'contributors': extract_contributors(openalex_data.get('authorships', [])),
        'journal': {
            'title': openalex_data.get('host_venue', {}).get('display_name', ''),
            'uuid': ''  # Placeholder for journal UUID
        },
        'publication_year': publication_year,
        'publication_month': publication_month,
        'language_uri': '/languages/en',  # Placeholder
        'language_term': 'English',  # Placeholder
        'peer_review': True,  # Assuming peer-reviewed
        'submission_year': publication_year,
        'doi': openalex_data.get('doi', ''),
        'visibility_key': 'PUBLIC',  # Placeholder
        'workflow_step': 'forApproval'  # Placeholder
    }

    return transformed_data

# Example usage
api_url =  'https://api.openalex.org/works/https://doi.org/10.1002/ijc.34742'
openalex_data = get_openalex_data(api_url)
pure_data = transform_openalex_to_pure(openalex_data)

# Print or process the transformed data
print(json.dumps(pure_data, indent=4))


