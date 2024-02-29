import pandas as pd
import json
from nameparser import HumanName
from datetime import datetime
import pathlib
# import ricgraph as rcg
import requests
import configparser

# Load the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the minimal fields and defaults from the config file
FIELDS = config.get('FIELDS', 'article').split(', ')
DEFAULTS = {key: config.get('DEFAULTS', key) for key in config.options('DEFAULTS')}


# # ######################################################
# # Parameters for harvesting persons and research outputs from OpenAlex
# # ######################################################
# OPENALEX_API_URL = 'https://api.openalex.org/'
# OPENALEX_ENDPOINT = 'works'
# OPENALEX_HARVEST_FROM_FILE = False
# OPENALEX_HARVEST_FILENAME = 'openalex_harvest.json'
# OPENALEX_DATA_FILENAME = 'openalex_data.csv'
# OPENALEX_RESOUT_YEARS = ['2020']
# # This number is the max recs to harvest per year, not total
# OPENALEX_MAX_RECS_TO_HARVEST = 0                             # 0 = all records
# OPENALEX_FIELDS = 'doi,publication_year,title,type,authorships'
#
#
# # ######################################################
# # Mapping from OpenAlex research output types to Ricgraph research output types.
# # ######################################################
# ROTYPE_MAPPING_OPENALEX = {
#     'article': rcg.ROTYPE_JOURNAL_ARTICLE,
#     'book': rcg.ROTYPE_BOOK,
#     'book-chapter': rcg.ROTYPE_BOOKCHAPTER,
#     'dataset': rcg.ROTYPE_DATASET,
#     'dissertation': rcg.ROTYPE_PHDTHESIS,
#     'editorial': rcg.ROTYPE_EDITORIAL,
#     'erratum': rcg.ROTYPE_MEMORANDUM,
#     'letter': rcg.ROTYPE_LETTER,
#     'monograph': rcg.ROTYPE_BOOK,
#     'other': rcg.ROTYPE_OTHER_CONTRIBUTION,
#     'paratext': rcg.ROTYPE_OTHER_CONTRIBUTION,
#             # OpenAlex 'paratext': stuff that's in scholarly venue (like a journal)
#             # but is about the venue rather than a scholarly work properly speaking.
#             # https://docs.openalex.org/api-entities/works/work-object.
#     'peer-review': rcg.ROTYPE_REVIEW,
#     'posted-content': rcg.ROTYPE_PREPRINT,
#     'proceedings': rcg.ROTYPE_CONFERENCE_ARTICLE,
#     'proceedings-article': rcg.ROTYPE_CONFERENCE_ARTICLE,
#     'reference-entry': rcg.ROTYPE_ENTRY,
#     'report': rcg.ROTYPE_REPORT
# }



def extract_journal_issn(publication):
    primary_location = publication.get('primary_location', {})
    source = primary_location.get('source', {})
    return source.get('issn_l', 'No ISSN')

# Function to extract relevant data from OpenAlex JSON and format it into a DataFrame
def transform_openalex_to_df(openalex_data):

    processed_publications = []
    not_processed_publications = []

    if not isinstance(openalex_data, list):
        openalex_data = [openalex_data]

    for publication in openalex_data:
        title = publication.get('title')
        type = publication.get('type')
        doi = publication.get('doi')
        publication_date = publication.get('publication_date', '')
        year, month, day = extract_date_components(publication_date)
        contributors = parse_contributors(publication.get('authorships', []))

        if not all([title, type, doi, year, contributors]):
            reason = "Missing fields: "
            missing_fields = [field for field in ["title", "type", "doi", "year", "contributors"] if not locals()[field]]
            logging.info(f"Publication ID {publication.get('id', 'Unknown')} not processed. {reason}{' '.join(missing_fields)}")
            not_processed_publications.append(publication)
            continue

        processed_publications.append({
            'research_output_id': publication.get('id', 'No id'),
            'title': title,
            'type': type,
            'peer_review': config['DEFAULTS']['peer_review'],
            'doi': doi,
            'submission_year': year,
            'publication_year': year,
            'publication_month': month,
            'publication_day': day,
            'contributors': contributors,
            'journal_issn': extract_journal_issn(publication),
            'language_term': 'Undefined/Unknown',
            'language_uri': config['DEFAULTS']['language_uri'],
            'visibility_key': config['DEFAULTS']['visibility_key'],
            'workflow_step': config['DEFAULTS']['workflow_step']
        })

    df_processed = pd.DataFrame(processed_publications)
    df_not_processed = pd.DataFrame(not_processed_publications)

    return df_processed, df_not_processed

def parse_contributors(contributors):
    parsed_contributors = []

    for author in contributors:

        author_details = author.get('author', {})
        name = author_details.get('display_name', 'Unknown Author')
        orcid = extract_orcid_id(author_details.get('orcid', ''))
        openalex_id = author_details.get('id', 'No OpenAlex ID')


        # Parse the name using nameparser
        human_name = HumanName(name)
        first_name = human_name.first
        last_name = human_name.last

        # Create a dictionary of IDs
        ids_dict = {}
        if orcid:
            ids_dict['ORCID'] = orcid
        if openalex_id:
            ids_dict['OpenAlex'] = openalex_id

        parsed_contributors.append({
            'name': name,
            'first_name': first_name,
            'last_name': last_name,
            'ids': ids_dict
        })
    print(parsed_contributors)
    return parsed_contributors

def extract_date_components(date_string):
    try:
        # Parse the date string into a datetime object
        date_obj = datetime.strptime(date_string, '%Y-%m-%d')
        # Extract year, month, and day
        return date_obj.year, date_obj.month, date_obj.day
    except ValueError:
        # Return None or default values if the date string is not in the expected format
        return None, None, None

# Transform OpenAlex data to DataFrame

def get_df_from_openalex(openalex_data):
    # Load the OpenAlex JSON file

    # file_path = 'openalex.json'
    # with open(file_path, 'r') as file:
    #     openalex_data = json.load(file)
    try:
        publications = openalex_data['results']
    except:
        publications = openalex_data
    df, df2 = transform_openalex_to_df(publications)

    return df, df2

def extract_orcid_id(orcid):
    # Check if the ORCID is in URL format
    if orcid and orcid.startswith('https://orcid.org/'):
        # Extract just the ID part
        return orcid.split('/')[-1]
    elif orcid:
        # Return the ORCID as is, assuming it's already in the correct format
        return orcid
    else:
        # Return an empty string if orcid is None
        return ''

#FIELDS
# df = get_df_from_openalex()
# for t in df.iterrows():
#     print(t)
OPENALEX_HEADERS = {'Accept': 'application/json',
                    # The following will be read in __main__
                    'User-Agent': 'mailto:d.h.j.grotebeverborg@uu.nl'
                    }
OPENALEX_MAX_RECS_TO_HARVEST = 3
doi = 'doi.org/10.1002/ijc.34742'
url = 'https://api.openalex.org/works/' + doi
response = requests.get(url, headers=OPENALEX_HEADERS)

print (response.text)