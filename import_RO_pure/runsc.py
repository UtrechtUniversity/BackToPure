import logging
import configparser
import pandas as pd
from pure_api_utils import get_contributors_details, get_journal_uuid, construct_research_output_json, parse_contributors, create_research_output
import openalex_utils
import os
from requests.structures import CaseInsensitiveDict

# Ensure the log directory exists
log_directory = "logs"

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging
logging.basicConfig(filename=os.path.join(log_directory, 'output.log'),
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')


# Load configuration settings from config.ini
# Check if the config file exists
print(f"Current working directory: {os.getcwd()}")
config_path = 'config.ini'
if not os.path.exists(config_path):
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read('config.ini')
BASE_URL = config['API']['BaseURL']
API_KEY = config['API']['APIKey']
# headers
headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/json"
headers["accept"] = "application/json"
headers["api-key"] = API_KEY

def main():

    # choose source
    df, user_choice = choose_source()

    # Process each research output
    for _, row in df.iterrows():
        try:
            # Fetch contributors details

            if user_choice == 'csv':
                contributors = row['contributors'].split(', ')
                contributors = parse_contributors(row['contributors'])
            elif user_choice == 'alex':
                contributors = row['contributors']

            contributors_details = get_contributors_details(contributors, headers)

            if contributors_details:

                # Fetch journal UUID
                journal = get_journal_uuid(row['journal_issn'], headers)
                print("journal: ", journal)
                # Construct the research output JSON
                research_output_json = construct_research_output_json(
                    row['research_output_id'],
                    row['title'],
                    contributors_details,
                    journal,
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

                # uuid_ro = create_research_output(research_output_json, headers)
                # Here, you can process the research_output_json as needed
                logging.info(f"Processed research output {row['research_output_id']} successfully.")
            else:
                logging.info(f"skipped research output {row['research_output_id']}.")

        except Exception as e:
            logging.error(f"Error processing research output {row['research_output_id']}: {e}")


def choose_source():
    user_choice = input("Enter 'csv' to load data from CSV or 'alex' to load data from OpenAlex JSON: ").strip().lower()

    if user_choice == 'csv':
        df = pd.read_csv('ro.csv')
    elif user_choice == 'alex':

        df, errors = openalex_utils.get_df_from_openalex()

        errors.to_csv('not_processed.csv', index=False)
    else:
        print("Invalid choice. Please enter 'csv' or 'json'.")
        return

    return df, user_choice


if __name__ == "__main__":
    main()
