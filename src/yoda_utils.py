# Re-processing the JSON data, this time skipping the affiliations
import json
import pandas as pd
# Path to your JSON file
file_path = 'other_files/vault_metadata_export_vu.json'



def get_df_from_yoda(filename):
    # Open the file and load the JSON data
    with open(filename, 'r') as file:
        data = json.load(file)

    all_datasets_aggregated = []

    # Iterate through each dataset in the JSON file
    for dataset_path, dataset_info in data.items():
        metadata = dataset_info.get('metadata', {})
        doi = dataset_info.get('doi', 'N/A')
        title = metadata.get('Title', 'N/A')
        description = metadata.get('Description', 'N/A')

        # Extract year, month, and day from 'Start_Date'
        start_date = metadata.get('Collected', {}).get('Start_Date')

        if not start_date:
            # Fallback to the 'modified' field and extract only the date part
            modified_date = dataset_info.get('modified', '')

            start_date = modified_date.split('T')[0]  # Splits at 'T' and takes the first part (date)


        # Split and convert to integers if possible
        date_parts = start_date.split("-")
        if len(date_parts) == 3:
            try:
                publication_year = int(date_parts[0])
                publication_month = int(date_parts[1])
                publication_day = int(date_parts[2])
            except ValueError:
                # Handle the case where the date parts are not valid integers
                # Could log an error or assign default values here
                pass

        # Create a list to hold all persons associated with the dataset
        persons = []

        # Iterate through each creator in the dataset
        for creator in metadata.get('Creator', []):
            name = f"{creator['Name'].get('Given_Name', '')} {creator['Name'].get('Family_Name', '')}".strip()

            # Create a list for person identifiers
            person_ids = []
            for identifier in creator.get('Person_Identifier', []):
                if isinstance(identifier, dict) and identifier:
                    person_ids.append({
                        'id': identifier.get('Name_Identifier_Scheme', 'N/A'),
                        'value': identifier.get('Name_Identifier', 'N/A')
                    })

            # Add the person's information to the persons list
            persons.append({
                'name': name,
                'person_ids': person_ids
            })

        # Add the dataset record with aggregated persons to the list

        all_datasets_aggregated.append({
            'doi': doi,
            'title': title,
            'description': description,
            'publisher': 'default',
            'publication_year': publication_year,
            'publication_month': publication_month,
            'publication_day': publication_day,
            'persons': persons
        })

    # Create a DataFrame from the aggregated data
    df_datasets_aggregated = pd.DataFrame(all_datasets_aggregated)
    return (df_datasets_aggregated)


df = get_df_from_yoda(file_path)



