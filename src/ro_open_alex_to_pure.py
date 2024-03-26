# Research output from Open Alex to Pure using Ricgraph
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

import openalex_utils, pure_researchoutputs
import csv
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

dois = get_dois_from_csv('output.csv')

responses = openalex_utils.get_jsons_from_open_alex(dois)
df, errors = openalex_utils.transform_openalex_to_df(responses)
pure_researchoutputs.df_to_pure(df)
print(df.head(), errors.head())