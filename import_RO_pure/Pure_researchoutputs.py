import pandas as pd
import json
import requests
from datetime import datetime
import configparser
import os
import logging

from dateutil import parser

config_path = 'config.ini'
if not os.path.exists(config_path):
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read('config.ini')
BASE_URL = config['API']['BaseURL']
API_KEY = config['API']['APIKey']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='pure_utilities.log',
)

headers = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": API_KEY
}

def get_researchoutput(uuid):

    api_url = BASE_URL + 'research-outputs/' + uuid
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data

json_pub = get_researchoutput('15175319-4649-4edc-ba1f-ff2f3a1e8f10')

# Specify the filename
filename = 'dissertation.json'

# Write JSON data to a file
with open(filename, 'w') as file:
    json.dump(json_pub, file, indent=4)