import configparser
import os

config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
if not os.path.exists(config_path):
    raise FileNotFoundError(f"The configuration file {config_path} does not exist.")

config = configparser.ConfigParser()
config.read(config_path)

PURE_BASE_URL = config['PURE-API']['BaseURL']
PURE_API_KEY = config['PURE-API']['APIKey']
RIC_BASE_URL = config['RICGRAPH-API']['BaseURL']
FACULTY_PREFIX = config['RICGRAPH-API']['FacultyPrefix']
OPENALEX_BASE_URL = config['OPENALEX_PURE']['BaseURL']
OPENALEX_ID_URI = config['ID_URI']['OPENALEX']
ORCID_ID_URI = config['ID_URI']['OPENALEX']
ID_URI = config['ID_URI']
TYPE_URI = config['URI']


DEFAULTS = config['DEFAULTS']


PURE_HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": PURE_API_KEY
}

OPENALEX_HEADERS = {'Accept': 'application/json',
                    'User-Agent': 'mailto:d.h.j.grotebeverborg@uu.nl'
                    }