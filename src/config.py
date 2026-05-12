import os
import configparser
from pathlib import Path


def _config_path() -> Path:
    configured_path = os.environ.get("BTP_CONFIG_PATH")
    if configured_path:
        return Path(configured_path).expanduser()
    return Path(__file__).resolve().parent / "config.ini"


config_path = _config_path()

if not config_path.exists():
    example_path = Path(__file__).resolve().parent / "config.example.ini"
    raise FileNotFoundError(
        f"The configuration file {config_path} does not exist. "
        f"Copy {example_path} to {Path(__file__).resolve().parent / 'config.ini'} "
        "or set BTP_CONFIG_PATH to a private config file."
    )

config = configparser.ConfigParser()
config.read(config_path)

required_options = {
    "PURE-API": ("BaseURL", "APIKey"),
    "RICGRAPH-API": ("BaseURL", "FacultyPrefix", "rescat"),
    "OPENALEX_PURE": ("BaseURL", "email"),
    "ID_URI": ("OPENALEX", "OPENALEXEX", "ORCIDEXT", "ROR_ID_URI"),
    "URI": (),
    "DEFAULTS": (),
}

missing = [
    f"{section}.{option}"
    for section, options in required_options.items()
    for option in options
    if not config.has_option(section, option)
]
missing.extend(
    section
    for section, options in required_options.items()
    if not options and not config.has_section(section)
)
if missing:
    raise KeyError(f"Missing required configuration values: {', '.join(missing)}")

PURE_BASE_URL = config['PURE-API']['BaseURL']
PURE_API_KEY = config['PURE-API']['APIKey']
RIC_BASE_URL = config['RICGRAPH-API']['BaseURL']
FACULTY_PREFIX = config['RICGRAPH-API']['FacultyPrefix']
OPENALEX_BASE_URL = config['OPENALEX_PURE']['BaseURL']
EMAIL = config['OPENALEX_PURE']['email']
OPENALEX_ID_URI = config['ID_URI']['OPENALEX']
OPENALEXEX_ID_URI = config['ID_URI']['OPENALEXEX']

ORCID_ID_URI = config['ID_URI']['ORCIDEXT']
ROR_ID_URI = config['ID_URI']['ROR_ID_URI']
ID_URI = config['ID_URI']
TYPE_URI = config['URI']
CATEGORIES = config['RICGRAPH-API']['rescat']

DEFAULTS = config['DEFAULTS']


PURE_HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": PURE_API_KEY
}

OPENALEX_HEADERS = {'Accept': 'application/json',
                    'User-Agent': EMAIL
                    }
