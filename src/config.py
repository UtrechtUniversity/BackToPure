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


PURE_URI_CONFIG_REQUIREMENTS = {
    "ID_URI": {
        "OPENALEX": "internal person OpenAlex source URI",
        "OPENALEXEX": "external person OpenAlex source URI",
        "ORCIDEXT": "external person ORCID source URI",
        "ROR_ID_URI": "external organization ROR source URI",
    },
    "URI": {
        "contributor": "dataset contributor role URI",
        "creator": "dataset creator role URI",
        "type_dataset": "dataset type URI",
        "supervisor": "research output supervisor role URI",
        "cosupervisor": "research output co-supervisor role URI",
    },
    "DEFAULTS": {
        "publisher": "default publisher UUID",
        "university": "default university organization UUID",
        "visibility_key": "default visibility key",
        "workflow_step": "default workflow step",
        "language_uri": "default language URI",
    },
}


def _looks_like_placeholder(value: str) -> bool:
    value = str(value or "").strip().lower()
    return not value or "replace-with" in value or value.startswith("your-") or value.startswith("your ")


def validate_pure_uri_config(config_parser=None) -> list[str]:
    parser = config_parser or config
    issues = []
    for section, options in PURE_URI_CONFIG_REQUIREMENTS.items():
        if not parser.has_section(section):
            issues.append(f"Missing section [{section}]")
            continue
        for option, label in options.items():
            if not parser.has_option(section, option):
                issues.append(f"Missing {label}: [{section}] {option}")
                continue
            value = parser.get(section, option).strip()
            if _looks_like_placeholder(value):
                issues.append(f"Placeholder or empty value for {label}: [{section}] {option}")
                continue
            if option.endswith("_uri") or section in {"ID_URI", "URI"}:
                if option not in {"publisher", "university", "visibility_key", "workflow_step"} and not value.startswith("/"):
                    issues.append(f"Pure URI should start with '/': [{section}] {option}={value}")
    return issues

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


def _split_config_list(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


_default_primary_org_prefix = f"{FACULTY_PREFIX.strip()}:"
_default_excluded_org_prefix = f"{FACULTY_PREFIX.strip()} research:"
PRIMARY_ORGANIZATION_PREFIXES = _split_config_list(
    config.get(
        "RICGRAPH-API",
        "PrimaryOrganizationPrefixes",
        fallback=_default_primary_org_prefix,
    )
)
EXCLUDED_ORGANIZATION_PREFIXES = _split_config_list(
    config.get(
        "RICGRAPH-API",
        "ExcludedOrganizationPrefixes",
        fallback=_default_excluded_org_prefix,
    )
)


def _starts_with_any(value: str, prefixes: tuple[str, ...]) -> bool:
    value = value.lower()
    return any(value.startswith(prefix.lower()) for prefix in prefixes)


def is_primary_organization_key(key: str | None) -> bool:
    if not isinstance(key, str):
        return False
    if PRIMARY_ORGANIZATION_PREFIXES and not _starts_with_any(key, PRIMARY_ORGANIZATION_PREFIXES):
        return False
    return not _starts_with_any(key, EXCLUDED_ORGANIZATION_PREFIXES)


def resolve_faculty_selection(faculty_choice: str, available_keys) -> list[str]:
    """Resolve a faculty choice against the keys Ricgraph actually returned.

    ``is_primary_organization_key`` only checks the *shape* of a key, so a
    well-formed but non-existent choice used to pass straight through and every
    downstream stage then reported zero rows - a silently empty run that reads
    exactly like "nothing to do". Validate against the real list instead.

    Matching is case-insensitive and returns the canonical key.
    """
    primary = [key for key in available_keys if is_primary_organization_key(key)]

    if isinstance(faculty_choice, str) and faculty_choice.lower() == "all":
        return primary

    by_lowercase = {key.lower(): key for key in primary}
    canonical = by_lowercase.get(faculty_choice.lower()) if isinstance(faculty_choice, str) else None
    if canonical:
        return [canonical]

    raise ValueError(
        f"Unknown faculty key {faculty_choice!r}. "
        f"Ricgraph returned {len(primary)} faculty key(s): {', '.join(sorted(primary)) or '(none)'}"
    )


def is_excluded_organization_key(key: str | None) -> bool:
    return isinstance(key, str) and _starts_with_any(key, EXCLUDED_ORGANIZATION_PREFIXES)


PURE_HEADERS = {
    "Content-Type": "application/json",
    "accept": "application/json",
    "api-key": PURE_API_KEY
}

def current_pure_credentials() -> tuple[str, dict]:
    """Re-read the Pure base URL and API key from disk.

    Module-level PURE_BASE_URL/PURE_HEADERS are captured at import time. The
    apply step runs as a subprocess and so always sees the current config, but
    anything running inside the long-lived web process (rollback, for example)
    would keep using whatever was on disk when the server started. A config
    edit then makes rollback fail with 401 while apply succeeds.
    """
    fresh = configparser.ConfigParser()
    fresh.read(config_path)
    try:
        base_url = fresh['PURE-API']['BaseURL']
        api_key = fresh['PURE-API']['APIKey']
    except KeyError:
        return PURE_BASE_URL, dict(PURE_HEADERS)
    return base_url, {
        "Content-Type": "application/json",
        "accept": "application/json",
        "api-key": api_key,
    }


OPENALEX_HEADERS = {'Accept': 'application/json',
                    'User-Agent': EMAIL
                    }
