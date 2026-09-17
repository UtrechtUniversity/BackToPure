import json
import os
import time
import logging
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import EMAIL

logger = logging.getLogger("btp")

CACHE_DIR = "output/openalex_cache"
WORKS_CACHE_FILE = os.path.join(CACHE_DIR, "openalex_works_by_doi.json")
WORKS_STATE_FILE = os.path.join(CACHE_DIR, "openalex_works_state.json")
INSTITUTIONS_CACHE_FILE = os.path.join(CACHE_DIR, "openalex_institutions_by_ror.json")
INSTITUTIONS_STATE_FILE = os.path.join(CACHE_DIR, "openalex_institutions_state.json")
MAX_CACHE_FILE_SIZE_MB = int(os.getenv("OPENALEX_CACHE_MAX_FILE_MB", "512"))


def _normalize_doi(doi_value):
    if not doi_value:
        return None
    value = str(doi_value).strip().lower()
    value = value.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return value or None


def _clean_ror(ror_value):
    if not ror_value:
        return None
    value = str(ror_value).strip()
    return value or None


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _load_json(path, default):
    if not os.path.exists(path):
        return default
    max_size_bytes = MAX_CACHE_FILE_SIZE_MB * 1024 * 1024
    try:
        current_size = os.path.getsize(path)
    except OSError as exc:
        logger.warning(f"Could not stat cache file {path}: {exc}")
        return default

    if current_size > max_size_bytes:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        oversized_path = f"{path}.oversized_{timestamp}"
        logger.warning(
            f"Cache file {path} is {current_size / (1024 * 1024):.1f}MB "
            f"(limit {MAX_CACHE_FILE_SIZE_MB}MB). Rotating to {oversized_path}."
        )
        try:
            os.replace(path, oversized_path)
        except OSError as exc:
            logger.warning(f"Failed to rotate oversized cache file {path}: {exc}")
        return default
    try:
        with open(path, "r") as handle:
            return json.load(handle)
    except Exception as exc:
        logger.warning(f"Could not read cache file {path}: {exc}")
        return default


def _save_json(path, data):
    _ensure_cache_dir()
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as handle:
        json.dump(data, handle, indent=2)
    os.replace(tmp_path, path)


def _make_session(retries=3, backoff=0.5):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _openalex_headers():
    return {
        "Accept": "application/json",
        "User-Agent": f"mailto:{EMAIL}",
    }


def _chunk(items, chunk_size):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def fetch_openalex_works_cached(dois, batch_size=10, request_delay=1.5, force_refresh=False):
    normalized_dois = []
    for doi in dois:
        normalized = _normalize_doi(doi)
        if normalized:
            normalized_dois.append(normalized)
    normalized_dois = list(dict.fromkeys(normalized_dois))

    if not normalized_dois:
        logger.info("OpenAlex cache fetch: no DOI values provided")
        return {"results": [], "by_doi": {}}

    by_doi = _load_json(WORKS_CACHE_FILE, {})
    if force_refresh:
        by_doi = {}

    pending = [doi for doi in normalized_dois if doi not in by_doi]
    logger.info(
        f"OpenAlex works cache: requested={len(normalized_dois)}, "
        f"cached={len(normalized_dois) - len(pending)}, missing={len(pending)}"
    )
    if not pending:
        return {"results": [], "by_doi": {doi: by_doi[doi] for doi in normalized_dois if doi in by_doi}}

    session = _make_session()
    batches = list(_chunk(pending, batch_size))
    hard_failures = 0
    cache_dirty = False
    save_every_batches = 25

    for idx, batch in enumerate(batches, start=1):
        batch_seed = batch[0]
        logger.info(f"OpenAlex works batch {idx}/{len(batches)} (seed DOI: {batch_seed})")
        results = []
        try:
            params = {"filter": f"doi:{'|'.join(batch)}", "per-page": 50}
            response = session.get("https://api.openalex.org/works", headers=_openalex_headers(), params=params, timeout=(5, 20))
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after and str(retry_after).isdigit() else 15
                logger.warning(f"OpenAlex rate limit on works batch {idx}; sleeping {wait_seconds}s")
                time.sleep(wait_seconds)
                response = session.get("https://api.openalex.org/works", headers=_openalex_headers(), params=params, timeout=(5, 20))
            response.raise_for_status()
            results = response.json().get("results", [])
        except requests.RequestException as exc:
            hard_failures += 1
            logger.error(f"OpenAlex works batch failed ({batch_seed}): {exc}")

        for work in results:
            work_doi = _normalize_doi(work.get("doi"))
            if work_doi and work_doi not in by_doi:
                by_doi[work_doi] = work
                cache_dirty = True

        missing_after_batch = [doi for doi in batch if doi not in by_doi]
        if missing_after_batch:
            for doi in missing_after_batch:
                doi_url = f"https://api.openalex.org/works/https://doi.org/{quote(doi, safe='')}"
                try:
                    fallback_resp = session.get(doi_url, headers=_openalex_headers(), timeout=(5, 20))
                    if fallback_resp.status_code == 200:
                        work = fallback_resp.json()
                        work_doi = _normalize_doi(work.get("doi"))
                        if work_doi and work_doi not in by_doi:
                            by_doi[work_doi] = work
                            cache_dirty = True
                    elif fallback_resp.status_code not in (404,):
                        logger.warning(f"OpenAlex fallback DOI fetch status {fallback_resp.status_code} for {doi}")
                except requests.RequestException as exc:
                    logger.warning(f"OpenAlex fallback DOI fetch failed for {doi}: {exc}")

        should_flush_cache = cache_dirty and (
            idx % save_every_batches == 0 or idx == len(batches) or hard_failures >= 3
        )
        if should_flush_cache:
            _save_json(WORKS_CACHE_FILE, by_doi)
            cache_dirty = False

        remaining = len([doi for doi in normalized_dois if doi not in by_doi])
        _save_json(
            WORKS_STATE_FILE,
            {
                "last_batch_index": idx,
                "total_batches": len(batches),
                "remaining_dois": remaining,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        logger.info(f"OpenAlex works progress: {idx}/{len(batches)} batch(es), remaining DOI(s): {remaining}")

        if hard_failures >= 3:
            logger.warning("Stopping OpenAlex works fetch early after repeated failures; resume on next run from cache state")
            break
        time.sleep(request_delay)

    if cache_dirty:
        _save_json(WORKS_CACHE_FILE, by_doi)

    return {"results": [], "by_doi": {doi: by_doi[doi] for doi in normalized_dois if doi in by_doi}}


def extract_rors_from_openalex_works(by_doi):
    rors = []
    for work in by_doi.values():
        for authorship in work.get("authorships", []) or []:
            for institution in authorship.get("institutions", []) or []:
                ror = _clean_ror(institution.get("ror"))
                if ror:
                    rors.append(ror)
    return list(dict.fromkeys(rors))


def fetch_openalex_institutions_cached(rors, chunk_size=20, request_delay=1.0, force_refresh=False):
    cleaned_rors = []
    for ror in rors:
        cleaned = _clean_ror(ror)
        if cleaned:
            cleaned_rors.append(cleaned)
    cleaned_rors = list(dict.fromkeys(cleaned_rors))

    if not cleaned_rors:
        logger.info("OpenAlex institutions cache fetch: no ROR values provided")
        return {"results": []}

    by_ror = _load_json(INSTITUTIONS_CACHE_FILE, {})
    if force_refresh:
        by_ror = {}
    pending = [ror for ror in cleaned_rors if ror not in by_ror]
    logger.info(
        f"OpenAlex institutions cache: requested={len(cleaned_rors)}, "
        f"cached={len(cleaned_rors) - len(pending)}, missing={len(pending)}"
    )
    if not pending:
        return {"results": [by_ror[ror] for ror in cleaned_rors if ror in by_ror]}

    session = _make_session()
    batches = list(_chunk(pending, chunk_size))
    hard_failures = 0
    cache_dirty = False
    save_every_batches = 25

    for idx, batch in enumerate(batches, start=1):
        batch_seed = batch[0]
        logger.info(f"OpenAlex institutions batch {idx}/{len(batches)} (seed ROR: {batch_seed})")
        try:
            params = {"filter": f"ror:{'|'.join(batch)}"}
            response = session.get("https://api.openalex.org/institutions", headers=_openalex_headers(), params=params, timeout=(5, 20))
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after and str(retry_after).isdigit() else 15
                logger.warning(f"OpenAlex rate limit on institutions batch {idx}; sleeping {wait_seconds}s")
                time.sleep(wait_seconds)
                response = session.get("https://api.openalex.org/institutions", headers=_openalex_headers(), params=params, timeout=(5, 20))
            response.raise_for_status()
            results = response.json().get("results", [])
        except requests.RequestException as exc:
            hard_failures += 1
            logger.error(f"OpenAlex institutions batch failed ({batch_seed}): {exc}")
            results = []

        for item in results:
            ror = _clean_ror(item.get("ids", {}).get("ror")) or _clean_ror(item.get("ror"))
            if ror and ror not in by_ror:
                by_ror[ror] = item
                cache_dirty = True

        should_flush_cache = cache_dirty and (
            idx % save_every_batches == 0 or idx == len(batches) or hard_failures >= 3
        )
        if should_flush_cache:
            _save_json(INSTITUTIONS_CACHE_FILE, by_ror)
            cache_dirty = False

        remaining = len([ror for ror in cleaned_rors if ror not in by_ror])
        _save_json(
            INSTITUTIONS_STATE_FILE,
            {
                "last_batch_index": idx,
                "total_batches": len(batches),
                "remaining_rors": remaining,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        logger.info(f"OpenAlex institutions progress: {idx}/{len(batches)} batch(es), remaining ROR(s): {remaining}")

        if hard_failures >= 3:
            logger.warning("Stopping OpenAlex institutions fetch early after repeated failures; resume on next run from cache state")
            break
        time.sleep(request_delay)

    if cache_dirty:
        _save_json(INSTITUTIONS_CACHE_FILE, by_ror)

    return {"results": [by_ror[ror] for ror in cleaned_rors if ror in by_ror]}
