
import logging
import time
import sys
import pure_researchoutputs
import pure_datasets as puda
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS
from logging_config import setup_logging
from datetime import datetime
import json
import pandas as pd
import os
import requests
from pure_countries import load_external_org_countries
import tempfile
from functools import wraps
from pathlib import Path
from apply_state import append_jsonl, current_apply, journal_event, request_write
from safe_creation import create_once, normalize_doi as normalize_write_doi, tenant_key
from reviewed_changes import merge_reviewed_changes
from workflow_http import FatalWorkflowError, request_read, json_object

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from enrich_pure_external_persons import normalize_doi
from fulltext_deposit import DepositError, build_file_electronic_version, upload_pdf
from fulltext_fetch import download_and_validate
#Setup logger

logger = setup_logging('btp', level=logging.INFO)
logger.handlers[0].stream.flush = lambda: sys.stdout.flush()
datetimetoday = datetime.now().strftime('%Y%m%d')
# Set up a single session for all requests
session = requests.Session()
retry_strategy = Retry(
    total=0,
    raise_on_status=False,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)
# Disable only the single InsecureRequestWarning from urllib3 needed to use the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
headers = {
    'Accept': 'application/json',
    'api-key': PURE_API_KEY,
}


def _resolve_output_directory(referer_page):
    explicit_directory = os.environ.get("BTP_OUTPUT_DIR")
    if explicit_directory:
        return explicit_directory

    if 'enrich_external_persons' in referer_page:
        return "output/external_persons"
    if 'enrich_internal_persons_with_ids' in referer_page:
        return "output/internal_persons"
    if 'enrich_external_orgs' in referer_page:
        return "output/external_orgs"
    if 'import_datasets' in referer_page:
        return "output/datasets"
    if 'import_research_output' in referer_page:
        return "output/research_output"
    if 'import_research_outputs' in referer_page:
        return "output/research_output"
    if 'deposit_full_text' in referer_page:
        return "output/full_text"
    return None


def _selected_for_update(csv_file):
    """Return rows marked for update, accepting x/X with optional whitespace."""
    markers = csv_file['to_be_updated'].fillna('').astype(str).str.strip().str.lower()
    updated = csv_file.get('updated', pd.Series('', index=csv_file.index)).fillna('').astype(str).str.strip().str.lower()
    return csv_file[(markers == 'x') & (updated != 'x')]


def _parse_target_filenames(raw_value):
    if not raw_value:
        return None
    return {item.strip() for item in raw_value.split(",") if item.strip()}


def _append_apply_manifest_entry(entry):
    manifest_path = os.environ.get("BTP_APPLY_MANIFEST_FILE")
    if not manifest_path:
        return
    append_jsonl(manifest_path, entry)


def get_csv_files(directory, allowed_filenames=None):
    """
    Reads all CSV files from the given directory that contain 'update' in the filename and returns a list of DataFrames.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        list: A list of pandas DataFrames for each CSV file found.
    """



    csv_files = {}
    if not os.path.isdir(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return csv_files
    for filename in sorted(os.listdir(directory)):
        if allowed_filenames is not None and filename not in allowed_filenames:
            continue
        if filename.endswith(".csv") and 'update' in filename:
            file_path = os.path.join(directory, filename)
            try:
                df = pd.read_csv(file_path)
                if df.empty:
                    logger.warning(f"File {filename} is empty. Skipping...")
                    continue
                csv_files[filename] = df
                logger.info(f"Successfully loaded file {filename}.")
            except pd.errors.EmptyDataError:
                logger.warning(f"File {filename} is empty or malformed. Skipping...")
            except (OSError, ValueError, pd.errors.ParserError) as e:
                raise FatalWorkflowError(f'Cannot read apply CSV file {filename}: {e}') from e
    if allowed_filenames:
        missing_files = sorted(set(allowed_filenames) - set(csv_files))
        if missing_files:
            raise FatalWorkflowError(f'Cannot load selected apply CSV files: {missing_files}')
        for filename in missing_files:
            file_path = os.path.join(directory, filename)
            if os.path.exists(file_path):
                logger.warning(f"Target CSV file {filename} was present but could not be loaded.")
            else:
                logger.warning(f"Target CSV file {filename} was not found in {directory}.")
    return csv_files


def get_json_files(directory, allowed_filenames=None):
    """
    Reads all JSON files from the given directory and returns a list of JSON objects.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        list: A list of JSON objects for each JSON file found.
    """
    json_files = []
    if not os.path.isdir(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return json_files
    for filename in sorted(os.listdir(directory)):
        if allowed_filenames is not None and filename not in allowed_filenames:
            continue
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, "r") as json_file:
                    json_data = json.load(json_file)
                    json_files.append(json_data)
            except (OSError, ValueError) as e:
                raise FatalWorkflowError(f'Cannot read apply JSON file {filename}: {e}') from e
    if allowed_filenames:
        loaded_names = {
            filename for filename in sorted(os.listdir(directory))
            if filename.endswith(".json") and (allowed_filenames is None or filename in allowed_filenames)
        }
        missing_files = sorted(set(allowed_filenames) - loaded_names)
        if missing_files:
            raise FatalWorkflowError(f'Cannot load selected apply JSON files: {missing_files}')
        for filename in missing_files:
            logger.warning(f"Target JSON file {filename} was not found in {directory}.")
    return json_files


def read_directory_files(directory, allowed_csv_filenames=None, allowed_json_filenames=None):
    """
    Reads all CSV and JSON files from the given directory, storing CSV files as DataFrames
    and JSON files as JSON objects.

    Args:
        directory (str): The directory path containing the files.

    Returns:
        tuple: A tuple containing two lists - one for CSV DataFrames and one for JSON objects.
    """
    csv_files = get_csv_files(directory, allowed_filenames=allowed_csv_filenames)
    json_files = get_json_files(directory, allowed_filenames=allowed_json_filenames)
    return csv_files, json_files


def _checkpoint(filename, frame, fallback):
    directory = _resolve_output_directory(os.environ.get('REFERER_PAGE', 'unknown')) or fallback
    os.makedirs(directory, exist_ok=True)
    destination = os.path.join(directory, filename)
    fd, temporary = tempfile.mkstemp(prefix='.apply-', suffix='.csv', dir=directory)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as handle:
            frame.to_csv(handle, index=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _prepare(filename, frame, required, fallback):
    missing = required - set(frame.columns)
    if missing:
        raise FatalWorkflowError(f'CSV {filename} is missing required columns: {sorted(missing)}')
    for column in ('updated', 'to_be_updated', 'apply_error', 'apply_status'):
        if column not in frame:
            frame[column] = ''
        frame[column] = frame[column].fillna('').astype(str)
    _checkpoint(filename, frame, fallback)
    directory = _resolve_output_directory(os.environ.get('REFERER_PAGE', 'unknown')) or fallback
    manifest = os.environ.get('BTP_APPLY_MANIFEST_FILE')
    path = (Path(manifest).parent if manifest else Path(directory)) / 'apply_journal.jsonl'
    return {'path': path, 'history': _history(path), 'pending': 0}


def _history(path):
    state = {}
    if path.exists():
        for line in path.read_text(encoding='utf-8').splitlines():
            try:
                event = json.loads(line)
                if event.get('target_tenant') and event['target_tenant'] != tenant_key(PURE_BASE_URL):
                    raise FatalWorkflowError('Apply journal belongs to a different Pure tenant; restore the original target before continuing.')
                identity = (event['filename'], event['job_type'], event['item_key'])
                if event['status'] == 'child_created':
                    state[identity] = {**event, 'status': 'partial'}
                elif event['status'] == 'failed' and state.get(identity, {}).get('status') == 'partial':
                    pass
                else:
                    state[identity] = event
            except (ValueError, KeyError, TypeError) as exc:
                raise FatalWorkflowError('Apply journal is damaged; reconcile previous writes before retrying.') from exc
    return state


def _complete_rows(frame, indices, job_type, evidence):
    handled_only = evidence.get('disposition') in ('existing', 'noop')
    frame.loc[indices, 'updated'] = '' if handled_only else ('X' if job_type in ('internal_persons', 'external_persons', 'external_orgs') else 'x')
    frame.loc[indices, 'to_be_updated'] = ''
    frame.loc[indices, 'apply_error'] = ''
    frame.loc[indices, 'apply_status'] = evidence['disposition'] if handled_only else 'success'
    if job_type == 'internal_persons' and 'owned_changes' in evidence:
        owned = {change['item_key'] for change in evidence['owned_changes']}
        for index in indices:
            row = frame.loc[index]
            key = '|'.join(str(row[column]).strip() for column in ('PURE_UUID_PERS', 'new_id', 'new_value'))
            if key not in owned:
                frame.loc[index, 'updated'] = ''
                frame.loc[index, 'apply_status'] = 'noop'


def _run_item(filename, frame, indices, key, job_type, fallback, journal, operation):
    identity = {'filename': filename, 'job_type': job_type, 'item_key': str(key), 'target_tenant': tenant_key(PURE_BASE_URL)}
    history_key = (filename, job_type, str(key))
    previous_event = journal['history'].get(history_key, {})
    previous = previous_event.get('status')
    if previous == 'success':
        _complete_rows(frame, indices, job_type, previous_event)
        _checkpoint(filename, frame, fallback)
        return
    if previous in ('started', 'writing', 'unknown', 'partial'):
        raise FatalWorkflowError(f'Previous write for {key} has an unknown outcome; reconcile it in Pure before retrying.')
    children = []
    token = current_apply.set((journal['path'], identity, children))
    try:
        journal_event('started')
        journal['history'][history_key] = {'status': 'started'}
        try:
            result = operation()
        except (ValueError, KeyError, TypeError) as exc:
            result = {'success': False, 'error': f'Invalid record: {exc}'}
        except Exception as exc:
            frame.loc[indices, 'apply_error'] = str(exc)
            frame.loc[indices, 'apply_status'] = 'unknown'
            journal_event('unknown', error=str(exc))
            _checkpoint(filename, frame, fallback)
            raise
        if result.get('success'):
            entry = {**identity, **{k: v for k, v in result.items() if k != 'success'}}
            entry['record_uuid'] = entry.pop('uuid', entry.get('record_uuid'))
            journal_event('success', **{k: v for k, v in entry.items() if k not in identity})
            journal['history'][history_key] = {**entry, 'status': 'success'}
            _append_apply_manifest_entry(entry)
            _complete_rows(frame, indices, job_type, entry)
        else:
            error = str(result.get('error') or 'Pure rejected the record; check the job log.')
            status = 'partial' if (children or result.get('created_external_persons')) else ('unknown' if result.get('unknown') else 'failed')
            journal_event(status, error=error)
            journal['history'][history_key] = {'status': status}
            frame.loc[indices, 'apply_error'] = error
            frame.loc[indices, 'apply_status'] = status
            logger.error(f'Could not apply {job_type} {key}: {error}')
        journal['pending'] += 1
        if journal['pending'] >= 100:
            _checkpoint(filename, frame, fallback)
            journal['pending'] = 0
    finally:
        current_apply.reset(token)


def _records(data):
    if not isinstance(data, list):
        raise FatalWorkflowError('Apply JSON must contain a list of records.')
    return [record for record in data if isinstance(record, dict)]


def find_json_by_uuid(pure_uuid, json_data):
    return next((record for record in _records(json_data)
                 if (record.get('UUID') or record.get('uuid')) == pure_uuid), None)


def _update_people(filename, frame, data, kind, key_column, endpoint, fallback):
    required = {'to_be_updated', 'updated', key_column}
    if kind == 'internal_persons':
        required |= {'new_id', 'new_value'}
    journal = _prepare(filename, frame, required, fallback)
    lookup = {(item.get('UUID') or item.get('uuid')): item for item in _records(data)}
    countries = None
    for key, group in _selected_for_update(frame).groupby(key_column, dropna=False):
        def operation():
            nonlocal countries
            if pd.isna(key) or not str(key).strip():
                return {'success': False, 'error': 'Missing Pure UUID'}
            source = lookup.get(key)
            if not source:
                return {'success': False, 'error': 'No matching JSON record for Pure UUID'}
            current = json_object(request_read(session.get, PURE_BASE_URL + endpoint + '/' + str(key), headers=PURE_HEADERS))
            reviewed_rows = group.astype(object).where(pd.notna(group), None).to_dict('records')
            # Keep original row keys for tracking while normalizing the written ORCID in the helper.
            try:
                entry, owned_changes = merge_reviewed_changes(kind, source, reviewed_rows, current, countries=countries)
            except ValueError as exc:
                if kind != 'external_orgs' or countries is not None or str(exc) != 'Country vocabulary required for reviewed country change':
                    raise
                countries = load_external_org_countries(session, PURE_BASE_URL, PURE_HEADERS)
                entry, owned_changes = merge_reviewed_changes(kind, source, reviewed_rows, current, countries=countries)
            if not owned_changes:
                return {'success': True, 'uuid': str(key), 'disposition': 'noop', 'owned_changes': []}
            # Intent is durable before the remote write; acknowledgement carries the same field ownership.
            journal_event('writing', owned_changes=owned_changes)
            response = request_write(session.put, PURE_BASE_URL + endpoint + '/' + str(key),
                                     headers=PURE_HEADERS, json=entry)
            return {'success': response.status_code in (200, 201, 204), 'uuid': str(key),
                    'disposition': 'updated', 'owned_changes': owned_changes,
                    'error': f'HTTP {response.status_code}: {response.text[:300]}'}
        _run_item(filename, frame, group.index, key, kind, fallback, journal, operation)


def _save_on_exit(fallback):
    def decorate(process):
        @wraps(process)
        def run(filename, frame, *args, **kwargs):
            try:
                return process(filename, frame, *args, **kwargs)
            finally:
                _checkpoint(filename, frame, fallback)
        return run
    return decorate


@_save_on_exit('output/internal_persons')
def process_internal_persons(filename, csv_file, json_data):
    _update_people(filename, csv_file, json_data, 'internal_persons', 'PURE_UUID_PERS', 'persons', 'output/internal_persons')


@_save_on_exit('output/external_persons')
def process_external_persons(filename, csv_file, big_json_data):
    _update_people(filename, csv_file, big_json_data, 'external_persons', 'Pure_UUID', 'external-persons', 'output/external_persons')


@_save_on_exit('output/external_orgs')
def process_external_orgs(filename, csv_file, big_json_data):
    _update_people(filename, csv_file, big_json_data, 'external_orgs', 'uuid', 'external-organizations', 'output/external_orgs')


def _doi(value):
    if not isinstance(value, str):
        return ''
    return normalize_write_doi(value)


def _create_records(filename, frame, data, kind, fallback, create):
    journal = _prepare(filename, frame, {'to_be_updated', 'updated', 'doi'}, fallback)
    lookup = {}
    for item in _records(data):
        if kind == 'datasets':
            value = item.get('doi')
            values = [value.get('doi')] if isinstance(value, dict) else []
        else:
            versions = item.get('electronicVersions') or []
            values = [v.get('doi') for v in versions if isinstance(v, dict)] if isinstance(versions, list) else []
        for value in values:
            if _doi(value):
                lookup.setdefault(_doi(value), item)
    for index, row in _selected_for_update(frame).iterrows():
        key = _doi(row['doi'])
        def operation():
            item = lookup.get(key)
            if not item:
                return {'success': False, 'error': 'No matching JSON record for DOI'}
            result = create_once(session, PURE_BASE_URL, PURE_HEADERS, kind, key, lambda: create(item))
            if result.get('success'):
                logger.info(f"{result.get('disposition', 'handled')} {kind} {result.get('uuid')} for DOI {key}")
            return {**result, 'doi': key}
        _run_item(filename, frame, [index], key, kind, fallback, journal, operation)


@_save_on_exit('output/research_output')
def process_research_output(filename, csv_file, big_json_data):
    _create_records(filename, csv_file, big_json_data, 'research_outputs', 'output/research_output', pure_researchoutputs.create_research_output)


@_save_on_exit('output/datasets')
def process_datasets(filename, csv_file, big_json_data):
    _create_records(filename, csv_file, big_json_data, 'datasets', 'output/datasets', puda.create_dataset)


@_save_on_exit('output/full_text')
def process_full_text(filename, csv_file, json_data=None):
    """Deposit each approved file with durable progress and rollback evidence."""
    fallback = 'output/full_text'
    journal = _prepare(filename, csv_file, {'to_be_updated', 'updated', 'doi', 'pure_uuid', 'candidate_url'}, fallback)
    with requests.Session() as deposit_session:
        for index, row in _selected_for_update(csv_file).iterrows():
            doi = normalize_doi(_doi(row.get('doi')))
            record_uuid = row.get('pure_uuid')
            url = row.get('candidate_url')
            def operation():
                if not doi:
                    return {'success': False, 'error': 'DOI does not normalise'}
                if not isinstance(record_uuid, str) or not record_uuid.strip() or not isinstance(url, str) or not url.strip():
                    return {'success': False, 'error': 'Missing Pure UUID or candidate URL'}
                candidate = {
                    'version': row.get('version') if isinstance(row.get('version'), str) else '',
                    'license': row.get('licence') if isinstance(row.get('licence'), str) else None,
                    'access_status': row.get('access_status') if isinstance(row.get('access_status'), str) and row.get('access_status').strip() else 'open',
                }
                try:
                    payload, file_name = download_and_validate(deposit_session, url)
                except (requests.RequestException, ValueError, RuntimeError) as exc:
                    return {'success': False, 'error': f'Could not fetch PDF: {exc}'}
                try:
                    upload_key = upload_pdf(deposit_session, payload, file_name)
                except (requests.RequestException, DepositError) as exc:
                    return {'success': False, 'error': f'Could not upload PDF: {exc}'}
                response = request_read(requests.get, f'{PURE_BASE_URL}research-outputs/{record_uuid}',
                                        headers=PURE_HEADERS, timeout=60)
                record = json_object(response)
                previous_versions = record.get('electronicVersions') or []
                if not isinstance(previous_versions, list):
                    raise ValueError('Pure electronicVersions must be a list')
                entry = build_file_electronic_version(candidate, upload_key, file_name)
                record['electronicVersions'] = list(previous_versions) + [entry]
                put = request_write(requests.put, f'{PURE_BASE_URL}research-outputs/{record_uuid}',
                                    headers=PURE_HEADERS, json=record, timeout=180)
                if put.status_code not in (200, 204):
                    return {'success': False, 'error': f'Pure rejected deposit: HTTP {put.status_code} {put.text[:200]}'}
                logger.info(f'attached {file_name} to research output {record_uuid} for {doi}')
                return {'success': True, 'doi': doi, 'record_uuid': record_uuid,
                        'previous_electronic_versions': previous_versions,
                        'upload_key': upload_key, 'file_name': file_name}
            _run_item(filename, csv_file, [index], doi or str(row.get('doi')), 'full_text', fallback, journal, operation)


def main():
    # Step 1: Retrieve the REFERER_PAGE environment variable
    referer_page = os.environ.get('REFERER_PAGE', 'unknown')

    logger.debug(f"Script called from page: {referer_page}")

    # Step 2: Execute specific logic based on the Referer
    directory = _resolve_output_directory(referer_page)

    if not directory:
        logger.error(f"Unknown referer page '{referer_page}'. Cannot determine update directory.")
        sys.exit(1)

    allowed_csv_filenames = _parse_target_filenames(os.environ.get('BTP_TARGET_CSV_FILES'))
    allowed_json_filenames = _parse_target_filenames(os.environ.get('BTP_TARGET_JSON_FILES'))

    csv_files, json_files = read_directory_files(
        directory,
        allowed_csv_filenames=allowed_csv_filenames,
        allowed_json_filenames=allowed_json_filenames,
    )
    if not csv_files:
        logger.info(f"No update CSV files found in {directory}")
        logger.info(f"script to update Pure has ended")
        sys.exit(0)

    if 'deposit_full_text' in referer_page:
        for filename, csv_file in csv_files.items():
            process_full_text(filename, csv_file)
        logger.info("script to update Pure has ended")
        sys.exit(0)

    if len(json_files) != 1:
        raise FatalWorkflowError(f'Expected exactly one apply JSON file in {directory}; found {len(json_files)}.')

    if json_files:
        big_json_data = json_files[0]
        for filename, csv_file in csv_files.items():
            if 'enrich_external_persons' in referer_page:
                process_external_persons(filename, csv_file, big_json_data)
            elif 'enrich_internal_persons_with_ids' in referer_page:
                process_internal_persons(filename, csv_file, big_json_data)
            elif 'import_research_outputs' in referer_page:
                process_research_output(filename, csv_file, big_json_data)
            elif 'import_datasets' in referer_page:
                process_datasets(filename, csv_file, big_json_data)
            elif 'enrich_external_orgs' in referer_page:
                process_external_orgs(filename, csv_file, big_json_data)
    logger.info(f"script to update Pure has ended")


if __name__ == "__main__":
    main()
