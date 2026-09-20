# OA Full-Text Deposit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deposit the open-access PDFs the `full_text` report identified — upload to Pure, attach to the research output, and be able to take it back off.

**Architecture:** A small module does upload and payload building. The apply script gains a `process_full_text` handler beside the existing workflows. The job service gains a change-set collector, a rollback executor, and lets `full_text` through the apply guard. Rollback restores the `electronicVersions` snapshot the apply step recorded.

**Tech Stack:** Python 3.12, `requests`, `unittest` — no new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-20-oa-fulltext-deposit-design.md`

## Global Constraints

- **No new dependencies.** `requirements.txt` must not change.
- **Module logger only:** `logger.x(...)`, never `logging.x(...)` — root-logger calls in this repo have no file handler and vanish.
- **No live network in tests.** Stub with `unittest.mock`. Only Task 5 touches staging, and it is explicitly limited to **2 records**.
- **`versionType` is never hardcoded.** It comes from `pure_version_type_for(candidate_version)`. A hardcoded `publishersversion` would label an accepted manuscript as the final published version.
- **Every failure is a recorded per-item reason**, never a silently dropped record.
- **Uploaded files expire 2 hours after upload if unreferenced** — upload and record-update belong in one operation.
- **`electronicVersions` REPLACES on PUT** (verified on staging). `address` merges. Do not assume either for a collection you have not tested.
- Run `.venv/bin/python -m pytest -q` before every commit. Baseline: **269 passed, 5 skipped**.

---

### Task 1: Upload and payload building

**Files:**
- Create: `src/fulltext_deposit.py`
- Test: `tests/test_fulltext_deposit.py`

**Interfaces:**
- Consumes: `fulltext_candidates.map_access_type`, `map_license_type`, `pure_version_type_for`
- Produces: `upload_pdf(session, payload: bytes, filename: str) -> str` (returns the upload key, raises `DepositError`), `build_file_electronic_version(candidate: dict, upload_key: str, filename: str) -> dict`, `DepositError`

- [ ] **Step 1: Write the failing upload test**

```python
# tests/test_fulltext_deposit.py
import unittest
from unittest.mock import MagicMock

import fulltext_deposit as fd


def _response(status=200, body=None, text=""):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = body if body is not None else {}
    response.text = text
    return response


class UploadTests(unittest.TestCase):
    def test_returns_the_key_from_a_successful_upload(self):
        session = MagicMock()
        session.put.return_value = _response(body={"key": "abc-123", "size": 42})

        key = fd.upload_pdf(session, b"%PDF-1.7 stub", "paper.pdf")

        self.assertEqual("abc-123", key)

    def test_sends_raw_bytes_with_a_pdf_content_type(self):
        session = MagicMock()
        session.put.return_value = _response(body={"key": "abc-123"})

        fd.upload_pdf(session, b"%PDF-1.7 stub", "paper.pdf")

        _args, kwargs = session.put.call_args
        self.assertEqual(b"%PDF-1.7 stub", kwargs["data"])
        self.assertEqual("application/pdf", kwargs["headers"]["Content-Type"])

    def test_missing_key_is_an_error_not_a_silent_none(self):
        session = MagicMock()
        session.put.return_value = _response(body={"size": 42})

        with self.assertRaises(fd.DepositError) as ctx:
            fd.upload_pdf(session, b"%PDF-", "paper.pdf")
        self.assertIn("no upload key", str(ctx.exception).lower())

    def test_error_status_is_reported_with_its_status(self):
        session = MagicMock()
        session.put.return_value = _response(status=413, text="too large")

        with self.assertRaises(fd.DepositError) as ctx:
            fd.upload_pdf(session, b"%PDF-", "paper.pdf")
        self.assertIn("413", str(ctx.exception))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_deposit.py -k Upload`
Expected: FAIL with `ModuleNotFoundError: No module named 'fulltext_deposit'`

- [ ] **Step 3: Implement upload**

```python
# src/fulltext_deposit.py
"""Deposit a validated open access PDF onto a Pure research output.

Two steps that belong together: Pure's file upload returns a key that
expires after two hours if nothing references it, so uploading and
attaching are one operation, not two phases.
"""
import logging

from config import PURE_BASE_URL, PURE_HEADERS
from fulltext_candidates import map_access_type, map_license_type, pure_version_type_for
from logging_config import setup_logging

logger = setup_logging('btp', level=logging.INFO)

UPLOAD_TIMEOUT = 180


class DepositError(Exception):
    """A deposit step failed. The message becomes the row's reason."""


def upload_pdf(session, payload, filename):
    """Upload raw PDF bytes to Pure and return the upload key."""
    headers = dict(PURE_HEADERS)
    headers['Content-Type'] = 'application/pdf'
    headers['Accept'] = 'application/json'
    response = session.put(
        PURE_BASE_URL + 'research-outputs/file-uploads',
        headers=headers,
        data=payload,
        timeout=UPLOAD_TIMEOUT,
    )
    if response.status_code not in (200, 201):
        raise DepositError(
            f"Pure rejected the file upload for {filename}: HTTP {response.status_code} {response.text[:200]}"
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise DepositError(f"Pure returned a non-JSON upload response for {filename}") from exc
    key = body.get('key') or ((body.get('uploadedFile') or {}).get('key'))
    if not key:
        raise DepositError(f"Pure returned no upload key for {filename}")
    logger.debug(f"uploaded {filename} to Pure as {key}")
    return key
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_deposit.py -k Upload`
Expected: PASS (4 tests)

- [ ] **Step 5: Write the failing payload test**

```python
class FileEntryTests(unittest.TestCase):
    def _candidate(self, version="publishedVersion", licence="cc-by"):
        return {
            "url": "https://publisher.org/a.pdf",
            "version": version,
            "license": licence,
            "access_status": "open",
        }

    def test_entry_points_at_the_uploaded_file(self):
        entry = fd.build_file_electronic_version(self._candidate(), "key-1", "a.pdf")

        self.assertEqual("FileElectronicVersion", entry["typeDiscriminator"])
        self.assertEqual("key-1", entry["file"]["uploadedFile"]["key"])
        self.assertEqual("a.pdf", entry["file"]["fileName"])
        self.assertEqual("application/pdf", entry["file"]["mimeType"])

    def test_version_type_is_taken_from_the_candidate_never_hardcoded(self):
        published = fd.build_file_electronic_version(self._candidate(), "k", "a.pdf")
        accepted = fd.build_file_electronic_version(
            self._candidate(version="acceptedVersion"), "k", "a.pdf"
        )

        self.assertTrue(published["versionType"]["uri"].endswith("publishersversion"))
        self.assertTrue(accepted["versionType"]["uri"].endswith("authorsversion"))
        self.assertEqual("Accepted author manuscript", accepted["versionType"]["term"]["en_GB"])

    def test_known_licence_becomes_a_uri(self):
        entry = fd.build_file_electronic_version(self._candidate(), "k", "a.pdf")

        self.assertTrue(entry["licenseType"]["uri"].endswith("cc_by"))
        self.assertNotIn("userDefinedLicense", entry)

    def test_unmapped_licence_is_kept_as_free_text(self):
        entry = fd.build_file_electronic_version(
            self._candidate(licence="publisher-specific"), "k", "a.pdf"
        )

        self.assertNotIn("licenseType", entry)
        self.assertEqual("publisher-specific", entry["userDefinedLicense"])

    def test_no_licence_sets_neither_field(self):
        entry = fd.build_file_electronic_version(self._candidate(licence=None), "k", "a.pdf")

        self.assertNotIn("licenseType", entry)
        self.assertNotIn("userDefinedLicense", entry)

    def test_unknown_version_refuses_rather_than_guessing(self):
        with self.assertRaises(ValueError):
            fd.build_file_electronic_version(self._candidate(version="draft"), "k", "a.pdf")
```

- [ ] **Step 6: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_deposit.py -k FileEntry`
Expected: FAIL with `AttributeError: module 'fulltext_deposit' has no attribute 'build_file_electronic_version'`

- [ ] **Step 7: Implement the payload builder**

```python
def build_file_electronic_version(candidate, upload_key, filename):
    """The electronicVersions entry that attaches an uploaded file.

    versionType comes from the candidate. Hardcoding publishersversion --
    which the sibling project does -- would label an accepted manuscript as
    the final published version once the version policy is widened.
    """
    access_uri, access_term = map_access_type(candidate.get('access_status') or 'open')
    version_uri, version_term = pure_version_type_for(candidate.get('version'))

    entry = {
        "typeDiscriminator": "FileElectronicVersion",
        "title": filename.rsplit('.', maxsplit=1)[0],
        "accessType": {"uri": access_uri, "term": {"en_GB": access_term}},
        "versionType": {"uri": version_uri, "term": {"en_GB": version_term}},
        "file": {
            "fileName": filename,
            "mimeType": "application/pdf",
            "uploadedFile": {"key": upload_key},
        },
    }

    licence = candidate.get('license')
    if licence:
        licence_uri, licence_term = map_license_type(licence)
        if licence_uri:
            entry["licenseType"] = {"uri": licence_uri, "term": {"en_GB": licence_term}}
        else:
            entry["userDefinedLicense"] = licence_term
    return entry
```

- [ ] **Step 8: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_deposit.py -k FileEntry`
Expected: PASS (6 tests)

- [ ] **Step 9: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add src/fulltext_deposit.py tests/test_fulltext_deposit.py
git commit -m "Add PDF upload and file-entry building for Pure deposits

versionType comes from the candidate, never hardcoded: under a widened
version policy a fixed publishersversion would label an accepted
manuscript as the final published version."
```

---

### Task 2: The apply handler

**Files:**
- Modify: `src/apply_updates_to_pure.py` (add `process_full_text`, register it in `_resolve_output_directory` and the dispatch in `main`)
- Test: `tests/test_fulltext_deposit.py`

**Interfaces:**
- Consumes: Task 1's `upload_pdf`, `build_file_electronic_version`, `DepositError`; existing `fulltext_fetch.download_and_validate`; existing `_append_apply_manifest_entry`
- Produces: `process_full_text(filename, csv_file, json_data=None) -> None`, writing one manifest entry per deposited row

The manifest entry shape, which Task 4's rollback reads:

```python
{
    "job_type": "full_text",
    "item_key": "<normalised doi>",
    "doi": "<normalised doi>",
    "record_uuid": "<pure research output uuid>",
    "previous_electronic_versions": [...],   # snapshot taken before the PUT
    "upload_key": "<key>",
    "file_name": "<filename>",
}
```

- [ ] **Step 1: Write the failing test**

```python
class ProcessFullTextTests(unittest.TestCase):
    """Deposit is a chain of network steps; each failure must land in the row
    as a reason rather than aborting the run."""

    def _csv(self):
        import pandas as pd

        return pd.DataFrame([
            {
                "to_be_updated": "X", "updated": " ",
                "doi": "10.1/a", "pure_uuid": "rec-1",
                "candidate_url": "https://publisher.org/a.pdf",
                "version": "publishedVersion", "licence": "cc-by",
                "access_status": "open",
            }
        ])

    def test_deposits_and_records_the_previous_versions(self):
        import apply_updates_to_pure as apply_mod

        record = {"uuid": "rec-1", "electronicVersions": [{"typeDiscriminator": "DoiElectronicVersion"}]}
        entries = []
        with patch.object(apply_mod, "download_and_validate", return_value=(b"%PDF-", "a.pdf")), patch.object(
            apply_mod, "upload_pdf", return_value="key-1"
        ), patch.object(apply_mod.requests, "get", return_value=MagicMock(status_code=200, json=lambda: record)), patch.object(
            apply_mod.requests, "put", return_value=MagicMock(status_code=200)
        ), patch.object(apply_mod, "_append_apply_manifest_entry", side_effect=entries.append):
            apply_mod.process_full_text("to_be_updated.csv", self._csv())

        self.assertEqual(1, len(entries))
        self.assertEqual("rec-1", entries[0]["record_uuid"])
        self.assertEqual(
            [{"typeDiscriminator": "DoiElectronicVersion"}],
            entries[0]["previous_electronic_versions"],
            "the snapshot is what rollback restores",
        )

    def test_a_failed_upload_records_no_manifest_entry(self):
        import apply_updates_to_pure as apply_mod
        import fulltext_deposit as fd_mod

        entries = []
        with patch.object(apply_mod, "download_and_validate", return_value=(b"%PDF-", "a.pdf")), patch.object(
            apply_mod, "upload_pdf", side_effect=fd_mod.DepositError("HTTP 500")
        ), patch.object(apply_mod, "_append_apply_manifest_entry", side_effect=entries.append):
            apply_mod.process_full_text("to_be_updated.csv", self._csv())

        self.assertEqual([], entries)

    def test_unticked_rows_are_not_deposited(self):
        import apply_updates_to_pure as apply_mod

        frame = self._csv()
        frame.loc[0, "to_be_updated"] = ""
        entries = []
        with patch.object(apply_mod, "download_and_validate") as download, patch.object(
            apply_mod, "_append_apply_manifest_entry", side_effect=entries.append
        ):
            apply_mod.process_full_text("to_be_updated.csv", frame)

        download.assert_not_called()
        self.assertEqual([], entries)
```

Add `from unittest.mock import patch` to the test file's imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_deposit.py -k ProcessFullText`
Expected: FAIL with `AttributeError: module 'apply_updates_to_pure' has no attribute 'process_full_text'`

- [ ] **Step 3: Implement the handler**

Add these imports near the top of `src/apply_updates_to_pure.py`:

```python
from fulltext_deposit import DepositError, build_file_electronic_version, upload_pdf
from fulltext_fetch import download_and_validate
```

and the handler beside the other `process_*` functions:

```python
def process_full_text(filename, csv_file, json_data=None):
    """Attach an open access PDF to each approved research output.

    The bytes are fetched again here rather than cached from the report: no
    copyrighted file sits in the artifact directory, and a candidate that
    changed or vanished since review is caught instead of deposited blind.
    """
    session = requests.Session()
    deposited = 0
    for index, row in csv_file.iterrows():
        if str(row.get('to_be_updated') or '').strip().upper() != 'X':
            continue
        doi = normalize_doi(str(row.get('doi') or ''))
        record_uuid = str(row.get('pure_uuid') or '').strip()
        url = str(row.get('candidate_url') or '').strip()
        if not record_uuid or not url:
            logger.error(f"Cannot deposit {doi}: missing Pure UUID or candidate URL")
            continue

        candidate = {
            'version': str(row.get('version') or '').strip(),
            'license': str(row.get('licence') or '').strip() or None,
            'access_status': str(row.get('access_status') or 'open').strip(),
        }

        try:
            payload, file_name = download_and_validate(session, url)
        except Exception as exc:
            logger.error(f"Could not fetch the PDF for {doi}: {exc}")
            continue

        try:
            upload_key = upload_pdf(session, payload, file_name)
        except DepositError as exc:
            logger.error(f"Could not upload the PDF for {doi}: {exc}")
            continue

        response = requests.get(f"{PURE_BASE_URL}research-outputs/{record_uuid}",
                                headers=PURE_HEADERS, timeout=60)
        if response.status_code != 200:
            logger.error(f"Could not load research output {record_uuid} for {doi}: HTTP {response.status_code}")
            continue
        record = response.json()
        previous_versions = record.get('electronicVersions') or []

        try:
            entry = build_file_electronic_version(candidate, upload_key, file_name)
        except ValueError as exc:
            logger.error(f"Cannot build a file entry for {doi}: {exc}")
            continue

        record['electronicVersions'] = list(previous_versions) + [entry]
        put = requests.put(f"{PURE_BASE_URL}research-outputs/{record_uuid}",
                           headers=PURE_HEADERS, json=record, timeout=180)
        if put.status_code != 200:
            logger.error(f"Pure rejected the deposit for {doi}: HTTP {put.status_code} {put.text[:200]}")
            continue

        csv_file.loc[index, 'updated'] = 'x'
        csv_file.loc[index, 'to_be_updated'] = ''
        _append_apply_manifest_entry({
            "job_type": "full_text",
            "item_key": doi,
            "doi": doi,
            "record_uuid": record_uuid,
            "previous_electronic_versions": previous_versions,
            "upload_key": upload_key,
            "file_name": file_name,
        })
        deposited += 1
        logger.info(f"attached {file_name} ({len(payload)} bytes) to research output {record_uuid} for {doi}")

    logger.info(f"{deposited} full text(s) attached in Pure")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_deposit.py -k ProcessFullText`
Expected: PASS (3 tests)

- [ ] **Step 5: Register the handler in the dispatch**

In `_resolve_output_directory`, add beside the other referers:

```python
    if 'deposit_full_text' in referer_page:
        return os.path.join(base, 'full_text')
```

(match the surrounding style — read the existing branches and follow exactly how they build the path.)

In `main`, the dispatch currently runs only `if json_files:`. The full-text job writes no JSON, so add a branch that does not depend on one, before that block:

```python
    if 'deposit_full_text' in referer_page:
        for filename, csv_file in csv_files.items():
            process_full_text(filename, csv_file)
        logger.info("script to update Pure has ended")
        sys.exit(0)
```

- [ ] **Step 6: Verify the dispatch reaches the handler**

```python
def test_dispatch_reaches_full_text_without_a_json_file(self):
    """The full text job writes only a CSV; the JSON-gated dispatch would skip it."""
    import apply_updates_to_pure as apply_mod
    import inspect

    source = inspect.getsource(apply_mod.main)
    self.assertIn("deposit_full_text", source)
```

Run: `.venv/bin/python -m pytest -q tests/test_fulltext_deposit.py`
Expected: PASS

- [ ] **Step 7: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add src/apply_updates_to_pure.py tests/test_fulltext_deposit.py
git commit -m "Deposit approved full texts onto their Pure research outputs

Snapshots electronicVersions before the PUT: that snapshot is the only
thing rollback can restore. Bytes are re-fetched rather than cached, so
nothing copyrighted sits in the artifact directory."
```

---

### Task 3: Let the job apply

**Files:**
- Modify: `app/services/jobs.py` — `_APPLY_SUPPORTED_JOB_TYPES` (line ~38), `_referer_page_for_job_type`, `_capture_apply_change_set`, `_finalize_apply_change_set`
- Test: `tests/test_app_structure.py`

**Interfaces:**
- Consumes: Task 2's manifest entries
- Produces: a `full_text` job that can be applied; change-set items keyed on DOI with `identifier_type="full_text"`

- [ ] **Step 1: Write the failing test**

```python
class FullTextApplySupportTests(unittest.TestCase):
    def test_full_text_is_now_an_apply_supported_job_type(self):
        from app.services.jobs import _APPLY_SUPPORTED_JOB_TYPES
        from app.models.jobs import JobType

        self.assertIn(JobType.FULL_TEXT.value, _APPLY_SUPPORTED_JOB_TYPES)

    def test_full_text_has_a_referer_page(self):
        from app.services.jobs import JobService

        self.assertEqual("deposit_full_text", JobService._referer_page_for_job_type("full_text"))

    def test_change_set_items_are_keyed_on_doi(self):
        import csv as _csv

        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)
            artifact_dir = os.path.join(tmpdir, "output", "full_text", "job-ft")
            os.makedirs(artifact_dir, exist_ok=True)
            with open(os.path.join(artifact_dir, "to_be_updated.csv"), "w", newline="", encoding="utf-8") as handle:
                writer = _csv.DictWriter(handle, fieldnames=["to_be_updated", "updated", "doi", "pure_uuid", "title"])
                writer.writeheader()
                writer.writerow({"to_be_updated": "X", "updated": " ", "doi": "10.1/a",
                                 "pure_uuid": "rec-1", "title": "A paper"})
                writer.writerow({"to_be_updated": "", "updated": " ", "doi": "10.1/b",
                                 "pure_uuid": "rec-2", "title": "Not selected"})

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=Path(__file__).resolve().parents[1],
                runtime_root=Path(tmpdir),
            )
            job = {
                "job_type": "full_text",
                "artifact_dir": "output/full_text/job-ft",
                "artifact_state": {"artifacts": {"csv": ["to_be_updated.csv"], "json": []}},
            }
            changes = service._collect_full_text_apply_changes(job)

        self.assertEqual(1, len(changes), "only ticked rows become change set items")
        self.assertEqual("10.1/a", changes[0]["item_key"])
        self.assertEqual("rec-1", (changes[0]["new_value"] or {}).get("record_uuid"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_app_structure.py -k FullTextApplySupport`
Expected: FAIL — `full_text` is not in `_APPLY_SUPPORTED_JOB_TYPES`

- [ ] **Step 3: Wire the job type through**

Add to `_APPLY_SUPPORTED_JOB_TYPES`:

```python
    JobType.FULL_TEXT.value,
```

Add to the `referers` map in `_referer_page_for_job_type`:

```python
            JobType.FULL_TEXT.value: "deposit_full_text",
```

Add a collector beside `_collect_record_creation_apply_changes`:

```python
    def _collect_full_text_apply_changes(self, job: dict) -> list[dict[str, Any]]:
        """One change set item per approved deposit, keyed on DOI.

        old_value stays None here: the electronicVersions snapshot is taken by
        the apply script at deposit time and arrives through the manifest,
        because only then do we know what the record looked like.
        """
        artifact_state = job["artifact_state"] or {}
        tracked_csv = tuple((artifact_state.get("artifacts") or {}).get("csv") or ())
        if not tracked_csv:
            return []

        definition = get_job_type_definition(job["job_type"])
        artifact_dir = self._artifact_directory(job.get("artifact_dir"))
        changes: list[dict[str, Any]] = []
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    if not self._marker_is_selected(row.get("to_be_updated")):
                        continue
                    doi = self._normalize_doi(str(row.get("doi") or ""))
                    record_uuid = str(row.get("pure_uuid") or "").strip()
                    if not doi or not record_uuid:
                        continue
                    changes.append(
                        {
                            "item_key": doi,
                            "entity_uuid": doi,
                            "entity_label": str(row.get("title") or "").strip() or None,
                            "field_name": "electronicVersions",
                            "identifier_type": "full_text",
                            "old_value": None,
                            "new_value": {"doi": doi, "record_uuid": record_uuid},
                        }
                    )
        return changes
```

Register it in `_capture_apply_change_set`, beside the other branches:

```python
        elif job["job_type"] == JobType.FULL_TEXT.value:
            changes = self._collect_full_text_apply_changes(job)
```

In `_finalize_apply_change_set`, `full_text` reads its results from the apply manifest exactly as research outputs and datasets do. Change that branch's condition to include it:

```python
        elif job["job_type"] in {JobType.RESEARCH_OUTPUTS.value, JobType.DATASETS.value, JobType.FULL_TEXT.value}:
            applied_records = self._load_apply_manifest(job)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_app_structure.py -k FullTextApplySupport`
Expected: PASS (3 tests)

- [ ] **Step 5: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add app/services/jobs.py tests/test_app_structure.py
git commit -m "Allow full_text jobs to apply

The apply guard refused them while depositing was unbuilt; it now has an
implementation to guard."
```

---

### Task 4: Rollback

**Files:**
- Modify: `app/services/jobs.py` — add `_execute_full_text_rollback`, register it in the rollback dispatch
- Test: `tests/test_app_structure.py`

**Interfaces:**
- Consumes: manifest entries from Task 2 (`record_uuid`, `previous_electronic_versions`)
- Produces: `_execute_full_text_rollback(change_set, log_path) -> dict` with keys `checked`, `rolled_back`, `conflicts`, `failed`

Find how the other rollbacks are dispatched (search for `_execute_datasets_rollback`) and register this one the same way.

- [ ] **Step 1: Write the failing test**

```python
class FullTextRollbackTests(unittest.TestCase):
    """Restoring the snapshot is the whole rollback: Pure has no delete for an
    uploaded file, and electronicVersions replaces on PUT (verified on staging)."""

    def _change_set(self, rollback_status="pending", apply_status="applied"):
        return {
            "items": [
                {
                    "id": 1,
                    "entity_uuid": "10.1/a",
                    "apply_status": apply_status,
                    "rollback_status": rollback_status,
                    "new_value": {
                        "doi": "10.1/a",
                        "record_uuid": "rec-1",
                        "previous_electronic_versions": [{"typeDiscriminator": "DoiElectronicVersion"}],
                    },
                }
            ]
        }

    def _service(self, tmpdir):
        app = create_app()
        app.config["BTP_DATA_DIR"] = tmpdir
        init_db(app)
        return JobService(
            app.extensions["btp_db"]["db_path"],
            project_root=Path(__file__).resolve().parents[1],
            runtime_root=Path(tmpdir),
        )

    def test_restores_the_previous_electronic_versions(self):
        deposited = {"uuid": "rec-1", "electronicVersions": [
            {"typeDiscriminator": "DoiElectronicVersion"},
            {"typeDiscriminator": "FileElectronicVersion"},
        ]}
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            log_path = Path(tmpdir) / "rollback.log"
            with patch("app.services.jobs.requests.get", return_value=MagicMock(status_code=200, json=lambda: deposited)), patch(
                "app.services.jobs.requests.put", return_value=MagicMock(status_code=200)
            ) as put:
                stats = service._execute_full_text_rollback(self._change_set(), log_path)

        self.assertEqual(1, stats["rolled_back"])
        sent = put.call_args.kwargs["json"]
        self.assertEqual([{"typeDiscriminator": "DoiElectronicVersion"}], sent["electronicVersions"])

    def test_unapplied_items_are_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            with patch("app.services.jobs.requests.put") as put:
                stats = service._execute_full_text_rollback(
                    self._change_set(apply_status="not_applied"), Path(tmpdir) / "r.log"
                )

        put.assert_not_called()
        self.assertEqual(0, stats["rolled_back"])

    def test_a_missing_record_is_a_conflict_not_a_failure_to_notice(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            with patch("app.services.jobs.requests.get", return_value=MagicMock(status_code=404)), patch(
                "app.services.jobs.requests.put"
            ) as put:
                stats = service._execute_full_text_rollback(self._change_set(), Path(tmpdir) / "r.log")

        put.assert_not_called()
        self.assertEqual(0, stats["rolled_back"])
        self.assertGreaterEqual(stats["conflicts"] + stats["failed"], 1)

    def test_missing_snapshot_is_a_conflict_rather_than_an_empty_put(self):
        """Without a snapshot there is nothing to restore; PUTing an empty list
        would strip electronicVersions the deposit never touched."""
        change_set = self._change_set()
        change_set["items"][0]["new_value"].pop("previous_electronic_versions")
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            with patch("app.services.jobs.requests.put") as put:
                stats = service._execute_full_text_rollback(change_set, Path(tmpdir) / "r.log")

        put.assert_not_called()
        self.assertEqual(1, stats["conflicts"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest -q tests/test_app_structure.py -k FullTextRollback`
Expected: FAIL with `AttributeError: 'JobService' object has no attribute '_execute_full_text_rollback'`

- [ ] **Step 3: Implement rollback**

```python
    def _execute_full_text_rollback(self, change_set: dict[str, Any], log_path: Path) -> dict[str, int]:
        """Put back the electronicVersions the deposit replaced.

        Pure exposes no delete for an uploaded file, so the file itself may
        remain in Pure's store, unreferenced and unreachable through the API.
        Restoring the record is the whole of what we can do.
        """
        stats = {"checked": 0, "rolled_back": 0, "conflicts": 0, "failed": 0}

        with connect_db(self.db_path) as connection:
            for item in change_set["items"]:
                if item["apply_status"] != "applied":
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("skipped", "apply_status_not_applied", item["id"]),
                    )
                    continue
                if item["rollback_status"] in {"rolled_back", "conflict", "skipped"}:
                    continue

                stats["checked"] += 1
                payload = item.get("new_value") or {}
                record_uuid = str(payload.get("record_uuid") or "").strip()
                previous = payload.get("previous_electronic_versions")

                if not record_uuid or previous is None:
                    stats["conflicts"] += 1
                    reason = f"No deposit snapshot was recorded for {item['entity_uuid']}"
                    self._append_log(log_path, f"CONFLICT {reason}\n")
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("conflict", reason, item["id"]),
                    )
                    continue

                try:
                    response = requests.get(
                        f"{_pure_base_url()}research-outputs/{record_uuid}",
                        headers=_pure_headers(),
                        timeout=30,
                    )
                    response.raise_for_status()
                    record = response.json()
                except Exception as exc:
                    stats["failed"] += 1
                    reason = f"Could not load research output {record_uuid}: {exc}"
                    self._append_log(log_path, f"ERROR {reason}\n")
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("failed", reason, item["id"]),
                    )
                    continue

                record["electronicVersions"] = previous
                try:
                    put = requests.put(
                        f"{_pure_base_url()}research-outputs/{record_uuid}",
                        headers=_pure_headers(),
                        json=record,
                        timeout=180,
                    )
                    put.raise_for_status()
                except Exception as exc:
                    stats["failed"] += 1
                    reason = f"Could not restore research output {record_uuid}: {exc}"
                    self._append_log(log_path, f"ERROR {reason}\n")
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("failed", reason, item["id"]),
                    )
                    continue

                stats["rolled_back"] += 1
                self._append_log(log_path, f"ROLLED BACK full text on {record_uuid} ({payload.get('doi')})\n")
                connection.execute(
                    "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = NULL WHERE id = ?",
                    ("rolled_back", item["id"]),
                )
            connection.commit()

        return stats
```

Then fix the rollback dispatch in `rollback_job` (around `app/services/jobs.py:750`). **Read this carefully — the current shape is a hazard.** It ends in a bare `else` that routes anything unrecognised to the datasets executor:

```python
        elif job["job_type"] == JobType.RESEARCH_OUTPUTS.value:
            stats = self._execute_research_outputs_rollback(change_set, log_path)
        else:
            stats = self._execute_datasets_rollback(change_set, log_path)
```

`_execute_datasets_rollback` DELETES records by UUID. Once Task 3 lets `full_text` apply, a `full_text` rollback would fall into that `else` and try to delete the research outputs we only attached a file to. Replace the fallback with explicit branches and an error:

```python
        elif job["job_type"] == JobType.RESEARCH_OUTPUTS.value:
            stats = self._execute_research_outputs_rollback(change_set, log_path)
        elif job["job_type"] == JobType.DATASETS.value:
            stats = self._execute_datasets_rollback(change_set, log_path)
        elif job["job_type"] == JobType.FULL_TEXT.value:
            stats = self._execute_full_text_rollback(change_set, log_path)
        else:
            # Never fall through to a destructive executor: the datasets
            # rollback deletes records, and an unrecognised job type reaching
            # it would delete records it never created.
            raise ValueError(f"Job type '{job['job_type']}' has no rollback implementation")
```

Add a test for the fallback too:

```python
    def test_an_unknown_job_type_does_not_fall_through_to_dataset_deletion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._service(tmpdir)
            with patch.object(service, "_execute_datasets_rollback") as datasets:
                with self.assertRaises(ValueError):
                    service._rollback_executor_for("something_else")
            datasets.assert_not_called()
```

If `rollback_job` has no separately testable executor-selection helper, extract one (`_rollback_executor_for(job_type)`) returning the bound method, and have `rollback_job` call it — that keeps the dangerous branch under test rather than reachable only through a full rollback.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest -q tests/test_app_structure.py -k FullTextRollback`
Expected: PASS (4 tests)

- [ ] **Step 5: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`

```bash
git add app/services/jobs.py tests/test_app_structure.py
git commit -m "Roll back a full text deposit by restoring electronicVersions

Verified on staging that electronicVersions replaces on PUT, so restoring
the pre-deposit snapshot removes our entry. Pure exposes no delete for an
uploaded file, so the file itself may remain in its store unreferenced."
```

---

### Task 5: Acceptance test against staging — at most 2 records

**Files:**
- No source changes expected. Any change needed here is a bug fix with its own test.

**Interfaces:**
- Consumes: everything above

This is the only task that touches a live system. **Deposit at most 2 records.** Roll both back before finishing.

- [ ] **Step 1: Produce a report with at least 2 validated candidates**

The smallest faculties yield none — their publications already hold files. Use a faculty known to produce validated rows, and stop as soon as two are available:

```bash
cd /home/david/PycharmProjects/BackToPure
PYTHONPATH=src .venv/bin/python src/harvest_fulltext_candidates.py \
  "uu faculty: faculteit betawetenschappen|organization_name" "yes" "published" \
  2>&1 | tail -5
```

Then check what validated:

```bash
python3 -c "
import csv
rows=[r for r in csv.DictReader(open('output/full_text/to_be_updated.csv')) if r['to_be_updated']=='X']
print('validated candidates:', len(rows))
for r in rows[:3]: print(' ', r['doi'], r['pure_uuid'], r['size_bytes'], r['candidate_url'][:60])
"
```

If fewer than 2 validate, try another faculty. Do not proceed with zero.

- [ ] **Step 2: Cut the review file down to exactly 2 rows**

```bash
python3 -c "
import csv
path='output/full_text/to_be_updated.csv'
rows=list(csv.DictReader(open(path)))
keep=[r for r in rows if r['to_be_updated']=='X'][:2]
assert len(keep)==2, f'need 2 validated rows, found {len(keep)}'
for r in rows:
    if r not in keep: r['to_be_updated']=''
with open(path,'w',newline='',encoding='utf-8') as h:
    w=csv.DictWriter(h, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
print('ticked rows now:', sum(1 for r in rows if r['to_be_updated']=='X'))
"
```

- [ ] **Step 3: Record the before state of both records**

```bash
PYTHONPATH=src .venv/bin/python -c "
import csv, json, requests
from config import PURE_BASE_URL, PURE_HEADERS
rows=[r for r in csv.DictReader(open('output/full_text/to_be_updated.csv')) if r['to_be_updated']=='X']
before={}
for r in rows:
    d=requests.get(PURE_BASE_URL+'research-outputs/'+r['pure_uuid'], headers=PURE_HEADERS, timeout=60).json()
    before[r['pure_uuid']]=d.get('electronicVersions') or []
    print(r['pure_uuid'], '->', [e.get('typeDiscriminator') for e in before[r['pure_uuid']]])
json.dump(before, open('/tmp/ft_before.json','w'))
"
```

- [ ] **Step 4: Apply through the job API**

Restart the app so it loads the new code, create a job pointing at this artifact directory, and apply it. If driving the API is awkward for an existing on-disk report, run the apply script directly with the same environment the job runner uses:

```bash
cd /home/david/PycharmProjects/BackToPure
REFERER_PAGE=deposit_full_text BTP_OUTPUT_DIR=output/full_text \
BTP_TARGET_CSV_FILES=to_be_updated.csv \
BTP_APPLY_MANIFEST_FILE=/tmp/ft_manifest.jsonl \
PYTHONPATH=src .venv/bin/python src/apply_updates_to_pure.py 2>&1 | tail -10
```

Expected: two `attached ... to research output ...` lines and `2 full text(s) attached in Pure`.

- [ ] **Step 5: Verify both deposits landed, and that the files are real**

```bash
PYTHONPATH=src .venv/bin/python -c "
import json, requests
from config import PURE_BASE_URL, PURE_HEADERS
for line in open('/tmp/ft_manifest.jsonl'):
    e=json.loads(line)
    d=requests.get(PURE_BASE_URL+'research-outputs/'+e['record_uuid'], headers=PURE_HEADERS, timeout=60).json()
    files=[v for v in (d.get('electronicVersions') or []) if v.get('typeDiscriminator')=='FileElectronicVersion']
    print(e['record_uuid'], '| file entries:', len(files), '| names:', [(f.get('file') or {}).get('fileName') for f in files])
    print('   versionType:', [(f.get('versionType') or {}).get('uri','').rsplit('/',1)[-1] for f in files])
"
```

Expected: one `FileElectronicVersion` per record, the filename from the download, and `versionType` matching the candidate's version (`publishersversion` under the default policy).

- [ ] **Step 6: Roll both back**

Through the job API if the apply ran as a job; otherwise exercise the same code path:

```bash
PYTHONPATH=src .venv/bin/python -c "
import json, requests
from config import PURE_BASE_URL, PURE_HEADERS
for line in open('/tmp/ft_manifest.jsonl'):
    e=json.loads(line)
    rec=requests.get(PURE_BASE_URL+'research-outputs/'+e['record_uuid'], headers=PURE_HEADERS, timeout=60).json()
    rec['electronicVersions']=e['previous_electronic_versions']
    r=requests.put(PURE_BASE_URL+'research-outputs/'+e['record_uuid'], headers=PURE_HEADERS, json=rec, timeout=180)
    print('restore', e['record_uuid'], r.status_code)
"
```

Prefer the real rollback path (`POST /api/jobs/<id>/rollback`) if the apply was run as a job — that exercises the code Task 4 added rather than a hand-rolled equivalent.

- [ ] **Step 7: Verify both records match their before state exactly**

```bash
PYTHONPATH=src .venv/bin/python -c "
import json, requests
from config import PURE_BASE_URL, PURE_HEADERS
before=json.load(open('/tmp/ft_before.json'))
ok=True
for uuid, prev in before.items():
    d=requests.get(PURE_BASE_URL+'research-outputs/'+uuid, headers=PURE_HEADERS, timeout=60).json()
    now=d.get('electronicVersions') or []
    same = [e.get('typeDiscriminator') for e in now] == [e.get('typeDiscriminator') for e in prev]
    ok &= same
    print(('RESTORED' if same else 'DIFFERS '), uuid, [e.get('typeDiscriminator') for e in now])
print('all restored:', ok)
assert ok
"
```

Expected: both `RESTORED`, `all restored: True`.

- [ ] **Step 8: Report**

Report to the user: how many deposited, the `versionType` that was written, whether rollback restored both records exactly, and anything the run showed that the unit tests could not — the pattern that has caught real defects twice in this feature already.

---

## Notes for the implementer

- `normalize_doi`, `_append_apply_manifest_entry`, `PURE_BASE_URL` and `PURE_HEADERS` are already imported in `src/apply_updates_to_pure.py`. Check before adding imports.
- In `app/services/jobs.py`, use `_pure_base_url()` and `_pure_headers()` — not the module-level constants. The constants are captured at import time, and this code runs inside the long-lived web process; that mismatch made rollback fail with 401 while apply succeeded.
- Do not add a retry loop around the upload. An uploaded file expires in 2 hours, and a stacked retry in the sibling project caused up to 12 PUTs for one action.
- A deposit failure is a logged reason and a skipped row, never an abort. The apply-wide rule still holds: a run where nothing was written fails rather than reporting success.
