# OA full-text deposit: apply and rollback

Date: 2026-09-20
Issue: #6 (phase 2)
Builds on: `docs/superpowers/specs/2026-09-19-oa-fulltext-candidates-design.md` (the report, PR #28)
Status: design approved, not implemented

## Goal

Deposit the open-access PDFs the report identified: upload the file to Pure,
attach it to the research output, and be able to take it back off again.

The report answers "what could be attached". This answers "attach it".

## What the API actually does

Probed against staging on 2026-09-19 before designing, because the rollback
shape depends on it.

| Fact | Consequence |
|---|---|
| `PUT research-outputs/file-uploads`, `Content-Type: application/pdf`, raw bytes -> `{key, digest, size, expires}` | the upload step |
| An uploaded file **expires after 2 hours** if nothing references it | upload and record-update are one operation, not two phases |
| `electronicVersions` **replaces** on PUT (verified: attached a file, PUT the original empty array, entry gone) | rollback by restoring the prior array genuinely works |
| **No delete for uploaded files** - `DELETE` and `GET` on `file-uploads/{key}` both return 404 | we cannot clean up a dereferenced file; say so rather than implying we do |

That last row overrides the original intention to delete the uploaded file on
rollback. It is not available through this API.

Note the contrast with `address`, which **merges** on PUT (the bug fixed in
PR #25). Two collections on the same API behave differently; neither should be
assumed.

## Apply

Per approved row, in order:

1. **Re-download and re-validate** the candidate through the existing
   `download_and_validate`. Bytes are not cached between report and apply: no
   copyrighted material sits in the artifact directory, and a candidate that
   changed or disappeared since review is caught rather than deposited blind.
2. **Upload** the bytes, keep the returned `key`.
3. **GET the record and snapshot `electronicVersions`** into the change set
   before touching it. This snapshot is what rollback restores; without it,
   rollback has nothing to put back.
4. **Append a `FileElectronicVersion`**:
   - `file.uploadedFile.key` from step 2
   - `accessType` via `map_access_type` from the candidate's access status
   - `licenseType` via `map_license_type`, falling back to
     `userDefinedLicense` when the licence has no Pure URI
   - `versionType` via **`pure_version_type_for(candidate['version'])`** -
     never hardcoded. Under a widened version policy a hardcoded
     `publishersversion` would label an accepted manuscript as the final
     published version, which is the correctness bug issue #6 names.
5. **PUT the record**, and record the deposit in the change set.

A failure at any step is a recorded per-item failure with its reason, not an
aborted run. The apply-wide rule from PR #25 still applies: a run where nothing
was written fails rather than reporting success.

## Rollback

Restore the `electronicVersions` snapshot taken in step 3, following the
existing `_execute_created_record_rollback` pattern:

- an item whose `apply_status` is not `applied` is skipped
- if the record changed since apply, the item becomes a **conflict** for a
  human rather than being overwritten
- otherwise PUT the snapshot back and mark the item `rolled_back`

The uploaded file may remain in Pure's internal store, unreferenced and not
reachable through the API. Unreferenced uploads that were never attached expire
after two hours; what happens to one that was attached and then dereferenced is
not visible to us. This is a question for the Pure administrator and must be
stated in the PR, not glossed.

## Components

- `src/fulltext_deposit.py` - `upload_pdf(session, payload, filename) -> key` and
  `build_file_electronic_version(candidate, key, filename) -> dict`. Pure
  functions plus one HTTP call; no job or CSV knowledge.
- `src/apply_updates_to_pure.py` - a `process_full_text` handler beside the
  existing workflows, reading the approved rows and writing the apply manifest.
- `app/services/jobs.py` - `_execute_full_text_rollback`, and `full_text` comes
  off the apply-refusal list added in PR #28.

## Testing

Unit tests with stubbed sessions: upload returns a key; the built entry carries
the right access, licence and version URIs for each version; a licence with no
Pure URI falls back to `userDefinedLicense`; a failed upload records a reason
and does not attach; rollback restores the snapshot, skips unapplied items, and
conflicts when the record changed.

**Acceptance test against staging, at most 2 records:** deposit two PDFs,
confirm both are attached and the files are retrievable, then roll back and
confirm both records' `electronicVersions` are byte-identical to their
pre-apply state.

## Out of scope

Bulk deposit across a faculty, retrying a failed deposit, and any cleanup of
orphaned uploads (not possible through this API).
